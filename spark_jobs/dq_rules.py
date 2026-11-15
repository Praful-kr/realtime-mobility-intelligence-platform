"""Decoding + data-quality rule catalog for Silver (single source of truth for rules).

ERROR rules  -> record is quarantined (never reaches Silver).
WARN rules   -> record stays in Silver, dq_score is reduced by WARN_PENALTY per warning.

All rules are null-safe: a rule only fires on *decodable* rows and a NULL comparison counts as
"not violated" (missing values are judged by explicit null rules, not by accident).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

MIN_PICKUP_TS = os.getenv("DQ_MIN_PICKUP_TS", "2023-01-01 00:00:00")
MAX_FUTURE_DAYS = int(os.getenv("DQ_MAX_FUTURE_DAYS", "1"))
MAX_DISTANCE_MI = float(os.getenv("DQ_MAX_DISTANCE_MI", "200"))
MAX_FARE = float(os.getenv("DQ_MAX_FARE", "1000"))
MAX_TRIP_SECONDS = int(os.getenv("DQ_MAX_TRIP_SECONDS", str(24 * 3600)))
MAX_ZONE_ID = 265
WARN_PENALTY = 0.1


@dataclass(frozen=True)
class RuleSpec:
    name: str
    severity: str  # "error" | "warn"
    category: str  # schema | null | timestamp | value | business
    description: str
    violated: Callable[[], Column]  # lazily built: Column needs an active SparkContext


def _c(name: str) -> Column:
    return F.col(name)


def _zone_bad(col: str) -> Column:
    return _c(col).isNull() | (_c(col) < 1) | (_c(col) > MAX_ZONE_ID)


RULE_SPECS: list[RuleSpec] = [
    RuleSpec("undecodable_payload", "error", "schema",
             "Payload is null, lacks the 0x00 magic byte, or is not valid Avro for the registered schema",
             lambda: _c("undecodable")),
    RuleSpec("invalid_taxi_type", "error", "schema", "taxi_type is not yellow|green",
             lambda: ~_c("taxi_type").isin("yellow", "green")),
    RuleSpec("pickup_out_of_range", "error", "timestamp",
             "pickup before DQ_MIN_PICKUP_TS or more than DQ_MAX_FUTURE_DAYS after ingestion",
             lambda: (_c("pickup_ts") < F.lit(MIN_PICKUP_TS).cast("timestamp"))
             | (_c("pickup_ts") > _c("ingestion_time") + F.expr(f"INTERVAL {MAX_FUTURE_DAYS} DAYS"))),
    RuleSpec("dropoff_before_pickup", "error", "timestamp", "dropoff earlier than pickup",
             lambda: _c("dropoff_ts") < _c("pickup_ts")),
    RuleSpec("trip_too_long", "error", "business", "trip duration exceeds DQ_MAX_TRIP_SECONDS",
             lambda: (_c("dropoff_ts").cast("long") - _c("pickup_ts").cast("long")) > MAX_TRIP_SECONDS),
    RuleSpec("null_fare", "error", "null", "fare_amount is null", lambda: _c("fare_amount").isNull()),
    RuleSpec("negative_fare", "error", "value", "fare_amount < 0", lambda: _c("fare_amount") < 0),
    RuleSpec("negative_total", "error", "value", "total_amount < 0", lambda: _c("total_amount") < 0),
    RuleSpec("negative_distance", "error", "value", "trip_distance < 0", lambda: _c("trip_distance") < 0),
    RuleSpec("implausible_distance", "error", "business", "trip_distance > DQ_MAX_DISTANCE_MI",
             lambda: _c("trip_distance") > MAX_DISTANCE_MI),
    RuleSpec("implausible_fare", "error", "business", "fare_amount > DQ_MAX_FARE",
             lambda: _c("fare_amount") > MAX_FARE),
    RuleSpec("invalid_pickup_zone", "error", "business", "pu_location_id null or outside 1..265",
             lambda: _zone_bad("pu_location_id")),
    RuleSpec("invalid_dropoff_zone", "error", "business", "do_location_id null or outside 1..265",
             lambda: _zone_bad("do_location_id")),
    RuleSpec("invalid_passenger_count", "error", "value", "passenger_count outside 0..9",
             lambda: (_c("passenger_count") < 0) | (_c("passenger_count") > 9)),
    RuleSpec("missing_dropoff", "warn", "null", "dropoff_ts is null", lambda: _c("dropoff_ts").isNull()),
    RuleSpec("null_passenger_count", "warn", "null", "passenger_count is null",
             lambda: _c("passenger_count").isNull()),
    RuleSpec("zero_passenger_count", "warn", "value", "passenger_count = 0",
             lambda: _c("passenger_count") == 0),
    RuleSpec("zero_distance", "warn", "value", "trip_distance = 0", lambda: _c("trip_distance") == 0),
    RuleSpec("null_payment_type", "warn", "null", "payment_type is null", lambda: _c("payment_type").isNull()),
    RuleSpec("total_less_than_fare", "warn", "business", "total_amount < fare_amount - 0.01",
             lambda: _c("total_amount") < _c("fare_amount") - 0.01),
]
ERROR_RULES = [r.name for r in RULE_SPECS if r.severity == "error"]
WARN_RULES = [r.name for r in RULE_SPECS if r.severity == "warn"]


def decode_bronze(bronze: DataFrame, schema_json: str) -> DataFrame:
    """Confluent wire format -> typed columns. PERMISSIVE: undecodable bytes yield NULL, never a crash."""
    body = F.expr("substring(raw_payload, 6, length(raw_payload) - 5)")
    df = bronze.withColumn("e", from_avro(body, schema_json, {"mode": "PERMISSIVE"}))
    # PERMISSIVE does NOT yield a NULL struct on garbage: it yields a struct whose fields are all NULL
    # (found by tests/spark). event_id and pickup_ts are non-nullable in the schema, so NULL there
    # can only mean the decode failed.
    df = df.withColumn(
        "undecodable",
        _c("raw_payload").isNull() | (F.hex(F.substring("raw_payload", 1, 1)) != "00") | _c("e").isNull()
        | _c("e.event_id").isNull() | _c("e.pickup_ts").isNull())
    keep = [c for c in bronze.columns] + ["undecodable"]
    return df.select(*keep, "e.*").withColumn(
        "trip_duration_seconds", _c("dropoff_ts").cast("long") - _c("pickup_ts").cast("long"))


def apply_rules(df: DataFrame) -> DataFrame:
    """Adds errors[], warnings[], dq_score. Expects the output of decode_bronze."""
    ok = ~_c("undecodable")

    def fired(spec: RuleSpec) -> Column:
        cond = spec.violated()
        # undecodable_payload is evaluated on every row; all others only on decodable rows
        guard = cond if spec.name == "undecodable_payload" else ok & F.coalesce(cond, F.lit(False))
        return F.when(guard, F.lit(spec.name))

    def collect(sev: str) -> Column:
        arr = F.array(*[fired(r) for r in RULE_SPECS if r.severity == sev])
        return F.filter(arr, lambda x: x.isNotNull())

    return (df.withColumn("errors", collect("error")).withColumn("warnings", collect("warn"))
            .withColumn("dq_score",
                        F.when(F.size("errors") > 0, F.lit(0.0))
                        .otherwise(F.greatest(F.lit(0.0), F.lit(1.0) - F.lit(WARN_PENALTY) * F.size("warnings")))))
