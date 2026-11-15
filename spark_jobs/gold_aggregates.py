"""Gold aggregation definitions. Pure DataFrame builders used by BOTH the streaming job (with a
watermark) and the batch verifier (without), so semantics cannot silently diverge.

Every table stores *additive* measures (counts and sums) so averages are derived, never averaged
twice, and updates are idempotent upserts keyed on (window, dimensions).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SILVER_NEEDED = ["event_time", "taxi_type", "pu_location_id", "do_location_id", "total_amount", "fare_amount",
                 "tip_amount", "trip_distance", "trip_duration_seconds"]

# Trip-level outlier detection on records that already PASSED validation (plausible but suspicious).
OUTLIER_TYPES = ["speed_over_80mph", "fare_per_mile_over_50", "tip_over_100", "duration_over_4h"]


@dataclass(frozen=True)
class GoldSpec:
    name: str
    table: str                    # logical name; Delta path = <gold_root>/<table>, Postgres = gold.<table>
    keys: List[str]
    columns: List[str]            # exact output columns (also the Postgres columns, minus updated_at)
    build: Callable[[DataFrame, Optional[str]], DataFrame]   # (silver, watermark_delay|None) -> agg df
    finalize: Callable[[DataFrame, Optional[DataFrame]], DataFrame]  # agg df (+zones) -> output columns
    partition_by_date: bool = False


def _wm(df: DataFrame, delay: Optional[str]) -> DataFrame:
    return df.withWatermark("event_time", delay) if delay else df


# ---- trips per minute ---------------------------------------------------------------------------
def _minute_build(src, delay):
    return (_wm(src, delay).groupBy(F.window("event_time", "1 minute").alias("w"), "taxi_type")
            .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue"),
                 F.sum("fare_amount").alias("fare_sum"), F.sum("tip_amount").alias("tip_sum"),
                 F.sum("trip_distance").alias("distance_sum"),
                 F.sum("trip_duration_seconds").alias("duration_sum_s"),
                 F.count("trip_duration_seconds").alias("duration_n")))


def _minute_final(df, _zones):
    return df.select(
        F.col("w.start").alias("window_start"), "taxi_type", "trips", "revenue", "fare_sum", "tip_sum",
        "distance_sum", "duration_sum_s", "duration_n",
        (F.col("duration_sum_s") / F.col("duration_n")).alias("avg_duration_s"),
        (F.col("fare_sum") / F.col("trips")).alias("avg_fare"))


# ---- sliding 15m / 5m ---------------------------------------------------------------------------
def _sliding_build(src, delay):
    return (_wm(src, delay).groupBy(F.window("event_time", "15 minutes", "5 minutes").alias("w"))
            .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue")))


def _sliding_final(df, _zones):
    return df.select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "trips", "revenue")


# ---- zone demand (pickup + drop-off in one aggregation via union) ---------------------------------
def _zone_build(src, delay):
    pick = src.select("event_time", F.col("pu_location_id").alias("zone_id"), F.lit("pickup").alias("role"),
                      "total_amount")
    drop = src.select("event_time", F.col("do_location_id").alias("zone_id"), F.lit("dropoff").alias("role"),
                      "total_amount")
    return (_wm(pick.unionByName(drop), delay)
            .groupBy(F.window("event_time", "5 minutes").alias("w"), "zone_id", "role")
            .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue")))


def _zone_final(df, zones):
    out = df.select(F.col("w.start").alias("window_start"), "zone_id", "role", "trips", "revenue")
    if zones is not None:
        out = out.join(F.broadcast(zones), out["zone_id"] == zones["LocationID"], "left")
    else:
        out = out.withColumn("Borough", F.lit(None).cast("string")).withColumn("Zone", F.lit(None).cast("string"))
    return out.select("window_start", "zone_id", "role", F.col("Borough").alias("borough"),
                      F.col("Zone").alias("zone_name"), "trips", "revenue")


# ---- hourly demand --------------------------------------------------------------------------------
def _hourly_build(src, delay):
    return (_wm(src, delay).groupBy(F.window("event_time", "1 hour").alias("w"), "taxi_type")
            .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue"),
                 F.sum("fare_amount").alias("fare_sum")))


def _hourly_final(df, _zones):
    return df.select(F.col("w.start").alias("window_start"), "taxi_type", "trips", "revenue", "fare_sum")


# ---- anomaly counts ---------------------------------------------------------------------------------
def outlier_flags(df: DataFrame) -> DataFrame:
    dur, dist, fare, tip = (F.col("trip_duration_seconds"), F.col("trip_distance"), F.col("fare_amount"),
                            F.col("tip_amount"))
    flags = F.array(
        F.when((dur >= 60) & (dist > 0) & (dist / (dur / 3600.0) > 80), F.lit("speed_over_80mph")),
        F.when((dist >= 0.5) & (fare / dist > 50), F.lit("fare_per_mile_over_50")),
        F.when(tip > 100, F.lit("tip_over_100")),
        F.when(dur > 4 * 3600, F.lit("duration_over_4h")))
    return df.select("event_time", F.explode(F.filter(flags, lambda x: x.isNotNull())).alias("anomaly_type"))


def _anomaly_build(src, delay):
    return (_wm(outlier_flags(src), delay).groupBy(F.window("event_time", "1 minute").alias("w"), "anomaly_type")
            .agg(F.count("*").alias("n")))


def _anomaly_final(df, _zones):
    return df.select(F.col("w.start").alias("window_start"), "anomaly_type", "n")


GOLD_SPECS: List[GoldSpec] = [
    GoldSpec("trips_per_minute", "trips_per_minute", ["window_start", "taxi_type"],
             ["window_start", "taxi_type", "trips", "revenue", "fare_sum", "tip_sum", "distance_sum",
              "duration_sum_s", "duration_n", "avg_duration_s", "avg_fare"], _minute_build, _minute_final, True),
    GoldSpec("demand_sliding_15m", "demand_sliding_15m", ["window_start"],
             ["window_start", "window_end", "trips", "revenue"], _sliding_build, _sliding_final),
    GoldSpec("zone_demand_5m", "zone_demand_5m", ["window_start", "zone_id", "role"],
             ["window_start", "zone_id", "role", "borough", "zone_name", "trips", "revenue"],
             _zone_build, _zone_final),
    GoldSpec("hourly_demand", "hourly_demand", ["window_start", "taxi_type"],
             ["window_start", "taxi_type", "trips", "revenue", "fare_sum"], _hourly_build, _hourly_final),
    GoldSpec("anomaly_counts", "anomaly_counts", ["window_start", "anomaly_type"],
             ["window_start", "anomaly_type", "n"], _anomaly_build, _anomaly_final, True),
]
SPECS_BY_NAME = {s.name: s for s in GOLD_SPECS}
