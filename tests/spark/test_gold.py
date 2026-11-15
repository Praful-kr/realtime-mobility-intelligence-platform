import re
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

from spark_jobs.gold_aggregates import GOLD_SPECS, SILVER_NEEDED, SPECS_BY_NAME, outlier_flags
from spark_jobs.gold_sinks import PgSink
from spark_jobs.gold_stream import make_handler, start_gold_queries
from tests.spark.conftest import UTC

SILVER_SCHEMA = StructType([
    StructField("event_time", TimestampType()), StructField("taxi_type", StringType()),
    StructField("pu_location_id", IntegerType()), StructField("do_location_id", IntegerType()),
    StructField("total_amount", DoubleType()), StructField("fare_amount", DoubleType()),
    StructField("tip_amount", DoubleType()), StructField("trip_distance", DoubleType()),
    StructField("trip_duration_seconds", LongType()),
])
assert [f.name for f in SILVER_SCHEMA] == SILVER_NEEDED


def ev(ts, taxi="yellow", pu=161, do=237, total=20.0, fare=15.0, tip=2.0, dist=3.0, dur=600):
    h, m, s = (int(x) for x in ts.split(":"))
    return (datetime(2024, 1, 5, h, m, s, tzinfo=UTC), taxi, pu, do, total, fare, tip, dist, dur)


def silver(spark, events):
    return spark.createDataFrame(events, SILVER_SCHEMA)


def rows(df, *keys):
    return {tuple(r[k] for k in keys): r.asDict() for r in df.collect()}


# ---- aggregate semantics ---------------------------------------------------------------------------
def test_trips_per_minute_sums_and_averages(spark):
    s = silver(spark, [ev("10:00:10", total=10.0, fare=8.0, dur=300), ev("10:00:50", total=30.0, fare=22.0, dur=900),
                       ev("10:01:05", taxi="green"), ev("10:00:20", taxi="green")])
    spec = SPECS_BY_NAME["trips_per_minute"]
    out = rows(spec.finalize(spec.build(s, None), None), "window_start", "taxi_type")
    y = out[(datetime(2024, 1, 5, 10, 0), "yellow")]
    assert (y["trips"], y["revenue"], y["fare_sum"], y["duration_n"]) == (2, 40.0, 30.0, 2)
    assert y["avg_duration_s"] == 600.0 and y["avg_fare"] == 15.0
    assert out[(datetime(2024, 1, 5, 10, 0), "green")]["trips"] == 1 and len(out) == 3


def test_sliding_window_assigns_each_event_to_three_windows(spark):
    spec = SPECS_BY_NAME["demand_sliding_15m"]
    out = rows(spec.finalize(spec.build(silver(spark, [ev("10:07:00", total=10.0)]), None), None), "window_start")
    assert sorted(k[0].strftime("%H:%M") for k in out) == ["09:55", "10:00", "10:05"]
    assert all(r["trips"] == 1 and r["revenue"] == 10.0 for r in out.values())
    assert all((r["window_end"] - r["window_start"]).seconds == 900 for r in out.values())


def test_zone_demand_counts_pickups_and_dropoffs_with_borough_names(spark):
    zones = spark.createDataFrame([(161, "Manhattan", "Midtown Center"), (237, "Manhattan", "UES South")],
                                  "LocationID int, Borough string, Zone string")
    s = silver(spark, [ev("10:01:00", pu=161, do=237), ev("10:02:00", pu=161, do=161)])
    spec = SPECS_BY_NAME["zone_demand_5m"]
    out = rows(spec.finalize(spec.build(s, None), zones), "zone_id", "role")
    assert out[(161, "pickup")]["trips"] == 2 and out[(161, "dropoff")]["trips"] == 1
    assert out[(237, "dropoff")]["trips"] == 1 and (237, "pickup") not in out
    assert out[(161, "pickup")]["zone_name"] == "Midtown Center" and out[(161, "pickup")]["borough"] == "Manhattan"


def test_hourly_demand(spark):
    s = silver(spark, [ev("10:05:00"), ev("10:55:00"), ev("11:00:00")])
    spec = SPECS_BY_NAME["hourly_demand"]
    out = rows(spec.finalize(spec.build(s, None), None), "window_start")
    assert [out[(datetime(2024, 1, 5, h),)]["trips"] for h in (10, 11)] == [2, 1]


def test_outlier_flags_each_type_and_clean_trip(spark):
    s = silver(spark, [
        ev("10:00:01", dist=50.0, dur=1800),            # 100 mph
        ev("10:00:02", dist=1.0, fare=80.0),            # $80/mile
        ev("10:00:03", tip=250.0),                      # huge tip
        ev("10:00:04", dur=5 * 3600, dist=10.0),        # 5 h trip (7 mph)
        ev("10:00:05"),                                 # clean
        ev("10:00:06", dist=0.0, dur=30, fare=3.0),     # zero distance, short: not an outlier
    ])
    got = sorted(r["anomaly_type"] for r in outlier_flags(s).collect())
    assert got == ["duration_over_4h", "fare_per_mile_over_50", "speed_over_80mph", "tip_over_100"]


# ---- Postgres contract -----------------------------------------------------------------------------
def test_spec_columns_match_postgres_ddl():
    ddl = (Path(__file__).parents[2] / "infrastructure/postgres/init.sql").read_text()
    for spec in GOLD_SPECS:
        m = re.search(rf"CREATE TABLE IF NOT EXISTS gold\.{spec.table} \((.*?)\);", ddl, re.S)
        assert m, spec.table
        body = m.group(1)
        for col in spec.columns:
            assert re.search(rf"\b{col}\b", body), f"{spec.table}.{col} missing from init.sql"
        for k in spec.keys:
            assert k in spec.columns


def test_pg_upsert_sql_shape():
    sql = PgSink.upsert_sql(SPECS_BY_NAME["anomaly_counts"])
    assert sql == ("INSERT INTO gold.anomaly_counts (window_start, anomaly_type, n) VALUES %s "
                   "ON CONFLICT (window_start, anomaly_type) DO UPDATE SET n = EXCLUDED.n, updated_at = now()")


# ---- sink idempotency --------------------------------------------------------------------------------
def test_handler_is_idempotent_when_a_batch_is_replayed(spark, tmp_path):
    spec = SPECS_BY_NAME["trips_per_minute"]
    batch = spec.build(silver(spark, [ev("10:00:10"), ev("10:00:20"), ev("10:03:00")]), None)
    handle = make_handler(spark, spec, str(tmp_path / "gold"), None, None)
    handle(batch, 0)
    handle(batch, 0)  # replay after crash
    out = spark.read.format("delta").load(str(tmp_path / "gold/trips_per_minute"))
    assert out.count() == 2 and out.agg(F.sum("trips")).first()[0] == 3


# ---- watermark / late data (the real job code on real Delta tables) ---------------------------------
def test_late_data_within_watermark_is_merged_and_beyond_is_dropped_and_counted(spark, tmp_path):
    silver_path, gold_root, ckpt = str(tmp_path / "silver"), str(tmp_path / "gold"), str(tmp_path / "ckpt")
    spec = SPECS_BY_NAME["trips_per_minute"]

    def commit(events):
        silver(spark, events).write.format("delta").mode("append").save(silver_path)

    def run():
        qs = start_gold_queries(spark, silver_path, gold_root, ckpt, "30 minutes", "availableNow", None, None,
                                specs=[spec])
        for q in qs:
            q.awaitTermination()
        return sum(o.get("numRowsDroppedByWatermark", 0) for q in qs for p in q.recentProgress
                   for o in (p.get("stateOperators") or []))

    def gold():
        return {r["window_start"].strftime("%H:%M"): r["trips"]
                for r in spark.read.format("delta").load(f"{gold_root}/trips_per_minute").collect()}

    commit([ev("10:00:30"), ev("10:00:50"), ev("10:15:10"), ev("10:30:00")])
    assert run() == 0
    commit([ev("12:30:00"), ev("12:30:30")])               # advances the watermark to ~12:00
    assert run() == 0
    commit([ev("10:15:10"),                                 # 2h15m late: window already evicted -> dropped
            ev("12:10:00")])                                # 20 min late: inside the 30 min allowance -> merged
    dropped = run()

    g = gold()
    assert dropped == 1
    assert g["10:15"] == 1, "evicted window must not be modified by a too-late event"
    assert g["12:10"] == 1, "late-but-within-watermark event must be aggregated"
    silver_rows = spark.read.format("delta").load(silver_path).count()
    assert silver_rows == 8 and sum(g.values()) + dropped == silver_rows   # holds exactly here because each late event is its own group


def test_watermark_drop_metric_counts_groups_not_events(spark, tmp_path):
    """Pins Spark 3.5.3 behaviour found in the live run: numRowsDroppedByWatermark counts AGGREGATED groups
    (after partial aggregation), so it is only a lower bound on lost events. Exact loss = Silver - Gold."""
    silver_path, gold_root, ckpt = str(tmp_path / "silver"), str(tmp_path / "gold"), str(tmp_path / "ckpt")
    spec = SPECS_BY_NAME["trips_per_minute"]

    def commit(events):
        silver(spark, events).write.format("delta").mode("append").save(silver_path)

    def run():
        qs = start_gold_queries(spark, silver_path, gold_root, ckpt, "30 minutes", "availableNow", None, None,
                                specs=[spec])
        for q in qs:
            q.awaitTermination()
        return sum(o.get("numRowsDroppedByWatermark", 0) for q in qs for p in q.recentProgress
                   for o in (p.get("stateOperators") or []))

    commit([ev("10:15:10"), ev("10:30:00")]); run()
    commit([ev("12:30:00")]); run()
    commit([ev("10:15:20"), ev("10:15:30"), ev("10:15:40")])      # 3 events, ONE window group, all too late
    dropped = run()
    gold_total = sum(r["trips"] for r in spark.read.format("delta").load(f"{gold_root}/trips_per_minute").collect())
    events_lost = spark.read.format("delta").load(silver_path).count() - gold_total
    assert events_lost == 3
    assert 0 < dropped < events_lost, f"metric reported {dropped}; if it equals {events_lost} Spark changed semantics"
