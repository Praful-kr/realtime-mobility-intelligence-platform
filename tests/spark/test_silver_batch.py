from datetime import datetime

from pyspark.sql import functions as F

from spark_jobs.silver_stream import Paths, process_batch
from tests.spark.conftest import SCHEMA_JSON, UTC, bronze_df, encode, good_event


def paths(tmp_path):
    return Paths(str(tmp_path / "silver"), str(tmp_path / "quarantine"), str(tmp_path / "dq"))


def read(spark, p):
    return spark.read.format("delta").load(p)


def test_batch_splits_valid_invalid_and_dedups(spark, tmp_path):
    p = paths(tmp_path)
    payloads = [
        encode(good_event(1)), encode(good_event(1)),          # exact duplicate delivery
        encode(good_event(2, passenger_count=0)),               # valid with warning
        encode(good_event(3, fare_amount=-9.0)),                # quarantined
        b"\x00\x00\x00\x00\x01\xff\xfegarbage",                # poison
    ]
    m = process_batch(spark, bronze_df(spark, payloads), 0, p, SCHEMA_JSON)
    assert (m["bronze_rows"], m["valid_rows"], m["quarantined"], m["undecodable"]) == (5, 3, 2, 1)
    assert (m["dup_in_batch"], m["dup_vs_silver"], m["silver_inserted"]) == (1, 0, 2)
    silver = read(spark, p.silver)
    assert silver.count() == 2 and silver.select("event_id").distinct().count() == 2
    assert silver.where("event_id='evt-2'").first()["dq_score"] == 0.9
    q = {r["event_id"]: r["errors"] for r in read(spark, p.quarantine).collect()}
    assert q["evt-3"] == ["negative_fare"] and q[None] == ["undecodable_payload"]
    assert m["rule_counts"]["negative_fare"] == 1 and m["rule_counts"]["zero_passenger_count"] == 1


def test_duplicates_across_batches_are_dropped_by_merge(spark, tmp_path):
    p = paths(tmp_path)
    process_batch(spark, bronze_df(spark, [encode(good_event(1)), encode(good_event(2))]), 0, p, SCHEMA_JSON)
    m = process_batch(spark, bronze_df(spark, [encode(good_event(2)), encode(good_event(3))], 10), 1, p, SCHEMA_JSON)
    assert (m["silver_inserted"], m["dup_vs_silver"]) == (1, 1)
    assert read(spark, p.silver).select("event_id").distinct().count() == 3 == read(spark, p.silver).count()


def test_retrying_the_same_batch_is_idempotent(spark, tmp_path):
    p = paths(tmp_path)
    df = bronze_df(spark, [encode(good_event(1)), encode(good_event(2, fare_amount=-1.0))])
    process_batch(spark, df, 7, p, SCHEMA_JSON)
    process_batch(spark, df, 7, p, SCHEMA_JSON)   # simulated replay after crash before checkpoint
    assert read(spark, p.silver).count() == 1
    assert read(spark, p.quarantine).count() == 1  # txnAppId/txnVersion skipped the second append
    assert read(spark, p.dq_metrics).count() == 1


def test_all_invalid_batch_still_creates_silver_table(spark, tmp_path):
    p = paths(tmp_path)
    m = process_batch(spark, bronze_df(spark, [b"junk"]), 0, p, SCHEMA_JSON)
    assert m["silver_inserted"] == 0 and read(spark, p.silver).count() == 0
