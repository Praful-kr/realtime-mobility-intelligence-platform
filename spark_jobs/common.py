"""Shared Spark session / path helpers. All settings come from env so jobs run unchanged in compose."""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

BRONZE_PATH = os.getenv("BRONZE_PATH", "s3a://lakehouse/bronze/trips_raw")
CHECKPOINT_ROOT = os.getenv("CHECKPOINT_ROOT", "s3a://checkpoints")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "trips.raw.v1")
SILVER_PATH = os.getenv("SILVER_PATH", "s3a://lakehouse/silver/trips")
QUARANTINE_PATH = os.getenv("QUARANTINE_PATH", "s3a://lakehouse/silver/trips_quarantine")
DQ_METRICS_PATH = os.getenv("DQ_METRICS_PATH", "s3a://lakehouse/silver/dq_metrics")
TRIP_SCHEMA_PATH = os.getenv("TRIP_SCHEMA_PATH", "/opt/app/schemas/trip_event_v1.avsc")


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "6"))
        .config("spark.sql.streaming.metricsEnabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def wait_for_delta_table(spark: SparkSession, path: str, poll_seconds: int = 5, timeout_seconds: int = 0) -> None:
    """Block until an upstream Delta table exists AND has a schema. Streaming-reading a table that its
    producer has not written yet fails with DELTA_SCHEMA_NOT_SET; on a cold start the layers come up in
    any order, so each downstream job waits for its source (timeout 0 = wait forever).

    Existence is checked against the OBJECT STORE (does _delta_log contain commits?), never via DeltaTable.isDeltaTable:
    a waiter that polled before another process created the table kept a stale 'no such table' view and never saw the
    table (reproduced across two containers, tests/recovery/repro_wait_s3a.py). Cached Delta logs and Spark file
    indexes are dropped before the first real read.
    """
    import time

    jvm = spark._jvm
    waited, last_err_log = 0, -60
    while True:
        err = None
        try:
            log_dir = jvm.org.apache.hadoop.fs.Path(path.rstrip("/") + "/_delta_log")
            fs = log_dir.getFileSystem(spark._jsc.hadoopConfiguration())
            if fs.exists(log_dir) and len(fs.listStatus(log_dir)) > 0:
                jvm.org.apache.spark.sql.delta.DeltaLog.clearCache()
                spark.catalog.refreshByPath(path)
                if spark.read.format("delta").load(path).schema.fields:
                    return
        except Exception as e:  # noqa: BLE001 - table mid-creation; retry, but never swallow silently
            err = repr(e)[:300]
        if timeout_seconds and waited >= timeout_seconds:
            raise TimeoutError(f"upstream table not available: {path}")
        print(f"WAITING_FOR_UPSTREAM {path}", flush=True)
        if err and waited - last_err_log >= 60:
            print(f"WAITING_FOR_UPSTREAM_ERROR {err}", flush=True)
            last_err_log = waited
        time.sleep(poll_seconds)
        waited += poll_seconds


def run_until_stopped(queries, grace_seconds: int = 50) -> None:
    """Block until the queries finish, one fails, or SIGTERM/SIGINT arrives.

    Graceful stop = wait (up to grace_seconds) until no micro-batch is executing, THEN stop the queries, so a batch is not
    interrupted half-way (StreamingQuery.stop() interrupts the running batch). Anything interrupted anyway is still safe:
    sinks are idempotent and offsets are committed only after the sink write. Raises if any query failed so the process
    exits non-zero and the orchestrator restarts it.
    """
    import signal
    import threading
    import time

    stop = threading.Event()

    def _signal(signum, _frame):
        print(f"SHUTDOWN_REQUESTED signal={signum}", flush=True)
        stop.set()

    signal.signal(signal.SIGTERM, _signal)
    signal.signal(signal.SIGINT, _signal)
    while not stop.is_set() and any(q.isActive for q in queries):
        if any(q.exception() is not None for q in queries):
            break
        stop.wait(1.0)
    if stop.is_set():
        deadline = time.time() + grace_seconds
        while time.time() < deadline and any(q.status.get("isTriggerActive") for q in queries):
            time.sleep(0.3)
        print("SHUTDOWN_STOPPING_QUERIES (no batch in flight)" if time.time() < deadline
              else "SHUTDOWN_GRACE_EXPIRED stopping mid-batch (safe: idempotent sinks)", flush=True)
        for q in queries:
            q.stop()
        print("SHUTDOWN_COMPLETE", flush=True)
        return
    failed = [q for q in queries if q.exception() is not None]
    for q in queries:
        if q.isActive:
            q.stop()
    if failed:
        raise RuntimeError(f"streaming query {failed[0].name} failed: {failed[0].exception()}")
