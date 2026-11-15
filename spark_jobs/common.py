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
    any order, so each downstream job waits for its source (timeout 0 = wait forever)."""
    import time

    from delta.tables import DeltaTable
    waited = 0
    while True:
        try:
            if DeltaTable.isDeltaTable(spark, path) and spark.read.format("delta").load(path).schema.fields:
                return
        except Exception:  # noqa: BLE001 - table mid-creation; retry
            pass
        if timeout_seconds and waited >= timeout_seconds:
            raise TimeoutError(f"upstream table not available: {path}")
        print(f"WAITING_FOR_UPSTREAM {path}", flush=True)
        time.sleep(poll_seconds)
        waited += poll_seconds
