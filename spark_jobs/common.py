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
