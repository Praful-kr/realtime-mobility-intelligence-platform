"""Spark tests run INSIDE the spark image (host has no JVM):  make test-spark"""
import io
import json
import struct
from datetime import datetime, timezone
from pathlib import Path

import fastavro
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (BinaryType, IntegerType, LongType, StringType, StructField, StructType,
                               TimestampType)

SCHEMA_FILE = Path(__file__).parents[2] / "schemas" / "trip_event_v1.avsc"
SCHEMA_JSON = SCHEMA_FILE.read_text()
PARSED = fastavro.parse_schema(json.loads(SCHEMA_JSON))
UTC = timezone.utc
BRONZE_SCHEMA = StructType([
    StructField("kafka_topic", StringType()), StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()), StructField("kafka_timestamp", TimestampType()),
    StructField("kafka_key", StringType()), StructField("schema_id", IntegerType()),
    StructField("raw_payload", BinaryType()), StructField("bronze_ingested_at", TimestampType()),
])


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .config("spark.sql.session.timeZone", "UTC").config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false").config("spark.driver.host", "localhost").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def good_event(i=1, **over):
    e = {"event_id": f"evt-{i}", "taxi_type": "yellow",
         "event_time": datetime(2024, 1, 5, 8, 30, tzinfo=UTC), "ingestion_time": datetime(2026, 1, 1, tzinfo=UTC),
         "source_file": "f.parquet", "source_row": i, "vendor_id": 2,
         "pickup_ts": datetime(2024, 1, 5, 8, 30, tzinfo=UTC), "dropoff_ts": datetime(2024, 1, 5, 8, 50, tzinfo=UTC),
         "passenger_count": 1, "trip_distance": 3.2, "ratecode_id": 1, "store_and_fwd_flag": "N",
         "pu_location_id": 161, "do_location_id": 237, "payment_type": 1, "fare_amount": 15.5, "extra": 1.0,
         "mta_tax": 0.5, "tip_amount": 2.0, "tolls_amount": 0.0, "improvement_surcharge": 1.0,
         "total_amount": 20.0, "congestion_surcharge": 2.5, "airport_fee": None, "trip_type": None,
         "ehail_fee": None}
    e.update(over)
    return e


def encode(event, schema_id=1) -> bytes:
    buf = io.BytesIO()
    buf.write(b"\x00" + struct.pack(">I", schema_id))
    fastavro.schemaless_writer(buf, PARSED, event)
    return buf.getvalue()


def bronze_df(spark, payloads, start_offset=0):
    now = datetime(2026, 1, 1, tzinfo=UTC)
    rows = [("trips.raw.v1", i % 2, start_offset + i, now, f"k{i}", 1, p, now) for i, p in enumerate(payloads)]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)
