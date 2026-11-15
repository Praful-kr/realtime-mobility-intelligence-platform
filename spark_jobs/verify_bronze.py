"""Verify Bronze against Kafka ground truth. Exit 0 only if everything reconciles.

Checks: (1) Bronze rows == sum of topic end offsets (-start offsets), (2) no duplicate
(topic,partition,offset), (3) every payload decodes as Avro with the registered schema and
event_ids are distinct (given the producer emitted distinct rows).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.avro.functions import from_avro

from spark_jobs.common import BRONZE_PATH, KAFKA_BOOTSTRAP, RAW_TOPIC, build_spark

SCHEMA = Path("/opt/app/schemas/trip_event_v1.avsc").read_text()


def main() -> int:
    spark = build_spark("verify-bronze")
    bronze = spark.read.format("delta").load(BRONZE_PATH)
    rows = bronze.count()

    kafka = (spark.read.format("kafka").option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
             .option("subscribe", RAW_TOPIC).option("startingOffsets", "earliest")
             .option("endingOffsets", "latest").load())
    kafka_rows = kafka.count()  # batch read over [earliest, latest): ground truth per partition

    distinct_coords = bronze.select("kafka_topic", "kafka_partition", "kafka_offset").distinct().count()
    decoded = bronze.select(from_avro(F.expr("substring(raw_payload, 6, length(raw_payload) - 5)"),
                                      SCHEMA, {"mode": "FAILFAST"}).alias("e"))
    distinct_ids = decoded.select("e.event_id").distinct().count()
    by_type = {r["taxi_type"]: r["n"] for r in decoded.groupBy("e.taxi_type").agg(F.count("*").alias("n")).collect()}
    schema_ids = [r[0] for r in bronze.select("schema_id").distinct().collect()]

    hist = DeltaTable.forPath(spark, BRONZE_PATH).history().select("version", "operation", "operationMetrics").collect()
    commit_rows = [int(r["operationMetrics"].get("numOutputRows", 0)) for r in sorted(hist, key=lambda r: r["version"])
                   if r["operation"] == "WRITE" and r["operationMetrics"].get("numOutputRows")]
    report = {"commit_rows": commit_rows, "bronze_rows": rows, "kafka_rows": kafka_rows, "distinct_offsets": distinct_coords,
              "distinct_event_ids": distinct_ids, "by_taxi_type": by_type, "schema_ids": schema_ids}
    print("VERIFY_REPORT " + json.dumps(report))
    ok = rows == kafka_rows == distinct_coords == distinct_ids
    print("VERIFY_RESULT " + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
