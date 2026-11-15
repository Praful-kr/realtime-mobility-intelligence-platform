"""Silver: Bronze Delta -> validated, deduplicated, typed trips + quarantine + DQ metrics.

Per micro-batch (foreachBatch):
  1. decode Avro + apply rule catalog (dq_rules)            -> ERROR rows are quarantined with reasons
  2. dedup inside the batch (earliest Kafka offset wins)
  3. MERGE insert-only into Silver on event_id, pruned to the batch's event_date partitions
     -> exact dedup across batches, idempotent on retry (a re-run inserts nothing).
  4. one DQ metrics row per batch.

Why MERGE instead of dropDuplicatesWithinWatermark: the streaming dedup operator silently DROPS rows
older than the watermark. For a platform that must account for every record, silent loss is worse
than the cost of a pruned merge. Watermarks/late data are handled in Gold windows.

Quarantine is *not* deduplicated: every rejected delivery is kept (audit trail + reprocessing).
"""
from __future__ import annotations

import argparse
import signal
from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (LongType, MapType, StringType, StructField, StructType, TimestampType,
                               DoubleType)

from spark_jobs.common import (BRONZE_PATH, CHECKPOINT_ROOT, DQ_METRICS_PATH, QUARANTINE_PATH, SILVER_PATH,
                               TRIP_SCHEMA_PATH, build_spark, wait_for_delta_table)
from spark_jobs.dq_rules import apply_rules, decode_bronze

SILVER_COLUMNS = [
    "event_id", "taxi_type", "event_time", "dropoff_ts", "trip_duration_seconds", "ingestion_time",
    "source_file", "source_row", "vendor_id", "passenger_count", "trip_distance", "ratecode_id",
    "store_and_fwd_flag", "pu_location_id", "do_location_id", "payment_type", "fare_amount", "extra",
    "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount",
    "congestion_surcharge", "airport_fee", "trip_type", "ehail_fee", "dq_score", "warnings",
    "kafka_partition", "kafka_offset",
]
METRICS_SCHEMA = StructType([
    StructField("batch_id", LongType()), StructField("processed_at", TimestampType()),
    StructField("bronze_rows", LongType()), StructField("undecodable", LongType()),
    StructField("quarantined", LongType()), StructField("valid_rows", LongType()),
    StructField("dup_in_batch", LongType()), StructField("dup_vs_silver", LongType()),
    StructField("silver_inserted", LongType()), StructField("avg_dq_score", DoubleType()),
    StructField("rule_counts", MapType(StringType(), LongType())),
])


@dataclass(frozen=True)
class Paths:
    silver: str = SILVER_PATH
    quarantine: str = QUARANTINE_PATH
    dq_metrics: str = DQ_METRICS_PATH


def _append_idempotent(df: DataFrame, path: str, app_id: str, batch_id: int, partition_by: str | None) -> None:
    w = df.write.format("delta").mode("append").option("txnAppId", app_id).option("txnVersion", batch_id)
    (w.partitionBy(partition_by) if partition_by else w).save(path)


def process_batch(spark: SparkSession, batch_df: DataFrame, batch_id: int, paths: Paths,
                  schema_json: str) -> dict:
    checked = apply_rules(decode_bronze(batch_df, schema_json)).persist()
    try:
        bad = checked.where(F.size("errors") > 0)
        good = checked.where(F.size("errors") == 0)

        # 1. quarantine (keeps raw bytes so rejected records can be reprocessed after a rule/schema fix)
        quarantine = bad.select(
            "event_id", "taxi_type", "errors", "warnings", "kafka_topic", "kafka_partition", "kafka_offset",
            "raw_payload", "bronze_ingested_at", F.current_timestamp().alias("quarantined_at"),
            F.to_date(F.current_timestamp()).alias("quarantine_date"))
        _append_idempotent(quarantine, paths.quarantine, "silver-quarantine", batch_id, "quarantine_date")

        # 2. in-batch dedup (deterministic: lowest partition/offset wins)
        w = Window.partitionBy("event_id").orderBy("kafka_partition", "kafka_offset")
        unique = (good.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
                  .withColumn("event_date", F.to_date("event_time"))
                  .select(*SILVER_COLUMNS, "event_date", F.current_timestamp().alias("silver_processed_at"))
                  .persist())
        try:
            n_unique = unique.count()
            if not DeltaTable.isDeltaTable(spark, paths.silver):
                unique.limit(0).write.format("delta").partitionBy("event_date").save(paths.silver)

            # 3. exact cross-batch dedup: insert-only merge, pruned to the dates present in this batch
            inserted = 0
            if n_unique:
                dates = [r[0].isoformat() for r in unique.select("event_date").distinct().collect()]
                in_list = ",".join(f"DATE'{d}'" for d in dates)
                target = DeltaTable.forPath(spark, paths.silver)
                (target.alias("t").merge(unique.alias("s"),
                                         f"t.event_id = s.event_id AND t.event_date IN ({in_list})")
                 .whenNotMatchedInsertAll().execute())
                inserted = int(target.history(1).select("operationMetrics").first()[0]["numTargetRowsInserted"])
            avg_score = unique.agg(F.avg("dq_score")).first()[0]
        finally:
            unique.unpersist()

        # 4. DQ metrics
        totals = checked.agg(
            F.count("*").alias("n"), F.sum(F.col("undecodable").cast("long")).alias("undecodable"),
            F.sum((F.size("errors") > 0).cast("long")).alias("quarantined")).first()
        n_rows, n_undec, n_quar = int(totals["n"] or 0), int(totals["undecodable"] or 0), int(totals["quarantined"] or 0)
        rule_counts = {r["rule"]: int(r["n"]) for r in
                       checked.select(F.explode(F.concat("errors", "warnings")).alias("rule"))
                       .groupBy("rule").agg(F.count("*").alias("n")).collect()}
        n_valid = n_rows - n_quar
        metrics = {
            "batch_id": batch_id, "bronze_rows": n_rows, "undecodable": n_undec, "quarantined": n_quar,
            "valid_rows": n_valid, "dup_in_batch": n_valid - n_unique, "dup_vs_silver": n_unique - inserted,
            "silver_inserted": inserted, "avg_dq_score": avg_score, "rule_counts": rule_counts,
        }
        row = (batch_id, None, n_rows, n_undec, n_quar, n_valid, metrics["dup_in_batch"],
               metrics["dup_vs_silver"], inserted, avg_score, rule_counts)
        mdf = spark.createDataFrame([row], METRICS_SCHEMA).withColumn("processed_at", F.current_timestamp())
        _append_idempotent(mdf, paths.dq_metrics, "silver-dq-metrics", batch_id, None)
        return metrics
    finally:
        checked.unpersist()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", default="5", help="'availableNow' or seconds")
    ap.add_argument("--max-files-per-trigger", type=int, default=20)
    args = ap.parse_args()

    spark = build_spark("silver-stream")
    schema_json = Path(TRIP_SCHEMA_PATH).read_text()
    paths = Paths()
    wait_for_delta_table(spark, BRONZE_PATH)
    source = (spark.readStream.format("delta").option("maxFilesPerTrigger", args.max_files_per_trigger)
              .load(BRONZE_PATH))

    def handle(df: DataFrame, batch_id: int) -> None:
        m = process_batch(spark, df, batch_id, paths, schema_json)
        print("SILVER_BATCH " + str(m), flush=True)

    writer = (source.writeStream.foreachBatch(handle)
              .option("checkpointLocation", f"{CHECKPOINT_ROOT}/silver_trips").queryName("silver-trips"))
    writer = (writer.trigger(availableNow=True) if args.trigger == "availableNow"
              else writer.trigger(processingTime=f"{int(args.trigger)} seconds"))
    query = writer.start()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: query.stop())
    query.awaitTermination()


if __name__ == "__main__":
    main()
