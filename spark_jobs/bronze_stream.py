"""Bronze: Kafka -> Delta, raw and immutable.

Stores the *original Avro bytes* (never parsed here, so a bad record can never fail the job or be
lost) plus Kafka coordinates and ingestion metadata. Decoding/validation is Silver's job.

Delivery semantics: Kafka source is replayable by offset (Spark checkpoints offsets); the sink write
in foreachBatch is made idempotent with Delta's (txnAppId, txnVersion=batchId): if a batch is
re-run after a crash between the Delta commit and the offset checkpoint, Delta skips the duplicate
write. => effectively-once into Bronze. (Demonstrated by tests/recovery, not assumed.)

  spark-submit bronze_stream.py --trigger availableNow     # drain topic then stop
  spark-submit bronze_stream.py --trigger 5                # continuous, 5s micro-batches
"""
from __future__ import annotations

import argparse
import signal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs.common import BRONZE_PATH, CHECKPOINT_ROOT, KAFKA_BOOTSTRAP, RAW_TOPIC, build_spark

APP_ID = "bronze-trips-raw"


def to_bronze(df: DataFrame) -> DataFrame:
    """Pure transformation (unit-testable): Kafka source columns -> Bronze columns."""
    return df.select(
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("key").cast("string").alias("kafka_key"),
        # Confluent wire format: magic byte 0x00 + 4-byte big-endian schema id + Avro body
        F.conv(F.hex(F.substring("value", 2, 4)), 16, 10).cast("int").alias("schema_id"),
        F.col("value").alias("raw_payload"),
    )


def make_batch_writer(path: str):
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        out = (to_bronze(batch_df)
               .withColumn("bronze_ingested_at", F.current_timestamp())
               .withColumn("ingest_date", F.to_date("bronze_ingested_at")))
        (out.write.format("delta").mode("append")
            .option("txnAppId", APP_ID).option("txnVersion", batch_id)  # idempotent re-run of a batch
            .partitionBy("ingest_date").save(path))
    return write_batch


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", default="5", help="'availableNow' or micro-batch seconds")
    ap.add_argument("--max-offsets-per-trigger", type=int, default=200000, help="backpressure bound")
    ap.add_argument("--starting-offsets", default="earliest")
    args = ap.parse_args()

    spark = build_spark("bronze-stream")
    source = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
              .option("subscribe", RAW_TOPIC)
              .option("startingOffsets", args.starting_offsets)
              .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
              .option("failOnDataLoss", "true")
              .load())

    writer = (source.writeStream.foreachBatch(make_batch_writer(BRONZE_PATH))
              .option("checkpointLocation", f"{CHECKPOINT_ROOT}/bronze_trips_raw")
              .queryName(APP_ID))
    writer = (writer.trigger(availableNow=True) if args.trigger == "availableNow"
              else writer.trigger(processingTime=f"{int(args.trigger)} seconds"))
    query = writer.start()

    def _graceful(_signum, _frame):  # finish the in-flight batch, then stop
        query.stop()

    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)
    query.awaitTermination()


if __name__ == "__main__":
    main()
