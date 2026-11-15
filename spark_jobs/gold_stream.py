"""Gold: Silver Delta (stream) -> windowed aggregates -> Delta (MERGE) + Postgres (upsert).

One streaming query per Gold table (own state store, checkpoint and watermark-drop counter), all in
one driver. Event-time windows + watermark bound the state: windows older than
(max event_time seen - GOLD_WATERMARK) are finalized and evicted; rows that arrive for an evicted
window are DROPPED by Spark. `numRowsDroppedByWatermark` counts aggregated groups, i.e. it is a LOWER BOUND
on lost events (exported to gold.stream_metrics so loss is flagged, never silent); the exact loss is
measured by gold_verify (Silver - Gold) and repaired by a batch recompute of the affected windows.

  spark-submit gold_stream.py --trigger availableNow
  spark-submit gold_stream.py --trigger 5 --watermark "30 minutes"
"""
from __future__ import annotations

import argparse
import json
import os
from typing import Callable, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery, StreamingQueryListener

from spark_jobs.common import CHECKPOINT_ROOT, SILVER_PATH, build_spark, run_until_stopped, wait_for_delta_table
from spark_jobs.gold_aggregates import GOLD_SPECS, SILVER_NEEDED, GoldSpec
from spark_jobs.gold_sinks import PgSink, delta_upsert
from spark_jobs.metrics import start_metrics

GOLD_ROOT = os.getenv("GOLD_ROOT", "s3a://lakehouse/gold")
ZONES_PATH = os.getenv("ZONES_PATH", "/opt/app/data/tlc/taxi_zone_lookup.csv")


def load_zones(spark: SparkSession, path: str) -> Optional[DataFrame]:
    if not os.path.exists(path):
        return None
    return (spark.read.option("header", True).csv(path)
            .selectExpr("cast(LocationID as int) as LocationID", "Borough", "Zone"))


def make_handler(spark: SparkSession, spec: GoldSpec, gold_root: str, zones: Optional[DataFrame],
                 pg: Optional[PgSink]) -> Callable[[DataFrame, int], None]:
    path = f"{gold_root}/{spec.table}"

    def handle(batch_df: DataFrame, batch_id: int) -> None:
        out = spec.finalize(batch_df, zones).persist()
        try:
            delta_upsert(spark, out, path, spec)
            if pg is not None:
                pg.upsert(out, spec)
        finally:
            out.unpersist()
    return handle


def start_gold_queries(spark: SparkSession, silver_path: str, gold_root: str, checkpoint_root: str,
                       watermark: str, trigger: str, pg: Optional[PgSink], zones: Optional[DataFrame],
                       specs: Optional[List[GoldSpec]] = None,
                       max_files_per_trigger: int = 50) -> List[StreamingQuery]:
    src = (spark.readStream.format("delta").option("maxFilesPerTrigger", max_files_per_trigger)
           .load(silver_path).select(*SILVER_NEEDED))
    queries = []
    for spec in (specs or GOLD_SPECS):
        w = (spec.build(src, watermark).writeStream.outputMode("update")
             .foreachBatch(make_handler(spark, spec, gold_root, zones, pg))
             .option("checkpointLocation", f"{checkpoint_root}/gold_{spec.name}").queryName(f"gold_{spec.name}"))
        w = (w.trigger(availableNow=True) if trigger == "availableNow"
             else w.trigger(processingTime=f"{int(trigger)} seconds"))
        queries.append(w.start())
    return queries


def progress_row(p: dict) -> tuple:
    """StreamingQueryProgress json -> (query_name, run_id, batch_id, input_rows, dropped, state_rows, ms, rows/s)."""
    ops = p.get("stateOperators") or []
    return (p.get("name"), p.get("runId"), p.get("batchId"), p.get("numInputRows"),
            sum(o.get("numRowsDroppedByWatermark", 0) for o in ops), sum(o.get("numRowsTotal", 0) for o in ops),
            (p.get("durationMs") or {}).get("triggerExecution"), p.get("processedRowsPerSecond"))


class MetricsListener(StreamingQueryListener):
    """Exports per-batch progress (throughput, state size, watermark drops) to logs + Postgres."""

    def __init__(self, pg: Optional[PgSink]):
        super().__init__()
        self.pg = pg

    def onQueryStarted(self, event):  # noqa: N802
        pass

    def onQueryProgress(self, event):  # noqa: N802
        row = progress_row(json.loads(event.progress.json))
        print("GOLD_PROGRESS " + json.dumps(dict(zip(
            ["query", "run_id", "batch_id", "input_rows", "dropped_by_watermark", "state_rows", "ms", "rows_per_s"],
            row))), flush=True)
        if self.pg is not None and row[3]:
            try:
                self.pg.insert_metrics([row])
            except Exception as e:  # metrics must never kill the stream
                print("GOLD_METRICS_ERROR " + str(e), flush=True)

    def onQueryTerminated(self, event):  # noqa: N802
        pass


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger", default="5")
    ap.add_argument("--watermark", default=os.getenv("GOLD_WATERMARK", "30 minutes"))
    ap.add_argument("--max-files-per-trigger", type=int, default=50)
    ap.add_argument("--no-postgres", action="store_true")
    args = ap.parse_args()

    spark = build_spark("gold-stream")
    pg = None if args.no_postgres else PgSink()
    prom_listener = start_metrics(spark)   # BEFORE waiting for upstream: a cold-starting job must be scrapeable (else TargetDown fires)
    wait_for_delta_table(spark, SILVER_PATH)
    listener = MetricsListener(pg)
    spark.streams.addListener(listener)
    queries = start_gold_queries(spark, SILVER_PATH, GOLD_ROOT, CHECKPOINT_ROOT, args.watermark, args.trigger,
                                 pg, load_zones(spark, ZONES_PATH), max_files_per_trigger=args.max_files_per_trigger)

    try:
        run_until_stopped(queries)
    finally:
        spark.streams.removeListener(listener)    # avoids py4j callback errors during JVM shutdown
        if prom_listener is not None:
            spark.streams.removeListener(prom_listener)
        spark.stop()


if __name__ == "__main__":
    main()
