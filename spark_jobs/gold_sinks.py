"""Idempotent sinks for Gold: Delta MERGE upsert and Postgres INSERT .. ON CONFLICT upsert.

Spark's update-mode aggregation emits the FULL current value of every changed window, so replacing
the stored row is correct, and re-running a batch after a crash produces identical rows (no double
counting, unlike an additive sink).
"""
from __future__ import annotations

import os
from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from spark_jobs.gold_aggregates import GoldSpec


def delta_upsert(spark: SparkSession, df: DataFrame, path: str, spec: GoldSpec) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        w = df.limit(0).withColumn("window_date", F.to_date("window_start")) if spec.partition_by_date \
            else df.limit(0)
        wr = w.write.format("delta")
        (wr.partitionBy("window_date") if spec.partition_by_date else wr).save(path)
    src = df.withColumn("window_date", F.to_date("window_start")) if spec.partition_by_date else df
    cond = " AND ".join(f"t.{k} <=> s.{k}" for k in spec.keys)  # null-safe equality
    if spec.partition_by_date:  # lets Delta prune to the date partitions touched by this batch
        dates = [r[0].isoformat() for r in src.select("window_date").distinct().collect()]
        if not dates:
            return
        cond += " AND t.window_date IN (" + ",".join(f"DATE'{d}'" for d in dates) + ")"
    (DeltaTable.forPath(spark, path).alias("t").merge(src.alias("s"), cond)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


class PgSink:
    """Upserts into gold.<table> using psycopg2 on the driver (aggregated batches are small)."""

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, user: Optional[str] = None,
                 password: Optional[str] = None, dbname: Optional[str] = None):
        self.dsn = dict(host=host or os.getenv("POSTGRES_HOST", "postgres"),
                        port=int(port or os.getenv("POSTGRES_PORT", "5432")),
                        user=user or os.environ["POSTGRES_USER"], password=password or os.environ["POSTGRES_PASSWORD"],
                        dbname=dbname or os.environ["POSTGRES_DB"], options="-c timezone=UTC")

    def connect(self):
        import psycopg2
        return psycopg2.connect(**self.dsn)

    @staticmethod
    def upsert_sql(spec: GoldSpec) -> str:
        cols = spec.columns
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in spec.keys)
        return (f"INSERT INTO gold.{spec.table} ({', '.join(cols)}) VALUES %s "
                f"ON CONFLICT ({', '.join(spec.keys)}) DO UPDATE SET {updates}, updated_at = now()")

    def upsert(self, df: DataFrame, spec: GoldSpec) -> int:
        from psycopg2.extras import execute_values
        rows = [tuple(r[c] for c in spec.columns) for r in df.select(*spec.columns).collect()]
        if not rows:
            return 0
        with self.connect() as conn, conn.cursor() as cur:   # one transaction per batch
            execute_values(cur, self.upsert_sql(spec), rows, page_size=2000)
        return len(rows)

    def insert_metrics(self, rows: List[tuple]) -> None:
        from psycopg2.extras import execute_values
        with self.connect() as conn, conn.cursor() as cur:
            execute_values(cur, "INSERT INTO gold.stream_metrics (query_name, run_id, batch_id, input_rows, "
                                "dropped_by_watermark, state_rows, trigger_ms, processed_rows_per_s) VALUES %s", rows)
