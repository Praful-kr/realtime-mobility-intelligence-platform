"""Prints SILVER_REPORT {json}: what is actually in Silver/quarantine/dq_metrics (compared to an oracle by tests/e2e)."""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from spark_jobs.common import BRONZE_PATH, DQ_METRICS_PATH, QUARANTINE_PATH, SILVER_PATH, build_spark


def main() -> None:
    spark = build_spark("verify-silver")
    rd = lambda p: spark.read.format("delta").load(p)  # noqa: E731
    silver, quar, dq, bronze = rd(SILVER_PATH), rd(QUARANTINE_PATH), rd(DQ_METRICS_PATH), rd(BRONZE_PATH)

    def counts(df, col):
        return {r["rule"]: r["n"] for r in df.select(F.explode(col).alias("rule")).groupBy("rule")
                .agg(F.count("*").alias("n")).collect()}

    m = dq.agg(F.sum("bronze_rows"), F.sum("quarantined"), F.sum("dup_in_batch"), F.sum("dup_vs_silver"),
               F.sum("silver_inserted"), F.count("*")).first()
    report = {
        "bronze_rows": bronze.count(),
        "silver_rows": silver.count(),
        "silver_distinct_event_ids": silver.select("event_id").distinct().count(),
        "quarantine_rows": quar.count(),
        "quarantine_error_counts": counts(quar, "errors"),
        "silver_warning_counts": counts(silver, "warnings"),
        "silver_invalid_dates": silver.where("event_date IS NULL OR event_time IS NULL").count(),
        "silver_min_event_time": str(silver.agg(F.min("event_time")).first()[0]),
        "silver_avg_dq_score": silver.agg(F.avg("dq_score")).first()[0],
        "metrics": {"batches": m[5], "bronze_rows": m[0], "quarantined": m[1],
                    "dup_dropped": (m[2] or 0) + (m[3] or 0), "silver_inserted": m[4]},
    }
    print("SILVER_REPORT " + json.dumps(report, default=str))


if __name__ == "__main__":
    main()
