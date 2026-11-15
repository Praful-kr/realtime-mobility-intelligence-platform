"""Markdown data-quality report from the lakehouse tables (stdout). `make dq-report > docs/dq_report.md`"""
from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import functions as F

from spark_jobs.common import DQ_METRICS_PATH, QUARANTINE_PATH, SILVER_PATH, build_spark
from spark_jobs.dq_rules import RULE_SPECS


def main() -> None:
    spark = build_spark("dq-report")
    rd = lambda p: spark.read.format("delta").load(p)  # noqa: E731
    silver, quar, dq = rd(SILVER_PATH), rd(QUARANTINE_PATH), rd(DQ_METRICS_PATH)
    t = dq.agg(F.sum("bronze_rows").alias("rows"), F.sum("quarantined").alias("q"), F.sum("undecodable").alias("u"),
               F.sum("dup_in_batch").alias("d1"), F.sum("dup_vs_silver").alias("d2"),
               F.sum("silver_inserted").alias("ins"), F.count("*").alias("batches")).first()
    rows, q, ins = int(t["rows"]), int(t["q"]), int(t["ins"])
    dups = int(t["d1"] + t["d2"])
    avg = silver.agg(F.avg("dq_score")).first()[0] or 0.0
    perfect = silver.where("dq_score = 1.0").count()
    spec = {r.name: r for r in RULE_SPECS}

    def table(df, col, header):
        out = [f"| Rule | Category | Rows |", "|---|---|---:|"]
        for r in df.select(F.explode(col).alias("rule")).groupBy("rule").agg(F.count("*").alias("n")) \
                   .orderBy(F.desc("n")).collect():
            out.append(f"| `{r['rule']}` | {spec[r['rule']].category} | {r['n']:,} |")
        return [f"### {header}", ""] + out + [""]

    lines = [
        "# Data Quality Report", "",
        f"_Generated {datetime.now(timezone.utc).isoformat(timespec='seconds')} from Delta tables "
        f"(silver, quarantine, dq_metrics); {int(t['batches'])} micro-batches._", "",
        "## Summary", "",
        "| Metric | Value |", "|---|---:|",
        f"| Records received (Bronze) | {rows:,} |",
        f"| Quarantined (ERROR rules / undecodable) | {q:,} ({q / rows:.2%}) |",
        f"|   of which undecodable | {int(t['u']):,} |",
        f"| Duplicate deliveries dropped | {dups:,} ({dups / rows:.2%}) |",
        f"| Accepted into Silver (distinct events) | {ins:,} |",
        f"| Silver rows with zero warnings (score 1.0) | {perfect:,} ({perfect / max(ins, 1):.2%}) |",
        f"| Mean Silver DQ score | {avg:.4f} |",
        f"| Reconciliation: received = quarantined + dropped dups + inserted | "
        f"{'OK' if rows == q + dups + ins else 'MISMATCH'} |", "",
    ]
    lines += table(quar, "errors", "Quarantine reasons (a record can hit several)")
    lines += table(silver, "warnings", "Warnings on accepted Silver rows (reduce DQ score by 0.1 each)")
    lines += ["## Rule catalog", "", "| Rule | Severity | Category | Description |", "|---|---|---|---|"]
    lines += [f"| `{r.name}` | {r.severity} | {r.category} | {r.description} |" for r in RULE_SPECS]
    print("\n".join(lines))


if __name__ == "__main__":
    main()
