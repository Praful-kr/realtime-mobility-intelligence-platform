"""Gold verification against a plain batch recomputation from Silver (same builders, no watermark/state).

Default (strict): Delta == batch AND Postgres == batch for every table, floats within 1e-6 relative.
--live (late data present, watermark may legitimately have dropped rows): requires
  * Delta == Postgres exactly,
  * no window is invented or over-counted relative to the batch recompute,
  * loss is VISIBLE: deficit(batch total - delta total) = events lost to the watermark; Spark's
    numRowsDroppedByWatermark counts aggregated groups (a lower bound, pinned by tests/spark), so require
    deficit == 0 <=> dropped == 0, and dropped <= deficit.
Prints GOLD_REPORT {json}; exit 0 only on PASS.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from spark_jobs.common import SILVER_PATH, build_spark
from spark_jobs.gold_aggregates import GOLD_SPECS, SILVER_NEEDED
from spark_jobs.gold_sinks import PgSink
from spark_jobs.gold_stream import GOLD_ROOT, ZONES_PATH, load_zones


def norm(v):
    if isinstance(v, datetime):
        return (v.astimezone(timezone.utc) if v.tzinfo else v).replace(tzinfo=None)
    return v


def same(a, b) -> bool:
    if isinstance(a, float) or isinstance(b, float):
        return a is not None and b is not None and abs(a - b) <= 1e-6 * max(1.0, abs(a))
    return a == b


def diff(expected: dict, actual: dict, cols) -> dict:
    missing = [k for k in expected if k not in actual]
    extra = [k for k in actual if k not in expected]
    bad = [k for k in expected if k in actual and not all(same(expected[k][c], actual[k][c]) for c in cols)]
    return {"missing": len(missing), "extra": len(extra), "mismatch": len(bad), "rows": len(actual),
            "sample": [str(k) for k in (missing + extra + bad)[:3]]}


def clean(d: dict) -> bool:
    return d["missing"] == d["extra"] == d["mismatch"] == 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()
    spark = build_spark("gold-verify")
    silver = spark.read.format("delta").load(SILVER_PATH).select(*SILVER_NEEDED)
    zones, pg = load_zones(spark, ZONES_PATH), PgSink()
    report, ok = {"silver_rows": silver.count(), "mode": "live" if args.live else "strict", "tables": {}}, True
    for spec in GOLD_SPECS:
        val_cols = [c for c in spec.columns if c not in spec.keys]
        measure = "n" if "n" in spec.columns else "trips"
        key = lambda r: tuple(norm(r[k]) for k in spec.keys)  # noqa: E731
        norm_row = lambda r: {c: norm(r[c]) for c in spec.columns}  # noqa: E731
        expected = {key(r): norm_row(r) for r in spec.finalize(spec.build(silver, None), zones).collect()}
        delta = {key(r): norm_row(r) for r in spark.read.format("delta").load(f"{GOLD_ROOT}/{spec.table}")
                 .select(*spec.columns).collect()}
        with pg.connect() as conn, conn.cursor() as cur:
            cur.execute(f"SELECT {', '.join(spec.columns)} FROM gold.{spec.table}")
            pgrows = {key(r): norm_row(r) for r in (dict(zip(spec.columns, t)) for t in cur.fetchall())}
            cur.execute("SELECT COALESCE(SUM(dropped_by_watermark), 0) FROM gold.stream_metrics WHERE query_name = %s",
                        (f"gold_{spec.name}",))
            dropped = int(cur.fetchone()[0])
        t = {"expected_rows": len(expected), "delta_rows": len(delta), "postgres_rows": len(pgrows),
             "delta_vs_postgres": diff(delta, pgrows, val_cols)}
        if args.live:
            over = [k for k in delta if k not in expected or delta[k][measure] > expected[k][measure]]
            deficit = sum(r[measure] for r in expected.values()) - sum(r[measure] for r in delta.values())
            t.update({"windows_overcounted_or_invented": len(over), "events_lost_to_watermark": deficit,
                      "dropped_groups_reported": dropped})
            visible = (deficit == 0 and dropped == 0) or (deficit > 0 and 0 < dropped <= deficit)
            good = clean(t["delta_vs_postgres"]) and not over and visible
        else:
            t.update({"delta_vs_batch": diff(expected, delta, val_cols),
                      "postgres_vs_batch": diff(expected, pgrows, val_cols), "dropped_by_watermark": dropped})
            good = clean(t["delta_vs_batch"]) and clean(t["postgres_vs_batch"]) and dropped == 0
        t["ok"] = good
        ok &= good
        report["tables"][spec.name] = t
    print("GOLD_REPORT " + json.dumps(report, default=str))
    print("GOLD_RESULT " + ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
