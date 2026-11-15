"""Independent Gold oracle: recompute Gold from the RAW parquet in plain Python (own rules, own windowing,
no Spark, no Silver) and compare with the Postgres serving tables.

  python -m tests.e2e.gold_oracle --n 300000 --dup-every 50 --poison-every 997 --null-every 1009
"""
from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

import psycopg2

from producers.replay_producer import load_unified_table
from tests.e2e.silver_oracle import TLC, judge, nz

PG = dict(host="localhost", port=int(os.getenv("PG_HOST_PORT", "5433")), user=os.getenv("POSTGRES_USER", "mobility"),
          password=os.getenv("POSTGRES_PASSWORD", "mobility_dev"), dbname=os.getenv("POSTGRES_DB", "mobility"),
          options="-c timezone=UTC")


def floor(ts: datetime, minutes: int) -> datetime:
    base = ts.replace(second=0, microsecond=0)
    return base - timedelta(minutes=(base.hour * 60 + base.minute) % minutes)


def expected(n, null_every):
    rows = load_unified_table({t: TLC / f"{t}_tripdata_2024-01.parquet" for t in ("yellow", "green")}).slice(0, n).to_pylist()
    minute = defaultdict(lambda: [0, 0.0, 0.0, 0, 0])  # trips, revenue, fare_sum, dur_sum, dur_n
    sliding, hourly = defaultdict(lambda: [0, 0.0]), defaultdict(lambda: [0, 0.0])
    zone, anomaly = defaultdict(lambda: [0, 0.0]), defaultdict(int)
    valid = 0
    for pos, r in enumerate(rows):
        if null_every and (pos + 1) % null_every == 0:
            continue                      # diverted to DLQ by the producer
        if judge(r)[0]:
            continue                      # quarantined; duplicates are dropped so each event counts once
        valid += 1
        pu, do = r["pickup_ts"], nz(r["dropoff_ts"])
        total, fare, tip, dist = (nz(r[k]) for k in ("total_amount", "fare_amount", "tip_amount", "trip_distance"))
        dur = int((do - pu).total_seconds()) if do else None
        m = minute[(floor(pu, 1), r["taxi_type"])]
        m[0] += 1; m[1] += total or 0.0; m[2] += fare or 0.0
        if dur is not None:
            m[3] += dur; m[4] += 1
        for off in (0, 5, 10):
            s = sliding[floor(pu, 5) - timedelta(minutes=off)]
            s[0] += 1; s[1] += total or 0.0
        h = hourly[(floor(pu, 60), r["taxi_type"])]
        h[0] += 1; h[1] += total or 0.0
        for role, col in (("pickup", "pu_location_id"), ("dropoff", "do_location_id")):
            z = zone[(floor(pu, 5), r[col], role)]
            z[0] += 1; z[1] += total or 0.0
        if dur is not None and dur >= 60 and dist and dist > 0 and dist / (dur / 3600.0) > 80:
            anomaly[(floor(pu, 1), "speed_over_80mph")] += 1
        if dist is not None and fare is not None and dist >= 0.5 and fare / dist > 50:
            anomaly[(floor(pu, 1), "fare_per_mile_over_50")] += 1
        if tip is not None and tip > 100:
            anomaly[(floor(pu, 1), "tip_over_100")] += 1
        if dur is not None and dur > 4 * 3600:
            anomaly[(floor(pu, 1), "duration_over_4h")] += 1
    return valid, minute, sliding, hourly, zone, anomaly


def fetch(cur, sql):
    cur.execute(sql)
    return {tuple(x.replace(tzinfo=None) if isinstance(x, datetime) else x for x in r[:-1]): r[-1] for r in cur.fetchall()}


def close(a, b):
    return abs((a or 0.0) - (b or 0.0)) <= 1e-6 * max(1.0, abs(a or 0.0))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--dup-every", type=int, default=0)       # accepted for symmetry; dups are dropped upstream
    ap.add_argument("--poison-every", type=int, default=0)
    ap.add_argument("--null-every", type=int, default=0)
    a = ap.parse_args()
    valid, minute, sliding, hourly, zone, anomaly = expected(a.n, a.null_every)
    failures = []
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        checks = {
            "trips_per_minute.trips": ({k: v[0] for k, v in minute.items()}, fetch(cur, "SELECT window_start, taxi_type, trips FROM gold.trips_per_minute")),
            "trips_per_minute.revenue": ({k: v[1] for k, v in minute.items()}, fetch(cur, "SELECT window_start, taxi_type, revenue FROM gold.trips_per_minute")),
            "trips_per_minute.fare_sum": ({k: v[2] for k, v in minute.items()}, fetch(cur, "SELECT window_start, taxi_type, fare_sum FROM gold.trips_per_minute")),
            "trips_per_minute.duration_sum_s": ({k: v[3] for k, v in minute.items()}, fetch(cur, "SELECT window_start, taxi_type, COALESCE(duration_sum_s,0) FROM gold.trips_per_minute")),
            "demand_sliding_15m.trips": ({(k,): v[0] for k, v in sliding.items()}, fetch(cur, "SELECT window_start, trips FROM gold.demand_sliding_15m")),
            "demand_sliding_15m.revenue": ({(k,): v[1] for k, v in sliding.items()}, fetch(cur, "SELECT window_start, revenue FROM gold.demand_sliding_15m")),
            "hourly_demand.trips": ({k: v[0] for k, v in hourly.items()}, fetch(cur, "SELECT window_start, taxi_type, trips FROM gold.hourly_demand")),
            "zone_demand_5m.trips": ({k: v[0] for k, v in zone.items()}, fetch(cur, "SELECT window_start, zone_id, role, trips FROM gold.zone_demand_5m")),
            "zone_demand_5m.revenue": ({k: v[1] for k, v in zone.items()}, fetch(cur, "SELECT window_start, zone_id, role, revenue FROM gold.zone_demand_5m")),
            "anomaly_counts.n": (dict(anomaly), fetch(cur, "SELECT window_start, anomaly_type, n FROM gold.anomaly_counts")),
        }
    for name, (exp, act) in checks.items():
        bad = [k for k in set(exp) | set(act) if not close(exp.get(k), act.get(k))]
        print(f"{'OK  ' if not bad else 'FAIL'} {name:34s} expected_rows={len(exp):6d} postgres_rows={len(act):6d} bad={len(bad)}")
        if bad:
            failures.append((name, bad[:3]))
    tot = sum(v[0] for v in minute.values())
    print(f"valid distinct events expected: {valid}  (sum of per-minute trips: {tot})")
    print("GOLD_ORACLE_RESULT", "PASS" if not failures else "FAIL", failures if failures else "")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
