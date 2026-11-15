"""Independent oracle for Silver. Re-implements the DQ rules in plain Python (NOT sharing code with
spark_jobs/dq_rules.py) over the same source rows and the same position-based fault injection, and
predicts every count. e2e passes only if Spark's lakehouse tables match the prediction exactly.

  python -m tests.e2e.silver_oracle --n 300000 --dup-every 50 --poison-every 997 --null-every 1009 \
         --report data/silver_report.json --dlq-count 297
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

from producers.replay_producer import load_unified_table

MIN_TS = datetime(2023, 1, 1)
TLC = Path("data/tlc")


def nz(v):
    return None if isinstance(v, float) and math.isnan(v) else v


def judge(r: dict) -> tuple[list[str], list[str]]:
    g = lambda k: nz(r.get(k))  # noqa: E731
    pu, do, fare, tot, dist, pax = g("pickup_ts"), g("dropoff_ts"), g("fare_amount"), g("total_amount"), g("trip_distance"), g("passenger_count")
    e: list[str] = []
    w: list[str] = []
    if pu < MIN_TS:
        e.append("pickup_out_of_range")
    if do is not None and do < pu:
        e.append("dropoff_before_pickup")
    if do is not None and (do - pu).total_seconds() > 86400:
        e.append("trip_too_long")
    if fare is None:
        e.append("null_fare")
    elif fare < 0:
        e.append("negative_fare")
    elif fare > 1000:
        e.append("implausible_fare")
    if tot is not None and tot < 0:
        e.append("negative_total")
    if dist is not None and dist < 0:
        e.append("negative_distance")
    elif dist is not None and dist > 200:
        e.append("implausible_distance")
    for col, name in (("pu_location_id", "invalid_pickup_zone"), ("do_location_id", "invalid_dropoff_zone")):
        z = g(col)
        if z is None or not 1 <= z <= 265:
            e.append(name)
    if pax is not None and not 0 <= pax <= 9:
        e.append("invalid_passenger_count")
    if do is None:
        w.append("missing_dropoff")
    if pax is None:
        w.append("null_passenger_count")
    elif pax == 0:
        w.append("zero_passenger_count")
    if dist == 0:
        w.append("zero_distance")
    if g("payment_type") is None:
        w.append("null_payment_type")
    if tot is not None and fare is not None and tot < fare - 0.01:
        w.append("total_less_than_fare")
    return e, w


def expected(n: int, dup_every: int, poison_every: int, null_every: int) -> dict:
    rows = load_unified_table({t: TLC / f"{t}_tripdata_2024-01.parquet" for t in ("yellow", "green")}).slice(0, n).to_pylist()
    hit = lambda pos, k: bool(k) and (pos + 1) % k == 0  # noqa: E731
    msgs = quarantined = silver = dup_dropped = dlq = poison = 0
    err: Counter = Counter()
    warn: Counter = Counter()
    for pos, r in enumerate(rows):
        if hit(pos, null_every):
            dlq += 1
        else:
            copies = 2 if hit(pos, dup_every) else 1
            msgs += copies
            e, w = judge(r)
            if e:
                quarantined += copies            # quarantine keeps every rejected delivery
                for rule in e:
                    err[rule] += copies
            else:
                silver += 1
                dup_dropped += copies - 1
                warn.update(w)                   # warnings counted on distinct Silver rows
        if hit(pos, poison_every):
            msgs += 1
            poison += 1
            quarantined += 1
            err["undecodable_payload"] += 1
    return {"bronze_rows": msgs, "silver_rows": silver, "silver_distinct_event_ids": silver,
            "quarantine_rows": quarantined, "quarantine_error_counts": dict(err),
            "silver_warning_counts": dict(warn), "dlq": dlq, "dup_dropped": dup_dropped}


def compare(exp: dict, act: dict, dlq_actual: int | None) -> list[str]:
    bad = []
    for k in ("bronze_rows", "silver_rows", "silver_distinct_event_ids", "quarantine_rows",
              "quarantine_error_counts", "silver_warning_counts"):
        if exp[k] != act[k]:
            bad.append(f"{k}: expected {exp[k]} actual {act[k]}")
    if exp["dup_dropped"] != act["metrics"]["dup_dropped"]:
        bad.append(f"dup_dropped: expected {exp['dup_dropped']} actual {act['metrics']['dup_dropped']}")
    if act["silver_invalid_dates"]:
        bad.append(f"silver has {act['silver_invalid_dates']} rows with null event date/time")
    if dlq_actual is not None and dlq_actual != exp["dlq"]:
        bad.append(f"dlq: expected {exp['dlq']} actual {dlq_actual}")
    return bad


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--dup-every", type=int, default=0)
    ap.add_argument("--poison-every", type=int, default=0)
    ap.add_argument("--null-every", type=int, default=0)
    ap.add_argument("--report", required=True)
    ap.add_argument("--dlq-count", type=int)
    a = ap.parse_args()
    exp = expected(a.n, a.dup_every, a.poison_every, a.null_every)
    act = json.loads(Path(a.report).read_text())
    print("EXPECTED", json.dumps(exp, sort_keys=True))
    print("ACTUAL  ", json.dumps({k: act[k] for k in exp if k in act}, sort_keys=True))
    bad = compare(exp, act, a.dlq_count)
    print("ORACLE_RESULT", "PASS" if not bad else "FAIL")
    for b in bad:
        print(" -", b)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
