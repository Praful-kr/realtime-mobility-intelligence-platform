"""E2E: query the RUNNING API and compare every answer with the independent raw-parquet oracle
(tests/e2e/gold_oracle.expected: plain Python, no Spark, no Silver, no Postgres).

  python -m tests.e2e.api_check --n 300000 --null-every 1009 [--base http://localhost:8000]
"""
from __future__ import annotations

import argparse
import statistics
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import httpx

from tests.e2e.gold_oracle import expected

FULL = {"start": "2023-12-01T00:00:00Z", "end": "2024-03-01T00:00:00Z"}
failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'OK  ' if ok else 'FAIL'} {name} {detail}")
    if not ok:
        failures.append(name)


def ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def close(a, b) -> bool:
    return abs((a or 0.0) - (b or 0.0)) <= 1e-6 * max(1.0, abs(a or 0.0))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--null-every", type=int, default=0)
    ap.add_argument("--base", default="http://localhost:8000")
    a = ap.parse_args()
    valid, minute, sliding, hourly, zone, anomaly = expected(a.n, a.null_every)
    c = httpx.Client(base_url=a.base, timeout=30)

    r = c.get("/health/ready")
    check("health/ready", r.status_code == 200, r.text)
    check("health/live", c.get("/health/live").status_code == 200)

    rows = c.get("/v1/trips/per-minute", params={**FULL, "limit": 20000}).json()["data"]
    got = {(ts(x["window_start"]), x["taxi_type"]): x for x in rows}
    check("per-minute: row count", len(got) == len(minute), f"api={len(got)} oracle={len(minute)}")
    check("per-minute: every trips value", all(got.get(k, {}).get("trips") == v[0] for k, v in minute.items()))
    check("per-minute: every revenue value", all(close(got.get(k, {}).get("revenue"), v[1]) for k, v in minute.items()))
    check("per-minute: SUM(trips) == valid distinct events", sum(x["trips"] for x in rows) == valid, f"{valid}")
    yl = c.get("/v1/trips/per-minute", params={**FULL, "taxi_type": "green", "limit": 20000}).json()["data"]
    check("per-minute: taxi_type filter", {x["taxi_type"] for x in yl} == {"green"}
          and sum(x["trips"] for x in yl) == sum(v[0] for k, v in minute.items() if k[1] == "green"))

    sl = c.get("/v1/demand/sliding", params={**FULL, "limit": 20000}).json()["data"]
    check("sliding: all windows", {ts(x["window_start"]): x["trips"] for x in sl} == {k: v[0] for k, v in sliding.items()})

    hod = c.get("/v1/demand/by-hour-of-day", params={"taxi_type": "yellow"}).json()["data"]
    exp_hod: dict = defaultdict(int)
    for (ws, tt), v in hourly.items():
        if tt == "yellow":
            exp_hod[ws.hour] += v[0]
    check("hour-of-day: yellow trips per hour", {x["hour_of_day"]: x["trips"] for x in hod} == dict(exp_hod))

    exp_zone: dict = defaultdict(lambda: [0, 0.0])
    for (ws, zid, role), v in zone.items():
        if role == "pickup":
            exp_zone[zid][0] += v[0]
            exp_zone[zid][1] += v[1]
    top_exp = sorted(exp_zone.items(), key=lambda kv: (-kv[1][0], kv[0]))[:10]
    top = c.get("/v1/zones/top", params={**FULL, "role": "pickup", "limit": 10}).json()["data"]
    check("zones/top: top-10 pickup zones (ids, trips, revenue)",
          [(z["zone_id"], z["trips"]) for z in top] == [(k, v[0]) for k, v in top_exp]
          and all(close(z["revenue"], v[1]) for z, (_, v) in zip(top, top_exp)),
          f"#1 = {top[0]['zone_name']} ({top[0]['trips']} trips)" if top else "")
    z0 = top_exp[0][0]
    trend = c.get(f"/v1/zones/{z0}/trend", params={**FULL, "limit": 20000}).json()["data"]
    check("zones/{id}/trend", {ts(p["window_start"]): p["trips"] for p in trend}
          == {ws: v[0] for (ws, zid, role), v in zone.items() if zid == z0 and role == "pickup"})

    ac = c.get("/v1/anomalies/counts", params={**FULL, "limit": 20000}).json()["data"]
    check("anomalies/counts: every (minute,type)", {(ts(x["window_start"]), x["anomaly_type"]): x["n"] for x in ac}
          == dict(anomaly))
    d = c.get("/v1/anomalies/demand").json()["data"]
    zs = [abs(x["z_score"]) for x in d]
    check("anomalies/demand: |z|>3 and sorted desc", bool(d) and min(zs) > 3 and zs == sorted(zs, reverse=True),
          f"{len(d)} rows, top z={zs[0]:.1f} at {d[0]['window_start']}" if d else "no rows")

    ps = c.get("/v1/pipeline/status").json()
    check("pipeline/status: 5 queries reporting", len(ps["queries"]) == 5, str([q["query_name"] for q in ps["queries"]]))

    # ---- concurrent load smoke (correctness under concurrency, not a benchmark) ------------------------------
    paths = ["/v1/trips/per-minute?limit=500", "/v1/zones/top?limit=10", "/v1/demand/by-hour-of-day",
             "/v1/demand/sliding?limit=200", "/v1/anomalies/counts?limit=200", f"/v1/zones/{z0}/trend?limit=200",
             "/v1/pipeline/status", "/health/ready"]
    lat: list[float] = []

    def hit(i: int):
        t0 = time.perf_counter()
        resp = httpx.get(a.base + paths[i % len(paths)], timeout=30)
        lat.append(time.perf_counter() - t0)
        return resp.status_code

    with ThreadPoolExecutor(max_workers=20) as ex:
        codes = list(ex.map(hit, range(400)))
    lat.sort()
    check("load smoke: 400 requests / 20 threads all 200", set(codes) == {200},
          f"p50={statistics.median(lat) * 1000:.0f}ms p95={lat[int(len(lat) * .95)] * 1000:.0f}ms max={lat[-1] * 1000:.0f}ms")
    print("API_CHECK_RESULT", "PASS" if not failures else f"FAIL {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
