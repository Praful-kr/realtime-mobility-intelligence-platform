"""Observability verification. Everything is checked against the RUNNING stack, nothing is assumed:

 1. Prometheus: required targets are up, alert rules loaded and healthy.
 2. Grafana: datasources healthy, 3 dashboards provisioned.
 3. EVERY panel target of EVERY dashboard is executed through Grafana's own query API (/api/ds/query, so macros,
    datasource provisioning and credentials are all exercised) and must return rows, unless the panel says [optional].
 4. Metric VALUES are cross-checked against ground truth (producer's own final counts, Kafka topic offsets).

  python -m tests.e2e.observability_check [--expect-sent N --expect-dlq D] [--expect-spark-up]
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

import httpx

failures: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> bool:
    print(f"{'OK  ' if ok else 'FAIL'} {name} {detail}")
    if not ok:
        failures.append(name)
    return ok


# NOTE: the producer is a short-lived host process; once it exits Prometheus marks its series stale, so its final
# counters must be read with *_over_time, not an instant query.
def prom_value(p: httpx.Client, expr: str):
    r = p.get("/api/v1/query", params={"query": expr}).json()["data"]["result"]
    return float(r[0]["value"][1]) if r else None


def iso_ms(s: str) -> int:
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp() * 1000)


def rows_in(result: dict) -> int:
    n = 0
    for f in result.get("frames", []):
        vals = f.get("data", {}).get("values", [])
        n += len(vals[0]) if vals else 0
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", default="http://localhost:9090")
    ap.add_argument("--grafana", default="http://localhost:3000")
    ap.add_argument("--auth", default="admin:admin")
    ap.add_argument("--expect-sent", type=int)
    ap.add_argument("--expect-dlq", type=int)
    ap.add_argument("--expect-spark-up", action="store_true")
    ap.add_argument("--lookback-min", type=int, default=90)
    a = ap.parse_args()
    user, pw = a.auth.split(":")
    p = httpx.Client(base_url=a.prom, timeout=30)
    g = httpx.Client(base_url=a.grafana, auth=(user, pw), timeout=60)

    # ---- 1. Prometheus ---------------------------------------------------------------------------------------
    targets = {t["labels"]["job"]: t["health"] for t in p.get("/api/v1/targets").json()["data"]["activeTargets"]}
    need = {"api", "kafka", "prometheus"} | ({"spark-bronze", "spark-silver", "spark-gold"} if a.expect_spark_up else set())
    check("prometheus targets up", all(targets.get(j) == "up" for j in need), str({j: targets.get(j) for j in sorted(need)}))
    rules = [r for grp in p.get("/api/v1/rules").json()["data"]["groups"] for r in grp["rules"]]
    check("alert rules loaded and healthy", len(rules) >= 8 and all(r["health"] == "ok" for r in rules), f"{len(rules)} rules")

    # ---- 2. Grafana ------------------------------------------------------------------------------------------
    check("grafana health", g.get("/api/health").json().get("database") == "ok")
    for uid in ("prometheus", "postgres"):
        h = g.get(f"/api/datasources/uid/{uid}/health").json()
        check(f"datasource {uid}", h.get("status") == "OK", h.get("message", ""))
    boards = {d["uid"]: d["title"] for d in g.get("/api/search", params={"tag": "mobility"}).json()}
    check("3 dashboards provisioned", set(boards) == {"mobility-pipeline", "mobility-api", "mobility-business"}, str(sorted(boards.values())))

    # ---- 3. every panel target, executed through Grafana -----------------------------------------------------
    now_ms = int(time.time() * 1000)
    total = empty = 0
    for uid in sorted(boards):
        dash = g.get(f"/api/dashboards/uid/{uid}").json()["dashboard"]
        t = dash["time"]
        if t["from"].startswith("now"):
            frm, to = now_ms - a.lookback_min * 60_000, now_ms
        else:
            frm, to = iso_ms(t["from"]), iso_ms(t["to"])
        for panel in [x for x in dash["panels"] if x["type"] != "row"]:
            optional = "[optional]" in panel.get("description", "")
            for tgt in panel["targets"]:
                q = {**tgt, "intervalMs": 5000, "maxDataPoints": 400}
                body = g.post("/api/ds/query", json={"queries": [q], "from": str(frm), "to": str(to)})
                total += 1
                res = body.json().get("results", {}).get(tgt["refId"], {}) if body.status_code == 200 else {}
                err = res.get("error") or (body.text[:150] if body.status_code != 200 else "")
                n = rows_in(res)
                ok = not err and (n > 0 or optional)
                if not ok:
                    empty += 1
                check(f"[{uid.replace('mobility-', '')}] {panel['title'][:46]:46s} ({tgt['refId']})", ok, err or f"rows={n}")
    check(f"all {total} panel queries returned data", empty == 0, f"{empty} failing")

    # ---- 4. values vs ground truth --------------------------------------------------------------------------
    if a.expect_sent is not None:
        sent = a.expect_sent
        check("producer sent == expected", prom_value(p, "max(max_over_time(producer_events_sent_total[3h]))") == sent)
        check("producer acked == sent (acks=all)", prom_value(p, "max(max_over_time(producer_events_acked_total[3h]))") == sent)
        check("producer failed == 0", (prom_value(p, "max(max_over_time(producer_events_failed_total[3h]))") or 0) == 0)
        check("Kafka raw topic end offsets == sent", prom_value(p, 'sum(kafka_topic_partition_current_offset{topic="trips.raw.v1"})') == sent)
        check("Bronze input rows == sent", prom_value(p, 'sum(spark_input_rows_total{query="bronze-trips-raw"})') == sent)
        # sum(): aggregate across jobs, never trust "the first series"; and refuse a vacuous 0 == 0 pass
        rec = prom_value(p, 'sum(spark_silver_rows_total{outcome="received"})')
        parts = sum(prom_value(p, f'sum(spark_silver_rows_total{{outcome="{o}"}})') or 0 for o in ("valid", "quarantined", "duplicate"))
        check("Silver received == sent", rec == sent, f"received={rec}")
        check("Silver valid + quarantined + duplicate == received (non-vacuous)", rec > 0 and parts == rec, f"{parts} of {rec}")
        true_lag = prom_value(p, 'sum(kafka_topic_partition_current_offset{topic="trips.raw.v1"}) - sum(spark_kafka_processed_offset_sum{topic="trips.raw.v1"})')
        check("TRUE Kafka lag (log-end - processed) drained to 0", true_lag == 0, f"lag={true_lag}")
        check("Silver/Gold delta version lag == 0", (prom_value(p, "sum(spark_delta_version_lag)") or 0) == 0)
        lat = prom_value(p, 'spark_silver_latency_seconds{stat="max"}')
        check("processing latency reported", lat is not None and lat >= 0, f"last-batch max={lat}s")
    if a.expect_dlq is not None:
        check("DLQ topic offsets == expected diverted events",
              prom_value(p, 'sum(kafka_topic_partition_current_offset{topic="trips.dlq.v1"})') == a.expect_dlq)

    print("OBSERVABILITY_RESULT", "PASS" if not failures else f"FAIL ({len(failures)}): {failures[:5]}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
