"""Reliability drills. Each drill injects a REAL failure into the running stack, then checks the outcome against the
independent oracles / Kafka ground truth. Output: PASS/FAIL per check + data/drills/<name>.json evidence.

  python -m tests.recovery.drills producer | graceful | crash | broker | sigkill | rebuild
"""
from __future__ import annotations

import json
import os
import re
import signal
import sys
import time

from tests.recovery import harness as h
from tests.recovery.harness import CONTAINER, dc, log, sh

N, DUP, POISON, NULLP = 100000, 50, 997, 1009
INJECT = f"--inject-dup-every {DUP} --inject-poison-every {POISON} --inject-null-pickup-every {NULLP}"


def pg_total_trips() -> int:
    r = sh("docker exec mobility-postgres-1 psql -U mobility -d mobility -Atc "
           "\"select coalesce(sum(trips),0) from gold.trips_per_minute\"")
    return int(r.stdout.strip())


def wait_quiescent(expected_trips: int, timeout: float = 900) -> dict:
    """Gold total == expected (stable 3 polls), or unchanged for 6 minutes (then we report what we got)."""
    end, prev, same, hit = time.time() + timeout, -1, 0, 0
    while time.time() < end:
        cur, lag = pg_total_trips(), h.prom(h.TRUE_LAG)
        hit = hit + 1 if (cur == expected_trips and (lag or 0) == 0) else 0
        same = same + 1 if cur == prev else 0
        prev = cur
        if hit >= 3:
            return {"reached_expected": True, "gold_trips": cur}
        if same >= 36 and (lag or 0) == 0:   # Gold catch-up batches take up to ~100 s each after restarts
            return {"reached_expected": False, "gold_trips": cur}
        time.sleep(10)
    return {"reached_expected": False, "gold_trips": prev, "timed_out": True}


# =========================================================================================================
def drill_producer() -> bool:
    ev = {}
    log("backpressure: tiny local queue, unthrottled producer")
    sh("bash infrastructure/scripts/reset_data.sh")
    p = h.start_producer("producer_backpressure.log", 0, 60000, "--max-queue 500", metrics=False)
    rc = p.wait(timeout=300)
    s = h.producer_summary("producer_backpressure.log")
    ev["backpressure"] = {**s, "exit_code": rc, "kafka_raw_end_offsets": h.topic_end_sum("trips.raw.v1")}
    log("graceful producer shutdown: SIGTERM mid-replay")
    sh("bash infrastructure/scripts/reset_data.sh")
    p = h.start_producer("producer_sigterm.log", 100, 200000, metrics=False)
    time.sleep(25)
    os.killpg(p.pid, signal.SIGTERM)
    rc2 = p.wait(timeout=120)
    s2 = h.producer_summary("producer_sigterm.log")
    ev["sigterm"] = {**s2, "exit_code": rc2, "kafka_raw_end_offsets": h.topic_end_sum("trips.raw.v1")}
    h.save("producer", ev)
    b, g = ev["backpressure"], ev["sigterm"]
    return h.verdict("producer", {
        f"backpressure engaged ({b['backpressure_events']} waits on a full queue) and nothing was lost": b["backpressure_events"] > 0 and b["sent"] == b["acked"] == b["kafka_raw_end_offsets"] == 60000,
        "backpressure run: failed == 0, exit 0": b["failed"] == 0 and b["exit_code"] == 0,
        f"SIGTERM: stopped early ({g['sent']} of 200000), flushed, interrupted=True": g["interrupted"] is True and g["sent"] < 200000 and g["unflushed"] == 0,
        "SIGTERM: every sent message acked and present in Kafka (sent == acked == end offsets)": g["sent"] == g["acked"] == g["kafka_raw_end_offsets"] and g["failed"] == 0,
        "SIGTERM: process exited 0": g["exit_code"] == 0})


# =========================================================================================================
def drill_graceful() -> bool:
    n = 200000
    ev: dict = {}
    h.prepare(streaming=True)
    p = h.start_producer("graceful_producer.log", 0, n, metrics=False)
    p.wait(timeout=300)
    time.sleep(8)                       # Bronze has data, Silver is working through its first batch(es)
    t0 = time.time()
    r = sh(f"docker stop -t 60 {CONTAINER['spark-silver']}", timeout=90)
    ev["silver_stop_seconds"] = round(time.time() - t0, 1)
    ev["silver_exit_code"] = h.exit_code(CONTAINER["spark-silver"])
    lg = h.logs(CONTAINER["spark-silver"])
    pos = {k: lg.rfind(k) for k in ("SHUTDOWN_REQUESTED", "SHUTDOWN_STOPPING_QUERIES", "SHUTDOWN_COMPLETE")}
    batch_lines = [m.start() for m in re.finditer("SILVER_BATCH", lg)]
    ev["log_markers"] = pos
    ev["batch_finished_after_stop_request"] = any(b > pos["SHUTDOWN_REQUESTED"] >= 0 for b in batch_lines)
    log(f"silver stopped in {ev['silver_stop_seconds']}s, exit={ev['silver_exit_code']}, "
        f"batch finished after request: {ev['batch_finished_after_stop_request']}")
    dc("--profile streaming start spark-silver")
    ok_resume = h.wait_until(lambda: h.silver_rows_in_logs() >= n, 420, 5, f"silver processed {n} rows")
    ev["silver_rows_processed_total"] = h.silver_rows_in_logs()
    expected_valid = __import__("tests.e2e.silver_oracle", fromlist=["x"]).expected(n, 0, 0, 0)["silver_rows"]
    q = wait_quiescent(expected_valid)
    ev["gold_quiescence"] = q
    stops = {}
    for svc in h.STREAMING + ("api",):
        t = time.time()
        sh(f"docker stop -t 60 {CONTAINER[svc]}", timeout=90)
        stops[svc] = {"seconds": round(time.time() - t, 1), "exit_code": h.exit_code(CONTAINER[svc])}
    ev["final_stops"] = stops
    dc("up -d --wait api")
    ev["silver_check"] = h.silver_check(n)
    ev["gold_check"] = h.gold_check(n)
    h.save("graceful", ev)
    return h.verdict("graceful", {
        "docker stop -> exit code 0 (was 143 before the fix: handler never ran)": ev["silver_exit_code"] == 0,
        "log shows SHUTDOWN_REQUESTED then SHUTDOWN_COMPLETE": 0 <= pos["SHUTDOWN_REQUESTED"] < pos["SHUTDOWN_COMPLETE"],
        "stop completed inside the 60 s grace period (it waited for the in-flight batch)": ev["silver_stop_seconds"] < 60,
        "an in-flight batch FINISHED after the stop request (graceful, not interrupted)": ev["batch_finished_after_stop_request"],
        "restarted Silver resumed from its checkpoint and processed every Bronze row": ok_resume,
        "no row processed twice (total rows over both runs == produced)": ev["silver_rows_processed_total"] == n,
        "Silver == independent oracle after stop/restart": ev["silver_check"]["ok"],
        "Gold == batch recompute and independent oracle": ev["gold_check"]["strict_batch_recompute"] and ev["gold_check"]["independent_oracle"],
        "all of bronze/silver/gold/api stop with exit 0": all(v["exit_code"] == 0 for v in stops.values())})


# =========================================================================================================
def _bronze_run(chaos_env: str) -> tuple:
    cmd = (f"--profile jobs run --rm {chaos_env} spark-job bronze_stream --trigger availableNow "
           f"--max-offsets-per-trigger 20000")
    r = dc(cmd, check=False, timeout=600)
    return r.returncode, r.stdout + r.stderr


def _verify_bronze() -> dict:
    out = sh("make -s verify-bronze", check=False, timeout=400).stdout
    rep = re.search(r"VERIFY_REPORT (.*)", out)
    return {**(json.loads(rep.group(1)) if rep else {}), "passed": "VERIFY_RESULT PASS" in out}


def drill_crash() -> bool:
    ev: dict = {}
    for label, extra in (("idempotent", ""), ("control_no_txn", "-e BRONZE_DISABLE_TXN=1")):
        log(f"crash-after-write drill: {label}")
        h.prepare(streaming=False)
        p = h.start_producer(f"crash_{label}_producer.log", 0, N, metrics=False)
        p.wait(timeout=300)
        sent = h.producer_summary(f"crash_{label}_producer.log")["sent"]
        rc1, out1 = _bronze_run(f"-e CHAOS_CRASH_AFTER_WRITE=bronze:1 {extra}")
        rc2, out2 = _bronze_run(f"-e CHAOS_CRASH_AFTER_WRITE=bronze:1 {extra}")
        ev[label] = {"produced": sent, "run1_exit": rc1, "run1_crashed_after_write": "CHAOS_CRASH" in out1,
                     "run2_exit": rc2, "run2_skipped_chaos": "CHAOS_SKIP" in out2, "verify": _verify_bronze()}
    h.save("crash", ev)
    a, c = ev["idempotent"], ev["control_no_txn"]
    return h.verdict("crash-after-write", {
        "run 1 died after the sink write, before the offset commit (exit 137)": a["run1_crashed_after_write"] and a["run1_exit"] == 137,
        "run 2 resumed from the checkpoint and completed": a["run2_exit"] == 0 and a["run2_skipped_chaos"],
        f"WITH idempotent writes: Bronze rows == Kafka rows == {a['produced']}, no duplicate offsets or event ids": a["verify"]["passed"] and a["verify"].get("bronze_rows") == a["produced"],
        "CONTROL (idempotence off): the same crash DOES create duplicates (test can detect them)": (not c["verify"]["passed"]) and c["verify"].get("bronze_rows", 0) > c["produced"],
        "control duplicates == exactly ONE replayed micro-batch (that batch size appears twice in Bronze's commit history)": (
            lambda dups, sizes: dups > 0 and sizes.count(dups) == 2)(c["verify"].get("bronze_rows", 0) - c["produced"], c["verify"].get("commit_rows", [])),
        "idempotent run: every micro-batch committed exactly once (commit sizes sum to produced)": sum(a["verify"].get("commit_rows", [])) == a["produced"]})


# =========================================================================================================
def _pipeline_under_faults(name: str, schedule, extra_ev=None) -> dict:
    """Common run: clean state, 3 streaming services, faulty replay, scheduled faults, drain, oracle verification."""
    from tests.e2e import silver_oracle
    ev: dict = {"schedule": []}
    h.prepare(streaming=True)
    t_start = time.time()
    p = h.start_producer(f"{name}_producer.log", 1000, N, f"{INJECT}")
    for at, label, fn in schedule:
        while time.time() - t_start < at:
            time.sleep(0.5)
        log(f"t+{at}s FAULT: {label}")
        ev["schedule"].append({"t": round(time.time() - t_start, 1), "fault": label, "result": fn()})
    p.wait(timeout=600)
    prod = h.producer_summary(f"{name}_producer.log")
    ev["producer"] = prod
    exp = silver_oracle.expected(N, DUP, POISON, NULLP)
    ev["quiescence"] = wait_quiescent(exp["silver_rows"])
    # Look back only over THIS drill (a fixed 40 m window also saw earlier drills: identical peaks in two runs gave it away)
    since = int(time.time() - t_start)
    ev["peak_true_lag"] = h.prom(f"max_over_time(({h.TRUE_LAG})[{since}s:5s])")
    fired = h.prom_vec(f'max_over_time(ALERTS{{alertstate="firing"}}[{since}s])')
    ev["alerts_fired"] = sorted({json.loads(k)["alertname"] for k in fired})
    ev["kafka_raw_end_offsets"] = h.topic_end_sum("trips.raw.v1")
    # The pipeline is drained and all live metrics are captured. Stop the drivers (graceful SIGTERM) BEFORE verifying:
    # a 4th 2 GB verifier JVM next to three live drivers starves an 8 GB Docker VM (first attempt: 400 s timeout).
    dc(f"--profile streaming stop {' '.join(h.STREAMING)}", timeout=240)
    ev["stop_exit_codes"] = {s: h.exit_code(CONTAINER[s]) for s in h.STREAMING}
    ev["silver_check"] = h.silver_check(N, DUP, POISON, NULLP)
    ev["gold_check"] = h.gold_check(N, NULLP)
    return ev


def drill_broker() -> bool:
    def stop_broker():
        t0 = time.time()
        sh(f"docker stop {CONTAINER['kafka-2']}", timeout=60)
        det = h.wait_until(lambda: (h.prom("kafka_brokers") or 3) < 3, 60, 2, "exporter sees broker missing")
        urp = h.wait_until(lambda: (h.prom("sum(kafka_topic_partition_under_replicated_partition)") or 0) > 0, 60, 2, "URP>0")
        return {"detected_seconds": round(time.time() - t0, 1), "broker_missing_seen": det, "under_replicated_seen": urp,
                "brokers_now": h.prom("kafka_brokers")}

    def start_broker():
        t0 = time.time()
        sh(f"docker start {CONTAINER['kafka-2']}", timeout=60)
        def recovered():   # NB: never write `prom(x) or 1`: a legitimate 0.0 is falsy and would be replaced
            brokers = h.prom("kafka_brokers")
            urp = h.prom("sum(kafka_topic_partition_under_replicated_partition)")
            return brokers == 3 and urp == 0
        ok = h.wait_until(recovered, 240, 3, "ISR recovered")
        return {"recovery_seconds": round(time.time() - t0, 1), "isr_fully_recovered": ok}

    ev = _pipeline_under_faults("broker", [(25, "stop kafka-2", stop_broker), (70, "start kafka-2", start_broker)])
    h.save("broker", ev)
    pr, sch = ev["producer"], ev["schedule"]
    return h.verdict("broker-failure", {
        "broker loss detected (kafka_brokers < 3) and partitions reported under-replicated": sch[0]["result"]["broker_missing_seen"] and sch[0]["result"]["under_replicated_seen"],
        "producer survived: failed == 0, sent == acked (acks=all, RF=3, min ISR 2)": pr["failed"] == 0 and pr["sent"] == pr["acked"],
        "broker rejoined and ALL partitions back in sync": sch[1]["result"]["isr_fully_recovered"],
        "alerts fired during the outage: " + ",".join(ev["alerts_fired"]): "KafkaUnderReplicatedPartitions" in ev["alerts_fired"] and "KafkaBrokerMissing" in ev["alerts_fired"],
        "Spark jobs were NOT disturbed by the broker outage (TargetDown never fired during the drill)": "TargetDown" not in ev["alerts_fired"],
        "no message lost: Kafka raw end offsets == producer sent": ev["kafka_raw_end_offsets"] == pr["sent"],
        "pipeline reached the expected Gold total": ev["quiescence"]["reached_expected"],
        "all three drivers shut down gracefully after the outage (exit 0)": set(ev["stop_exit_codes"].values()) == {0},
        "Silver == independent oracle (exact: rows, quarantine reasons, dup counts, DLQ)": ev["silver_check"]["ok"],
        "Gold == batch recompute AND independent oracle": ev["gold_check"]["strict_batch_recompute"] and ev["gold_check"]["independent_oracle"]})


def drill_sigkill() -> bool:
    def kill(svc):
        def f():
            name, before = CONTAINER[svc], h.restart_count(CONTAINER[svc])
            t0 = t_kill = time.time()
            # `docker kill` is a USER stop: Docker deliberately never applies the restart policy to it (verified).
            # An OOM-kill / crash is the real failure mode, so SIGKILL the JVM *inside* the container: the Python
            # driver dies non-zero on its own and the `restart: on-failure` policy is what must bring it back.
            sh(f"docker exec {name} pkill -9 java", timeout=30)
            auto = h.wait_until(lambda: h.running(name) and h.restart_count(name) > before, 90, 1, f"{svc} auto-restart")
            if not auto:                                   # fall back to a manual start, and RECORD that it was needed
                sh(f"docker start {name}", check=False)
            # NB: targets_up() alone is stale (Prometheus still holds the pre-kill scrape). Require a NEW process:
            # its process_start_time_seconds must be later than the kill, and the target must be scraping again.
            job = svc
            def new_process_up():
                started = h.prom(f'max(process_start_time_seconds{{job="{job}"}})')
                return started is not None and started > t_kill and h.prom(f'up{{job="{job}"}}') == 1
            up = h.wait_until(new_process_up, 240, 2, f"{svc} restarted process serving metrics")
            return {"auto_restarted_by_compose": auto, "recovery_seconds": round(time.time() - t0, 1),
                    "new_process_serving_metrics": up}
        return f

    ev = _pipeline_under_faults("sigkill", [(20, "SIGKILL spark-bronze", kill("spark-bronze")),
                                            (70, "SIGKILL spark-silver", kill("spark-silver")),
                                            (120, "SIGKILL spark-gold", kill("spark-gold"))])
    h.save("sigkill", ev)
    pr = ev["producer"]
    checks = {f"{s['fault']}: new process serving metrics after {s['result']['recovery_seconds']}s (restarted by compose policy: {s['result']['auto_restarted_by_compose']})": s["result"]["new_process_serving_metrics"] for s in ev["schedule"]}
    checks.update({
        "producer unaffected: failed == 0, sent == acked": pr["failed"] == 0 and pr["sent"] == pr["acked"],
        "pipeline reached the expected Gold total after three hard crashes": ev["quiescence"]["reached_expected"],
        "Silver == independent oracle (exactly-once effect: no loss, no duplicates)": ev["silver_check"]["ok"],
        "Gold == batch recompute AND independent oracle": ev["gold_check"]["strict_batch_recompute"] and ev["gold_check"]["independent_oracle"]})
    return h.verdict("sigkill", checks)


# =========================================================================================================
def drill_rebuild() -> bool:
    ev: dict = {}
    h.prepare(streaming=False)
    p = h.start_producer("rebuild_producer.log", 0, N, INJECT, metrics=False)
    p.wait(timeout=300)
    for t in ("bronze", "silver", "gold"):
        sh(f"make -s {t}", timeout=600)
    ev["baseline"] = {"silver": h.silver_check(N, DUP, POISON, NULLP), "gold": h.gold_check(N, NULLP), "trips": pg_total_trips()}
    log("DISASTER: wiping Silver, quarantine, DQ metrics, Gold, their checkpoints and the Postgres Gold tables")
    sh("docker exec mobility-minio-1 sh -c 'mc alias set l http://localhost:9000 \"$MINIO_ROOT_USER\" \"$MINIO_ROOT_PASSWORD\" >/dev/null; "
       "mc rm --recursive --force l/lakehouse/silver >/dev/null; mc rm --recursive --force l/lakehouse/gold >/dev/null; "
       "for c in silver_trips gold_trips_per_minute gold_demand_sliding_15m gold_zone_demand_5m gold_hourly_demand gold_anomaly_counts; "
       "do mc rm --recursive --force l/checkpoints/$c >/dev/null; done; true'")
    sh("docker exec mobility-postgres-1 psql -U mobility -d mobility -q -c \"TRUNCATE gold.trips_per_minute, gold.demand_sliding_15m, "
       "gold.zone_demand_5m, gold.hourly_demand, gold.anomaly_counts\"")
    ev["after_wipe_trips"] = pg_total_trips()
    t0 = time.time()
    sh("make -s silver", timeout=600)
    sh("make -s gold", timeout=600)
    ev["rebuild_seconds"] = round(time.time() - t0, 1)
    ev["rebuilt"] = {"silver": h.silver_check(N, DUP, POISON, NULLP), "gold": h.gold_check(N, NULLP), "trips": pg_total_trips()}
    h.save("rebuild", ev)
    b, r = ev["baseline"], ev["rebuilt"]
    return h.verdict("rebuild-from-bronze", {
        "baseline verified against the oracles before the disaster": b["silver"]["ok"] and b["gold"]["strict_batch_recompute"] and b["gold"]["independent_oracle"],
        "disaster real: Gold serving table empty after the wipe": ev["after_wipe_trips"] == 0,
        f"Silver rebuilt from Bronze alone == oracle ({r['silver']['silver_rows']} rows)": r["silver"]["ok"],
        "Gold rebuilt (Delta + Postgres) == batch recompute and independent oracle": r["gold"]["strict_batch_recompute"] and r["gold"]["independent_oracle"],
        "rebuilt state identical to the pre-disaster state (silver rows, quarantine, total trips)": (
            b["silver"]["silver_rows"], b["silver"]["quarantine_rows"], b["trips"]) == (r["silver"]["silver_rows"], r["silver"]["quarantine_rows"], r["trips"])})


def drill_coldstart() -> bool:
    """Cross-process: a downstream job that started BEFORE its upstream table existed must notice the table when another
    process creates it (regression for the Phase 7 'Silver stuck in WAITING_FOR_UPSTREAM forever' bug)."""
    import subprocess
    path = f"s3a://lakehouse/_repro/coldstart_{int(time.time())}"
    run = (f"{h.COMPOSE} --profile jobs run --rm --entrypoint bash spark-job "
           f"/opt/app/infrastructure/spark/run_python.sh tests/recovery/repro_wait_s3a.py")
    waiter = subprocess.Popen(f"{run} wait {path} 180", shell=True, cwd=h.ROOT, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True)
    time.sleep(45)                                    # the waiter has been polling (and missing) for a while
    sh(f"{run} write {path}", timeout=300)
    out = waiter.communicate(timeout=300)[0]
    found = re.search(r"RESULT FOUND after ([\d.]+)s", out)
    misses = out.count("WAITING_FOR_UPSTREAM")
    h.save("coldstart", {"misses_before_table_existed": misses, "result": (found.group(0) if found else out[-300:])})
    return h.verdict("coldstart", {f"waiter polled and missed {misses}x, then saw the table created by ANOTHER process": bool(found) and misses > 5})


DRILLS = {"coldstart": drill_coldstart, "producer": drill_producer, "graceful": drill_graceful, "crash": drill_crash, "broker": drill_broker,
          "sigkill": drill_sigkill, "rebuild": drill_rebuild}

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else ""
    if which not in DRILLS:
        sys.exit(f"usage: python -m tests.recovery.drills {{{'|'.join(DRILLS)}}}")
    sys.exit(0 if DRILLS[which]() else 1)
