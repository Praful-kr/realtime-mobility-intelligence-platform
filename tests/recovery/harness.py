"""Helpers for the reliability drills: docker/compose, Prometheus, producer control, and verification against the
INDEPENDENT oracles (tests/e2e). Nothing here fakes a result: every check reads real state from the running stack."""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Callable, Dict, Optional

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data" / "drills"
DATA.mkdir(parents=True, exist_ok=True)
COMPOSE = f"docker compose --env-file {ROOT}/.env -f {ROOT}/infrastructure/docker-compose.yml"
TRUE_LAG = ('sum(kafka_topic_partition_current_offset{topic="trips.raw.v1"}) '
            '- sum(spark_kafka_processed_offset_sum{topic="trips.raw.v1"})')
STREAMING = ("spark-bronze", "spark-silver", "spark-gold")
CONTAINER = {"spark-bronze": "mobility-spark-bronze-1", "spark-silver": "mobility-spark-silver-1",
             "spark-gold": "mobility-spark-gold-1", "kafka-1": "mobility-kafka-1-1", "kafka-2": "mobility-kafka-2-1",
             "kafka-3": "mobility-kafka-3-1", "api": "mobility-api-1"}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def sh(cmd: str, check: bool = True, timeout: Optional[int] = None, env: Optional[Dict[str, str]] = None):
    e = {**os.environ, "PYTHONPATH": str(ROOT), **(env or {})}
    r = subprocess.run(cmd, shell=True, cwd=ROOT, capture_output=True, text=True, timeout=timeout, env=e)
    if check and r.returncode != 0:
        raise RuntimeError(f"command failed ({r.returncode}): {cmd}\n{r.stdout[-800:]}\n{r.stderr[-800:]}")
    return r


def dc(args: str, **kw):
    return sh(f"{COMPOSE} {args}", **kw)


def wait_until(pred: Callable[[], bool], timeout: float, every: float = 3.0, desc: str = "") -> bool:
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return True
        except Exception:  # noqa: BLE001 - transient (service restarting)
            pass
        time.sleep(every)
    log(f"TIMEOUT waiting for: {desc}")
    return False


# ---- Prometheus ---------------------------------------------------------------------------------------------
def prom(expr: str) -> Optional[float]:
    q = urllib.parse.urlencode({"query": expr})
    r = json.load(urllib.request.urlopen(f"http://localhost:9090/api/v1/query?{q}", timeout=10))["data"]["result"]
    return float(r[0]["value"][1]) if r else None


def prom_vec(expr: str) -> Dict[str, float]:
    q = urllib.parse.urlencode({"query": expr})
    r = json.load(urllib.request.urlopen(f"http://localhost:9090/api/v1/query?{q}", timeout=10))["data"]["result"]
    return {json.dumps(x["metric"], sort_keys=True): float(x["value"][1]) for x in r}


def targets_up() -> int:
    t = json.load(urllib.request.urlopen("http://localhost:9090/api/v1/targets", timeout=10))["data"]["activeTargets"]
    return sum(1 for x in t if x["health"] == "up" and x["labels"]["job"].startswith("spark-"))


# ---- containers ---------------------------------------------------------------------------------------------
def running(name: str) -> bool:
    return sh(f"docker inspect -f '{{{{.State.Running}}}}' {name}", check=False).stdout.strip() == "true"


def exit_code(name: str) -> int:
    return int(sh(f"docker inspect -f '{{{{.State.ExitCode}}}}' {name}").stdout.strip())


def restart_count(name: str) -> int:
    return int(sh(f"docker inspect -f '{{{{.RestartCount}}}}' {name}").stdout.strip())


def logs(name: str) -> str:
    r = sh(f"docker logs {name} 2>&1", check=False)
    return r.stdout + r.stderr


def silver_rows_in_logs(name: str = CONTAINER["spark-silver"]) -> int:
    return sum(int(m) for m in re.findall(r"SILVER_BATCH \{'batch_id': \d+, 'bronze_rows': (\d+)", logs(name)))


def prepare(streaming: bool = True) -> None:
    """Clean slate + monitoring + (optionally) the three streaming services. Kafka UI is stopped to save RAM."""
    dc("stop kafka-ui", check=False)
    dc("up -d --wait kafka-exporter prometheus api")
    dc(f"--profile streaming stop {' '.join(STREAMING)}", check=False)
    sh("bash infrastructure/scripts/reset_data.sh")
    # reset also wiped the checkpoints, so the services must start from scratch (they were stopped above)
    dc(f"--profile streaming rm -f {' '.join(STREAMING)}", check=False)
    if streaming:
        dc(f"--profile streaming up -d {' '.join(STREAMING)}")
        wait_until(lambda: targets_up() == 3, 240, 5, "3 spark targets up")


# ---- Kafka ground truth --------------------------------------------------------------------------------------
def topic_end_sum(topic: str) -> int:
    for broker in ("kafka-1", "kafka-3", "kafka-2"):
        if not running(CONTAINER[broker]):
            continue
        r = sh(f"docker exec {CONTAINER[broker]} /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server {broker}:9092 "
               f"--topic {topic}", check=False)
        if r.returncode == 0:
            return sum(int(line.split(":")[2]) for line in r.stdout.strip().splitlines())
    raise RuntimeError("no broker reachable")


# ---- producer ------------------------------------------------------------------------------------------------
def start_producer(logfile: str, speed: float, limit: int, extra: str = "", metrics: bool = True):
    cmd = (f".venv/bin/python -m producers.replay_producer --speed {speed} --limit {limit} "
           f"--metrics-port {'8010' if metrics else '0'} --metrics-linger 0 {extra}")
    f = open(DATA / logfile, "w")
    return subprocess.Popen(cmd, shell=True, cwd=ROOT, stdout=f, stderr=subprocess.STDOUT, preexec_fn=os.setsid,
                            env={**os.environ, "PYTHONPATH": str(ROOT)})


def producer_summary(logfile: str) -> dict:
    for line in reversed((DATA / logfile).read_text().strip().splitlines()):
        if '"replay finished"' in line:
            return json.loads(line)
    raise RuntimeError(f"producer produced no 'replay finished' line: {logfile}")


# ---- verification against the independent oracles --------------------------------------------------------------
def silver_check(n: int, dup: int = 0, poison: int = 0, null: int = 0) -> dict:
    from tests.e2e import silver_oracle
    out = sh("make -s verify-silver", timeout=400).stdout
    act = json.loads(re.search(r"SILVER_REPORT (.*)", out).group(1))
    exp = silver_oracle.expected(n, dup, poison, null)
    bad = silver_oracle.compare(exp, act, topic_end_sum("trips.dlq.v1"))
    return {"ok": not bad, "mismatches": bad, "bronze_rows": act["bronze_rows"], "silver_rows": act["silver_rows"],
            "quarantine_rows": act["quarantine_rows"], "dup_dropped": act["metrics"]["dup_dropped"],
            "expected_silver_rows": exp["silver_rows"], "expected_bronze_rows": exp["bronze_rows"]}


def gold_check(n: int, null: int = 0) -> dict:
    strict = sh("make -s verify-gold", check=False, timeout=400).stdout
    oracle = sh(f".venv/bin/python -m tests.e2e.gold_oracle --n {n} --null-every {null}", check=False, timeout=400)
    return {"strict_batch_recompute": "GOLD_RESULT PASS" in strict,
            "independent_oracle": "GOLD_ORACLE_RESULT PASS" in oracle.stdout,
            "oracle_tail": oracle.stdout.strip().splitlines()[-2:]}


def drained(sent: int, timeout: float = 420) -> bool:
    """Silver has received every produced message AND true lag is 0 (needs Prometheus scraping the streaming jobs)."""
    def ok():
        rec = prom('sum(spark_silver_rows_total{outcome="received"})')
        lag = prom(TRUE_LAG)
        return rec is not None and int(rec) == sent and lag == 0 and (prom("sum(spark_delta_version_lag)") or 0) == 0
    return wait_until(ok, timeout, 6, f"pipeline drained (silver received == {sent})")


def save(name: str, evidence: dict) -> None:
    (DATA / f"{name}.json").write_text(json.dumps(evidence, indent=1, default=str))
    log(f"evidence written to data/drills/{name}.json")


def verdict(name: str, checks: Dict[str, bool]) -> bool:
    ok = all(checks.values())
    for k, v in checks.items():
        print(f"  {'OK  ' if v else 'FAIL'} {k}")
    print(f"DRILL_RESULT {name} {'PASS' if ok else 'FAIL'}", flush=True)
    return ok
