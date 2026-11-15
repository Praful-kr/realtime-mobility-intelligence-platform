#!/usr/bin/env bash
# Live observability run: fresh Prometheus -> streaming services -> faulty replay -> drain -> verify metrics/dashboards.
set -euo pipefail
cd "$(dirname "$0")/../.."
N=${N:-100000}; SPEED=${SPEED:-1000}
export PYTHONPATH=.
C="docker compose --env-file .env -f infrastructure/docker-compose.yml"
prom() { curl -s --data-urlencode "query=$1" localhost:9090/api/v1/query | python3 -c "import sys,json; r=json.load(sys.stdin)['data']['result']; print(r[0]['value'][1] if r else 'NaN')"; }
make reset-data
$C stop kafka-ui >/dev/null 2>&1 || true            # free ~270 MB for the three Spark drivers (8 GB Docker VM)
$C stop prometheus >/dev/null; $C rm -f prometheus >/dev/null; docker volume rm mobility_prometheus-data >/dev/null 2>&1 || true
$C up -d --wait prometheus grafana kafka-exporter api >/dev/null
$C --profile streaming up -d spark-bronze spark-silver spark-gold >/dev/null
echo "waiting for the three Spark drivers to be scraped..."
for i in $(seq 1 40); do
  up=$(prom 'sum(up{job=~"spark-.*"})'); [ "$up" = "3" ] && break; sleep 5
done
echo "spark targets up: $up"
.venv/bin/python -m producers.replay_producer --speed "$SPEED" --limit "$N" --inject-late-every 500 \
  --late-by-events 3000 --inject-dup-every 50 --inject-poison-every 997 --inject-null-pickup-every 1009 \
  2>&1 | tee data/obs_producer.log | tail -1 | cut -c1-330
SENT=$(grep "replay finished" data/obs_producer.log | python3 -c "import sys,json; d=json.loads(sys.stdin.read().strip().splitlines()[-1]); print(d['sent'])")
DLQ=$(grep "replay finished" data/obs_producer.log | python3 -c "import sys,json; d=json.loads(sys.stdin.read().strip().splitlines()[-1]); print(d['dlq'])")
echo "drain: waiting until Silver has received $SENT and all lag is 0 (3 stable polls)..."
stable=0
for i in $(seq 1 80); do
  rec=$(prom 'sum(spark_silver_rows_total{outcome="received"})'); lag=$(prom 'sum(spark_kafka_lag_records)'); dl=$(prom 'sum(spark_delta_version_lag)')
  if [ "$rec" = "$SENT.0" ] || [ "$rec" = "$SENT" ]; then
    if [ "$lag" = "0" ] || [ "$lag" = "0.0" ]; then if [ "$dl" = "0" ] || [ "$dl" = "0.0" ]; then stable=$((stable+1)); else stable=0; fi; else stable=0; fi
  else stable=0; fi
  [ $stable -ge 3 ] && break; sleep 10
done
echo "drained: silver_received=$rec lag=$lag delta_lag=$dl"
docker stats --no-stream --format "{{.Name}} {{.MemUsage}}" | grep -E "spark|prometheus|grafana" 
sleep 8
.venv/bin/python -m tests.e2e.observability_check --expect-sent "$SENT" --expect-dlq "$DLQ" --expect-spark-up | tee data/obs_check.txt | grep -E "FAIL|OBSERVABILITY_RESULT|all .* panel"
