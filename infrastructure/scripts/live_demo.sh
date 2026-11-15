#!/usr/bin/env bash
# LIVE: Bronze, Silver, Gold stream CONTINUOUSLY and concurrently while the producer replays at SPEED x.
# Env: N (events) SPEED  WATERMARK  LATE_EVERY LATE_BY  DUP POISON NULLP
set -euo pipefail
cd "$(dirname "$0")/../.."
N=${N:-100000}; SPEED=${SPEED:-1000}; WM=${WATERMARK:-10 minutes}
LATE_EVERY=${LATE_EVERY:-500}; LATE_BY=${LATE_BY:-3000}; DUP=${DUP:-50}; POISON=${POISON:-997}; NULLP=${NULLP:-1009}
export PYTHONPATH=.
C="docker compose --env-file .env -f infrastructure/docker-compose.yml --profile jobs"
make reset-data
docker rm -f live-bronze live-silver live-gold >/dev/null 2>&1 || true
$C run -d --name live-bronze spark-job bronze_stream --trigger 5 >/dev/null
$C run -d --name live-silver spark-job silver_stream --trigger 5 >/dev/null
$C run -d --name live-gold -e SPARK_DRIVER_MEMORY=1g spark-job gold_stream --trigger 5 --watermark "$WM" >/dev/null
echo "waiting 45s for the three Spark drivers to start..."; sleep 45
for c in live-bronze live-silver live-gold; do
  if [ "$(docker inspect -f '{{.State.Running}}' $c)" != "true" ]; then
    echo "FATAL: $c is not running"; docker logs --tail 15 $c 2>&1 | cut -c1-200; exit 1; fi
done
.venv/bin/python -m producers.replay_producer --speed "$SPEED" --limit "$N" --inject-late-every "$LATE_EVERY" \
  --late-by-events "$LATE_BY" --inject-dup-every "$DUP" --inject-poison-every "$POISON" \
  --inject-null-pickup-every "$NULLP" 2>&1 | tee data/live_producer.log | tail -1 | cut -c1-400
docker stats --no-stream --format "{{.Name}} mem={{.MemUsage}} cpu={{.CPUPerc}}" live-bronze live-silver live-gold
echo "waiting for the pipeline to drain (Postgres total stable for 3 polls)..."
prev=-1; stable=0
for i in $(seq 1 60); do
  cur=$(docker exec mobility-postgres-1 psql -U mobility -d mobility -Atc "select coalesce(sum(trips),0) from gold.trips_per_minute")
  if [ "$cur" = "$prev" ] && [ "$cur" != "0" ]; then stable=$((stable+1)); else stable=0; fi
  [ $stable -ge 3 ] && break
  for c in live-bronze live-silver live-gold; do
    [ "$(docker inspect -f '{{.State.Running}}' $c)" = "true" ] || { echo "FATAL: $c died while draining"; docker logs --tail 15 $c 2>&1 | cut -c1-200; exit 1; }
  done
  prev=$cur; sleep 10
done
echo "drained at total trips=$cur"
docker stop -t 30 live-bronze live-silver live-gold >/dev/null; docker rm live-bronze live-silver live-gold >/dev/null
make -s verify-gold LIVE=1 | tee data/gold_live_report.txt | grep GOLD_RESULT
