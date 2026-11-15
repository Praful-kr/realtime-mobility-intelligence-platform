#!/usr/bin/env bash
# DESTRUCTIVE (local dev only): wipe Kafka topics, lakehouse and checkpoints, then recreate topics.
set -euo pipefail
cd "$(dirname "$0")/../.."
B=kafka-1:9092,kafka-2:9092,kafka-3:9092
for t in trips.raw.v1 trips.dlq.v1; do
  docker exec mobility-kafka-1-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server $B --delete --topic "$t" 2>/dev/null || true
done
docker exec mobility-minio-1 sh -c 'mc alias set l http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc rm --recursive --force l/lakehouse >/dev/null 2>&1; mc rm --recursive --force l/checkpoints >/dev/null 2>&1; true'
docker exec mobility-postgres-1 psql -U "${POSTGRES_USER:-mobility}" -d "${POSTGRES_DB:-mobility}" -q -c "TRUNCATE gold.trips_per_minute, gold.demand_sliding_15m, gold.zone_demand_5m, gold.hourly_demand, gold.anomaly_counts, gold.stream_metrics" >/dev/null
for i in $(seq 1 30); do   # topic deletion is asynchronous
  if PYTHONPATH=. .venv/bin/python -m infrastructure.scripts.create_topics >/tmp/claude-501/topics.log 2>&1; then
    echo "reset complete"; exit 0; fi
  sleep 2
done
cat /tmp/claude-501/topics.log; exit 1
