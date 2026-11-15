#!/usr/bin/env bash
# E2E: reset -> produce (with injected faults) -> Bronze -> Silver -> compare against the independent oracle.
set -euo pipefail
cd "$(dirname "$0")/../.."
N=${N:-300000}; DUP=${DUP:-50}; POISON=${POISON:-997}; NULLP=${NULLP:-1009}
export PYTHONPATH=.
make reset-data
.venv/bin/python -m producers.replay_producer --speed 0 --limit "$N" --inject-dup-every "$DUP" \
  --inject-poison-every "$POISON" --inject-null-pickup-every "$NULLP" 2>&1 | tee data/producer.log | tail -1
make bronze >/dev/null 2>&1
make silver 2>&1 | grep -E "SILVER_BATCH" | tail -3
make -s verify-silver | sed 's/^SILVER_REPORT //' > data/silver_report.json
DLQ=$(docker exec mobility-kafka-1-1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka-1:9092 --topic trips.dlq.v1 | awk -F: '{s+=$3} END{print s+0}')
.venv/bin/python -m tests.e2e.silver_oracle --n "$N" --dup-every "$DUP" --poison-every "$POISON" \
  --null-every "$NULLP" --report data/silver_report.json --dlq-count "$DLQ"
