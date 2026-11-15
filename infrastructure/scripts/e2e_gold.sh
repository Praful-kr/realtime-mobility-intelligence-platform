#!/usr/bin/env bash
# E2E: reset -> faulty replay -> Bronze -> Silver -> Gold (all availableNow) -> 3-way verification.
set -euo pipefail
cd "$(dirname "$0")/../.."
N=${N:-300000}; DUP=${DUP:-50}; POISON=${POISON:-997}; NULLP=${NULLP:-1009}
export PYTHONPATH=.
make reset-data
.venv/bin/python -m producers.replay_producer --speed 0 --limit "$N" --inject-dup-every "$DUP" \
  --inject-poison-every "$POISON" --inject-null-pickup-every "$NULLP" 2>&1 | tail -1 | cut -c1-220
make -s bronze >/dev/null 2>&1
make -s silver 2>&1 | grep -c SILVER_BATCH | sed 's/^/silver batches: /'
time make -s gold 2>&1 | grep -E "Traceback|Error|Exception" | head -5 || true
make -s verify-gold | tee data/gold_report.txt | grep -E "GOLD_RESULT"
.venv/bin/python -m tests.e2e.gold_oracle --n "$N" --dup-every "$DUP" --poison-every "$POISON" --null-every "$NULLP"
