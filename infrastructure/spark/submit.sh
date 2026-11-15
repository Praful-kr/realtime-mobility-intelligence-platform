#!/usr/bin/env bash
# Usage: submit.sh <job_module_name> [job args...]   e.g. submit.sh bronze_stream --trigger 5
set -euo pipefail
JOB="$1"; shift
export PYTHONPATH="/opt/app:${PYTHONPATH:-}"
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-local[2]}" \
  --driver-memory "${SPARK_DRIVER_MEMORY:-1g}" \
  --conf spark.ui.showConsoleProgress=false \
  --conf spark.driver.host=localhost \
  "/opt/app/spark_jobs/${JOB}.py" "$@"
