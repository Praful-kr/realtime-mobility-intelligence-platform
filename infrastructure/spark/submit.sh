#!/usr/bin/env bash
# Usage: submit.sh <job_module_name> [job args...]   e.g. submit.sh bronze_stream --trigger 5
#
# The driver runs as PYTHON in the foreground (PID 1 via exec); PySpark launches the JVM as a child. With
# `spark-submit` as PID 1 the JVM got SIGTERM and the container exited 143 without ever running the job's Python
# shutdown handler (found in the Phase 7 drills). Now `docker stop` -> SIGTERM -> Python handler -> in-flight micro-batch
# finishes -> queries stop -> exit 0.
set -euo pipefail
JOB="$1"; shift
PY4J=(/opt/spark/python/lib/py4j-*-src.zip)
export PYTHONPATH="/opt/app:/opt/spark/python:${PY4J[0]}:${PYTHONPATH:-}"
export PYSPARK_SUBMIT_ARGS="--master ${SPARK_MASTER:-local[2]} --driver-memory ${SPARK_DRIVER_MEMORY:-1g} --conf spark.ui.showConsoleProgress=false --conf spark.driver.host=localhost pyspark-shell"
exec python3 "/opt/app/spark_jobs/${JOB}.py" "$@"
