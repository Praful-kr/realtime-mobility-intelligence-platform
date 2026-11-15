#!/usr/bin/env bash
# Run an arbitrary python script with the same driver environment as submit.sh (local[2], PySpark on the path).
set -euo pipefail
PY4J=(/opt/spark/python/lib/py4j-*-src.zip)
export PYTHONPATH="/opt/app:/opt/spark/python:${PY4J[0]}:${PYTHONPATH:-}"
export PYSPARK_SUBMIT_ARGS="--master local[2] --driver-memory 1g --conf spark.ui.showConsoleProgress=false --conf spark.driver.host=localhost pyspark-shell"
cd /opt/app
exec python3 "$@"
