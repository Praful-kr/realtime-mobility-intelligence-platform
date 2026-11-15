#!/usr/bin/env bash
# Runs pytest inside the Spark image with the same Python path spark-submit would set up.
set -uo pipefail
PY4J=(/opt/spark/python/lib/py4j-*-src.zip)
export PYTHONPATH="/opt/app:/opt/spark/python:${PY4J[0]}"
cd /opt/app
exec python3 -m pytest "${@:-tests/spark}" -q -p no:cacheprovider
