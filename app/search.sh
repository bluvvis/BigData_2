#!/bin/bash
set -e

echo "=========================================="
echo "         SEARCH QUERY PROCESSOR"
echo "=========================================="
echo ""

# Get query from argument
QUERY="${1:-information retrieval}"

echo "Query: '$QUERY'"
echo ""

source .venv/bin/activate

export PYSPARK_DRIVER_PYTHON=$(which python)

if [ "${USE_YARN:-0}" = "1" ]; then
  export PYSPARK_PYTHON=./.venv/bin/python
  if [ ! -f /app/spark-conf/yarn-archive.conf ]; then
    echo "ERROR: /app/spark-conf/yarn-archive.conf missing. Run start-services (docker compose) once so jars ZIP is uploaded."
    exit 1
  fi
  echo "Running query on YARN (spark.yarn.archive from yarn-archive.conf)..."
  spark-submit --master yarn --deploy-mode client \
    --properties-file /app/spark-conf/yarn-archive.conf \
    --archives /app/.venv.tar.gz#.venv \
    --driver-memory 1536m \
    query.py "$QUERY"
else
  unset PYSPARK_PYTHON
  echo "Running query processor (Spark local mode)..."
  spark-submit --master local[1] --deploy-mode client \
    --driver-memory 1536m query.py "$QUERY"
fi

echo ""
echo "=========================================="
