#!/bin/bash

set -e

source .venv/bin/activate
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "=== Data Preparation Phase ==="

if [ ! -f "a.parquet" ]; then
    echo "ERROR: a.parquet not found in /app"
    echo "Please download a.parquet from Kaggle and place it in the /app directory"
    exit 1
fi

echo "Step 1: Running prepare_data.py..."
spark-submit --master local[1] --deploy-mode client \
    --driver-memory 1536m --conf spark.driver.maxResultSize=512m prepare_data.py

echo "Step 2: Checking generated files..."
if ! compgen -G "data/*.txt" > /dev/null; then
    echo "ERROR: No .txt files in data/ after prepare_data.py"
    exit 1
fi
ls -lh data/ | head -10
echo "Total documents created: $(find data -maxdepth 1 -type f -name '*.txt' | wc -l)"

echo "Step 3: Putting data and indexer input to HDFS..."
hdfs dfs -rm -rf /data 2>/dev/null || true
hdfs dfs -rm -rf /indexer/data 2>/dev/null || true
hdfs dfs -rm -rf /indexer/input 2>/dev/null || true

hdfs dfs -mkdir -p /data
hdfs dfs -mkdir -p /indexer/data
hdfs dfs -mkdir -p /indexer/input

export LC_ALL="${LC_ALL:-C.UTF-8}"
while IFS= read -r -d '' f; do
  hdfs dfs -put "$f" /data/
done < <(find data -maxdepth 1 -type f -name '*.txt' -print0)

hdfs dfs -put indexer_input.txt /indexer/input/

echo "Step 4: Verifying HDFS directories..."
echo "Documents in /data:"
hdfs dfs -ls /data | wc -l

echo "Indexer input file:"
hdfs dfs -ls /indexer/input/

echo "=== Data Preparation Complete ==="

