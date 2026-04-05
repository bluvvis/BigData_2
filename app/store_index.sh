#!/bin/bash

set -e

echo "=== Cassandra Storage Phase ==="
echo "This script loads the inverted index into Cassandra/ScyllaDB"

source .venv/bin/activate

# Wait for Cassandra to be ready (retry mechanism)
echo "Checking Cassandra connectivity..."
CASSANDRA_OK=0
for i in {1..30}; do
    if python3 -c "from cassandra.cluster import Cluster; Cluster(['cassandra-server']).connect().shutdown()" 2>/dev/null; then
        echo "Cassandra is ready!"
        CASSANDRA_OK=1
        break
    fi
    echo "Waiting for Cassandra... ($i/30)"
    sleep 2
done
if [ "$CASSANDRA_OK" != "1" ]; then
    echo "ERROR: Cassandra did not become ready in time"
    exit 1
fi

# Run Python script to create tables and load data
echo "Running app.py to store index in Cassandra..."
python3 app.py

echo "=== Cassandra Storage Complete ==="

