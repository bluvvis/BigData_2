#!/bin/bash

set -e

echo "=========================================="
echo "    DISTRIBUTED SEARCH ENGINE"
echo "    Big Data Assignment 2"
echo "=========================================="
echo ""

# Start ssh server
echo "[1/6] Starting SSH server..."
service ssh restart

# Starting the services
echo "[2/6] Starting Hadoop services..."
bash start-services.sh

# Creating a virtual environment
echo "[3/6] Setting up Python environment..."
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
echo "[4/6] Installing dependencies..."
pip install -r requirements.txt  

# Package the virtual env.
echo "Packaging virtual environment..."
rm -f .venv.tar.gz
venv-pack -o .venv.tar.gz

# Collect data
echo "[5/6] Collecting and preparing data..."
bash prepare_data.sh

# Run the indexer
echo ""
echo "[6/6] Building index (MapReduce + Cassandra)..."
bash index.sh

# Run the ranker with example queries
echo ""
echo "=========================================="
echo "       SEARCH ENGINE READY"
echo "=========================================="
echo ""
echo "Example queries:"

bash search.sh "machine learning"
echo ""

bash search.sh "artificial intelligence"
echo ""

bash search.sh "deep learning neural network"
echo ""

echo "=========================================="
echo "         PIPELINE COMPLETE"
echo "=========================================="

if [ "${STAY_ALIVE_AFTER_PIPELINE:-0}" = "1" ]; then
  echo "STAY_ALIVE_AFTER_PIPELINE=1: master container stays up (docker exec -it cluster-master bash)."
  exec tail -f /dev/null
fi

