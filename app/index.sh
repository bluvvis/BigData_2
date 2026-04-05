#!/bin/bash

set -e

echo "=========================================="
echo "     BUILDING SEARCH ENGINE INDEX"
echo "=========================================="

echo ""
echo "Stage 1: Creating index using MapReduce..."
bash create_index.sh

echo ""
echo "Stage 2: Storing index in Cassandra..."
bash store_index.sh

echo ""
echo "=========================================="
echo "      INDEXING COMPLETE"
echo "=========================================="

