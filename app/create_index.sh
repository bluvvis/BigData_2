#!/bin/bash

set -e

echo "=== MapReduce Indexing Phase ==="
echo "Creating inverted index using Hadoop streaming..."

INPUT_PATH="${1:-/indexer/input/indexer_input.txt}"
OUTPUT_PATH="/indexer/output"
MAPPER="$(pwd)/mapreduce/mapper1.py"
REDUCER="$(pwd)/mapreduce/reducer1.py"

if ! hdfs dfs -test -f "$INPUT_PATH" 2>/dev/null && ! hdfs dfs -test -d "$INPUT_PATH" 2>/dev/null; then
    echo "ERROR: HDFS input not found: $INPUT_PATH"
    echo "Run prepare_data.sh first, or pass an existing HDFS file or directory."
    exit 1
fi

echo "Input: $INPUT_PATH"
echo "Output: $OUTPUT_PATH"
echo "Mapper: $MAPPER"
echo "Reducer: $REDUCER"

# Make scripts executable
chmod +x "$MAPPER" "$REDUCER"

# Remove previous output if exists
hdfs dfs -rm -rf "$OUTPUT_PATH" 2>/dev/null || true

echo "Running Hadoop Streaming MapReduce job..."
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -files "$MAPPER,$REDUCER" \
    -input "$INPUT_PATH" \
    -output "$OUTPUT_PATH" \
    -mapper "mapper1.py" \
    -reducer "reducer1.py" \
    -numReduceTasks 1

echo "MapReduce job completed!"

echo "Output files in HDFS:"
hdfs dfs -ls "$OUTPUT_PATH"

echo "Sample output (first 10 lines):"
hdfs dfs -cat "$OUTPUT_PATH/part-00000" 2>/dev/null | head -10 || true

# Copy output to /indexer/data for next stage
echo "Copying output to /indexer/data..."
hdfs dfs -rm -rf /indexer/data 2>/dev/null || true
hdfs dfs -cp "$OUTPUT_PATH/part-00000" /indexer/data/index
hdfs dfs -cat /indexer/data/index | wc -l
echo "Total index entries created: $(hdfs dfs -cat /indexer/data/index | wc -l)"

echo "=== MapReduce Indexing Complete ==="
