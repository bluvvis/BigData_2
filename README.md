# Big Data Assignment 2 — Simple search engine (MapReduce, Cassandra, Spark)

Template: [firas-jolha/big-data-assignment2](https://github.com/firas-jolha/big-data-assignment2).  
Dataset: [Wikipedia parquet on Kaggle](https://www.kaggle.com/datasets/jjinho/wikipedia-20230701?select=a.parquet) — place `a.parquet` in `app/` before running.

## Prerequisites

Docker and Docker Compose.

## Run

From the repository root:

```bash
docker compose up
```

The `cluster-master` container runs `app/app.sh`: starts Hadoop/YARN, installs Python deps, runs `prepare_data.sh`, `index.sh`, and example `search.sh` queries.


| Path | Role |
|------|------|
| `app/prepare_data.py` / `prepare_data.sh` | PySpark: parquet → local `data/*.txt` + `indexer_input.txt`, upload to HDFS `/data` and `/indexer/input/` |
| `app/mapreduce/mapper1.py`, `reducer1.py` | Hadoop streaming inverted index |
| `app/create_index.sh` | MapReduce job; optional arg: HDFS input file or directory (default `/indexer/input/indexer_input.txt`) |
| `app/store_index.sh` | Load HDFS index into Cassandra |
| `app/index.sh` | `create_index.sh` then `store_index.sh` |
| `app/query.py`, `search.sh` | BM25 search; `search.sh "your query"` |
| `app/start-services.sh` | HDFS/YARN/HistoryServer + Spark-on-YARN jar archive helper |



