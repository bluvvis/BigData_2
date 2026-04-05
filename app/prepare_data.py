import os
import re
import shutil
import unicodedata

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession


def ascii_slug(title: str, max_len: int = 100) -> str:
    """HDFS + bash globs break on some Unicode paths; keep assignment format id_title.txt safely."""
    t = sanitize_filename(str(title))
    t = unicodedata.normalize("NFKD", t)
    t = "".join(c for c in t if ord(c) < 128)
    t = re.sub(r"[^a-zA-Z0-9._-]+", "_", t).strip("._") or "doc"
    return t[:max_len]


# Clean output dir before Spark so failed runs do not leave stale .txt files.
if os.path.isdir("data"):
    shutil.rmtree("data")
os.makedirs("data", exist_ok=True)

# Keep driver heap bounded — full df.count() on multi-GB parquet OOMs small Docker VMs (exit 137).
spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local[1]") \
    .config("spark.driver.memory", "1536m") \
    .config("spark.driver.maxResultSize", "512m") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.files.maxPartitionBytes", "33554432") \
    .config("spark.sql.parquet.enableVectorizedReader", "false") \
    .getOrCreate()


# Read parquet file from local filesystem
df = spark.read.parquet("file:///app/a.parquet")

# Select 100 documents — avoid df.count(): it scans the entire corpus and often triggers OOM.
n = 100
# Wikipedia-scale parquet: tiny fraction still yields >> n rows; adjust if you ever get < n docs.
df = df.select(['id', 'title', 'text']).filter(df['text'].isNotNull()).sample(fraction=0.0001, seed=0).limit(n)

# Extract documents as individual text files
def create_doc(row):
    """Create a text file for each document"""
    try:
        # Format: <doc_id>_<doc_title>.txt
        doc_id = str(row['id']).replace(" ", "_")
        title = ascii_slug(row["title"]).replace(" ", "_")
        filename = f"data/{doc_id}_{title}.txt"
        
        with open(filename, "w", encoding="utf-8") as f:
            f.write(str(row['text']))
    except Exception as e:
        print(f"Error creating doc for {row['id']}: {e}")


# Create all documents
df.foreach(create_doc)

print(f"Successfully created {len(os.listdir('data'))} documents")

# Also create a combined file for MapReduce input
# Format: <doc_id> <doc_title> <doc_text>
with open("indexer_input.txt", "w", encoding="utf-8") as f:
    for row in df.collect():
        doc_id = str(row['id'])
        title = str(row['title']).replace('\n', ' ').replace('\t', ' ')
        text = str(row['text']).replace('\n', ' ').replace('\t', ' ')
        # Limit text to first 500 chars for performance
        text = text[:500]
        f.write(f"{doc_id}\t{title}\t{text}\n")