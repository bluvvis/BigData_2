#!/usr/bin/env python3
"""
Store inverted index in Cassandra/ScyllaDB
Reads index from HDFS and loads it into Cassandra tables
"""

from cassandra.cluster import Cluster
import subprocess
import sys

# Configuration
CASSANDRA_HOST = 'cassandra-server'
CASSANDRA_PORT = 9042
KEYSPACE = 'search_engine'
REPLICATION_FACTOR = 1

INDEX_FILE = '/indexer/data/index'

def connect_cassandra():
    """Connect to Cassandra cluster"""
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        print(f"Connected to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}")
        return cluster, session
    except Exception as e:
        print(f"Error connecting to Cassandra: {e}")
        print("Make sure Cassandra server is running")
        sys.exit(1)

def create_keyspace(session):
    """Create keyspace if it doesn't exist"""
    query = f"""
    CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
    WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': {REPLICATION_FACTOR}}}
    """
    session.execute(query)
    print(f"Keyspace '{KEYSPACE}' created or already exists")

def create_tables(session):
    """Create tables for storing index and metadata"""
    session.set_keyspace(KEYSPACE)
    
    # Table for inverted index
    # Stores: term -> (document_frequency, postings_list)
    query1 = """
    CREATE TABLE IF NOT EXISTS inverted_index (
        term TEXT PRIMARY KEY,
        document_frequency INT,
        num_docs INT,
        postings_data TEXT
    )
    """
    session.execute(query1)
    print("Table 'inverted_index' created")
    
    # Table for document metadata
    query2 = """
    CREATE TABLE IF NOT EXISTS documents (
        doc_id TEXT PRIMARY KEY,
        title TEXT,
        doc_length INT
    )
    """
    session.execute(query2)
    print("Table 'documents' created")
    
    # Table for global statistics (needed for BM25)
    query3 = """
    CREATE TABLE IF NOT EXISTS statistics (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """
    session.execute(query3)
    print("Table 'statistics' created")

def load_index_from_hdfs(session):
    """Load index from HDFS and store in Cassandra"""
    session.set_keyspace(KEYSPACE)
    
    print(f"\nLoading index from {INDEX_FILE}...")
    
    # Read index file from HDFS
    try:
        result = subprocess.run(['hdfs', 'dfs', '-cat', INDEX_FILE], 
                              capture_output=True, text=True, check=True)
        index_lines = result.stdout.strip().split('\n')
    except Exception as e:
        print(f"Error reading from HDFS: {e}")
        return
    
    # Parse and load each index entry
    doc_lengths = {}
    doc_titles = {}
    total_docs = 0
    
    for line in index_lines:
        if not line.strip():
            continue
        
        try:
            # Parse: <term> <df>|<posting1>|<posting2>|...
            parts = line.split('\t', 1)
            if len(parts) < 2:
                continue
            
            term = parts[0].strip()
            posting_info = parts[1].strip()
            
            # Parse posting info
            posting_parts = posting_info.split('|')
            df = int(posting_parts[0])  # Document frequency
            
            # Parse individual postings: doc_id|title|term_freq|doc_length
            doc_data = {}
            total_docs_in_term = 0
            
            for i in range(1, len(posting_parts), 4):
                if i + 3 < len(posting_parts):
                    doc_id = posting_parts[i]
                    title = posting_parts[i + 1]
                    term_freq = int(posting_parts[i + 2])
                    doc_length = int(posting_parts[i + 3])
                    
                    # Store document info
                    if doc_id not in doc_titles:
                        doc_titles[doc_id] = title
                        doc_lengths[doc_id] = doc_length
                        total_docs = max(total_docs, int(doc_id) if doc_id.isdigit() else 1)
                    
                    if doc_id not in doc_data:
                        doc_data[doc_id] = []
                    doc_data[doc_id].append((term_freq, doc_length))
                    total_docs_in_term += 1
            
            # Insert into Cassandra (parameterized — terms/titles may contain quotes)
            postings_str = str(doc_data)
            session.execute(
                """
                INSERT INTO inverted_index (term, document_frequency, num_docs, postings_data)
                VALUES (%s, %s, %s, %s)
                """,
                (term, df, total_docs_in_term, postings_str),
            )
            
        except Exception as e:
            print(f"Error parsing line: {line[:50]}... Error: {e}")
            continue
    
    print(f"Successfully loaded {len(index_lines)} index entries")
    
    # Store documents and statistics
    print("Storing document metadata...")
    total_length = 0
    for doc_id, title in doc_titles.items():
        doc_len = doc_lengths.get(doc_id, 0)
        total_length += doc_len
        
        session.execute(
            """
            INSERT INTO documents (doc_id, title, doc_length)
            VALUES (%s, %s, %s)
            """,
            (doc_id, title, doc_len),
        )
    
    # Store statistics for BM25
    num_docs = len(doc_titles)
    avg_doc_length = total_length / num_docs if num_docs > 0 else 0
    
    session.execute(
        "INSERT INTO statistics (key, value) VALUES (%s, %s)",
        ("num_docs", str(num_docs)),
    )
    session.execute(
        "INSERT INTO statistics (key, value) VALUES (%s, %s)",
        ("avg_doc_length", f"{avg_doc_length:.2f}"),
    )
    
    print(f"Stored {num_docs} documents with average length {avg_doc_length:.2f}")
    print("=== Cassandra Loading Complete ===")

def main():
    print("=== Cassandra Storage Phase ===")
    
    # Connect to Cassandra
    cluster, session = connect_cassandra()
    
    try:
        # Create keyspace and tables
        create_keyspace(session)
        create_tables(session)
        
        # Load index from HDFS
        load_index_from_hdfs(session)
        
        print("\nVerifying data in Cassandra...")
        session.set_keyspace(KEYSPACE)
        
        # Count index entries
        result = session.execute("SELECT COUNT(*) as count FROM inverted_index")
        count = result[0].count if result else 0
        print(f"Total terms in index: {count}")
        
        # Count documents
        result = session.execute("SELECT COUNT(*) as count FROM documents")
        count = result[0].count if result else 0
        print(f"Total documents: {count}")
        
        # Get statistics
        result = session.execute("SELECT * FROM statistics")
        for row in result:
            print(f"Statistic {row.key}: {row.value}")
        
        print("\n=== Cassandra Setup Complete ===")
        
    finally:
        session.shutdown()
        cluster.shutdown()

if __name__ == "__main__":
    main()
