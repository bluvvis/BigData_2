#!/usr/bin/env python3
"""
Query processor using BM25 ranking algorithm
Reads query from command line, retrieves documents from Cassandra,
and ranks them using BM25 score
"""

import ast
import sys
import re
import math
from cassandra.cluster import Cluster

# BM25 parameters
K1 = 1.5
B = 0.75

class BM25Ranker:
    def __init__(self, num_docs, avg_doc_length):
        self.num_docs = num_docs
        self.avg_doc_length = avg_doc_length
    
    def idf(self, doc_frequency):
        """Calculate IDF (Inverse Document Frequency)"""
        return math.log((self.num_docs - doc_frequency + 0.5) / (doc_frequency + 0.5))
    
    def bm25(self, term_freq, doc_length, idf):
        """Calculate BM25 score for a single term"""
        numerator = term_freq * (K1 + 1)
        denominator = term_freq + K1 * (1 - B + B * (doc_length / self.avg_doc_length))
        return idf * (numerator / denominator)

def tokenize(text):
    """Tokenize query"""
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text)
    tokens = text.split()
    return [t for t in tokens if len(t) >= 2]

def search(query_text, session):
    """Execute search query"""
    session.set_keyspace('search_engine')
    
    # Get statistics
    rows = session.execute("SELECT * FROM statistics")
    stats = {row.key: row.value for row in rows}
    
    num_docs = int(stats.get('num_docs', 1))
    avg_doc_length = float(stats.get('avg_doc_length', 100))
    
    print(f"[DEBUG] Total documents: {num_docs}, Avg length: {avg_doc_length:.2f}", file=sys.stderr)
    
    # Initialize ranker
    ranker = BM25Ranker(num_docs, avg_doc_length)
    
    # Tokenize query
    query_terms = tokenize(query_text)
    print(f"[DEBUG] Query terms: {query_terms}", file=sys.stderr)
    
    if not query_terms:
        print("No valid query terms")
        return []
    
    # Collect documents and their scores
    doc_scores = {}  # doc_id -> bm25_score
    doc_lengths = {}  # doc_id -> length
    doc_titles = {}  # doc_id -> title
    
    # Search for each query term
    for term in query_terms:
        try:
            # Get index entry for this term (parameterized — safe for any characters in term)
            row = session.execute(
                "SELECT * FROM inverted_index WHERE term = %s",
                (term,),
            ).one()
            if row is None:
                print(f"[DEBUG] Term '{term}' not found in index", file=sys.stderr)
                continue
            doc_frequency = row.document_frequency
            idf_score = ranker.idf(doc_frequency)
            
            print(f"[DEBUG] Term '{term}': df={doc_frequency}, idf={idf_score:.4f}", file=sys.stderr)
            
            # Parse postings data
            postings_str = row.postings_data
            # Postings format: {doc_id: [(term_freq, doc_length), ...], ...}
            try:
                postings = ast.literal_eval(postings_str)
            except (ValueError, SyntaxError):
                print(f"[DEBUG] Error parsing postings for term {term}", file=sys.stderr)
                continue
            
            # Score each document
            for doc_id, term_data_list in postings.items():
                if term_data_list:
                    term_freq, doc_length = term_data_list[0]
                    
                    # Calculate BM25 for this term in this document
                    score = ranker.bm25(term_freq, doc_length, idf_score)
                    
                    if doc_id not in doc_scores:
                        doc_scores[doc_id] = 0
                    doc_scores[doc_id] += score
                    doc_lengths[doc_id] = doc_length
        
        except Exception as e:
            print(f"[DEBUG] Error searching for term '{term}': {e}", file=sys.stderr)
            continue
    
    # Get document titles from metadata
    if doc_scores:
        query = "SELECT doc_id, title FROM documents"
        rows = session.execute(query)
        for row in rows:
            doc_titles[row.doc_id] = row.title
    
    # Sort documents by score
    ranked_docs = sorted(doc_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Return top 10 documents
    top_k = 10
    results = []
    for doc_id, score in ranked_docs[:top_k]:
        title = doc_titles.get(doc_id, "Unknown")
        results.append((doc_id, title, score))
    
    return results

def main():
    """Main entry point"""
    if len(sys.argv) < 2:
        print("Usage: query.py '<query_text>'", file=sys.stderr)
        print("No query provided, using default", file=sys.stderr)
        query_text = "information retrieval"
    else:
        query_text = sys.argv[1]

    print(f"[DEBUG] Query: '{query_text}'", file=sys.stderr)
    print("", file=sys.stderr)

    spark = None
    cluster = None
    session = None
    try:
        # spark-submit expects a Spark application; touch RDD API (course requirement).
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("bm25-query").getOrCreate()
        spark.sparkContext.parallelize([query_text], 1).count()

        cluster = Cluster(["cassandra-server"])
        session = cluster.connect()
        print("[DEBUG] Connected to Cassandra", file=sys.stderr)

        results = search(query_text, session)

        print("\n========================================")
        print(f"SEARCH RESULTS FOR: '{query_text}'")
        print("========================================\n")

        if results:
            for i, (doc_id, title, score) in enumerate(results, 1):
                print(f"{i}. [{doc_id}] {title}")
                print(f"   Score: {score:.6f}\n")
        else:
            print("No documents found matching the query")

        print(f"Total results: {len(results)}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
    finally:
        if session is not None:
            session.shutdown()
        if cluster is not None:
            cluster.shutdown()
        if spark is not None:
            spark.stop()

if __name__ == "__main__":
    main()
