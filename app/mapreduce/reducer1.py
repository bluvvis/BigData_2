#!/usr/bin/env python3
"""
Reducer for inverting and aggregating the index
Input: <term> <doc_id>|<title>|<term_freq>|<doc_length> (multiple lines per term)
Output: <term> <df>|<doc1_id>|<doc1_title>|<doc1_freq>|<doc1_len>|<doc2_id>|...
        where df = document frequency (how many docs contain this term)
"""

import sys
from collections import defaultdict

def main():
    current_term = None
    postings = []
    doc_freqs = defaultdict(int)
    
    for line in sys.stdin:
        line = line.rstrip('\n')
        
        # Parse input: <term> <doc_id>|<title>|<term_freq>|<doc_length>
        parts = line.split('\t', 1)
        if len(parts) < 2:
            continue
            
        term = parts[0].strip()
        posting_data = parts[1].strip()
        
        # When we get a new term, output the previous one
        if current_term and current_term != term:
            output_term(current_term, postings, doc_freqs)
            postings = []
            doc_freqs = defaultdict(int)
        
        # Parse posting: <doc_id>|<title>|<term_freq>|<doc_length>
        posting_parts = posting_data.split('|')
        if len(posting_parts) >= 4:
            doc_id = posting_parts[0]
            title = posting_parts[1]
            term_freq = int(posting_parts[2])
            doc_length = int(posting_parts[3])
            
            postings.append({
                'doc_id': doc_id,
                'title': title,
                'term_freq': term_freq,
                'doc_length': doc_length
            })
            doc_freqs[doc_id] = 1  # Count documents
        
        current_term = term
    
    # Don't forget the last term
    if current_term:
        output_term(current_term, postings, doc_freqs)

def output_term(term, postings, doc_freqs):
    """Output a term with its documents frequency and postings"""
    if not postings:
        return
    
    # Document frequency (number of unique documents containing this term)
    df = len(postings)
    
    # Create posting list: doc_id|title|term_freq|doc_length
    posting_strs = []
    for posting in postings:
        posting_str = f"{posting['doc_id']}|{posting['title']}|{posting['term_freq']}|{posting['doc_length']}"
        posting_strs.append(posting_str)
    
    # Output: <term> <df>|<posting1>|<posting2>|...
    posting_list = '|'.join(posting_strs)
    print(f"{term}\t{df}|{posting_list}")

if __name__ == "__main__":
    main()
