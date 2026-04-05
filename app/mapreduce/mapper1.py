#!/usr/bin/env python3
"""
Mapper for creating an inverted index
Input: <doc_id> <doc_title> <doc_text>
Output: <term> <doc_id>|<title>|<term_freq>|<doc_length>
"""

import sys
import re
from collections import Counter

def tokenize(text):
    """
    Tokenize text: lowercase, remove punctuation, split on whitespace
    """
    # Convert to lowercase
    text = text.lower()
    # Remove special characters, keep only alphanumeric and spaces
    text = re.sub(r'[^a-z0-9\s]', '', text)
    # Split into tokens
    tokens = text.split()
    return tokens

def main():
    for line in sys.stdin:
        line = line.rstrip('\n')
        
        # Parse input: <doc_id> <doc_title> <doc_text>
        parts = line.split('\t', 2)
        if len(parts) < 3:
            continue
            
        doc_id = parts[0].strip()
        title = parts[1].strip()
        text = parts[2].strip()
        
        # Tokenize the document text
        tokens = tokenize(text)
        
        if not tokens:
            continue
        
        # Calculate term frequencies
        term_freqs = Counter(tokens)
        doc_length = len(tokens)
        
        # Emit: <term> <doc_id>|<title>|<term_freq>|<doc_length>
        for term, freq in term_freqs.items():
            # Skip very short terms (noise)
            if len(term) < 2:
                continue
                
            output = f"{term}\t{doc_id}|{title}|{freq}|{doc_length}"
            print(output)

if __name__ == "__main__":
    main()
