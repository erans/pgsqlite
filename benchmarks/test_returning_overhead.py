#!/usr/bin/env python3
"""Test if RETURNING clause handling has different overhead in binary format."""

import psycopg2
import psycopg
import time
import statistics

PORT = 15432
ITERATIONS = 50

def test_text_with_returning():
    """Test text format with RETURNING."""
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS test_returning_text")
    cur.execute("CREATE TABLE test_returning_text (id SERIAL PRIMARY KEY, value TEXT)")
    conn.commit()
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO test_returning_text (value) VALUES (%s) RETURNING id", (f"test_{i}",))
        result_id = cur.fetchone()[0]
        conn.commit()
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    return statistics.mean(times)

def test_binary_with_returning():
    """Test binary format with RETURNING."""
    conn = psycopg.connect(f"host=/tmp port={PORT} dbname=main user=postgres", autocommit=True)
    cur = conn.cursor(binary=True)
    
    cur.execute("DROP TABLE IF EXISTS test_returning_binary")
    cur.execute("CREATE TABLE test_returning_binary (id SERIAL PRIMARY KEY, value TEXT)")
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO test_returning_binary (value) VALUES (%s) RETURNING id", (f"test_{i}",))
        result_id = cur.fetchone()[0]
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    return statistics.mean(times)

def test_text_without_returning():
    """Test text format without RETURNING."""
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS test_no_returning_text")
    cur.execute("CREATE TABLE test_no_returning_text (id SERIAL PRIMARY KEY, value TEXT)")
    conn.commit()
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO test_no_returning_text (value) VALUES (%s)", (f"test_{i}",))
        conn.commit()
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    return statistics.mean(times)

def test_binary_without_returning():
    """Test binary format without RETURNING."""
    conn = psycopg.connect(f"host=/tmp port={PORT} dbname=main user=postgres", autocommit=True)
    cur = conn.cursor(binary=True)
    
    cur.execute("DROP TABLE IF EXISTS test_no_returning_binary")
    cur.execute("CREATE TABLE test_no_returning_binary (id SERIAL PRIMARY KEY, value TEXT)")
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO test_no_returning_binary (value) VALUES (%s)", (f"test_{i}",))
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    return statistics.mean(times)

def main():
    print("Testing RETURNING clause impact on binary format...\n")
    
    text_with = test_text_with_returning()
    binary_with = test_binary_with_returning()
    text_without = test_text_without_returning()
    binary_without = test_binary_without_returning()
    
    print(f"With RETURNING:")
    print(f"  Text:   {text_with:.3f}ms")
    print(f"  Binary: {binary_with:.3f}ms ({binary_with/text_with:.1f}x slower)")
    
    print(f"\nWithout RETURNING:")
    print(f"  Text:   {text_without:.3f}ms")
    print(f"  Binary: {binary_without:.3f}ms ({binary_without/text_without:.1f}x slower)")
    
    print(f"\nRETURNING overhead:")
    print(f"  Text:   {text_with - text_without:.3f}ms")
    print(f"  Binary: {binary_with - binary_without:.3f}ms")

if __name__ == "__main__":
    main()