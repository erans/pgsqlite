#!/usr/bin/env python3
"""Test if RETURNING clause fix improves performance."""

import psycopg2
import time
import statistics

PORT = 15432
ITERATIONS = 50

def test_with_returning():
    """Test with RETURNING."""
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS test_returning")
    cur.execute("CREATE TABLE test_returning (id INTEGER PRIMARY KEY, value TEXT)")
    conn.commit()
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO test_returning (id, value) VALUES (%s, %s) RETURNING id", (i, f"test_{i}"))
        result_id = cur.fetchone()[0]
        conn.commit()
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    return statistics.mean(times)

def test_without_returning():
    """Test without RETURNING."""
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    
    cur.execute("DROP TABLE IF EXISTS test_no_returning")
    cur.execute("CREATE TABLE test_no_returning (id INTEGER PRIMARY KEY, value TEXT)")
    conn.commit()
    
    times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO test_no_returning (id, value) VALUES (%s, %s)", (i, f"test_{i}"))
        conn.commit()
        times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    return statistics.mean(times)

def main():
    print("Testing RETURNING clause performance after fix...\n")
    
    with_returning = test_with_returning()
    without_returning = test_without_returning()
    
    print(f"Without RETURNING: {without_returning:.3f}ms")
    print(f"With RETURNING:    {with_returning:.3f}ms")
    print(f"Overhead:          {with_returning - without_returning:.3f}ms ({with_returning/without_returning:.1f}x slower)")
    
    if with_returning / without_returning < 1.5:
        print("\n✅ RETURNING overhead is reasonable (< 50%)")
    else:
        print(f"\n❌ RETURNING overhead is too high ({(with_returning/without_returning - 1)*100:.0f}%)")

if __name__ == "__main__":
    main()