#!/usr/bin/env python3
"""Compare performance before and after RETURNING fix."""

import psycopg2
import time

PORT = 15433

def test_returning_performance():
    """Test INSERT with RETURNING performance."""
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    
    # Create table
    cur.execute("DROP TABLE IF EXISTS test_perf")
    cur.execute("CREATE TABLE test_perf (id INTEGER PRIMARY KEY, value TEXT)")
    conn.commit()
    
    # Warm up
    for i in range(10):
        cur.execute("INSERT INTO test_perf (id, value) VALUES (%s, %s) RETURNING id", (i, f"test_{i}"))
        cur.fetchone()
        conn.commit()
    
    # Measure performance
    start = time.perf_counter()
    for i in range(10, 110):
        cur.execute("INSERT INTO test_perf (id, value) VALUES (%s, %s) RETURNING id", (i, f"test_{i}"))
        cur.fetchone()
        conn.commit()
    elapsed = time.perf_counter() - start
    
    cur.close()
    conn.close()
    
    return elapsed / 100 * 1000  # ms per operation

def main():
    print("Testing RETURNING performance after fix...")
    avg_time = test_returning_performance()
    print(f"\nAverage INSERT with RETURNING: {avg_time:.3f}ms")
    print("\nBefore fix: ~1.39ms (12.7x slower than without RETURNING)")
    print(f"After fix:  {avg_time:.3f}ms")
    
    if avg_time < 0.3:
        print("\n✅ RETURNING performance is now excellent!")
    else:
        print("\n⚠️  RETURNING performance could be better")

if __name__ == "__main__":
    main()