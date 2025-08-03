#!/usr/bin/env python3
"""Test binary format performance after RETURNING fix."""

import psycopg2
import time
import statistics

PORT = 15433
ITERATIONS = 100

def benchmark_text_format():
    """Benchmark with text format."""
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    
    # Create table
    cur.execute("DROP TABLE IF EXISTS bench_text")
    cur.execute("""
        CREATE TABLE bench_text (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # INSERT benchmark
    insert_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "INSERT INTO bench_text (id, name, value) VALUES (%s, %s, %s) RETURNING id",
            (i, f"item_{i}", i * 100)
        )
        _ = cur.fetchone()
        conn.commit()
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark
    select_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench_text WHERE id = %s", (i,))
        _ = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # UPDATE benchmark
    update_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "UPDATE bench_text SET value = %s WHERE id = %s RETURNING value",
            (i * 200, i)
        )
        _ = cur.fetchone()
        conn.commit()
        update_times.append((time.perf_counter() - start) * 1000)
    
    # DELETE benchmark
    delete_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("DELETE FROM bench_text WHERE id = %s RETURNING id", (i,))
        _ = cur.fetchone()
        conn.commit()
        delete_times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    
    return {
        'insert': statistics.mean(insert_times),
        'select': statistics.mean(select_times),
        'update': statistics.mean(update_times),
        'delete': statistics.mean(delete_times)
    }

def main():
    print("Testing binary format performance after RETURNING fix...\n")
    
    text_results = benchmark_text_format()
    
    print("Text Format Results:")
    print(f"  INSERT: {text_results['insert']:.3f}ms")
    print(f"  SELECT: {text_results['select']:.3f}ms")
    print(f"  UPDATE: {text_results['update']:.3f}ms")
    print(f"  DELETE: {text_results['delete']:.3f}ms")
    print(f"  Average: {sum(text_results.values())/len(text_results):.3f}ms")

if __name__ == "__main__":
    main()