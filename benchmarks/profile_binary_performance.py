#!/usr/bin/env python3
"""Profile binary vs text format performance to identify bottlenecks."""

import psycopg2
import psycopg
import time
import statistics

# Test configuration
ITERATIONS = 100
PORT = 15432

def profile_text_format():
    """Profile text format performance."""
    conn = psycopg2.connect(
        host='/tmp',
        port=PORT,
        dbname='main',
        user='postgres'
    )
    cur = conn.cursor()
    
    # Create test table
    cur.execute("DROP TABLE IF EXISTS perf_test")
    cur.execute("""
        CREATE TABLE perf_test (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER,
            price DECIMAL(10,2)
        )
    """)
    conn.commit()
    
    # Profile INSERT operations
    insert_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "INSERT INTO perf_test (id, name, value, price) VALUES (%s, %s, %s, %s)",
            (i, f"test_{i}", i * 10, i * 1.5)
        )
        conn.commit()
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # Profile SELECT operations
    select_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT * FROM perf_test WHERE id = %s", (i,))
        result = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    
    return {
        'insert_avg': statistics.mean(insert_times),
        'insert_median': statistics.median(insert_times),
        'select_avg': statistics.mean(select_times),
        'select_median': statistics.median(select_times)
    }

def profile_binary_format():
    """Profile binary format performance."""
    conn = psycopg.connect(
        f"host=/tmp port={PORT} dbname=main user=postgres",
        autocommit=True
    )
    
    # Create cursor with binary format
    cur = conn.cursor(binary=True)
    
    # Create test table
    cur.execute("DROP TABLE IF EXISTS perf_test_binary")
    cur.execute("""
        CREATE TABLE perf_test_binary (
            id INTEGER PRIMARY KEY,
            name TEXT,
            value INTEGER,
            price DECIMAL(10,2)
        )
    """)
    
    # Profile INSERT operations
    insert_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "INSERT INTO perf_test_binary (id, name, value, price) VALUES (%s, %s, %s, %s)",
            (i, f"test_{i}", i * 10, i * 1.5)
        )
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # Profile SELECT operations
    select_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT * FROM perf_test_binary WHERE id = %s", (i,))
        result = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    cur.close()
    conn.close()
    
    return {
        'insert_avg': statistics.mean(insert_times),
        'insert_median': statistics.median(insert_times),
        'select_avg': statistics.mean(select_times),
        'select_median': statistics.median(select_times)
    }

def main():
    print("Profiling text format performance...")
    text_results = profile_text_format()
    
    print("Profiling binary format performance...")
    binary_results = profile_binary_format()
    
    print("\n=== PERFORMANCE COMPARISON ===")
    print(f"Operations: {ITERATIONS} iterations each\n")
    
    print("INSERT Performance:")
    print(f"  Text format:   avg={text_results['insert_avg']:.3f}ms, median={text_results['insert_median']:.3f}ms")
    print(f"  Binary format: avg={binary_results['insert_avg']:.3f}ms, median={binary_results['insert_median']:.3f}ms")
    print(f"  Binary overhead: {binary_results['insert_avg'] / text_results['insert_avg']:.1f}x slower\n")
    
    print("SELECT Performance:")
    print(f"  Text format:   avg={text_results['select_avg']:.3f}ms, median={text_results['select_median']:.3f}ms")
    print(f"  Binary format: avg={binary_results['select_avg']:.3f}ms, median={binary_results['select_median']:.3f}ms")
    print(f"  Binary overhead: {binary_results['select_avg'] / text_results['select_avg']:.1f}x slower")

if __name__ == "__main__":
    main()