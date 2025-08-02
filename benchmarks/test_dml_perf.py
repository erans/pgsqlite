#!/usr/bin/env python3
"""Test DML operation performance with binary vs text format"""

import time
import psycopg
import psycopg2

def benchmark_text_format():
    """Benchmark using text format with psycopg2"""
    print("\n=== Text Format Benchmark (psycopg2) ===")
    
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        dbname="benchmark_test.db",
        user="dummy",
        password="dummy",
        sslmode="disable"
    )
    cur = conn.cursor()
    
    # Create table
    cur.execute("DROP TABLE IF EXISTS perf_test")
    cur.execute("CREATE TABLE perf_test (id SERIAL PRIMARY KEY, value INTEGER)")
    conn.commit()
    
    # Benchmark INSERT
    start = time.perf_counter()
    for i in range(100):
        cur.execute("INSERT INTO perf_test (value) VALUES (%s)", (i,))
    conn.commit()
    insert_time = (time.perf_counter() - start) / 100 * 1000
    print(f"INSERT avg: {insert_time:.3f} ms")
    
    # Benchmark UPDATE
    start = time.perf_counter()
    for i in range(100):
        cur.execute("UPDATE perf_test SET value = %s WHERE id = %s", (i * 2, i + 1))
    conn.commit()
    update_time = (time.perf_counter() - start) / 100 * 1000
    print(f"UPDATE avg: {update_time:.3f} ms")
    
    # Benchmark SELECT
    start = time.perf_counter()
    for i in range(100):
        cur.execute("SELECT * FROM perf_test WHERE value = %s", (i,))
        cur.fetchall()
    select_time = (time.perf_counter() - start) / 100 * 1000
    print(f"SELECT avg: {select_time:.3f} ms")
    
    # Benchmark DELETE
    start = time.perf_counter()
    for i in range(100):
        cur.execute("DELETE FROM perf_test WHERE id = %s", (i + 1,))
    conn.commit()
    delete_time = (time.perf_counter() - start) / 100 * 1000
    print(f"DELETE avg: {delete_time:.3f} ms")
    
    conn.close()
    return insert_time, update_time, select_time, delete_time

def benchmark_binary_format():
    """Benchmark using binary format with psycopg3"""
    print("\n=== Binary Format Benchmark (psycopg3) ===")
    
    conn = psycopg.connect(
        host="localhost",
        port=5433,
        dbname="benchmark_test.db",
        user="dummy",
        password="dummy",
        sslmode="disable"
    )
    conn.autocommit = True
    
    # Create table
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS perf_test_bin")
        cur.execute("CREATE TABLE perf_test_bin (id SERIAL PRIMARY KEY, value INTEGER)")
    
    # Benchmark INSERT with binary cursor
    start = time.perf_counter()
    with conn.cursor(binary=True) as cur:
        for i in range(100):
            cur.execute("INSERT INTO perf_test_bin (value) VALUES (%s)", (i,))
    insert_time = (time.perf_counter() - start) / 100 * 1000
    print(f"INSERT avg: {insert_time:.3f} ms")
    
    # Benchmark UPDATE with binary cursor
    start = time.perf_counter()
    with conn.cursor(binary=True) as cur:
        for i in range(100):
            cur.execute("UPDATE perf_test_bin SET value = %s WHERE id = %s", (i * 2, i + 1))
    update_time = (time.perf_counter() - start) / 100 * 1000
    print(f"UPDATE avg: {update_time:.3f} ms")
    
    # Benchmark SELECT with binary cursor
    start = time.perf_counter()
    with conn.cursor(binary=True) as cur:
        for i in range(100):
            cur.execute("SELECT * FROM perf_test_bin WHERE value = %s", (i,))
            cur.fetchall()
    select_time = (time.perf_counter() - start) / 100 * 1000
    print(f"SELECT avg: {select_time:.3f} ms")
    
    # Benchmark DELETE with binary cursor
    start = time.perf_counter()
    with conn.cursor(binary=True) as cur:
        for i in range(100):
            cur.execute("DELETE FROM perf_test_bin WHERE id = %s", (i + 1,))
    delete_time = (time.perf_counter() - start) / 100 * 1000
    print(f"DELETE avg: {delete_time:.3f} ms")
    
    conn.close()
    return insert_time, update_time, select_time, delete_time

def main():
    # Start pgsqlite server first
    print("Make sure pgsqlite is running on port 5433")
    
    # Run benchmarks
    text_times = benchmark_text_format()
    binary_times = benchmark_binary_format()
    
    # Compare results
    print("\n=== Performance Comparison ===")
    operations = ["INSERT", "UPDATE", "SELECT", "DELETE"]
    for i, op in enumerate(operations):
        text_time = text_times[i]
        binary_time = binary_times[i]
        diff = binary_time - text_time
        pct = (diff / text_time * 100) if text_time > 0 else 0
        print(f"{op}: Text={text_time:.3f}ms, Binary={binary_time:.3f}ms, Diff={diff:+.3f}ms ({pct:+.1f}%)")

if __name__ == "__main__":
    main()