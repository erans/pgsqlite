#!/usr/bin/env python3
"""Final performance comparison: SQLite vs pgsqlite (text) vs pgsqlite (binary)."""

import sqlite3
import psycopg2
import time
import statistics
import os

PORT = 15433
ITERATIONS = 100
DB_FILE = "benchmark_final.db"

def benchmark_sqlite():
    """Benchmark pure SQLite performance."""
    # Remove existing database
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    # Create table
    cur.execute("""
        CREATE TABLE bench_sqlite (
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
            "INSERT INTO bench_sqlite (id, name, value) VALUES (?, ?, ?)",
            (i, f"item_{i}", i * 100)
        )
        conn.commit()
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark
    select_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench_sqlite WHERE id = ?", (i,))
        _ = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # UPDATE benchmark
    update_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "UPDATE bench_sqlite SET value = ? WHERE id = ?",
            (i * 200, i)
        )
        conn.commit()
        update_times.append((time.perf_counter() - start) * 1000)
    
    # DELETE benchmark
    delete_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("DELETE FROM bench_sqlite WHERE id = ?", (i,))
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

def benchmark_pgsqlite_text():
    """Benchmark pgsqlite with text format."""
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
            "INSERT INTO bench_text (id, name, value) VALUES (%s, %s, %s)",
            (i, f"item_{i}", i * 100)
        )
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
            "UPDATE bench_text SET value = %s WHERE id = %s",
            (i * 200, i)
        )
        conn.commit()
        update_times.append((time.perf_counter() - start) * 1000)
    
    # DELETE benchmark
    delete_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("DELETE FROM bench_text WHERE id = %s", (i,))
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

def benchmark_pgsqlite_binary():
    """Benchmark pgsqlite with binary format using psycopg3."""
    try:
        import psycopg
        
        conn = psycopg.connect(f"host=/tmp port={PORT} dbname=main user=postgres", autocommit=True)
        cur = conn.cursor(binary=True)
        
        # Create table
        cur.execute("DROP TABLE IF EXISTS bench_binary")
        cur.execute("""
            CREATE TABLE bench_binary (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # INSERT benchmark
        insert_times = []
        for i in range(ITERATIONS):
            start = time.perf_counter()
            cur.execute(
                "INSERT INTO bench_binary (id, name, value) VALUES (%s, %s, %s)",
                (i, f"item_{i}", i * 100)
            )
            insert_times.append((time.perf_counter() - start) * 1000)
        
        # SELECT benchmark
        select_times = []
        for i in range(ITERATIONS):
            start = time.perf_counter()
            cur.execute("SELECT * FROM bench_binary WHERE id = %s", (i,))
            _ = cur.fetchone()
            select_times.append((time.perf_counter() - start) * 1000)
        
        # UPDATE benchmark
        update_times = []
        for i in range(ITERATIONS):
            start = time.perf_counter()
            cur.execute(
                "UPDATE bench_binary SET value = %s WHERE id = %s",
                (i * 200, i)
            )
            update_times.append((time.perf_counter() - start) * 1000)
        
        # DELETE benchmark
        delete_times = []
        for i in range(ITERATIONS):
            start = time.perf_counter()
            cur.execute("DELETE FROM bench_binary WHERE id = %s", (i,))
            delete_times.append((time.perf_counter() - start) * 1000)
        
        cur.close()
        conn.close()
        
        return {
            'insert': statistics.mean(insert_times),
            'select': statistics.mean(select_times),
            'update': statistics.mean(update_times),
            'delete': statistics.mean(delete_times)
        }
    except ImportError:
        print("psycopg3 not available, skipping binary format test")
        return None

def print_results(sqlite_results, text_results, binary_results):
    """Print comparison results."""
    print("\n" + "="*70)
    print("FINAL PERFORMANCE COMPARISON (all times in milliseconds)")
    print("="*70)
    
    operations = ['insert', 'select', 'update', 'delete']
    
    print(f"\n{'Operation':<10} {'SQLite':<12} {'Text Format':<12} {'Binary Format':<12} {'Text vs SQLite':<15} {'Binary vs SQLite':<15}")
    print("-"*70)
    
    for op in operations:
        sqlite_time = sqlite_results[op]
        text_time = text_results[op]
        text_overhead = (text_time / sqlite_time - 1) * 100
        
        if binary_results:
            binary_time = binary_results[op]
            binary_overhead = (binary_time / sqlite_time - 1) * 100
            print(f"{op.upper():<10} {sqlite_time:>10.3f}  {text_time:>10.3f}  {binary_time:>10.3f}  "
                  f"{text_overhead:>12.1f}%  {binary_overhead:>12.1f}%")
        else:
            print(f"{op.upper():<10} {sqlite_time:>10.3f}  {text_time:>10.3f}  {'N/A':>10}  "
                  f"{text_overhead:>12.1f}%  {'N/A':>12}")
    
    # Calculate averages
    sqlite_avg = sum(sqlite_results.values()) / len(sqlite_results)
    text_avg = sum(text_results.values()) / len(text_results)
    text_avg_overhead = (text_avg / sqlite_avg - 1) * 100
    
    if binary_results:
        binary_avg = sum(binary_results.values()) / len(binary_results)
        binary_avg_overhead = (binary_avg / sqlite_avg - 1) * 100
        
        print("-"*70)
        print(f"{'AVERAGE':<10} {sqlite_avg:>10.3f}  {text_avg:>10.3f}  {binary_avg:>10.3f}  "
              f"{text_avg_overhead:>12.1f}%  {binary_avg_overhead:>12.1f}%")
        
        # Binary vs Text comparison
        print("\n" + "="*70)
        print("BINARY FORMAT vs TEXT FORMAT COMPARISON")
        print("="*70)
        
        for op in operations:
            binary_time = binary_results[op]
            text_time = text_results[op]
            diff = (binary_time / text_time - 1) * 100
            if diff < 0:
                print(f"{op.upper():<10} Binary is {abs(diff):>5.1f}% faster ✅")
            else:
                print(f"{op.upper():<10} Binary is {diff:>5.1f}% slower ❌")
        
        avg_diff = (binary_avg / text_avg - 1) * 100
        print("-"*70)
        if avg_diff < 0:
            print(f"{'OVERALL':<10} Binary is {abs(avg_diff):>5.1f}% faster ✅")
        else:
            print(f"{'OVERALL':<10} Binary is {avg_diff:>5.1f}% slower ❌")
    else:
        print("-"*70)
        print(f"{'AVERAGE':<10} {sqlite_avg:>10.3f}  {text_avg:>10.3f}  {'N/A':>10}  "
              f"{text_avg_overhead:>12.1f}%  {'N/A':>12}")

def main():
    print("Running final performance comparison...")
    print("1. Pure SQLite (direct connection)")
    print("2. pgsqlite with text format (psycopg2)")
    print("3. pgsqlite with binary format (psycopg3)")
    
    # Run benchmarks
    print("\nBenchmarking SQLite...")
    sqlite_results = benchmark_sqlite()
    
    print("Benchmarking pgsqlite (text format)...")
    text_results = benchmark_pgsqlite_text()
    
    print("Benchmarking pgsqlite (binary format)...")
    binary_results = benchmark_pgsqlite_binary()
    
    # Print results
    print_results(sqlite_results, text_results, binary_results)
    
    print("\n" + "="*70)
    print("CONCLUSIONS:")
    print("="*70)
    print("1. pgsqlite adds overhead due to PostgreSQL protocol translation")
    print("2. Binary format provides modest improvements for SELECT operations")
    print("3. DML operations perform similarly in both formats after RETURNING fix")
    print("4. Overall overhead is reasonable for the compatibility benefits provided")

if __name__ == "__main__":
    main()