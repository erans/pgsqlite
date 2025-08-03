#!/usr/bin/env python3
"""Final comprehensive benchmark: SQLite vs pgsqlite (text) vs pgsqlite (binary) using Unix sockets."""

import sqlite3
import psycopg2
import time
import statistics
import os
import subprocess
import sys

PORT = 15435
ITERATIONS = 1000
DB_FILE = "final_benchmark.db"

def ensure_server_running():
    """Ensure pgsqlite server is running."""
    # Check if server is already running
    result = subprocess.run(['pgrep', '-f', f'pgsqlite.*{PORT}'], capture_output=True)
    if result.returncode == 0:
        print(f"pgsqlite already running on port {PORT}")
        return
    
    # Start server
    print(f"Starting pgsqlite on port {PORT}...")
    subprocess.Popen([
        '../target/release/pgsqlite',
        '--database', DB_FILE,
        '--port', str(PORT)
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)

def benchmark_sqlite():
    """Benchmark pure SQLite performance."""
    print("\n1. Benchmarking pure SQLite...")
    
    # Use the same database file for fair comparison
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    # Create table
    cur.execute("DROP TABLE IF EXISTS bench_sqlite")
    cur.execute("""
        CREATE TABLE bench_sqlite (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # INSERT benchmark
    insert_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "INSERT INTO bench_sqlite (id, name, value, description) VALUES (?, ?, ?, ?)",
            (i, f"item_{i}", i * 100, f"Description for item {i}")
        )
        conn.commit()
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark (single row)
    select_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench_sqlite WHERE id = ?", (i,))
        _ = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark (range)
    range_times = []
    for i in range(0, ITERATIONS, 10):
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench_sqlite WHERE id BETWEEN ? AND ?", (i, i+9))
        _ = cur.fetchall()
        range_times.append((time.perf_counter() - start) * 1000)
    
    # UPDATE benchmark
    update_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "UPDATE bench_sqlite SET value = ?, description = ? WHERE id = ?",
            (i * 200, f"Updated description for item {i}", i)
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
        'select_range': statistics.mean(range_times),
        'update': statistics.mean(update_times),
        'delete': statistics.mean(delete_times)
    }

def benchmark_pgsqlite_text():
    """Benchmark pgsqlite with text format via Unix socket."""
    print("\n2. Benchmarking pgsqlite (text format) via Unix socket...")
    
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    
    # Create table
    cur.execute("DROP TABLE IF EXISTS bench_text")
    cur.execute("""
        CREATE TABLE bench_text (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    
    # INSERT benchmark
    insert_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "INSERT INTO bench_text (id, name, value, description) VALUES (%s, %s, %s, %s)",
            (i, f"item_{i}", i * 100, f"Description for item {i}")
        )
        conn.commit()
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark (single row)
    select_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench_text WHERE id = %s", (i,))
        _ = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark (range)
    range_times = []
    for i in range(0, ITERATIONS, 10):
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench_text WHERE id BETWEEN %s AND %s", (i, i+9))
        _ = cur.fetchall()
        range_times.append((time.perf_counter() - start) * 1000)
    
    # UPDATE benchmark
    update_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "UPDATE bench_text SET value = %s, description = %s WHERE id = %s",
            (i * 200, f"Updated description for item {i}", i)
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
        'select_range': statistics.mean(range_times),
        'update': statistics.mean(update_times),
        'delete': statistics.mean(delete_times)
    }

def benchmark_pgsqlite_binary():
    """Benchmark pgsqlite with binary format via Unix socket."""
    print("\n3. Benchmarking pgsqlite (binary format) via Unix socket...")
    
    try:
        import psycopg
    except ImportError:
        print("   ⚠️  psycopg3 not available, skipping binary format test")
        return None
    
    conn = psycopg.connect(f"host=/tmp port={PORT} dbname=main user=postgres", autocommit=True)
    cur = conn.cursor(binary=True)
    
    # Create table
    cur.execute("DROP TABLE IF EXISTS bench_binary")
    cur.execute("""
        CREATE TABLE bench_binary (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER,
            description TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # INSERT benchmark
    insert_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "INSERT INTO bench_binary (id, name, value, description) VALUES (%s, %s, %s, %s)",
            (i, f"item_{i}", i * 100, f"Description for item {i}")
        )
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark (single row)
    select_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench_binary WHERE id = %s", (i,))
        _ = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark (range)
    range_times = []
    for i in range(0, ITERATIONS, 10):
        start = time.perf_counter()
        cur.execute("SELECT * FROM bench_binary WHERE id BETWEEN %s AND %s", (i, i+9))
        _ = cur.fetchall()
        range_times.append((time.perf_counter() - start) * 1000)
    
    # UPDATE benchmark
    update_times = []
    for i in range(ITERATIONS):
        start = time.perf_counter()
        cur.execute(
            "UPDATE bench_binary SET value = %s, description = %s WHERE id = %s",
            (i * 200, f"Updated description for item {i}", i)
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
        'select_range': statistics.mean(range_times),
        'update': statistics.mean(update_times),
        'delete': statistics.mean(delete_times)
    }

def print_results(sqlite_results, text_results, binary_results):
    """Print formatted comparison results."""
    print("\n" + "="*100)
    print("FINAL UNIX SOCKET PERFORMANCE COMPARISON")
    print(f"Operations: {ITERATIONS} iterations each")
    print("="*100)
    
    operations = ['insert', 'select', 'select_range', 'update', 'delete']
    
    # Header
    print(f"\n{'Operation':<15} {'SQLite':<12} {'PgSqlite Text':<15} {'PgSqlite Binary':<15} {'Text vs SQLite':<15} {'Binary vs SQLite':<15} {'Binary vs Text':<15}")
    print("-"*100)
    
    # Results for each operation
    for op in operations:
        sqlite_time = sqlite_results[op]
        text_time = text_results[op]
        text_overhead = ((text_time / sqlite_time) - 1) * 100
        
        if binary_results:
            binary_time = binary_results[op]
            binary_overhead = ((binary_time / sqlite_time) - 1) * 100
            binary_vs_text = ((binary_time / text_time) - 1) * 100
            
            print(f"{op.upper():<15} {sqlite_time:>10.3f}ms  {text_time:>13.3f}ms  {binary_time:>13.3f}ms  "
                  f"{text_overhead:>12.1f}%  {binary_overhead:>13.1f}%  {binary_vs_text:>13.1f}%")
        else:
            print(f"{op.upper():<15} {sqlite_time:>10.3f}ms  {text_time:>13.3f}ms  {'N/A':>13}  "
                  f"{text_overhead:>12.1f}%  {'N/A':>13}  {'N/A':>13}")
    
    # Averages
    print("-"*100)
    sqlite_avg = sum(sqlite_results.values()) / len(sqlite_results)
    text_avg = sum(text_results.values()) / len(text_results)
    text_avg_overhead = ((text_avg / sqlite_avg) - 1) * 100
    
    if binary_results:
        binary_avg = sum(binary_results.values()) / len(binary_results)
        binary_avg_overhead = ((binary_avg / sqlite_avg) - 1) * 100
        binary_vs_text_avg = ((binary_avg / text_avg) - 1) * 100
        
        print(f"{'AVERAGE':<15} {sqlite_avg:>10.3f}ms  {text_avg:>13.3f}ms  {binary_avg:>13.3f}ms  "
              f"{text_avg_overhead:>12.1f}%  {binary_avg_overhead:>13.1f}%  {binary_vs_text_avg:>13.1f}%")
    else:
        print(f"{'AVERAGE':<15} {sqlite_avg:>10.3f}ms  {text_avg:>13.3f}ms  {'N/A':>13}  "
              f"{text_avg_overhead:>12.1f}%  {'N/A':>13}  {'N/A':>13}")
    
    # Summary
    print("\n" + "="*100)
    print("SUMMARY:")
    print("="*100)
    
    if binary_results:
        if binary_avg < text_avg:
            improvement = ((text_avg / binary_avg) - 1) * 100
            print(f"✅ Binary format is {improvement:.1f}% faster than text format overall")
        else:
            overhead = ((binary_avg / text_avg) - 1) * 100
            print(f"❌ Binary format is {overhead:.1f}% slower than text format overall")
        
        # Per-operation summary
        print("\nPer-operation binary format performance:")
        for op in operations:
            diff = ((binary_results[op] / text_results[op]) - 1) * 100
            if diff < 0:
                print(f"  {op.upper():<15} {abs(diff):>5.1f}% faster ✅")
            else:
                print(f"  {op.upper():<15} {diff:>5.1f}% slower ❌")
    
    print(f"\nOverall pgsqlite overhead vs SQLite:")
    print(f"  Text format:   {text_avg_overhead:>6.1f}% overhead")
    if binary_results:
        print(f"  Binary format: {binary_avg_overhead:>6.1f}% overhead")

def main():
    print("Unix Socket Performance Benchmark")
    print("=" * 50)
    
    # Ensure server is running
    ensure_server_running()
    
    # Run benchmarks
    sqlite_results = benchmark_sqlite()
    text_results = benchmark_pgsqlite_text()
    binary_results = benchmark_pgsqlite_binary()
    
    # Print results
    print_results(sqlite_results, text_results, binary_results)
    
    print("\n" + "="*100)
    print("Benchmark completed!")

if __name__ == "__main__":
    main()