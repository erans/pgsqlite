#!/usr/bin/env python3
"""Compare text vs binary format performance using Unix sockets"""

import time
import psycopg2
import psycopg
import statistics
import subprocess
import os

# Configuration
PORT = 5441
SOCKET_DIR = "/tmp"
OPERATIONS = 100

def start_server():
    """Start pgsqlite server"""
    print("Starting pgsqlite server on Unix socket...")
    proc = subprocess.Popen([
        "/home/eran/work/pgsqlite/target/release/pgsqlite",
        "--database", "benchmark_unix.db",
        "--port", str(PORT)
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    return proc

def run_text_benchmark():
    """Run benchmark with text format using psycopg2"""
    print("\n=== TEXT FORMAT BENCHMARK (psycopg2, Unix socket) ===")
    
    conn = psycopg2.connect(
        host=SOCKET_DIR,
        port=PORT,
        dbname="benchmark_unix.db",
        user="dummy",
        sslmode="disable"
    )
    cur = conn.cursor()
    
    # Create table
    cur.execute("DROP TABLE IF EXISTS perf_test")
    cur.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, value INTEGER, data TEXT)")
    conn.commit()
    
    # Warmup
    for i in range(10):
        cur.execute("INSERT INTO perf_test VALUES (%s, %s, %s)", (1000+i, i, f"warmup_{i}"))
    conn.commit()
    cur.execute("DELETE FROM perf_test WHERE id >= 1000")
    conn.commit()
    
    # INSERT benchmark
    insert_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO perf_test (id, value, data) VALUES (%s, %s, %s)", 
                   (i, i * 10, f"data_{i}"))
        conn.commit()
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # UPDATE benchmark
    update_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        cur.execute("UPDATE perf_test SET value = %s, data = %s WHERE id = %s", 
                   (i * 20, f"updated_{i}", i))
        conn.commit()
        update_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark
    select_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT id, value, data FROM perf_test WHERE id = %s", (i,))
        result = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # DELETE benchmark
    delete_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        cur.execute("DELETE FROM perf_test WHERE id = %s", (i,))
        conn.commit()
        delete_times.append((time.perf_counter() - start) * 1000)
    
    conn.close()
    
    # Remove outliers (top and bottom 10%)
    def remove_outliers(times):
        sorted_times = sorted(times)
        trim_count = len(times) // 10
        return sorted_times[trim_count:-trim_count] if trim_count > 0 else sorted_times
    
    return {
        "INSERT": statistics.mean(remove_outliers(insert_times)),
        "UPDATE": statistics.mean(remove_outliers(update_times)),
        "SELECT": statistics.mean(remove_outliers(select_times)),
        "DELETE": statistics.mean(remove_outliers(delete_times))
    }

def run_binary_benchmark():
    """Run benchmark with binary format using psycopg3"""
    print("\n=== BINARY FORMAT BENCHMARK (psycopg3, Unix socket) ===")
    
    conn = psycopg.connect(
        host=SOCKET_DIR,
        port=PORT,
        dbname="benchmark_unix.db",
        user="dummy",
        sslmode="disable"
    )
    
    # Create table with text cursor
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS binary_test")
        cur.execute("CREATE TABLE binary_test (id INTEGER PRIMARY KEY, value INTEGER, data TEXT)")
    conn.commit()
    
    # Warmup
    for i in range(10):
        with conn.cursor(binary=True) as cur:
            cur.execute("INSERT INTO binary_test VALUES (%s, %s, %s)", (1000+i, i, f"warmup_{i}"))
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM binary_test WHERE id >= 1000")
    conn.commit()
    
    # INSERT benchmark with binary cursor
    insert_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        with conn.cursor(binary=True) as cur:
            cur.execute("INSERT INTO binary_test (id, value, data) VALUES (%s, %s, %s)", 
                       (i, i * 10, f"data_{i}"))
        conn.commit()
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # UPDATE benchmark with binary cursor
    update_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        with conn.cursor(binary=True) as cur:
            cur.execute("UPDATE binary_test SET value = %s, data = %s WHERE id = %s", 
                       (i * 20, f"updated_{i}", i))
        conn.commit()
        update_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark with binary cursor
    select_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        with conn.cursor(binary=True) as cur:
            cur.execute("SELECT id, value, data FROM binary_test WHERE id = %s", (i,))
            result = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # DELETE benchmark with binary cursor
    delete_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        with conn.cursor(binary=True) as cur:
            cur.execute("DELETE FROM binary_test WHERE id = %s", (i,))
        conn.commit()
        delete_times.append((time.perf_counter() - start) * 1000)
    
    conn.close()
    
    # Remove outliers
    def remove_outliers(times):
        sorted_times = sorted(times)
        trim_count = len(times) // 10
        return sorted_times[trim_count:-trim_count] if trim_count > 0 else sorted_times
    
    return {
        "INSERT": statistics.mean(remove_outliers(insert_times)),
        "UPDATE": statistics.mean(remove_outliers(update_times)),
        "SELECT": statistics.mean(remove_outliers(select_times)),
        "DELETE": statistics.mean(remove_outliers(delete_times))
    }

def main():
    # Clean up old database
    try:
        os.remove("benchmark_unix.db")
    except:
        pass
        
    server = start_server()
    
    try:
        print("Performance Comparison: Text vs Binary Format (Unix Sockets)")
        print("=" * 70)
        print(f"Unix socket: {SOCKET_DIR}/.s.PGSQL.{PORT}")
        print(f"Operations per test: {OPERATIONS}")
        print(f"Outliers removed: top and bottom 10%")
        
        # Run text format benchmark
        text_results = run_text_benchmark()
        
        # Run binary format benchmark
        binary_results = run_binary_benchmark()
        
        # Print results
        print("\n" + "=" * 70)
        print("RESULTS (average ms per operation):")
        print("-" * 70)
        print(f"{'Operation':<10} {'Text (ms)':<15} {'Binary (ms)':<15} {'Difference':<15}")
        print("-" * 70)
        
        for op in ["INSERT", "UPDATE", "SELECT", "DELETE"]:
            text_time = text_results[op]
            binary_time = binary_results[op]
            diff_pct = ((binary_time / text_time - 1) * 100) if text_time > 0 else 0
            diff_str = f"{diff_pct:+.1f}%" if abs(diff_pct) < 1000 else f"{diff_pct:+.0f}%"
            print(f"{op:<10} {text_time:<15.3f} {binary_time:<15.3f} {diff_str:<15}")
        
        # Summary
        print("\n" + "=" * 70)
        print("PERFORMANCE SUMMARY:")
        print("-" * 70)
        
        # Check DML operations
        dml_ops = ["INSERT", "UPDATE", "DELETE"]
        dml_regressions = []
        for op in dml_ops:
            ratio = binary_results[op] / text_results[op]
            if ratio > 1.5:  # More than 50% slower
                dml_regressions.append(f"{op}: {ratio:.1f}x slower")
        
        if not dml_regressions:
            print("✅ DML operations: Binary format performance is GOOD!")
            print("   Fast path optimization is working correctly.")
        else:
            print("❌ DML operations still have regressions:")
            for reg in dml_regressions:
                print(f"   - {reg}")
                
        # Check SELECT performance
        select_ratio = binary_results["SELECT"] / text_results["SELECT"]
        if select_ratio < 1.2:
            print(f"✅ SELECT operations: Binary format is {(1-select_ratio)*100:.1f}% faster")
        else:
            print(f"❌ SELECT operations: Binary format is {(select_ratio-1)*100:.1f}% slower")
            
    finally:
        server.terminate()
        server.wait()
        print("\nServer stopped.")

if __name__ == "__main__":
    main()