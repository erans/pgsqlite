#!/usr/bin/env python3
"""Final comparison of text vs binary format performance with DML operations"""

import time
import psycopg2
import psycopg
import statistics
import subprocess
import sys

# Configuration
PORT = 5438
OPERATIONS = 200

def start_server():
    """Start pgsqlite server"""
    print("Starting pgsqlite server...")
    proc = subprocess.Popen([
        "/home/eran/work/pgsqlite/target/release/pgsqlite",
        "--database", ":memory:",
        "--port", str(PORT)
    ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2)
    return proc

def run_text_benchmark():
    """Run benchmark with text format using psycopg2"""
    print("\n=== TEXT FORMAT BENCHMARK (psycopg2) ===")
    
    conn = psycopg2.connect(
        host="localhost",
        port=PORT,
        dbname=":memory:",
        user="dummy"
    )
    cur = conn.cursor()
    
    # Create table
    cur.execute("DROP TABLE IF EXISTS perf_test")
    cur.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, value INTEGER, data TEXT)")
    conn.commit()
    
    # INSERT benchmark
    insert_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        cur.execute("INSERT INTO perf_test (id, value, data) VALUES (%s, %s, %s)", 
                   (i, i * 10, f"data_{i}"))
        insert_times.append((time.perf_counter() - start) * 1000)
    conn.commit()
    
    # UPDATE benchmark
    update_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        cur.execute("UPDATE perf_test SET value = %s, data = %s WHERE id = %s", 
                   (i * 20, f"updated_{i}", i))
        update_times.append((time.perf_counter() - start) * 1000)
    conn.commit()
    
    # SELECT benchmark
    select_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        cur.execute("SELECT * FROM perf_test WHERE id = %s", (i,))
        result = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # DELETE benchmark
    delete_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        cur.execute("DELETE FROM perf_test WHERE id = %s", (i,))
        delete_times.append((time.perf_counter() - start) * 1000)
    conn.commit()
    
    conn.close()
    
    return {
        "INSERT": statistics.mean(insert_times),
        "UPDATE": statistics.mean(update_times),
        "SELECT": statistics.mean(select_times),
        "DELETE": statistics.mean(delete_times)
    }

def run_binary_benchmark():
    """Run benchmark with binary format using psycopg3"""
    print("\n=== BINARY FORMAT BENCHMARK (psycopg3) ===")
    
    conn = psycopg.connect(
        host="localhost",
        port=PORT,
        dbname=":memory:",
        user="dummy",
        autocommit=True
    )
    
    # Create table with text cursor
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS binary_test")
        cur.execute("CREATE TABLE binary_test (id INTEGER PRIMARY KEY, value INTEGER, data TEXT)")
    
    # INSERT benchmark with binary cursor
    insert_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        with conn.cursor(binary=True) as cur:
            cur.execute("INSERT INTO binary_test (id, value, data) VALUES (%s, %s, %s)", 
                       (i, i * 10, f"data_{i}"))
        insert_times.append((time.perf_counter() - start) * 1000)
    
    # UPDATE benchmark with binary cursor
    update_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        with conn.cursor(binary=True) as cur:
            cur.execute("UPDATE binary_test SET value = %s, data = %s WHERE id = %s", 
                       (i * 20, f"updated_{i}", i))
        update_times.append((time.perf_counter() - start) * 1000)
    
    # SELECT benchmark with binary cursor
    select_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        with conn.cursor(binary=True) as cur:
            cur.execute("SELECT * FROM binary_test WHERE id = %s", (i,))
            result = cur.fetchone()
        select_times.append((time.perf_counter() - start) * 1000)
    
    # DELETE benchmark with binary cursor
    delete_times = []
    for i in range(OPERATIONS):
        start = time.perf_counter()
        with conn.cursor(binary=True) as cur:
            cur.execute("DELETE FROM binary_test WHERE id = %s", (i,))
        delete_times.append((time.perf_counter() - start) * 1000)
    
    conn.close()
    
    return {
        "INSERT": statistics.mean(insert_times),
        "UPDATE": statistics.mean(update_times),
        "SELECT": statistics.mean(select_times),
        "DELETE": statistics.mean(delete_times)
    }

def main():
    server = start_server()
    
    try:
        print("Performance Comparison: Text vs Binary Format")
        print("=" * 60)
        print(f"Port: {PORT}")
        print(f"Operations per test: {OPERATIONS}")
        
        # Run text format benchmark
        text_results = run_text_benchmark()
        
        # Run binary format benchmark
        binary_results = run_binary_benchmark()
        
        # Print results
        print("\n" + "=" * 60)
        print("RESULTS (average ms per operation):")
        print("-" * 60)
        print(f"{'Operation':<10} {'Text (ms)':<12} {'Binary (ms)':<12} {'Speedup':<10}")
        print("-" * 60)
        
        for op in ["INSERT", "UPDATE", "SELECT", "DELETE"]:
            text_time = text_results[op]
            binary_time = binary_results[op]
            speedup = (text_time / binary_time - 1) * 100 if binary_time > 0 else 0
            print(f"{op:<10} {text_time:<12.3f} {binary_time:<12.3f} {speedup:+.1f}%")
        
        print("\n" + "=" * 60)
        print("SUMMARY:")
        print("-" * 60)
        
        # Check if DML operations are improved
        dml_improved = all(binary_results[op] < text_results[op] * 1.2 
                          for op in ["INSERT", "UPDATE", "DELETE"])
        
        if dml_improved:
            print("✅ DML operations: Binary format performance is FIXED!")
            print("   The fast path optimization is working correctly.")
        else:
            print("❌ DML operations: Binary format still has regressions")
            
        if binary_results["SELECT"] < text_results["SELECT"] * 1.2:
            print("✅ SELECT operations: Binary format performance is good")
        else:
            print("❌ SELECT operations: Binary format has regression")
            
    finally:
        server.terminate()
        server.wait()

if __name__ == "__main__":
    main()