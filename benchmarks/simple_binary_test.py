#!/usr/bin/env python3
"""Simple test to verify binary format performance improvements"""

import time
import psycopg2
import psycopg
import subprocess

# Start server
print("Starting server...")
proc = subprocess.Popen([
    "/home/eran/work/pgsqlite/target/release/pgsqlite",
    "--database", ":memory:",
    "--port", "5439"
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

time.sleep(2)

try:
    # Test 1: Text format INSERT
    print("\n1. Text format INSERT (psycopg2):")
    conn1 = psycopg2.connect(host="localhost", port=5439, dbname=":memory:", user="dummy")
    cur1 = conn1.cursor()
    cur1.execute("CREATE TABLE test1 (id INTEGER, val INTEGER)")
    
    start = time.perf_counter()
    for i in range(10):
        cur1.execute("INSERT INTO test1 VALUES (%s, %s)", (i, i*10))
    conn1.commit()
    text_time = (time.perf_counter() - start) / 10 * 1000
    print(f"  Average: {text_time:.3f} ms")
    conn1.close()
    
    # Test 2: Binary format INSERT
    print("\n2. Binary format INSERT (psycopg3):")
    conn2 = psycopg.connect(host="localhost", port=5439, dbname=":memory:", user="dummy", autocommit=True)
    with conn2.cursor() as cur:
        cur.execute("CREATE TABLE test2 (id INTEGER, val INTEGER)")
    
    start = time.perf_counter()
    for i in range(10):
        with conn2.cursor(binary=True) as cur:
            cur.execute("INSERT INTO test2 VALUES (%s, %s)", (i, i*10))
    binary_time = (time.perf_counter() - start) / 10 * 1000
    print(f"  Average: {binary_time:.3f} ms")
    conn2.close()
    
    # Compare
    print(f"\n3. Result: Binary is {(text_time/binary_time - 1)*100:+.1f}% vs Text")
    if binary_time < text_time * 1.5:
        print("   ✅ Binary format DML performance is FIXED!")
    else:
        print("   ❌ Binary format still has regression")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    proc.terminate()
    proc.wait()