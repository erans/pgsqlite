#!/usr/bin/env python3
"""Test DML performance improvement with binary format"""

import time
import psycopg
import psycopg2
import subprocess
import sys
import os

# Start pgsqlite server
print("Starting pgsqlite server...")
server = subprocess.Popen(
    ["cargo", "run", "--release", "--bin", "pgsqlite", "--", "--database", "test_perf.db", "--port", "5433"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)

# Wait for server to start
time.sleep(3)

try:
    # Test with text format (psycopg2)
    print("\n=== Text Format (psycopg2) ===")
    conn2 = psycopg2.connect(
        host="localhost", port=5433, dbname="test_perf.db",
        user="dummy", password="dummy", sslmode="disable"
    )
    cur2 = conn2.cursor()

    # Create table
    cur2.execute("DROP TABLE IF EXISTS perf_test")
    cur2.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, value INTEGER)")
    conn2.commit()

    # Benchmark 50 INSERTs
    start = time.perf_counter()
    for i in range(50):
        cur2.execute("INSERT INTO perf_test (id, value) VALUES (%s, %s)", (i, i * 10))
    conn2.commit()
    text_insert_time = (time.perf_counter() - start) / 50 * 1000
    print(f"INSERT avg: {text_insert_time:.3f} ms")

    # Benchmark 50 UPDATEs
    start = time.perf_counter()
    for i in range(50):
        cur2.execute("UPDATE perf_test SET value = %s WHERE id = %s", (i * 20, i))
    conn2.commit()
    text_update_time = (time.perf_counter() - start) / 50 * 1000
    print(f"UPDATE avg: {text_update_time:.3f} ms")

    # Benchmark 50 DELETEs
    start = time.perf_counter()
    for i in range(50):
        cur2.execute("DELETE FROM perf_test WHERE id = %s", (i,))
    conn2.commit()
    text_delete_time = (time.perf_counter() - start) / 50 * 1000
    print(f"DELETE avg: {text_delete_time:.3f} ms")

    conn2.close()

    # Test with binary format (psycopg3)
    print("\n=== Binary Format (psycopg3) ===")
    conn3 = psycopg.connect(
        host="localhost", port=5433, dbname="test_perf.db",
        user="dummy", password="dummy", sslmode="disable"
    )
    conn3.autocommit = True

    # Create table
    with conn3.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS perf_test_bin")
        cur.execute("CREATE TABLE perf_test_bin (id INTEGER PRIMARY KEY, value INTEGER)")

    # Benchmark 50 INSERTs with binary cursor
    start = time.perf_counter()
    with conn3.cursor(binary=True) as cur:
        for i in range(50):
            cur.execute("INSERT INTO perf_test_bin (id, value) VALUES (%s, %s)", (i, i * 10))
    binary_insert_time = (time.perf_counter() - start) / 50 * 1000
    print(f"INSERT avg: {binary_insert_time:.3f} ms")

    # Benchmark 50 UPDATEs with binary cursor
    start = time.perf_counter()
    with conn3.cursor(binary=True) as cur:
        for i in range(50):
            cur.execute("UPDATE perf_test_bin SET value = %s WHERE id = %s", (i * 20, i))
    binary_update_time = (time.perf_counter() - start) / 50 * 1000
    print(f"UPDATE avg: {binary_update_time:.3f} ms")

    # Benchmark 50 DELETEs with binary cursor
    start = time.perf_counter()
    with conn3.cursor(binary=True) as cur:
        for i in range(50):
            cur.execute("DELETE FROM perf_test_bin WHERE id = %s", (i,))
    binary_delete_time = (time.perf_counter() - start) / 50 * 1000
    print(f"DELETE avg: {binary_delete_time:.3f} ms")

    conn3.close()

    # Compare
    print(f"\n=== Performance Comparison ===")
    print(f"INSERT: Text={text_insert_time:.3f}ms, Binary={binary_insert_time:.3f}ms, " +
          f"Improvement={((text_insert_time/binary_insert_time - 1) * 100):+.1f}%")
    print(f"UPDATE: Text={text_update_time:.3f}ms, Binary={binary_update_time:.3f}ms, " +
          f"Improvement={((text_update_time/binary_update_time - 1) * 100):+.1f}%")
    print(f"DELETE: Text={text_delete_time:.3f}ms, Binary={binary_delete_time:.3f}ms, " +
          f"Improvement={((text_delete_time/binary_delete_time - 1) * 100):+.1f}%")
    
    # Check if regressions are fixed
    if binary_insert_time < text_insert_time * 1.5:
        print("\n✅ INSERT regression FIXED!")
    else:
        print("\n❌ INSERT still has regression")
        
    if binary_update_time < text_update_time * 1.5:
        print("✅ UPDATE regression FIXED!")
    else:
        print("❌ UPDATE still has regression")
        
    if binary_delete_time < text_delete_time * 1.5:
        print("✅ DELETE regression FIXED!")
    else:
        print("❌ DELETE still has regression")

finally:
    # Kill server
    server.terminate()
    server.wait()
    # Clean up
    if os.path.exists("test_perf.db"):
        os.remove("test_perf.db")