#!/usr/bin/env python3
"""Quick performance test for binary vs text format"""

import time
import psycopg
import psycopg2

# Test with text format (psycopg2)
print("=== Text Format (psycopg2) ===")
conn2 = psycopg2.connect(
    host="localhost", port=5433, dbname="benchmark_test.db",
    user="dummy", password="dummy", sslmode="disable"
)
cur2 = conn2.cursor()

# Create table
cur2.execute("DROP TABLE IF EXISTS perf_test")
cur2.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, value INTEGER)")
conn2.commit()

# Benchmark 10 INSERTs
start = time.perf_counter()
for i in range(10):
    cur2.execute("INSERT INTO perf_test (id, value) VALUES (%s, %s)", (i, i * 10))
conn2.commit()
text_insert_time = (time.perf_counter() - start) / 10 * 1000
print(f"INSERT avg: {text_insert_time:.3f} ms")

conn2.close()

# Test with binary format (psycopg3)
print("\n=== Binary Format (psycopg3) ===")
conn3 = psycopg.connect(
    host="localhost", port=5433, dbname="benchmark_test.db",
    user="dummy", password="dummy", sslmode="disable"
)
conn3.autocommit = True

# Create table
with conn3.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS perf_test_bin")
    cur.execute("CREATE TABLE perf_test_bin (id INTEGER PRIMARY KEY, value INTEGER)")

# Benchmark 10 INSERTs with binary cursor
start = time.perf_counter()
with conn3.cursor(binary=True) as cur:
    for i in range(10):
        cur.execute("INSERT INTO perf_test_bin (id, value) VALUES (%s, %s)", (i, i * 10))
binary_insert_time = (time.perf_counter() - start) / 10 * 1000
print(f"INSERT avg: {binary_insert_time:.3f} ms")

conn3.close()

# Compare
print(f"\n=== Comparison ===")
print(f"Text format:   {text_insert_time:.3f} ms")
print(f"Binary format: {binary_insert_time:.3f} ms")
print(f"Difference:    {binary_insert_time - text_insert_time:+.3f} ms ({(binary_insert_time/text_insert_time - 1) * 100:+.1f}%)")