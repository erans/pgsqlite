#!/usr/bin/env python3
"""Quick DML performance test"""

import time
import psycopg
import psycopg2

# Quick test with just a few operations
print("Testing DML performance with binary format...")

# Text format
conn2 = psycopg2.connect(
    host="localhost", port=5433, dbname="test_perf.db",
    user="dummy", password="dummy", sslmode="disable"
)
cur2 = conn2.cursor()
cur2.execute("DROP TABLE IF EXISTS test1")
cur2.execute("CREATE TABLE test1 (id INTEGER PRIMARY KEY, value INTEGER)")
conn2.commit()

# Time 5 INSERTs
start = time.perf_counter()
for i in range(5):
    cur2.execute("INSERT INTO test1 (id, value) VALUES (%s, %s)", (i, i * 10))
conn2.commit()
text_time = (time.perf_counter() - start) / 5 * 1000
print(f"Text INSERT: {text_time:.3f}ms")
conn2.close()

# Binary format
conn3 = psycopg.connect(
    host="localhost", port=5433, dbname="test_perf.db",
    user="dummy", password="dummy", sslmode="disable"
)
conn3.autocommit = True

with conn3.cursor() as cur:
    cur.execute("DROP TABLE IF EXISTS test2")
    cur.execute("CREATE TABLE test2 (id INTEGER PRIMARY KEY, value INTEGER)")

# Time 5 INSERTs with binary
start = time.perf_counter()
with conn3.cursor(binary=True) as cur:
    for i in range(5):
        cur.execute("INSERT INTO test2 (id, value) VALUES (%s, %s)", (i, i * 10))
binary_time = (time.perf_counter() - start) / 5 * 1000
print(f"Binary INSERT: {binary_time:.3f}ms")

print(f"\nImprovement: {((text_time/binary_time - 1) * 100):+.1f}%")
if binary_time < text_time * 1.5:
    print("✅ Binary format performance is acceptable!")
else:
    print("❌ Binary format still has regression")

conn3.close()