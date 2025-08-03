#!/usr/bin/env python3
"""Reproduce the hang issue with binary format in benchmark"""

import psycopg
import time

print("Testing binary format hang issue...")

# Connect to pgsqlite with autocommit as benchmark does
conn = psycopg.connect(
    host="127.0.0.1",
    port=5434,
    dbname="benchmark_test.db",
    user="dummy",
    password="dummy",
    sslmode="disable",
    autocommit=True  # Same as benchmark
)

print("Connected successfully with autocommit=True")

# Create cursor with binary format
cursor = conn.cursor(binary=True)
print("Binary cursor created")

# Try CREATE TABLE operation (first operation in benchmark)
print("\nExecuting CREATE TABLE...")
start = time.time()
try:
    cursor.execute("""CREATE TABLE IF NOT EXISTS benchmark_table_pg (
        id SERIAL PRIMARY KEY,
        text_col TEXT,
        int_col INTEGER,
        real_col REAL,
        bool_col BOOLEAN
    )""")
    print(f"CREATE TABLE completed in {time.time() - start:.3f}s")
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

conn.close()
print("\nTest completed!")