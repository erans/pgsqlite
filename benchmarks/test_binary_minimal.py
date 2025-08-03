#!/usr/bin/env python3
"""Minimal test to reproduce binary format hang"""

import psycopg
import time

print("Testing binary format issue...")

# Connect to pgsqlite
conn = psycopg.connect(
    host="127.0.0.1",
    port=5434,
    dbname="benchmark_test.db",
    user="dummy",
    password="dummy",
    sslmode="disable",
    autocommit=True
)

print("Connected successfully")

# Create test table
with conn.cursor() as cur:
    cur.execute("CREATE TABLE IF NOT EXISTS test_users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)")
    cur.execute("INSERT INTO test_users (name, age) VALUES ('Alice', 30), ('Bob', 25)")
    print("Table created and data inserted")

# Test with text format
print("\n1. Testing TEXT format SELECT...")
start = time.time()
with conn.cursor() as cur:
    cur.execute("SELECT * FROM test_users WHERE id = %s", (1,))
    result = cur.fetchone()
    print(f"   Result: {result}")
    print(f"   Time: {time.time() - start:.3f}s")

# Test with binary format  
print("\n2. Testing BINARY format SELECT...")
start = time.time()
print("   Creating binary cursor...")
with conn.cursor(binary=True) as cur:
    print("   Executing query...")
    cur.execute("SELECT * FROM test_users WHERE id = %s", (1,))
    print("   Fetching result...")
    result = cur.fetchone()
    print(f"   Result: {result}")
    print(f"   Time: {time.time() - start:.3f}s")

print("\nTest completed!")
conn.close()