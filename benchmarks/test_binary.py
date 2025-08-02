#!/usr/bin/env python3
"""Test binary format with psycopg3"""

import psycopg

# Connect with binary format
conn = psycopg.connect(
    host="localhost",
    port=5433,
    dbname="benchmark_test.db",
    user="dummy",
    password="dummy",
    sslmode="disable"
)
conn.autocommit = True

# Create table
with conn.cursor() as cur:
    cur.execute("CREATE TABLE IF NOT EXISTS test_binary (id SERIAL PRIMARY KEY, value INTEGER)")
    print("Table created")

# Test INSERT with binary format
with conn.cursor(binary=True) as cur:
    print("Testing INSERT with binary cursor...")
    cur.execute("INSERT INTO test_binary (value) VALUES (%s) RETURNING id", (42,))
    result = cur.fetchone()
    print(f"INSERT returned: {result}")

# Test SELECT with binary format
with conn.cursor(binary=True) as cur:
    print("Testing SELECT with binary cursor...")
    cur.execute("SELECT * FROM test_binary WHERE value = %s", (42,))
    result = cur.fetchall()
    print(f"SELECT returned: {result}")

conn.close()
print("Binary format test completed successfully")