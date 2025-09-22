#!/usr/bin/env python3
"""Test what format psycopg3 sends arrays."""

import psycopg
import psycopg.sql as sql

# Connect with psycopg3 in text mode
print("Connecting in TEXT mode...")
conn = psycopg.connect(
    host="127.0.0.1",
    port=5433,
    user="postgres",
    dbname="test"
)
cur = conn.cursor()

# Create table
cur.execute("""
    DROP TABLE IF EXISTS test_formats;
""")
cur.execute("""
    CREATE TABLE test_formats (
        id INTEGER PRIMARY KEY,
        int_array INTEGER[]
    )
""")
conn.commit()

# Insert with psycopg3 text mode
print("\nInserting [1, 2, 3] via psycopg3 text mode...")
cur.execute(
    "INSERT INTO test_formats (id, int_array) VALUES (%s, %s)",
    (1, [1, 2, 3])
)
conn.commit()

# Check what was stored
print("\nChecking stored format via raw SQLite...")
import subprocess
result = subprocess.run(
    ['sqlite3', '/tmp/test_arrays.db', "SELECT int_array FROM test_formats WHERE id = 1;"],
    capture_output=True,
    text=True
)
print(f"Stored format: {result.stdout.strip()}")

# Try binary cursor
print("\n" + "="*50)
print("Testing BINARY mode...")
cur_binary = conn.cursor(binary=True)

# Insert with binary mode
print("Inserting [4, 5, 6] via psycopg3 binary mode...")
cur_binary.execute(
    "INSERT INTO test_formats (id, int_array) VALUES (%s, %s)",
    (2, [4, 5, 6])
)
conn.commit()

# Check what was stored
result = subprocess.run(
    ['sqlite3', '/tmp/test_arrays.db', "SELECT int_array FROM test_formats WHERE id = 2;"],
    capture_output=True,
    text=True
)
print(f"Stored format: {result.stdout.strip()}")

conn.close()
print("\nDone.")