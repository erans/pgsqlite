#!/usr/bin/env python3
"""Test binary format connection issue"""

import psycopg
import time

print("Testing binary format connection...")

# Connect without autocommit first
print("1. Connecting without autocommit...")
conn = psycopg.connect(
    host="127.0.0.1",
    port=5434,
    dbname="benchmark_test.db",
    user="dummy",
    password="dummy",
    sslmode="disable"
)
print("   Connected successfully")

# Create cursor with binary format
print("\n2. Creating binary cursor...")
cursor = conn.cursor(binary=True)
print("   Binary cursor created")

# Try a simple query
print("\n3. Executing simple query...")
start = time.time()
try:
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print(f"   Result: {result}")
    print(f"   Time: {time.time() - start:.3f}s")
except Exception as e:
    print(f"   ERROR: {e}")
    import traceback
    traceback.print_exc()

# Now enable autocommit and retry
print("\n4. Enabling autocommit...")
conn.autocommit = True
print("   Autocommit enabled")

print("\n5. Creating new binary cursor with autocommit...")
cursor2 = conn.cursor(binary=True)
print("   Binary cursor created")

# Try CREATE TABLE
print("\n6. Executing CREATE TABLE...")
start = time.time()
try:
    cursor2.execute("""CREATE TABLE IF NOT EXISTS test_table (
        id SERIAL PRIMARY KEY,
        name TEXT
    )""")
    print(f"   CREATE TABLE completed in {time.time() - start:.3f}s")
except Exception as e:
    print(f"   ERROR: {e}")
    import traceback
    traceback.print_exc()

conn.close()
print("\nTest completed!")