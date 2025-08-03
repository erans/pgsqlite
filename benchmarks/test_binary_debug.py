#!/usr/bin/env python3
"""Test binary format with detailed output"""

import psycopg
import time

print("Testing binary format issue with detailed logging...")

# First test - simple connection and query
print("\n1. Testing simple connection...")
conn = psycopg.connect(
    host="127.0.0.1",
    port=5434,
    dbname="benchmark_test.db",
    user="dummy",
    password="dummy",
    sslmode="disable"
)
print("   Connected successfully")

# Regular cursor test
print("\n2. Testing regular cursor...")
with conn.cursor() as cur:
    cur.execute("SELECT 1")
    result = cur.fetchone()
    print(f"   Result: {result}")

# Now test binary cursor
print("\n3. Testing binary cursor...")
with conn.cursor(binary=True) as cur:
    print("   Binary cursor created")
    cur.execute("SELECT 2")
    print("   Execute completed")
    result = cur.fetchone()
    print(f"   Result: {result}")

print("\n4. Closing connection...")
conn.close()
print("   Connection closed")

# Now try to connect again
print("\n5. Testing new connection after binary cursor...")
try:
    conn2 = psycopg.connect(
        host="127.0.0.1",
        port=5434,
        dbname="benchmark_test.db",
        user="dummy",
        password="dummy",
        sslmode="disable"
    )
    print("   Second connection successful!")
    conn2.close()
except Exception as e:
    print(f"   ERROR: Failed to connect: {e}")

print("\nTest completed!")