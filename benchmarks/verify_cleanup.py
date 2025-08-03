#!/usr/bin/env python3
"""Verify cleanup connection fix works."""

import psycopg2
import time

PORT = 15434

print("Testing cleanup fix...")

# Test 1: Simple connection
print("\n1. Simple connection test")
try:
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    print("   Connected successfully")
    conn.close()
    print("   ✓ Closed successfully")
except Exception as e:
    print(f"   ✗ Error: {e}")

time.sleep(1)

# Test 2: Multiple connections
print("\n2. Multiple connections test")
for i in range(3):
    try:
        conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
        cur = conn.cursor()
        cur.execute("SELECT %s", (i,))
        result = cur.fetchone()
        print(f"   Connection {i+1}: result = {result[0]}")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"   ✗ Connection {i+1} error: {e}")
        break

print("\n✅ All tests completed!")

# Check server log
print("\nServer log tail:")
with open('/tmp/cleanup_test.log', 'r') as f:
    lines = f.readlines()
    for line in lines[-20:]:
        if 'Cleaning up' in line or 'Removed connection' in line or 'closed' in line:
            print(f"   {line.strip()}")