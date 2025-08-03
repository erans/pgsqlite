#!/usr/bin/env python3
"""Simple test to check if binary format works at all"""

import psycopg
import psycopg2

print("Testing basic binary format functionality...")

# First test psycopg2 text format
print("\n1. Testing psycopg2 (text format)...")
try:
    conn = psycopg2.connect(
        host="/tmp",
        port=5434,
        dbname=":memory:",
        user="dummy",
        sslmode="disable"
    )
    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    print(f"   Result: {result}")
    conn.close()
    print("   ✅ Text format works")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Test psycopg3 without binary
print("\n2. Testing psycopg3 (text format)...")
try:
    conn = psycopg.connect(
        host="/tmp",
        port=5434,
        dbname=":memory:",
        user="dummy",
        sslmode="disable"
    )
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        result = cur.fetchone()
        print(f"   Result: {result}")
    conn.close()
    print("   ✅ psycopg3 text format works")
except Exception as e:
    print(f"   ❌ Error: {e}")

# Test psycopg3 with binary cursor
print("\n3. Testing psycopg3 (binary format)...")
try:
    conn = psycopg.connect(
        host="/tmp",
        port=5434,
        dbname=":memory:",
        user="dummy",
        sslmode="disable",
        autocommit=True
    )
    print("   Connected successfully")
    
    # Try a simple SELECT with binary cursor
    with conn.cursor(binary=True) as cur:
        print("   Created binary cursor")
        cur.execute("SELECT 1")
        print("   Executed query")
        result = cur.fetchone()
        print(f"   Result: {result}")
    
    conn.close()
    print("   ✅ Binary format works")
except Exception as e:
    print(f"   ❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\nDone!")