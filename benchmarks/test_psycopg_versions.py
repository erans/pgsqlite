#!/usr/bin/env python3
"""Test both psycopg2 and psycopg3 to isolate the issue"""

import psycopg2
import psycopg

print("Starting pgsqlite server...")
import subprocess
import time
import os

# Start the server
proc = subprocess.Popen([
    "/home/eran/work/pgsqlite/target/release/pgsqlite",
    "--database", ":memory:",
    "--port", "5435"
], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

time.sleep(2)  # Give server time to start

try:
    # Test 1: psycopg2 (works)
    print("\n=== Testing psycopg2 ===")
    conn2 = psycopg2.connect(
        host="localhost",
        port=5435,
        dbname=":memory:",
        user="dummy"
    )
    cur2 = conn2.cursor()
    cur2.execute("SELECT 1")
    print(f"psycopg2 result: {cur2.fetchone()}")
    cur2.close()
    conn2.close()
    print("✓ psycopg2 works")
    
    # Test 2: psycopg3 text mode
    print("\n=== Testing psycopg3 (text mode) ===")
    conn3 = psycopg.connect(
        host="localhost",
        port=5435,
        dbname=":memory:",
        user="dummy"
    )
    with conn3.cursor() as cur3:
        cur3.execute("SELECT 1")
        print(f"psycopg3 text result: {cur3.fetchone()}")
    conn3.close()
    print("✓ psycopg3 text mode works")
    
    # Test 3: psycopg3 binary mode - this is where it hangs
    print("\n=== Testing psycopg3 (binary mode) ===")
    print("Creating connection...")
    conn3b = psycopg.connect(
        host="localhost", 
        port=5435,
        dbname=":memory:",
        user="dummy"
    )
    print("Creating binary cursor...")
    with conn3b.cursor(binary=True) as cur3b:
        print("Executing SELECT 1 with binary cursor...")
        cur3b.execute("SELECT 1")
        print("Fetching result...")
        result = cur3b.fetchone()
        print(f"psycopg3 binary result: {result}")
    conn3b.close()
    print("✓ psycopg3 binary mode works")
    
finally:
    proc.terminate()
    proc.wait()
    print("\nServer stopped")