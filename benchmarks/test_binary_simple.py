#!/usr/bin/env python3
"""Minimal test of binary format with psycopg3"""

import subprocess
import time
import psycopg
from psycopg import sql

# Start server
print("Starting pgsqlite server...")
proc = subprocess.Popen([
    "/home/eran/work/pgsqlite/target/release/pgsqlite",
    "--database", ":memory:",
    "--port", "5437"
], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

time.sleep(2)

try:
    print("Connecting...")
    conn = psycopg.connect(
        "host=localhost port=5437 dbname=:memory: user=dummy",
        autocommit=True
    )
    
    # Test 1: Regular text cursor
    print("\nTest 1: Text cursor")
    with conn.cursor() as cur:
        cur.execute("SELECT 1::int4")
        print(f"Result: {cur.fetchone()}")
    
    # Test 2: Try client-side cursor with binary
    print("\nTest 2: Client-side cursor with binary")
    with conn.cursor(binary=True) as cur:
        print("Created client-side binary cursor")
        cur.execute("SELECT 1::int4")
        print("Executed query")
        result = cur.fetchone()
        print(f"Result: {result}")
    
    conn.close()
    print("\nSuccess!")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    proc.terminate()
    proc.wait()