#!/usr/bin/env python3
"""Test cleanup with active transaction."""

import psycopg2
import time
import subprocess

PORT = 15433

print("Starting server...")
server = subprocess.Popen([
    "../target/release/pgsqlite",
    "--database", "benchmark_test.db",
    "--port", str(PORT)
], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

# Wait for server to start
time.sleep(2)

try:
    print("Test 1: Connection without transaction")
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    print("  Connected!")
    conn.close()
    print("  ✓ Connection closed successfully!")
    
    time.sleep(0.5)
    
    print("\nTest 2: Connection with SELECT query") 
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    cur.execute("SELECT 1")
    result = cur.fetchone()
    print(f"  Query result: {result}")
    cur.close()
    conn.close()
    print("  ✓ Connection with query closed successfully!")
    
    time.sleep(0.5)
    
    print("\nTest 3: Connection with explicit transaction")
    conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
    cur = conn.cursor()
    cur.execute("BEGIN")
    cur.execute("SELECT 1")
    # Note: NOT committing or rolling back
    cur.close()
    conn.close()
    print("  ✓ Connection with transaction closed (should rollback automatically)")
    
    time.sleep(1)
    print("\n✅ All cleanup tests passed!")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    print("\nTerminating server...")
    server.terminate()
    
    # Get server output
    output, _ = server.communicate(timeout=5)
    print("\nServer output (last 2000 chars):")
    print(output.decode('utf-8')[-2000:])
    
    print("\nServer terminated.")