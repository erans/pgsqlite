#!/usr/bin/env python3
import psycopg
import subprocess
import time
import os

# Start pgsqlite with full debug logging
pgsqlite_path = "../../target/debug/pgsqlite"
log_file = open("/tmp/insert_returning_protocol.log", "w")

env = os.environ.copy()
env["RUST_LOG"] = "pgsqlite::query::extended=debug,pgsqlite::session=debug"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", ":memory:", "--port", "5441"],
    stdout=log_file,
    stderr=subprocess.STDOUT,
    env=env
)

# Give it time to start
time.sleep(1)

try:
    # Connect with psycopg3
    conn = psycopg.connect(
        "host=localhost port=5441 user=postgres dbname=main",
        autocommit=True
    )
    
    print("=== Testing INSERT RETURNING Protocol Flow ===")
    
    # Create a simple table
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE test_protocol (id INTEGER PRIMARY KEY, name TEXT)")
    
    # Test INSERT RETURNING with unique ID
    print("\n1. Testing INSERT RETURNING with ID=100...")
    with conn.cursor() as cur:
        try:
            cur.execute("INSERT INTO test_protocol (id, name) VALUES (%s, %s) RETURNING id", (100, "test100"))
            result = cur.fetchone()
            print(f"   ✅ SUCCESS: Got ID {result[0]}")
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
    
    # Check what's actually in the table
    print("\n2. Checking table contents...")
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM test_protocol ORDER BY id")
        rows = cur.fetchall()
        print(f"   Found {len(rows)} rows:")
        for row in rows:
            print(f"     ID: {row[0]}, Name: {row[1]}")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()
    log_file.close()
    
    # Print relevant parts of the log
    print("\n=== Protocol Log (INSERT RETURNING) ===")
    with open("/tmp/insert_returning_protocol.log", "r") as f:
        lines = f.readlines()
        in_insert_section = False
        for line in lines:
            if "INSERT INTO test_protocol" in line or "VALUES (100" in line:
                in_insert_section = True
            if in_insert_section:
                print(line.rstrip())
                if "CommandComplete" in line or "ERROR" in line and "constraint" in line:
                    break