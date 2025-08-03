#!/usr/bin/env python3
import psycopg
import subprocess
import time
import os
import datetime

# Start pgsqlite with debug logging  
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_insert_returning_clean.db"
log_file = open("/tmp/insert_returning_clean.log", "w")

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

# Set environment for debug logging
env = os.environ.copy()
env["RUST_LOG"] = "pgsqlite::query::extended=info"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", "5439"],
    stdout=log_file,
    stderr=subprocess.STDOUT,
    env=env
)

# Give it time to start
time.sleep(1)

try:
    # Connect with psycopg3
    conn = psycopg.connect(
        "host=localhost port=5439 user=postgres dbname=main",
        autocommit=True  # Use autocommit to avoid transaction issues
    )
    
    print("=== Testing INSERT RETURNING with parameter casts ===")
    
    # Create test table
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_insert_clean")
        cur.execute("CREATE TABLE test_insert_clean (id INTEGER PRIMARY KEY, name TEXT, created_at INTEGER)")
    
    # Test 1: Simple INSERT RETURNING without casts
    print("\n1. Testing INSERT RETURNING without casts...")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO test_insert_clean (id, name) VALUES (%s, %s) RETURNING id", (1, "test1"))
        result = cur.fetchone()
        print(f"   ✅ SUCCESS: Got ID {result[0]}")
    
    # Test 2: INSERT RETURNING with VARCHAR cast
    print("\n2. Testing INSERT RETURNING with VARCHAR cast...")
    with conn.cursor() as cur:
        cur.execute("INSERT INTO test_insert_clean (id, name) VALUES (%s, %s::VARCHAR) RETURNING id", (2, "test2"))
        result = cur.fetchone()
        print(f"   ✅ SUCCESS: Got ID {result[0]}")
    
    # Test 3: INSERT RETURNING with multiple casts and multiple RETURNING columns
    print("\n3. Testing INSERT RETURNING with multiple casts...")
    with conn.cursor() as cur:
        now = datetime.datetime.now()
        cur.execute(
            "INSERT INTO test_insert_clean (id, name, created_at) VALUES (%s, %s::VARCHAR, %s::TIMESTAMP WITHOUT TIME ZONE) RETURNING id, name",
            (3, "test3", now)
        )
        result = cur.fetchone()
        print(f"   ✅ SUCCESS: Got ID {result[0]}, name '{result[1]}'")
    
    # Test 4: Verify all data was inserted correctly
    print("\n4. Verifying all data...")
    with conn.cursor() as cur:
        cur.execute("SELECT id, name FROM test_insert_clean ORDER BY id")
        rows = cur.fetchall()
        print(f"   Found {len(rows)} rows:")
        for row in rows:
            print(f"     ID: {row[0]}, Name: {row[1]}")
    
    print("\n✅ All tests passed! INSERT RETURNING with parameter casts works correctly")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()
    log_file.close()
    
    # Clean up database
    for ext in ['', '-shm', '-wal']:
        db_file = f"{db_path}{ext}"
        if os.path.exists(db_file):
            os.remove(db_file)