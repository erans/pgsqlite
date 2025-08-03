#!/usr/bin/env python3
import psycopg
import subprocess
import time
import os

# Start pgsqlite with debug logging
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_insert_returning_cast.db"
log_file = open("/tmp/insert_returning_cast.log", "w")

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

# Set environment for debug logging
env = os.environ.copy()
env["RUST_LOG"] = "pgsqlite::query::extended=debug"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", "5438"],
    stdout=log_file,
    stderr=subprocess.STDOUT,
    env=env
)

# Give it time to start
time.sleep(1)

try:
    # Connect with psycopg3
    conn = psycopg.connect(
        "host=localhost port=5438 user=postgres dbname=main",
        autocommit=False
    )
    
    print("=== Testing INSERT RETURNING with parameter casts ===")
    
    # Create test table (drop if exists)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_insert")
        cur.execute("CREATE TABLE test_insert (id INTEGER PRIMARY KEY, name TEXT, created_at INTEGER)")
        conn.commit()
    
    # Test 1: Simple INSERT RETURNING without casts
    print("\n1. Testing INSERT RETURNING without casts...")
    with conn.cursor() as cur:
        try:
            cur.execute("INSERT INTO test_insert (id, name) VALUES (%s, %s) RETURNING id", (1, "test1"))
            result = cur.fetchone()
            print(f"   SUCCESS: Got ID {result[0]}")
            conn.commit()
        except Exception as e:
            print(f"   ERROR: {e}")
            conn.rollback()
    
    # Test 2: INSERT RETURNING with VARCHAR cast
    print("\n2. Testing INSERT RETURNING with VARCHAR cast...")
    with conn.cursor() as cur:
        try:
            cur.execute("INSERT INTO test_insert (id, name) VALUES (%s, %s::VARCHAR) RETURNING id", (2, "test2"))
            result = cur.fetchone()
            print(f"   SUCCESS: Got ID {result[0]}")
            conn.commit()
        except Exception as e:
            print(f"   ERROR: {e}")
            conn.rollback()
    
    # Test 3: INSERT RETURNING with multiple casts
    print("\n3. Testing INSERT RETURNING with multiple casts...")
    with conn.cursor() as cur:
        try:
            import datetime
            now = datetime.datetime.now()
            # Convert to microseconds since epoch for INTEGER storage
            microseconds = int(now.timestamp() * 1_000_000)
            cur.execute(
                "INSERT INTO test_insert (id, name, created_at) VALUES (%s, %s::VARCHAR, %s::TIMESTAMP WITHOUT TIME ZONE) RETURNING id, name",
                (3, "test3", now)
            )
            result = cur.fetchone()
            print(f"   SUCCESS: Got ID {result[0]}, name {result[1]}")
            conn.commit()
        except Exception as e:
            print(f"   ERROR: {e}")
            import traceback
            traceback.print_exc()
            conn.rollback()
    
    print("\n✅ Test completed")
    
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
    
    # Print relevant parts of the log
    print("\n=== Relevant Protocol Log ===")
    with open("/tmp/insert_returning_cast.log", "r") as f:
        lines = f.readlines()
        in_relevant_section = False
        for line in lines:
            if "Testing INSERT RETURNING with VARCHAR cast" in line or \
               "VALUES (%s, %s::VARCHAR) RETURNING" in line or \
               "INSERT INTO test_insert" in line and "::VARCHAR" in line:
                in_relevant_section = True
            if in_relevant_section:
                print(line.rstrip())
                if "CommandComplete" in line or "ERROR" in line:
                    in_relevant_section = False