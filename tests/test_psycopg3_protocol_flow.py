#!/usr/bin/env python3
import psycopg
import subprocess
import time
import os
import signal

# Start pgsqlite with debug logging
pgsqlite_path = "../../target/debug/pgsqlite"
log_file = open("/tmp/psycopg3_protocol.log", "w")

# Set environment for debug logging
env = os.environ.copy()
env["RUST_LOG"] = "pgsqlite::query::extended=debug,pgsqlite::session=debug"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", ":memory:", "--port", "5434"],
    stdout=log_file,
    stderr=subprocess.STDOUT,
    env=env
)

# Give it time to start
time.sleep(1)

try:
    # Connect with psycopg3
    conn = psycopg.connect(
        "host=localhost port=5434 user=postgres dbname=main",
        autocommit=False
    )
    
    # Create test table
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE test_table (id INTEGER, name TEXT)")
        cur.execute("INSERT INTO test_table VALUES (1, 'test')")
        conn.commit()
    
    print("=== Testing psycopg3 query with parameter cast ===")
    
    # This is the problematic query - using unnamed statement with cast
    with conn.cursor() as cur:
        # psycopg3 uses unnamed statements by default
        cur.execute("SELECT id FROM test_table WHERE name = %s::VARCHAR", ("test",))
        rows = cur.fetchall()
        print(f"Rows: {rows}")
        assert len(rows) == 1
        assert rows[0][0] == 1
    
    print("SUCCESS: Query executed correctly")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()
    log_file.close()
    
    # Print the log for analysis
    print("\n=== Protocol Log ===")
    with open("/tmp/psycopg3_protocol.log", "r") as f:
        print(f.read())