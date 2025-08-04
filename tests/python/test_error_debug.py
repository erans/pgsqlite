#!/usr/bin/env python3
"""Debug error handling flow."""

import psycopg
import subprocess
import time
import os

# Configuration
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_error_debug.db"
port = 5444

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

# Start pgsqlite with full debug logging
env = os.environ.copy()
env["RUST_LOG"] = "pgsqlite=debug"

print("Starting pgsqlite with debug logging...")
pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", str(port)],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    env=env
)

# Give it time to start
time.sleep(2)

try:
    print("\n=== Testing error flow ===")
    
    conn = psycopg.connect(
        f"host=localhost port={port} user=postgres dbname=main",
        autocommit=False
    )
    
    with conn.cursor() as cur:
        # Create table
        cur.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.commit()
        
        # Try to violate constraint with parameters (extended protocol)
        print("\nAttempting constraint violation...")
        try:
            cur.execute("INSERT INTO test (id) VALUES (%s)", (1,))
            cur.execute("INSERT INTO test (id) VALUES (%s)", (1,))
        except Exception as e:
            print(f"Error caught: {type(e).__name__}: {e}")
    
    conn.close()
    
    # Get pgsqlite logs
    print("\n=== pgsqlite debug logs ===")
    pgsqlite.terminate()
    time.sleep(0.5)
    stdout, stderr = pgsqlite.communicate()
    if stderr:
        lines = stderr.decode().split('\n')
        # Filter for relevant lines
        for line in lines:
            if any(x in line for x in ["Execute error", "ErrorResponse", "42000", "23505", "constraint"]):
                print(line)
    
except Exception as e:
    print(f"\n❌ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
finally:
    # Kill pgsqlite
    try:
        pgsqlite.terminate()
        pgsqlite.wait()
    except:
        pass
    
    # Clean up database
    for ext in ['', '-shm', '-wal']:
        db_file = f"{db_path}{ext}"
        if os.path.exists(db_file):
            os.remove(db_file)