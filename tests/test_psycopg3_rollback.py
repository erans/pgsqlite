#!/usr/bin/env python3
import psycopg
import subprocess
import time
import os

# Start pgsqlite with debug logging
pgsqlite_path = "../../target/debug/pgsqlite"
log_file = open("/tmp/psycopg3_rollback.log", "w")

# Set environment for debug logging
env = os.environ.copy()
env["RUST_LOG"] = "pgsqlite::query=debug,pgsqlite::session=debug"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", ":memory:", "--port", "5436"],
    stdout=log_file,
    stderr=subprocess.STDOUT,
    env=env
)

# Give it time to start
time.sleep(1)

try:
    # Connect with psycopg3
    conn = psycopg.connect(
        "host=localhost port=5436 user=postgres dbname=main",
        autocommit=False
    )
    
    print("=== Testing psycopg3 ROLLBACK handling ===")
    
    # Test 1: Simple rollback without transaction
    print("\n1. Testing ROLLBACK without active transaction...")
    with conn.cursor() as cur:
        try:
            cur.execute("ROLLBACK")
            print("   SUCCESS: ROLLBACK executed without error")
        except Exception as e:
            print(f"   ERROR: {e}")
    
    # Test 2: Rollback after failed query
    print("\n2. Testing ROLLBACK after failed query...")
    with conn.cursor() as cur:
        # Create a table
        cur.execute("CREATE TABLE test_rollback (id INTEGER PRIMARY KEY, value TEXT UNIQUE)")
        cur.execute("INSERT INTO test_rollback VALUES (1, 'test')")
        conn.commit()
        
        # Try to insert duplicate (will fail)
        try:
            cur.execute("INSERT INTO test_rollback VALUES (2, 'test')")  # UNIQUE violation
            conn.commit()
        except Exception as e:
            print(f"   Expected error: {e}")
            
            # Now try to rollback
            try:
                conn.rollback()
                print("   SUCCESS: ROLLBACK executed after failed query")
            except Exception as e:
                print(f"   ERROR during ROLLBACK: {e}")
                import traceback
                traceback.print_exc()
    
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
    
    # Print the log for analysis
    print("\n=== Protocol Log ===")
    with open("/tmp/psycopg3_rollback.log", "r") as f:
        print(f.read())