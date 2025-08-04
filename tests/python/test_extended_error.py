#!/usr/bin/env python3
"""Test error handling in extended query protocol."""

import psycopg
import subprocess
import time
import os

# Configuration
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_extended_error.db"
port = 5443

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

# Start pgsqlite
env = os.environ.copy()
env["RUST_LOG"] = "info"

print("Starting pgsqlite...")
pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", str(port)],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    env=env
)

# Give it time to start
time.sleep(2)

try:
    print("\n=== Testing extended query protocol error handling ===")
    
    conn = psycopg.connect(
        f"host=localhost port={port} user=postgres dbname=main",
        autocommit=False  # Use transactions to force extended protocol
    )
    
    # Test 1: Create table and insert with extended protocol
    print("\n1. Testing UNIQUE constraint with extended protocol...")
    try:
        with conn.cursor() as cur:
            # Create table
            cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
            conn.commit()
            
            # Insert first record
            cur.execute("INSERT INTO users (id, email) VALUES (%s, %s)", (1, "test@example.com"))
            conn.commit()
            
            # Try to insert duplicate (will use extended protocol due to parameters)
            cur.execute("INSERT INTO users (id, email) VALUES (%s, %s)", (2, "test@example.com"))
            conn.commit()
    except Exception as e:
        print(f"   Exception type: {type(e).__name__}")
        print(f"   Exception module: {type(e).__module__}")
        print(f"   Error message: {e}")
        
        # Check error details
        if hasattr(e, 'pgcode'):
            print(f"   PostgreSQL error code: {e.pgcode}")
        if hasattr(e, 'diag'):
            print(f"   Diagnostic info:")
            if e.diag:
                print(f"     - sqlstate: {e.diag.sqlstate}")
                print(f"     - message_primary: {e.diag.message_primary}")
                print(f"     - message_detail: {e.diag.message_detail}")
    
    # Test 2: Test with RETURNING clause
    print("\n2. Testing UNIQUE constraint with RETURNING clause...")
    try:
        with conn.cursor() as cur:
            # Rollback previous error
            conn.rollback()
            
            # Try duplicate insert with RETURNING
            cur.execute("INSERT INTO users (id, email) VALUES (%s, %s) RETURNING id", (3, "test@example.com"))
            result = cur.fetchone()
            print(f"   Unexpected success: {result}")
    except psycopg.errors.UniqueViolation as e:
        print("   ✅ Caught as psycopg.errors.UniqueViolation")
        print(f"   Error: {e}")
    except Exception as e:
        print(f"   ❌ Caught as {type(e).__name__} instead of UniqueViolation")
        print(f"   Error: {e}")
        if hasattr(e, 'diag') and e.diag:
            print(f"   sqlstate: {e.diag.sqlstate}")
    
    conn.close()
    
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