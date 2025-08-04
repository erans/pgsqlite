#!/usr/bin/env python3
"""Test PostgreSQL error code mapping."""

import psycopg
import subprocess
import time
import os

# Configuration
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_error_codes.db"
port = 5442

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
    print("\n=== Testing PostgreSQL error code mapping ===")
    
    conn = psycopg.connect(
        f"host=localhost port={port} user=postgres dbname=main",
        autocommit=True
    )
    
    # Test 1: Unique constraint violation
    print("\n1. Testing UNIQUE constraint violation...")
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
            cur.execute("INSERT INTO users (id, email) VALUES (1, 'test@example.com')")
            cur.execute("INSERT INTO users (id, email) VALUES (2, 'test@example.com')")
    except Exception as e:
        print(f"   Exception type: {type(e).__name__}")
        print(f"   Exception module: {type(e).__module__}")
        print(f"   Error message: {e}")
        
        # Check if it has PostgreSQL error details
        if hasattr(e, 'pgcode'):
            print(f"   PostgreSQL error code: {e.pgcode}")
        if hasattr(e, 'pgerror'):
            print(f"   PostgreSQL error detail: {e.pgerror}")
        if hasattr(e, 'diag'):
            print(f"   Diagnostic info available: {e.diag}")
            if e.diag:
                print(f"     - sqlstate: {e.diag.sqlstate}")
                print(f"     - message_primary: {e.diag.message_primary}")
                print(f"     - message_detail: {e.diag.message_detail}")
    
    # Test 2: Check error inheritance
    print("\n2. Checking psycopg3 error inheritance...")
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users (id, email) VALUES (3, 'test@example.com')")
    except psycopg.errors.UniqueViolation as e:
        print("   ✅ Caught as psycopg.errors.UniqueViolation")
        print(f"   Error: {e}")
    except psycopg.errors.IntegrityError as e:
        print("   ✅ Caught as psycopg.errors.IntegrityError")
        print(f"   Error: {e}")
    except Exception as e:
        print(f"   ❌ Caught as {type(e).__name__} instead of UniqueViolation")
        print(f"   Error: {e}")
    
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