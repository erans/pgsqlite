#!/usr/bin/env python3
"""Test psycopg3 ROLLBACK handling to identify the specific issue."""

import psycopg
import subprocess
import time
import os

# Configuration
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_rollback_simple.db"
port = 5441

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

# Start pgsqlite with debug logging for simple queries
env = os.environ.copy()
env["RUST_LOG"] = "pgsqlite::query::simple=debug"

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
    print("\n=== Testing psycopg3 ROLLBACK variations ===")
    
    # Test 1: Basic ROLLBACK in simple query mode
    print("\n1. Testing ROLLBACK with simple query protocol...")
    conn = psycopg.connect(
        f"host=localhost port={port} user=postgres dbname=main",
        autocommit=False
    )
    
    try:
        # Start a transaction
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            print("   - Started transaction")
            
            # Create a table
            cur.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)")
            print("   - Created table")
            
            # Now rollback using simple query
            print("   - Sending ROLLBACK...")
            cur.execute("ROLLBACK")
            print("   ✅ ROLLBACK succeeded!")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    conn.close()
    
    # Test 2: ROLLBACK after failed operation
    print("\n2. Testing ROLLBACK after failed INSERT...")
    conn = psycopg.connect(
        f"host=localhost port={port} user=postgres dbname=main",
        autocommit=False
    )
    
    try:
        with conn.cursor() as cur:
            # Create a table with constraint
            cur.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT UNIQUE)")
            cur.execute("INSERT INTO users (id, email) VALUES (1, 'test@example.com')")
            conn.commit()
            print("   - Created table and inserted first user")
            
            # Try to insert duplicate (will fail)
            try:
                cur.execute("INSERT INTO users (id, email) VALUES (2, 'test@example.com')")
            except psycopg.errors.UniqueViolation:
                print("   - INSERT failed as expected (unique violation)")
                
            # Now rollback
            print("   - Sending ROLLBACK...")
            conn.rollback()
            print("   ✅ ROLLBACK after error succeeded!")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    conn.close()
    
    # Test 3: Check result handling
    print("\n3. Testing ROLLBACK result handling...")
    conn = psycopg.connect(
        f"host=localhost port={port} user=postgres dbname=main",
        autocommit=False
    )
    
    try:
        with conn.cursor() as cur:
            cur.execute("BEGIN")
            
            # Execute ROLLBACK and check what it returns
            print("   - Executing ROLLBACK and checking result...")
            cur.execute("ROLLBACK")
            
            # Try to fetch result (this might be where the error occurs)
            try:
                result = cur.fetchone()
                print(f"   - fetchone() returned: {result}")
            except psycopg.ProgrammingError as e:
                print(f"   - fetchone() raised ProgrammingError (expected): {e}")
            
            # Check cursor properties
            print(f"   - cur.description: {cur.description}")
            print(f"   - cur.rowcount: {cur.rowcount}")
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    conn.close()
    
    # Get pgsqlite logs
    print("\n=== pgsqlite logs (last 100 lines) ===")
    pgsqlite.terminate()
    time.sleep(0.5)
    stdout, stderr = pgsqlite.communicate()
    if stderr:
        lines = stderr.decode().split('\n')
        for line in lines[-100:]:
            if line and 'ROLLBACK' in line:
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