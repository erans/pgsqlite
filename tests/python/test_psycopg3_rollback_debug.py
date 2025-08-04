#!/usr/bin/env python3
import psycopg
import subprocess
import time
import os

# Start pgsqlite with debug logging
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_rollback.db"

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

env = os.environ.copy()
env["RUST_LOG"] = "info"

print("Starting pgsqlite...")
pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", "5440"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    env=env
)

# Give it time to start
time.sleep(2)

try:
    print("\n=== Testing psycopg3 ROLLBACK handling ===")
    
    # Test basic connection and simple query
    print("\n1. Connecting with autocommit=True...")
    conn = psycopg.connect(
        "host=localhost port=5440 user=postgres dbname=main",
        autocommit=True
    )
    
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        result = cur.fetchone()
        print(f"   ✅ Simple query works: {result}")
    
    conn.close()
    
    # Test with autocommit=False (transaction mode)
    print("\n2. Connecting with autocommit=False (transaction mode)...")
    conn = psycopg.connect(
        "host=localhost port=5440 user=postgres dbname=main",
        autocommit=False
    )
    
    try:
        with conn.cursor() as cur:
            print("   - Executing SELECT 1...")
            cur.execute("SELECT 1")
            result = cur.fetchone()
            print(f"   ✅ Query result: {result}")
        
        print("   - Calling rollback()...")
        conn.rollback()
        print("   ✅ Rollback succeeded!")
        
    except Exception as e:
        print(f"   ❌ Error during rollback: {e}")
        import traceback
        traceback.print_exc()
    
    conn.close()
    
    # Test transaction with actual table operations
    print("\n3. Testing with table operations...")
    conn = psycopg.connect(
        "host=localhost port=5440 user=postgres dbname=main",
        autocommit=False
    )
    
    try:
        with conn.cursor() as cur:
            # Create table
            print("   - Creating table...")
            cur.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, value TEXT)")
            
            # Insert data
            print("   - Inserting data...")
            cur.execute("INSERT INTO test_table (id, value) VALUES (1, 'test')")
            
            # Now rollback
            print("   - Rolling back transaction...")
            conn.rollback()
            print("   ✅ Rollback succeeded!")
            
            # Verify table doesn't exist
            conn.autocommit = True
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test_table'")
            result = cur.fetchone()
            if result is None:
                print("   ✅ Table was rolled back successfully")
            else:
                print("   ❌ Table still exists after rollback!")
                
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        conn.close()
    
    # Get pgsqlite logs
    print("\n=== pgsqlite logs (last 50 lines) ===")
    pgsqlite.terminate()
    time.sleep(0.5)
    stdout, stderr = pgsqlite.communicate()
    if stderr:
        lines = stderr.decode().split('\n')
        for line in lines[-50:]:
            if line:
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