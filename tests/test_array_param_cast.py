#!/usr/bin/env python3
import psycopg
import subprocess
import time
import os

# Start pgsqlite with debug logging
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_array_param_cast.db"

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

env = os.environ.copy()
env["RUST_LOG"] = "pgsqlite::translator=debug"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", "5438"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
    env=env
)

# Give it time to start
time.sleep(1)

try:
    # Connect with psycopg3
    conn = psycopg.connect(
        "host=localhost port=5438 user=postgres dbname=main",
        autocommit=True
    )
    
    print("=== Testing ARRAY with parameter casts ===")
    
    # Test the exact query that's failing
    with conn.cursor() as cur:
        print("\n1. Testing ANY(ARRAY[...]) with parameter casts...")
        query = """
        SELECT pg_catalog.pg_class.relname 
        FROM pg_catalog.pg_class 
        JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
        WHERE pg_catalog.pg_class.relname = %s::VARCHAR 
        AND pg_catalog.pg_class.relkind = ANY (ARRAY[%s::VARCHAR, %s::VARCHAR, %s::VARCHAR, %s::VARCHAR, %s::VARCHAR]) 
        AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) 
        AND pg_catalog.pg_namespace.nspname != %s::VARCHAR
        """
        
        params = ('users', 'r', 'p', 'f', 'v', 'm', 'pg_catalog')
        
        try:
            cur.execute(query, params)
            results = cur.fetchall()
            print(f"   ✅ SUCCESS: Found {len(results)} results")
            for row in results:
                print(f"      - {row[0]}")
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
            
    # Test simpler version
    with conn.cursor() as cur:
        print("\n2. Testing simple ANY(ARRAY) with casts...")
        try:
            cur.execute("SELECT 'r' = ANY(ARRAY[%s::VARCHAR, %s::VARCHAR])", ('r', 'p'))
            result = cur.fetchone()
            print(f"   ✅ Result: {result[0]}")
        except Exception as e:
            print(f"   ❌ ERROR: {e}")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()
    
    # Clean up database
    for ext in ['', '-shm', '-wal']:
        db_file = f"{db_path}{ext}"
        if os.path.exists(db_file):
            os.remove(db_file)