#!/usr/bin/env python3
import psycopg
import subprocess
import time
import os

# Start pgsqlite
pgsqlite_path = "../../target/debug/pgsqlite"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", ":memory:", "--port", "5440"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)

# Give it time to start
time.sleep(1)

try:
    # Connect with psycopg3
    conn = psycopg.connect(
        "host=localhost port=5440 user=postgres dbname=main",
        autocommit=True
    )
    
    print("=== Debug INSERT Test ===")
    
    # Test creating and inserting into a fresh table
    with conn.cursor() as cur:
        # Create a uniquely named table
        table_name = f"test_table_{int(time.time())}"
        print(f"\n1. Creating table {table_name}...")
        cur.execute(f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY, name TEXT)")
        print("   ✅ Table created")
        
        # Insert without RETURNING first
        print("\n2. Testing simple INSERT...")
        cur.execute(f"INSERT INTO {table_name} (id, name) VALUES (%s, %s)", (1, "test1"))
        print("   ✅ INSERT succeeded")
        
        # Insert with RETURNING
        print("\n3. Testing INSERT RETURNING...")
        cur.execute(f"INSERT INTO {table_name} (id, name) VALUES (%s, %s) RETURNING id", (2, "test2"))
        result = cur.fetchone()
        print(f"   ✅ INSERT RETURNING succeeded, got ID: {result[0]}")
        
        # Insert with RETURNING and cast
        print("\n4. Testing INSERT RETURNING with cast...")
        cur.execute(f"INSERT INTO {table_name} (id, name) VALUES (%s, %s::VARCHAR) RETURNING id", (3, "test3"))
        result = cur.fetchone()
        print(f"   ✅ INSERT RETURNING with cast succeeded, got ID: {result[0]}")
        
        # Verify data
        print("\n5. Verifying data...")
        cur.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cur.fetchone()[0]
        print(f"   ✅ Found {count} rows")
    
    print("\n✅ All tests passed!")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()