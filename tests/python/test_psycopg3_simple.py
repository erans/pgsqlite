#!/usr/bin/env python3
"""Simple psycopg3 connectivity test."""

import psycopg

try:
    # Connect to pgsqlite
    conn = psycopg.connect("host=localhost port=15501 dbname=main user=postgres", autocommit=True)
    print("✅ Connected successfully with psycopg3")
    
    # Test simple query
    with conn.cursor() as cur:
        cur.execute("SELECT 1 as test")
        result = cur.fetchone()
        print(f"✅ Simple query result: {result}")
    
    # Test table creation
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)")
        print("✅ Table created successfully")
    
    conn.close()
    print("✅ All tests passed!")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()