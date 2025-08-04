#!/usr/bin/env python3
"""Simple test for psycopg3 text mode with pgsqlite."""

import psycopg
import sys

def main():
    try:
        # Connect using psycopg3
        conn = psycopg.connect(
            host="localhost",
            port=15500,
            dbname="main",
            user="postgres",
            autocommit=True
        )
        
        print("✅ Connected successfully")
        
        # Test 1: Simple query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            print(f"✅ Simple query result: {result}")
        
        # Test 2: System function
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            result = cur.fetchone()
            print(f"✅ Version: {result[0]}")
            
        # Test 3: to_regtype function
        with conn.cursor() as cur:
            cur.execute("SELECT to_regtype('integer')")
            result = cur.fetchone()
            print(f"✅ to_regtype('integer'): {result[0]}")
            
        # Test 4: to_regtype with non-existent type
        with conn.cursor() as cur:
            cur.execute("SELECT to_regtype('hstore')")
            result = cur.fetchone()
            print(f"✅ to_regtype('hstore'): {result[0]}")
            
        # Test 5: Catalog query with to_regtype
        with conn.cursor() as cur:
            cur.execute("""
                SELECT typname AS name, oid, typarray AS array_oid,
                       oid::regtype::text AS regtype, typdelim AS delimiter
                FROM pg_type t
                WHERE t.oid = to_regtype(%s)
                ORDER BY t.oid
            """, ('hstore',))
            result = cur.fetchall()
            print(f"✅ Catalog query result count: {len(result)}")
            
        # Test 6: Create table
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS test_table")
            cur.execute("""
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            """)
            print("✅ Table created")
            
        # Test 7: Insert with parameters
        with conn.cursor() as cur:
            cur.execute("INSERT INTO test_table (id, name) VALUES (%s, %s)", (1, "Test"))
            print("✅ Insert successful")
            
        # Test 8: Select with LIMIT parameter (INT2 binary)
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM test_table LIMIT %s", (1,))
            result = cur.fetchall()
            print(f"✅ Select with LIMIT result: {result}")
            
        print("\n🎉 All tests passed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()