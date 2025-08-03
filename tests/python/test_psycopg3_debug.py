#!/usr/bin/env python3
import psycopg

print("Testing psycopg3 SELECT with parameter...")

try:
    # Connect to pgsqlite
    conn = psycopg.connect(
        host="localhost",
        port=5433,
        dbname="main",
        user="postgres",
        password=""
    )
    
    print("Connected successfully")
    
    with conn.cursor() as cur:
        # Create a simple table
        cur.execute("CREATE TABLE IF NOT EXISTS test_simple (id INTEGER, name TEXT)")
        print("Table created")
        
        # Insert a row
        cur.execute("INSERT INTO test_simple VALUES (1, 'test')")
        print("Row inserted")
        
        # Simple SELECT with parameter (no cast)
        print("\nTesting SELECT with simple parameter...")
        cur.execute("SELECT * FROM test_simple WHERE id = %s", (1,))
        result = cur.fetchone()
        print(f"Result: {result}")
        
        # SELECT with parameter cast
        print("\nTesting SELECT with parameter cast...")
        cur.execute("SELECT * FROM test_simple WHERE id = %s::INTEGER", (1,))
        result = cur.fetchone()
        print(f"Result: {result}")
        
        # Clean up
        cur.execute("DROP TABLE test_simple")
        print("\nTable dropped")
    
    conn.commit()
    conn.close()
    print("\nAll tests passed!")
    
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()