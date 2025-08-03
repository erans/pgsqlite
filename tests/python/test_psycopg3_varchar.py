#!/usr/bin/env python3
import psycopg

print("Testing psycopg3 SELECT with VARCHAR cast...")

try:
    conn = psycopg.connect(
        host="localhost",
        port=5433,
        dbname="main",
        user="postgres",
        password=""
    )
    
    with conn.cursor() as cur:
        # Create table
        cur.execute("CREATE TABLE IF NOT EXISTS test_varchar (name VARCHAR(50))")
        cur.execute("INSERT INTO test_varchar VALUES ('test')")
        print("Table created and row inserted")
        
        # Test simple parameter (no cast) - should work
        print("\nTesting SELECT with parameter (no cast)...")
        cur.execute("SELECT * FROM test_varchar WHERE name = %s", ("test",))
        result = cur.fetchone()
        print(f"Result: {result}")
        
        # Test parameter with VARCHAR cast - this might fail
        print("\nTesting SELECT with VARCHAR cast...")
        cur.execute("SELECT * FROM test_varchar WHERE name = %s::VARCHAR", ("test",))
        result = cur.fetchone()
        print(f"Result: {result}")
        
        # Clean up
        cur.execute("DROP TABLE test_varchar")
    
    conn.commit()
    conn.close()
    print("\nTest passed!")
    
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()