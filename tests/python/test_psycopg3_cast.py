#!/usr/bin/env python3
import psycopg

print("Testing psycopg3 SELECT with parameter cast...")

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
        cur.execute("CREATE TABLE IF NOT EXISTS test_cast (id INTEGER)")
        cur.execute("INSERT INTO test_cast VALUES (1)")
        print("Table created and row inserted")
        
        # This is the failing query
        print("\nExecuting SELECT with cast...")
        try:
            cur.execute("SELECT * FROM test_cast WHERE id = %s::INTEGER", (1,))
            result = cur.fetchone()
            print(f"Result: {result}")
        except Exception as e:
            print(f"Query failed: {e}")
            # Try without cast
            print("\nTrying without cast...")
            cur.execute("SELECT * FROM test_cast WHERE id = %s", (1,))
            result = cur.fetchone()
            print(f"Result without cast: {result}")
        
        # Clean up
        cur.execute("DROP TABLE test_cast")
    
    conn.commit()
    conn.close()
    print("\nTest passed!")
    
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()