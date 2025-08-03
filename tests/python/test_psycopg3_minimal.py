#!/usr/bin/env python3
import psycopg

print("Testing minimal psycopg3 SELECT with cast...")

try:
    conn = psycopg.connect(
        host="localhost",
        port=5433,
        dbname="main",
        user="postgres",
        password=""
    )
    
    with conn.cursor() as cur:
        # Test literal cast
        print("\nTesting SELECT with literal cast...")
        cur.execute("SELECT 1::INTEGER")
        result = cur.fetchone()
        print(f"Result: {result}")
        
        # Test parameter without cast
        print("\nTesting SELECT with parameter (no cast)...")
        cur.execute("SELECT %s", (1,))
        result = cur.fetchone()
        print(f"Result: {result}")
        
        # Test parameter with cast (this is the failing case)
        print("\nTesting SELECT with parameter cast...")
        cur.execute("SELECT %s::INTEGER", (1,))
        result = cur.fetchone()
        print(f"Result: {result}")
    
    conn.close()
    print("\nTest passed!")
    
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()