#!/usr/bin/env python3
import psycopg

print("Testing only the failing case...")

try:
    conn = psycopg.connect(
        host="localhost",
        port=5433,
        dbname="main",
        user="postgres",
        password=""
    )
    
    with conn.cursor() as cur:
        # This is the failing case
        print("\nExecuting SELECT %s::INTEGER...")
        cur.execute("SELECT %s::INTEGER", (1,))
        result = cur.fetchone()
        print(f"Result: {result}")
    
    conn.close()
    print("\nTest passed!")
    
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()