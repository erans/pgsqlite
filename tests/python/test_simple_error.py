#!/usr/bin/env python3
"""Test simple error case."""

import psycopg
import time

print("Testing error handling...")

try:
    conn = psycopg.connect(
        "host=localhost port=5432 user=postgres dbname=main",
        autocommit=False
    )
    
    with conn.cursor() as cur:
        # Create table
        cur.execute("CREATE TABLE test (id INTEGER PRIMARY KEY)")
        conn.commit()
        
        # Insert first record
        cur.execute("INSERT INTO test (id) VALUES (%s)", (1,))
        conn.commit()
        
        # Try to insert duplicate (should fail)
        print("Attempting duplicate insert...")
        cur.execute("INSERT INTO test (id) VALUES (%s)", (1,))
        conn.commit()
        
except Exception as e:
    print(f"Error type: {type(e).__name__}")
    print(f"Error: {e}")
    if hasattr(e, 'diag') and e.diag:
        print(f"sqlstate: {e.diag.sqlstate}")
        print(f"message: {e.diag.message_primary}")
finally:
    try:
        conn.close()
    except:
        pass