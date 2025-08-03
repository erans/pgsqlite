#!/usr/bin/env python3
import psycopg
import logging

logging.basicConfig(level=logging.DEBUG)

print("Testing psycopg3 transaction commands...")

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
    
    # Test a simple query first
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        result = cur.fetchone()
        print(f"SELECT 1 result: {result}")
    
    # Now test transaction commands
    print("\nTesting COMMIT...")
    try:
        conn.commit()
        print("COMMIT successful")
    except Exception as e:
        print(f"COMMIT error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    
    # Test ROLLBACK
    print("\nTesting ROLLBACK...")
    try:
        conn.rollback()
        print("ROLLBACK successful")
    except Exception as e:
        print(f"ROLLBACK error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    
    # Test in a transaction block
    print("\nTesting transaction block...")
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE test_tx (id INTEGER)")
                cur.execute("INSERT INTO test_tx VALUES (1)")
                print("Transaction block operations successful")
        print("Transaction block committed successfully")
    except Exception as e:
        print(f"Transaction block error: {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
    
    conn.close()
    
except Exception as e:
    print(f"Connection error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()