#!/usr/bin/env python3
"""Test implicit transaction behavior"""

import psycopg2
import logging

# Enable detailed logging to see what's happening
logging.basicConfig(level=logging.DEBUG)

print("=== Testing Implicit Transaction Behavior ===")

# Test with autocommit=False (SQLAlchemy style)
print("\nTest 1: autocommit=False behavior")
conn = psycopg2.connect(
    host='localhost', 
    port=5435, 
    dbname='main', 
    user='postgres'
)
conn.autocommit = False  # This is what SQLAlchemy does

cur = conn.cursor()

try:
    # Create test table
    cur.execute("DROP TABLE IF EXISTS implicit_test")
    cur.execute("CREATE TABLE implicit_test (id INTEGER PRIMARY KEY, value TEXT)")
    conn.commit()  # Explicit commit for DDL
    
    # Test implicit transaction behavior
    print("\n--- Insert with implicit transaction ---")
    cur.execute("INSERT INTO implicit_test (value) VALUES ('test1')")
    print("✅ INSERT executed (no explicit BEGIN)")
    
    conn.commit()  # This should commit the implicit transaction
    print("✅ COMMIT executed")
    
    # Start another implicit transaction
    print("\n--- Update with implicit transaction ---")
    cur.execute("UPDATE implicit_test SET value = 'updated' WHERE id = 1")
    print("✅ UPDATE executed (no explicit BEGIN)")
    
    conn.commit()  # This should commit the update
    print("✅ COMMIT executed")
    
    # Check the result
    cur.execute("SELECT value FROM implicit_test WHERE id = 1")
    result = cur.fetchone()
    print(f"Result after commit: {result[0]}")
    
    # Now test what happens when we close without explicit rollback
    print("\n--- Testing connection close behavior ---")
    cur.execute("UPDATE implicit_test SET value = 'will_this_persist' WHERE id = 1")
    print("UPDATE executed, now closing connection without explicit commit/rollback...")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    cur.close()
    conn.close()  # This should trigger rollback if there's an uncommitted transaction

# Check what actually persisted
print("\n--- Checking what persisted after connection close ---")
conn2 = psycopg2.connect(
    host='localhost', 
    port=5435, 
    dbname='main', 
    user='postgres'
)
cur2 = conn2.cursor()

try:
    cur2.execute("SELECT value FROM implicit_test WHERE id = 1")
    result = cur2.fetchone()
    print(f"Final result: {result[0] if result else 'No data'}")
    
    if result and result[0] == 'will_this_persist':
        print("❌ Uncommitted transaction was not rolled back!")
    elif result and result[0] == 'updated':
        print("✅ Uncommitted transaction was properly rolled back")
    else:
        print(f"? Unexpected result: {result}")
        
except Exception as e:
    print(f"Error checking final state: {e}")
    
finally:
    cur2.close()
    conn2.close()

print("\n=== Test completed ===")