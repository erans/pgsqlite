#!/usr/bin/env python3
"""Minimal test to reproduce transaction visibility issue"""

import psycopg2
import sys
import time

def test_transaction_visibility(port=15500):
    # First connection - insert and update
    conn1 = psycopg2.connect(
        host="localhost",
        port=port,
        database="main",
        user="postgres"
    )
    conn1.autocommit = False
    cur1 = conn1.cursor()
    
    # Create table
    cur1.execute("CREATE TABLE IF NOT EXISTS test_tx (id INTEGER PRIMARY KEY, value TEXT)")
    conn1.commit()
    
    # Clean up any existing data
    cur1.execute("DELETE FROM test_tx")
    conn1.commit()
    
    # Insert initial data
    cur1.execute("INSERT INTO test_tx (id, value) VALUES (1, 'original')")
    conn1.commit()
    print("✅ Inserted: id=1, value='original'")
    
    # Update the data
    cur1.execute("UPDATE test_tx SET value='updated' WHERE id=1")
    print("✅ Updated: value='updated'")
    
    # Commit the update
    conn1.commit()
    print("✅ COMMITTED the update")
    
    # Check from same connection
    cur1.execute("SELECT value FROM test_tx WHERE id=1")
    result1 = cur1.fetchone()
    print(f"📍 Same connection sees: {result1[0]}")
    
    # Close first connection
    cur1.close()
    conn1.close()
    
    # Small delay
    time.sleep(0.1)
    
    # Create new connection
    conn2 = psycopg2.connect(
        host="localhost",
        port=port,
        database="main",
        user="postgres"
    )
    cur2 = conn2.cursor()
    
    # Check from new connection
    cur2.execute("SELECT value FROM test_tx WHERE id=1")
    result2 = cur2.fetchone()
    print(f"📍 New connection sees: {result2[0]}")
    
    # Cleanup
    cur2.execute("DROP TABLE test_tx")
    conn2.commit()
    cur2.close()
    conn2.close()
    
    # Return test result
    if result2[0] == 'updated':
        print("✅ Transaction visibility test PASSED")
        return True
    else:
        print("❌ Transaction visibility test FAILED")
        print(f"   Expected: 'updated', Got: '{result2[0]}'")
        return False

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 15500
    success = test_transaction_visibility(port)
    sys.exit(0 if success else 1)