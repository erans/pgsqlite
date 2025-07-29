#!/usr/bin/env python3
"""
Direct SQLite test to verify transaction visibility across connections.
This test bypasses pgsqlite entirely to check raw SQLite behavior.
"""

import sqlite3
import os
import tempfile

def test_sqlite_visibility():
    # Create a temporary database file
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
        db_path = f.name
    
    try:
        print(f"Testing SQLite visibility with database: {db_path}")
        
        # Connection 1: Create table and insert data
        conn1 = sqlite3.connect(db_path)
        conn1.execute("PRAGMA journal_mode = WAL")
        conn1.execute("PRAGMA synchronous = NORMAL")
        
        conn1.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
        conn1.execute("INSERT INTO test_table (name) VALUES ('Original Name')")
        conn1.commit()
        
        # Update the name
        conn1.execute("UPDATE test_table SET name = 'Updated Name' WHERE id = 1")
        conn1.commit()
        print("✅ Connection 1: Updated name to 'Updated Name' and committed")
        
        # Verify connection 1 sees the update
        result = conn1.execute("SELECT name FROM test_table WHERE id = 1").fetchone()
        print(f"✅ Connection 1 sees: '{result[0]}'")
        
        # Connection 2: New connection created AFTER commit
        conn2 = sqlite3.connect(db_path)
        conn2.execute("PRAGMA journal_mode = WAL")
        conn2.execute("PRAGMA synchronous = NORMAL")
        
        # Check what connection 2 sees
        result = conn2.execute("SELECT name FROM test_table WHERE id = 1").fetchone()
        print(f"📍 Connection 2 sees: '{result[0]}'")
        
        if result[0] == 'Updated Name':
            print("✅ SUCCESS: New connection sees committed update")
            return True
        else:
            print("❌ FAILURE: New connection does not see committed update")
            return False
            
    finally:
        # Cleanup
        try:
            conn1.close()
            conn2.close()
            os.unlink(db_path)
            # Also clean up WAL and SHM files
            wal_path = db_path + '-wal'
            shm_path = db_path + '-shm'
            if os.path.exists(wal_path):
                os.unlink(wal_path)
            if os.path.exists(shm_path):
                os.unlink(shm_path)
        except:
            pass

if __name__ == "__main__":
    success = test_sqlite_visibility()
    exit(0 if success else 1)