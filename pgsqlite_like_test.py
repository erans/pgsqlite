#!/usr/bin/env python3
"""
Test that mimics pgsqlite's SQLite setup to identify the visibility issue.
"""

import sqlite3
import os
import tempfile

def test_pgsqlite_like_setup():
    # Use the same path as the test
    db_path = "/tmp/pgsqlite_visibility_test.db"
    
    # Clean up any existing files
    for ext in ['', '-wal', '-shm']:
        try:
            os.unlink(db_path + ext)
        except:
            pass
    
    try:
        print(f"Testing pgsqlite-like setup with database: {db_path}")
        
        # Connection 1: Mimic pgsqlite connection setup
        conn1 = sqlite3.connect(db_path)
        
        # Apply the same pragmas as pgsqlite
        conn1.execute("PRAGMA journal_mode = WAL")
        conn1.execute("PRAGMA synchronous = NORMAL") 
        conn1.execute("PRAGMA cache_size = -64000")
        conn1.execute("PRAGMA temp_store = MEMORY")
        conn1.execute("PRAGMA mmap_size = 268435456")
        
        print("✅ Applied pgsqlite-like pragmas")
        
        # Create some tables like pgsqlite does (simplified)
        conn1.execute("""
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT UNIQUE,
                full_name TEXT
            )
        """)
        conn1.commit()
        
        # Insert and update data
        conn1.execute("INSERT INTO users (username, full_name) VALUES ('test_user', 'Original Name')")
        conn1.commit()
        print("✅ Connection 1: Inserted user")
        
        conn1.execute("UPDATE users SET full_name = 'Updated Name' WHERE username = 'test_user'")
        conn1.commit()
        print("✅ Connection 1: Updated name to 'Updated Name' and committed")
        
        # Verify connection 1 sees the update
        result = conn1.execute("SELECT full_name FROM users WHERE username = 'test_user'").fetchone()
        print(f"✅ Connection 1 sees: '{result[0]}'")
        
        # Close connection 1 to mimic what SQLAlchemy does
        conn1.close()
        print("✅ Connection 1 closed")
        
        # Connection 2: New connection created AFTER commit and close (like separate SQLAlchemy engine)
        conn2 = sqlite3.connect(db_path)
        
        # Apply same pragmas
        conn2.execute("PRAGMA journal_mode = WAL")
        conn2.execute("PRAGMA synchronous = NORMAL")
        conn2.execute("PRAGMA cache_size = -64000") 
        conn2.execute("PRAGMA temp_store = MEMORY")
        conn2.execute("PRAGMA mmap_size = 268435456")
        
        print("✅ Connection 2: Applied pragmas")
        
        # Check what connection 2 sees
        result = conn2.execute("SELECT full_name FROM users WHERE username = 'test_user'").fetchone()
        print(f"📍 Connection 2 sees: '{result[0]}'")
        
        conn2.close()
        
        if result[0] == 'Updated Name':
            print("✅ SUCCESS: New connection sees committed update")
            return True
        else:
            print("❌ FAILURE: New connection does not see committed update")
            return False
            
    finally:
        # Cleanup
        for ext in ['', '-wal', '-shm']:
            try:
                os.unlink(db_path + ext)
            except:
                pass

if __name__ == "__main__":
    success = test_pgsqlite_like_setup()
    exit(0 if success else 1)