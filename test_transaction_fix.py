#!/usr/bin/env python3

"""
Simple test to verify transaction persistence fix after disabling wire protocol cache.
This tests the exact same scenario that was failing before: committed updates from one
session should be visible to new sessions.
"""

import psycopg2
import time
import subprocess
import signal
import os
import sys

def test_transaction_persistence():
    """Test that a commit from one session is visible to a new session"""
    
    # Start pgsqlite server
    print("Starting pgsqlite server...")
    db_path = "/tmp/transaction_fix_test.db"
    
    # Clean up any existing database
    for file in [db_path, f"{db_path}-wal", f"{db_path}-shm"]:
        try:
            os.remove(file)
        except FileNotFoundError:
            pass
    
    # Build and start pgsqlite
    subprocess.run(["cargo", "build"], check=True, cwd="/home/eran/work/pgsqlite")
    
    server = subprocess.Popen([
        "/home/eran/work/pgsqlite/target/debug/pgsqlite",
        "--database", db_path,
        "--port", "15432"
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Wait for server to start
    time.sleep(2)
    
    try:
        # Connection 1: Create table, insert, update, commit
        print("Session 1: Creating table and inserting data...")
        conn1 = psycopg2.connect(
            host="localhost",
            port=15432,
            user="postgres", 
            database="main"
        )
        
        with conn1.cursor() as cur:
            # Create table
            cur.execute("""
                CREATE TABLE test_users (
                    id INTEGER PRIMARY KEY,
                    username TEXT UNIQUE,
                    full_name TEXT
                )
            """)
            
            # Insert data
            cur.execute("""
                INSERT INTO test_users (username, full_name) 
                VALUES ('test_user', 'Original Name')
            """)
            
            # Update data  
            cur.execute("""
                UPDATE test_users 
                SET full_name = 'Updated Name' 
                WHERE username = 'test_user'
            """)
            
            # Verify session 1 sees the update
            cur.execute("SELECT full_name FROM test_users WHERE username = 'test_user'")
            result = cur.fetchone()[0]
            print(f"Session 1 sees: '{result}'")
            assert result == "Updated Name", f"Session 1 should see 'Updated Name', got '{result}'"
        
        # Commit and close connection 1
        conn1.commit()
        conn1.close()
        print("Session 1: Committed and closed")
        
        # Small delay to ensure commit is processed
        time.sleep(0.1)
        
        # Connection 2: New session should see the committed update
        print("Session 2: Creating new connection...")
        conn2 = psycopg2.connect(
            host="localhost",
            port=15432,
            user="postgres",
            database="main"
        )
        
        with conn2.cursor() as cur:
            # This is the critical test - new session should see committed update
            cur.execute("SELECT full_name FROM test_users WHERE username = 'test_user'")
            result = cur.fetchone()[0] 
            print(f"Session 2 sees: '{result}'")
            
            if result == "Updated Name":
                print("✅ SUCCESS: Transaction persistence fixed! New session sees committed update")
                return True
            else:
                print(f"❌ FAILURE: New session sees '{result}' instead of 'Updated Name'")
                return False
        
        conn2.close()
        
    finally:
        # Cleanup
        try:
            server.terminate()
            server.wait(timeout=5)
        except:
            server.kill()
        
        for file in [db_path, f"{db_path}-wal", f"{db_path}-shm"]:
            try:
                os.remove(file)
            except FileNotFoundError:
                pass

if __name__ == "__main__":
    success = test_transaction_persistence()
    sys.exit(0 if success else 1)