#!/usr/bin/env python3
"""Test if SQLite supports RETURNING clause natively."""

import sqlite3
import sys

def test_sqlite_returning():
    """Test SQLite's native RETURNING support."""
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    
    # Get SQLite version
    cur.execute("SELECT sqlite_version()")
    version = cur.fetchone()[0]
    print(f"SQLite version: {version}")
    
    # SQLite 3.35.0 added RETURNING support
    major, minor, patch = map(int, version.split('.'))
    if major < 3 or (major == 3 and minor < 35):
        print(f"SQLite {version} does not support RETURNING clause (requires 3.35.0+)")
        return False
    
    try:
        # Create test table
        cur.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        
        # Test INSERT RETURNING
        print("\nTesting INSERT RETURNING...")
        cur.execute("INSERT INTO test (value) VALUES (?) RETURNING id, value", ("test1",))
        result = cur.fetchone()
        print(f"INSERT RETURNING result: {result}")
        
        # Test UPDATE RETURNING
        print("\nTesting UPDATE RETURNING...")
        cur.execute("UPDATE test SET value = ? WHERE id = ? RETURNING id, value", ("updated", result[0]))
        result = cur.fetchone()
        print(f"UPDATE RETURNING result: {result}")
        
        # Test DELETE RETURNING
        print("\nTesting DELETE RETURNING...")
        cur.execute("DELETE FROM test WHERE id = ? RETURNING id, value", (result[0],))
        result = cur.fetchone()
        print(f"DELETE RETURNING result: {result}")
        
        print("\nSQLite RETURNING support: ✓ CONFIRMED")
        return True
        
    except sqlite3.OperationalError as e:
        print(f"\nSQLite RETURNING not supported: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    supported = test_sqlite_returning()
    sys.exit(0 if supported else 1)