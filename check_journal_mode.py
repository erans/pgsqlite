#!/usr/bin/env python3
"""Check the journal mode in pgsqlite"""

import psycopg2

try:
    # Connect to pgsqlite
    conn = psycopg2.connect(
        host='localhost',
        port=5435,
        dbname='main',
        user='postgres'
    )
    
    cur = conn.cursor()
    
    # Check journal mode
    cur.execute("SELECT 'journal_mode', 'current' AS pragma")
    
    # Try to execute PRAGMA directly (pgsqlite might not support this)
    try:
        cur.execute("PRAGMA journal_mode")
        result = cur.fetchone()
        print(f"Journal mode: {result}")
    except Exception as e:
        print(f"Cannot query PRAGMA directly: {e}")
    
    # Let's check by looking at the database file itself
    import sqlite3
    conn2 = sqlite3.connect('main.db')
    cur2 = conn2.cursor()
    result = cur2.execute("PRAGMA journal_mode").fetchone()
    print(f"Direct SQLite check - Journal mode: {result[0]}")
    conn2.close()
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()