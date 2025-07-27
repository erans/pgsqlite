#!/usr/bin/env python3
"""Check database directly with sqlite3"""

import sqlite3

# Connect directly to SQLite
conn = sqlite3.connect("sqlalchemy_new.db")
cursor = conn.cursor()

try:
    # Check schema info
    cursor.execute("SELECT * FROM __pgsqlite_schema WHERE table_name = 'users'")
    print("Schema info for users table:")
    for row in cursor.fetchall():
        print(f"  {row}")
    
    # Get raw data
    cursor.execute("SELECT id, username, created_at, birth_date FROM users")
    print("\nRaw data in users table:")
    for row in cursor.fetchall():
        print(f"  id={row[0]}, username={row[1]}, created_at={row[2]}, birth_date={row[3]}")
        
except Exception as e:
    print(f"Error: {e}")
    
finally:
    conn.close()