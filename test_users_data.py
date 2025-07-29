#!/usr/bin/env python3
"""Check what's in the users table"""

import psycopg2
import traceback

# Connect to pgsqlite
conn = psycopg2.connect(
    host="localhost",
    port=5435,
    database="main",
    user="postgres"
)

try:
    cursor = conn.cursor()
    
    # Check if users table exists
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name='users'
    """)
    if not cursor.fetchone():
        print("Users table doesn't exist")
    else:
        # Get raw data from SQLite
        cursor.execute("""
            SELECT id, username, email, created_at, birth_date, 
                   typeof(created_at) as created_type,
                   typeof(birth_date) as birth_type,
                   created_at as raw_created,
                   birth_date as raw_birth
            FROM users
        """)
        
        print("Raw data in users table:")
        for row in cursor.fetchall():
            print(f"  id={row[0]}, username={row[1]}, email={row[2]}")
            print(f"    created_at: {row[3]} (type: {row[5]}, raw: {row[7]})")
            print(f"    birth_date: {row[4]} (type: {row[6]}, raw: {row[8]})")
    
except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()

finally:
    conn.close()