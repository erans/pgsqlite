#!/usr/bin/env python3
"""Test column mapping"""

import psycopg2
import datetime

# Connect to pgsqlite
conn = psycopg2.connect(
    host="localhost",
    port=5435,
    database="main",
    user="postgres"
)

try:
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50),
            birth_date DATE,
            created_at TIMESTAMP
        )
    """)
    conn.commit()
    
    # Insert test data
    cursor.execute("""
        INSERT INTO users (username, birth_date, created_at) 
        VALUES ('test_user', '2020-05-15'::date, '2025-07-27T05:00:00'::timestamp)
    """)
    conn.commit()
    
    # Query with aliases (like SQLAlchemy does)
    print("Querying with aliases...")
    cursor.execute("""
        SELECT users.id AS users_id, 
               users.username AS users_username,
               users.birth_date AS users_birth_date,
               users.created_at AS users_created_at
        FROM users
    """)
    
    print("Fetching results...")
    results = cursor.fetchall()
    print(f"Got {len(results)} results")
    for row in results:
        print(f"  Row: {row}")
        
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    conn.close()
