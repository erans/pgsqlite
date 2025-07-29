#!/usr/bin/env python3
"""Test the exact query that's failing"""

import psycopg2

# Connect to pgsqlite
conn = psycopg2.connect(
    host="localhost",
    port=5435,
    database="main",
    user="postgres"
)

try:
    cursor = conn.cursor()
    
    # Try the exact query from SQLAlchemy logs
    query = """
    SELECT users.id AS users_id, users.username AS users_username, 
           users.email AS users_email, users.full_name AS users_full_name, 
           users.is_active AS users_is_active, users.created_at AS users_created_at, 
           users.birth_date AS users_birth_date 
    FROM users 
    WHERE users.is_active = true
    """
    
    print("Executing query...")
    cursor.execute(query)
    
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