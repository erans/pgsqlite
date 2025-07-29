#!/usr/bin/env python3

import psycopg2

# Connect to pgsqlite
conn = psycopg2.connect(
    host="localhost",
    port=5435,
    database="main",
    user="postgres"
)

cur = conn.cursor()

# Create a simple test table
cur.execute("""
    CREATE TABLE IF NOT EXISTS test_users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(50),
        created_at TIMESTAMP
    )
""")
conn.commit()

# Test simple INSERT RETURNING
print("Testing simple INSERT RETURNING...")
try:
    cur.execute("""
        INSERT INTO test_users (username, created_at) 
        VALUES ('test_user', '2025-07-27T04:30:00'::timestamp) 
        RETURNING id
    """)
    
    result = cur.fetchone()
    print(f"SUCCESS: Got ID = {result}")
    conn.commit()
except Exception as e:
    print(f"FAILED: {e}")
    conn.rollback()

# Close connection
cur.close()
conn.close()