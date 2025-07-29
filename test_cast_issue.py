#!/usr/bin/env python3
"""Test to reproduce the cast translator RETURNING issue"""

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
    
    # Create table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS test_cast (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP
        )
    """)
    conn.commit()
    
    # Test the exact query that's failing
    query = """
        INSERT INTO test_cast (created_at) 
        VALUES ('2025-07-27T04:30:00'::timestamp) 
        RETURNING id
    """
    
    print(f"Executing query: {query}")
    cursor.execute(query)
    
    # This should return a row with the id
    result = cursor.fetchone()
    if result:
        print(f"✓ RETURNING worked! Got id: {result[0]}")
    else:
        print("✗ RETURNING failed - no row returned")
    
    conn.commit()
    
except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()

finally:
    conn.close()