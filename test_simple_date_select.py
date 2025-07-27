#!/usr/bin/env python3
"""Test simple date SELECT"""

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
    
    # Create a simple table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS date_test (
            id INTEGER PRIMARY KEY,
            test_date DATE
        )
    """)
    conn.commit()
    
    # Insert using raw SQL (no parameters)
    cursor.execute("""
        INSERT INTO date_test (id, test_date) 
        VALUES (1, '2025-07-27'::date)
    """)
    conn.commit()
    
    # Try to select it back
    print("Attempting SELECT...")
    cursor.execute("SELECT id, test_date FROM date_test WHERE id = 1")
    result = cursor.fetchone()
    print(f"Result: {result}")
    
except Exception as e:
    print(f"Error during operation: {e}")
    import traceback
    traceback.print_exc()

finally:
    conn.close()