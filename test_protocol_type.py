#!/usr/bin/env python3
"""Test what protocol psycopg2 uses"""

import psycopg2
import psycopg2.extras
import datetime

# Connect to pgsqlite
conn = psycopg2.connect(
    host="localhost",
    port=5435,
    database="main",
    user="postgres"
)

try:
    # Test with server-side cursor (uses extended protocol)
    cursor = conn.cursor(name='test_cursor')
    cursor.execute("SELECT 1")
    print("Server-side cursor result:", cursor.fetchone())
    cursor.close()
    
    # Test with regular cursor (uses simple protocol)
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    print("Regular cursor result:", cursor.fetchone())
    
    # Now test with dates
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS protocol_test (
            id SERIAL PRIMARY KEY,
            test_date DATE
        )
    """)
    conn.commit()
    
    # Insert with date
    cursor.execute("INSERT INTO protocol_test (test_date) VALUES (%s) RETURNING id, test_date", 
                   (datetime.date(2025, 7, 27),))
    print("INSERT RETURNING result:", cursor.fetchone())
    conn.commit()
    
    # Select back - this is where the error might happen
    cursor.execute("SELECT id, test_date FROM protocol_test")
    print("SELECT result:", cursor.fetchone())
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    conn.close()