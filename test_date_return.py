#!/usr/bin/env python3
"""Test date return values"""

import psycopg2
import datetime
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
        CREATE TABLE IF NOT EXISTS test_dates (
            id SERIAL PRIMARY KEY,
            test_date DATE,
            test_timestamp TIMESTAMP
        )
    """)
    conn.commit()
    
    # Insert a test date
    cursor.execute("""
        INSERT INTO test_dates (test_date, test_timestamp) 
        VALUES (%s, %s)
        RETURNING id, test_date, test_timestamp
    """, (datetime.date(2025, 7, 27), datetime.datetime(2025, 7, 27, 10, 30, 0)))
    
    result = cursor.fetchone()
    print(f"Inserted: id={result[0]}, date={result[1]}, timestamp={result[2]}")
    conn.commit()
    
    # Try to read it back
    cursor.execute("SELECT id, test_date, test_timestamp FROM test_dates WHERE id = %s", (result[0],))
    result = cursor.fetchone()
    print(f"Retrieved: id={result[0]}, date={result[1]}, timestamp={result[2]}")
    
except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()

finally:
    conn.close()