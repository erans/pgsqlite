#!/usr/bin/env python3
"""Test NULL date handling"""

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
        CREATE TABLE IF NOT EXISTS test_null_dates (
            id SERIAL PRIMARY KEY,
            nullable_date DATE,
            nullable_timestamp TIMESTAMP
        )
    """)
    conn.commit()
    
    # Insert test data with NULLs
    cursor.execute("""
        INSERT INTO test_null_dates (nullable_date, nullable_timestamp) 
        VALUES (NULL, NULL)
        RETURNING id, nullable_date, nullable_timestamp
    """)
    
    result = cursor.fetchone()
    print(f"Inserted with NULLs: id={result[0]}, date={result[1]}, timestamp={result[2]}")
    conn.commit()
    
    # Insert test data with actual values
    cursor.execute("""
        INSERT INTO test_null_dates (nullable_date, nullable_timestamp) 
        VALUES ('2025-07-27'::date, '2025-07-27T10:30:00'::timestamp)
        RETURNING id, nullable_date, nullable_timestamp
    """)
    
    result = cursor.fetchone()
    print(f"Inserted with values: id={result[0]}, date={result[1]}, timestamp={result[2]}")
    conn.commit()
    
    # Select all rows
    cursor.execute("SELECT id, nullable_date, nullable_timestamp FROM test_null_dates ORDER BY id")
    results = cursor.fetchall()
    for row in results:
        print(f"Selected: id={row[0]}, date={row[1]}, timestamp={row[2]}")
    
except Exception as e:
    print(f"Error: {e}")
    traceback.print_exc()

finally:
    conn.close()