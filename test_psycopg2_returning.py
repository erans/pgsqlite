#!/usr/bin/env python3
"""Test psycopg2 directly with RETURNING to understand the protocol."""

import sys
import psycopg2

def main(port):
    # Connect directly with psycopg2
    conn = psycopg2.connect(
        host="localhost",
        port=port,
        database="main",
        user="postgres",
        password="postgres"
    )
    
    cursor = conn.cursor()
    
    # Create test table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS psycopg2_test (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50)
        )
    """)
    conn.commit()
    
    # Test INSERT with RETURNING
    print("=== Testing INSERT with RETURNING ===")
    cursor.execute("INSERT INTO psycopg2_test (name) VALUES (%s) RETURNING id", ("Test User",))
    
    # Check cursor attributes
    print(f"cursor.description: {cursor.description}")
    print(f"cursor.rowcount: {cursor.rowcount}")
    
    # Try to fetch the result
    try:
        result = cursor.fetchone()
        print(f"Fetched result: {result}")
    except Exception as e:
        print(f"Error fetching: {e}")
    
    conn.commit()
    
    # Test regular INSERT without RETURNING
    print("\n=== Testing INSERT without RETURNING ===")
    cursor.execute("INSERT INTO psycopg2_test (name) VALUES (%s)", ("Test User 2",))
    
    # Check cursor attributes
    print(f"cursor.description: {cursor.description}")
    print(f"cursor.rowcount: {cursor.rowcount}")
    
    # Try to fetch (should fail)
    try:
        result = cursor.fetchone()
        print(f"Fetched result: {result}")
    except Exception as e:
        print(f"Error fetching (expected): {e}")
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_psycopg2_returning.py <port>")
        sys.exit(1)
    
    main(int(sys.argv[1]))