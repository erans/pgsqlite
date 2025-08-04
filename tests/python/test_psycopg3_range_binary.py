#!/usr/bin/env python3
"""
Test psycopg3 range types with binary format.
"""

import psycopg
from decimal import Decimal

def test_range_binary():
    """Test range types with binary format."""
    # Connect to pgsqlite
    conn = psycopg.connect("host=localhost port=15500 user=postgres dbname=main")
    
    try:
        with conn.cursor() as cur:
            # Create test table with range columns
            cur.execute("""
                CREATE TABLE IF NOT EXISTS range_test (
                    id INTEGER PRIMARY KEY,
                    int4_range INT4RANGE,
                    int8_range INT8RANGE,
                    num_range NUMRANGE
                )
            """)
            conn.commit()
            
            # Test data
            test_ranges = [
                (1, "[1,10)", "[1000000000000,2000000000000]", "[1.5,3.14159]"),
                (2, "(0,100]", "[0,9223372036854775807)", "[0,99.99)"),
                (3, "empty", "empty", "empty"),
                (4, "[42,42]", "[-1000000000000,-999999999999]", "[-3.14,-1.0]"),
                (5, "(,100]", "[0,)", "(,)")  # Infinite bounds
            ]
            
            # Insert with binary format
            for test_id, int4_val, int8_val, num_val in test_ranges:
                cur.execute(
                    """
                    INSERT INTO range_test (id, int4_range, int8_range, num_range)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (test_id, int4_val, int8_val, num_val),
                    binary=True  # Use binary format
                )
            conn.commit()
            
            # Query with binary results
            cur.execute("SELECT * FROM range_test ORDER BY id", binary=True)
            rows = cur.fetchall()
            
            print("✅ Range binary format test successful!")
            for row in rows:
                print(f"  ID: {row[0]}")
                print(f"    INT4RANGE: {row[1]}")
                print(f"    INT8RANGE: {row[2]}")
                print(f"    NUMRANGE: {row[3]}")
            
            # Test specific queries
            cur.execute(
                "SELECT int4_range FROM range_test WHERE id = %s",
                [1],
                binary=True
            )
            result = cur.fetchone()
            print(f"\n✅ Single range query: {result[0]}")
            
            # Test NULL ranges
            cur.execute(
                """
                INSERT INTO range_test (id, int4_range)
                VALUES (%s, %s)
                """,
                (6, None),
                binary=True
            )
            conn.commit()
            
            cur.execute("SELECT int4_range FROM range_test WHERE id = %s", [6], binary=True)
            result = cur.fetchone()
            print(f"✅ NULL range: {result[0]}")
            
            # Test operations on ranges
            cur.execute(
                """
                SELECT COUNT(*) FROM range_test 
                WHERE int4_range && %s
                """,
                ["[5,15)"],
                binary=True
            )
            count = cur.fetchone()[0]
            print(f"\n✅ Ranges overlapping [5,15): {count}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    test_range_binary()