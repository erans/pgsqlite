#!/usr/bin/env python3
"""
Test psycopg3 array binary format with pgsqlite.
"""

import psycopg
import json

def test_array_binary():
    """Test array types with binary format."""
    # Connect to pgsqlite
    conn = psycopg.connect("host=localhost port=15500 user=postgres dbname=main")
    
    try:
        with conn.cursor() as cur:
            # Create test table with array columns
            cur.execute("""
                CREATE TABLE IF NOT EXISTS array_test (
                    id INTEGER PRIMARY KEY,
                    int_array INTEGER[],
                    bigint_array BIGINT[],
                    text_array TEXT[],
                    float_array DOUBLE PRECISION[],
                    bool_array BOOLEAN[]
                )
            """)
            conn.commit()
            
            # Test data
            test_data = {
                "id": 1,
                "int_array": [1, 2, 3, 4, 5],
                "bigint_array": [1000000000000, 2000000000000],
                "text_array": ["hello", "world", "test"],
                "float_array": [3.14, 2.71, 1.41],
                "bool_array": [True, False, True, True]
            }
            
            # Insert with binary format (convert arrays to JSON for pgsqlite)
            cur.execute(
                """
                INSERT INTO array_test 
                (id, int_array, bigint_array, text_array, float_array, bool_array)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    test_data["id"],
                    json.dumps(test_data["int_array"]),
                    json.dumps(test_data["bigint_array"]),
                    json.dumps(test_data["text_array"]),
                    json.dumps(test_data["float_array"]),
                    json.dumps(test_data["bool_array"])
                ),
                binary=True  # Use binary format
            )
            conn.commit()
            
            # Query with binary results
            cur.execute("SELECT * FROM array_test WHERE id = %s", [1], binary=True)
            row = cur.fetchone()
            
            print("✅ Array binary format test successful!")
            print(f"  ID: {row[0]}")
            print(f"  Int array: {row[1]}")
            print(f"  Bigint array: {row[2]}")
            print(f"  Text array: {row[3]}")
            print(f"  Float array: {row[4]}")
            print(f"  Bool array: {row[5]}")
            
            # Test array with NULLs
            cur.execute(
                """
                INSERT INTO array_test (id, int_array)
                VALUES (%s, %s)
                """,
                (2, json.dumps([1, None, 3, None, 5])),
                binary=True
            )
            conn.commit()
            
            cur.execute("SELECT int_array FROM array_test WHERE id = %s", [2], binary=True)
            result = cur.fetchone()
            print(f"\n✅ Array with NULLs: {result[0]}")
            
            # Test empty array
            cur.execute(
                """
                INSERT INTO array_test (id, text_array)
                VALUES (%s, %s)
                """,
                (3, json.dumps([])),
                binary=True
            )
            conn.commit()
            
            cur.execute("SELECT text_array FROM array_test WHERE id = %s", [3], binary=True)
            result = cur.fetchone()
            print(f"✅ Empty array: {result[0]}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    test_array_binary()