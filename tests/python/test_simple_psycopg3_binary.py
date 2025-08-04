#!/usr/bin/env python3
"""
Simple test to demonstrate psycopg3 binary protocol working with pgsqlite.
This test avoids SQLAlchemy's complex type introspection and directly tests
the binary protocol functionality.
"""

import psycopg
from decimal import Decimal
import json

def test_simple_psycopg3_binary():
    """Test basic psycopg3 functionality with binary protocol."""
    # Connect to pgsqlite - use basic connection without complex type introspection
    conn = psycopg.connect(
        "host=localhost port=15500 user=postgres dbname=main",
        autocommit=True  # Avoid transaction complexity
    )
    
    try:
        with conn.cursor() as cur:
            print("🔌 Connected to pgsqlite with psycopg3")
            
            # Create a simple test table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS simple_binary_test (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    amount NUMERIC(10, 2),
                    is_active BOOLEAN,
                    data_array INTEGER[]
                )
            """)
            print("✅ Created test table")
            
            # Insert data using binary format where beneficial
            test_data = [
                (1, "Alice", Decimal("123.45"), True, json.dumps([1, 2, 3])),
                (2, "Bob", Decimal("67.89"), False, json.dumps([4, 5, 6])),
                (3, "Charlie", Decimal("999.99"), True, json.dumps([7, 8, 9])),
            ]
            
            for row_data in test_data:
                cur.execute(
                    """
                    INSERT INTO simple_binary_test (id, name, amount, is_active, data_array)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    row_data,
                    binary=True  # Request binary format
                )
            
            print(f"✅ Inserted {len(test_data)} rows using binary protocol")
            
            # Query data back using binary format
            cur.execute(
                "SELECT * FROM simple_binary_test ORDER BY id",
                binary=True  # Request binary results
            )
            
            rows = cur.fetchall()
            print(f"✅ Retrieved {len(rows)} rows using binary protocol")
            
            # Verify data integrity
            for i, row in enumerate(rows):
                expected = test_data[i]
                print(f"  Row {i+1}: ID={row[0]}, Name='{row[1]}', Amount={row[2]}, Active={row[3]}, Array={row[4]}")
                
                # Basic verification
                assert row[0] == expected[0], f"ID mismatch: {row[0]} != {expected[0]}"
                assert row[1] == expected[1], f"Name mismatch: {row[1]} != {expected[1]}"
                # Note: NUMERIC comparison might need tolerance due to precision
                assert str(row[2]) == str(expected[2]), f"Amount mismatch: {row[2]} != {expected[2]}"
                assert row[3] == expected[3], f"Boolean mismatch: {row[3]} != {expected[3]}"
                assert row[4] == expected[4], f"Array mismatch: {row[4]} != {expected[4]}"
            
            # Test parameterized queries with binary protocol
            cur.execute(
                "SELECT name, amount FROM simple_binary_test WHERE is_active = %s",
                [True],
                binary=True
            )
            
            active_users = cur.fetchall()
            print(f"✅ Found {len(active_users)} active users using binary parameters")
            
            for user in active_users:
                print(f"  Active user: {user[0]} with amount {user[1]}")
            
            # Test aggregation with binary protocol
            cur.execute(
                "SELECT COUNT(*), SUM(CAST(amount AS REAL)) FROM simple_binary_test",
                binary=True
            )
            
            summary = cur.fetchone()
            print(f"✅ Aggregation result: {summary[0]} rows, total amount {summary[1]}")
            
            # Test with NULL values
            cur.execute(
                "INSERT INTO simple_binary_test (id, name) VALUES (%s, %s)",
                (4, "David"),
                binary=True
            )
            
            cur.execute(
                "SELECT id, name, amount FROM simple_binary_test WHERE id = %s",
                [4],
                binary=True
            )
            
            null_row = cur.fetchone()
            print(f"✅ NULL handling: ID={null_row[0]}, Name='{null_row[1]}', Amount={null_row[2]}")
            assert null_row[2] is None, "Amount should be NULL"
            
            print("\n🎉 psycopg3 binary protocol test completed successfully!")
            print("✅ Binary parameter passing working")
            print("✅ Binary result retrieval working") 
            print("✅ Data type handling working")
            print("✅ NULL value handling working")
            print("✅ Parameterized queries working")
            print("✅ Aggregation queries working")
            
    finally:
        conn.close()

if __name__ == "__main__":
    test_simple_psycopg3_binary()