#!/usr/bin/env python3
"""
Test psycopg3 binary format with pgsqlite.
"""

import psycopg
from decimal import Decimal
from datetime import datetime, date
import uuid

def test_psycopg3_binary():
    """Test psycopg3 with binary format."""
    # Connect to pgsqlite
    conn = psycopg.connect("host=localhost port=15500 user=postgres dbname=main")
    
    try:
        with conn.cursor() as cur:
            # Create test table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS binary_test (
                    id INTEGER PRIMARY KEY,
                    num NUMERIC(10,2),
                    money MONEY,
                    uid UUID,
                    json_data JSON,
                    jsonb_data JSONB,
                    created_at TIMESTAMP,
                    birth_date DATE,
                    is_active BOOLEAN,
                    data BYTEA
                )
            """)
            conn.commit()
            
            # Insert data using binary format
            test_uuid = uuid.uuid4()
            test_data = {
                "id": 1,
                "num": Decimal("123.45"),
                "money": "$1,234.56",
                "uid": test_uuid,
                "json_data": '{"key": "value"}',
                "jsonb_data": '{"key": "value"}',
                "created_at": datetime.now(),
                "birth_date": date.today(),
                "is_active": True,
                "data": b"binary data"
            }
            
            # Use binary format for parameters
            cur.execute(
                """
                INSERT INTO binary_test 
                (id, num, money, uid, json_data, jsonb_data, created_at, birth_date, is_active, data)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                list(test_data.values()),
                binary=True  # Force binary format
            )
            conn.commit()
            
            # Query with binary results
            cur.execute("SELECT * FROM binary_test WHERE id = %s", [1], binary=True)
            row = cur.fetchone()
            
            print("✅ Binary format test successful!")
            print(f"  ID: {row[0]}")
            print(f"  Numeric: {row[1]}")
            print(f"  Money: {row[2]}")
            print(f"  UUID: {row[3]}")
            print(f"  JSON: {row[4]}")
            print(f"  JSONB: {row[5]}")
            print(f"  Timestamp: {row[6]}")
            print(f"  Date: {row[7]}")
            print(f"  Boolean: {row[8]}")
            print(f"  Bytea: {row[9]}")
            
            # Test numeric operations
            cur.execute(
                "SELECT num * 2, money + money FROM binary_test WHERE id = %s",
                [1],
                binary=True
            )
            result = cur.fetchone()
            print(f"\n✅ Numeric operations:")
            print(f"  num * 2 = {result[0]}")
            print(f"  money + money = {result[1]}")
            
    finally:
        conn.close()

if __name__ == "__main__":
    test_psycopg3_binary()