#!/usr/bin/env python3
"""
Test psycopg3 binary format with pgsqlite.

This script demonstrates using psycopg3's binary format feature
with pgsqlite for optimal performance.
"""

import argparse
import psycopg
from psycopg.rows import tuple_row
import time
from datetime import datetime
from decimal import Decimal

def test_binary_format(port: int):
    """Test psycopg3 binary format functionality."""
    print(f"🔌 Connecting to pgsqlite on port {port} with binary format...")
    
    # Connect with autocommit for simplicity
    conn = psycopg.connect(
        f"host=localhost port={port} dbname=main user=postgres",
        autocommit=True
    )
    
    # Test 1: Basic binary cursor
    print("\n📊 Test 1: Basic binary cursor")
    with conn.cursor(binary=True) as cur:
        cur.execute("SELECT 1::int4 as num, 'Hello'::text as txt, 3.14::float8 as flt")
        result = cur.fetchone()
        print(f"   Result: {result}")
        print(f"   Types: {[type(x).__name__ for x in result]}")
    
    # Test 2: Create table and test various types
    print("\n📊 Test 2: Testing various data types")
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS binary_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value DECIMAL(10,2),
                created_at TIMESTAMP,
                is_active BOOLEAN,
                score REAL
            )
        """)
        
    # Insert data with binary cursor
    print("\n📊 Test 3: Inserting data with binary format")
    with conn.cursor(binary=True) as cur:
        test_data = [
            (1, "Test One", Decimal("123.45"), datetime.now(), True, 98.5),
            (2, "Test Two", Decimal("678.90"), datetime.now(), False, 87.3),
            (3, "Test Three", Decimal("999.99"), datetime.now(), True, 92.7),
        ]
        
        for row in test_data:
            cur.execute("""
                INSERT INTO binary_test (id, name, value, created_at, is_active, score)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    value = EXCLUDED.value,
                    created_at = EXCLUDED.created_at,
                    is_active = EXCLUDED.is_active,
                    score = EXCLUDED.score
            """, row)
        print(f"   ✅ Inserted {len(test_data)} rows")
    
    # Read data with binary cursor
    print("\n📊 Test 4: Reading data with binary format")
    with conn.cursor(binary=True) as cur:
        cur.execute("SELECT * FROM binary_test ORDER BY id")
        rows = cur.fetchall()
        for row in rows:
            print(f"   Row: {row}")
    
    # Performance comparison
    print("\n📊 Test 5: Performance comparison")
    
    # Text format timing
    start = time.perf_counter()
    with conn.cursor(binary=False) as cur:
        for _ in range(100):
            cur.execute("SELECT * FROM binary_test WHERE id = %s", (1,))
            _ = cur.fetchone()
    text_time = (time.perf_counter() - start) * 1000
    
    # Binary format timing
    start = time.perf_counter()
    with conn.cursor(binary=True) as cur:
        for _ in range(100):
            cur.execute("SELECT * FROM binary_test WHERE id = %s", (1,))
            _ = cur.fetchone()
    binary_time = (time.perf_counter() - start) * 1000
    
    print(f"   Text format: {text_time:.2f}ms for 100 queries")
    print(f"   Binary format: {binary_time:.2f}ms for 100 queries")
    print(f"   Improvement: {((text_time / binary_time) - 1) * 100:.1f}% faster")
    
    # Test 6: RETURNING with binary format
    print("\n📊 Test 6: Testing RETURNING with binary format")
    with conn.cursor(binary=True) as cur:
        cur.execute("""
            UPDATE binary_test 
            SET score = score + 1 
            WHERE id = 1 
            RETURNING id, name, score
        """)
        result = cur.fetchone()
        print(f"   RETURNING result: {result}")
    
    # Cleanup
    with conn.cursor() as cur:
        cur.execute("DROP TABLE binary_test")
    
    conn.close()
    print("\n✅ All binary format tests completed successfully!")

def main():
    parser = argparse.ArgumentParser(description="Test psycopg3 binary format with pgsqlite")
    parser.add_argument("--port", type=int, default=5432, help="Port where pgsqlite is running")
    args = parser.parse_args()
    
    try:
        test_binary_format(args.port)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())