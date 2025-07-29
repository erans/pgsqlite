#!/usr/bin/env python3
"""
Test to isolate the WITHOUT TIME ZONE issue in INSERT statements
"""

import sys
import argparse

def test_timestamp_insert(port):
    """Test INSERT with TIMESTAMP WITHOUT TIME ZONE."""
    try:
        import psycopg2
        print("🧪 Testing TIMESTAMP WITHOUT TIME ZONE in INSERT")
        print("===============================================")
        print()
        
        # Connect to pgsqlite
        conn = psycopg2.connect(
            host="localhost",
            port=port,
            database="main",
            user="postgres",
            password="postgres"
        )
        
        cursor = conn.cursor()
        
        # Create a table with TIMESTAMP WITHOUT TIME ZONE
        print("🔍 Creating table with TIMESTAMP WITHOUT TIME ZONE...")
        cursor.execute("""
            CREATE TABLE test_timestamps (
                id INTEGER PRIMARY KEY,
                created_at TIMESTAMP WITHOUT TIME ZONE
            )
        """)
        print("✅ CREATE TABLE succeeded")
        
        # Test various INSERT patterns
        test_cases = [
            # Simple INSERT with string literal
            ("Simple string literal", "INSERT INTO test_timestamps (id, created_at) VALUES (1, '2025-01-01 12:00:00')"),
            
            # INSERT with cast
            ("String with cast", "INSERT INTO test_timestamps (id, created_at) VALUES (2, '2025-01-01 12:00:00'::timestamp)"),
            
            # INSERT with WITHOUT TIME ZONE cast (this might be the issue)
            ("WITHOUT TIME ZONE cast", "INSERT INTO test_timestamps (id, created_at) VALUES (3, '2025-01-01 12:00:00'::timestamp without time zone)"),
        ]
        
        for test_name, sql in test_cases:
            print(f"🔍 Testing {test_name}...")
            try:
                cursor.execute(sql)
                print(f"✅ {test_name} succeeded")
            except Exception as e:
                print(f"❌ {test_name} failed: {e}")
                # Continue with other tests
        
        # Test SELECT to see what we got
        cursor.execute("SELECT id, created_at FROM test_timestamps ORDER BY id")
        results = cursor.fetchall()
        print(f"✅ Retrieved {len(results)} rows:")
        for row in results:
            print(f"  - id={row[0]}, created_at={row[1]}")
        
        # Clean up
        cursor.execute("DROP TABLE test_timestamps")
        
        cursor.close()
        conn.close()
        
        print()
        print("🎉 SUCCESS: TIMESTAMP tests completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Test TIMESTAMP WITHOUT TIME ZONE")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_timestamp_insert(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())