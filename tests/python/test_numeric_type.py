#!/usr/bin/env python3
"""
Test to verify NUMERIC(10, 2) type handling
"""

import sys
import argparse

def test_numeric_type(port):
    """Test NUMERIC type with precision and scale."""
    try:
        import psycopg2
        print("🧪 Testing NUMERIC Type")
        print("======================")
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
        
        # Test CREATE TABLE with NUMERIC(precision, scale)
        print("🔍 Creating table with NUMERIC(10, 2) column...")
        create_sql = """
            CREATE TABLE test_numeric (
                id INTEGER PRIMARY KEY,
                price NUMERIC(10, 2) NOT NULL
            )
        """
        
        try:
            cursor.execute(create_sql)
            print("✅ CREATE TABLE with NUMERIC(10, 2) succeeded")
            
            # Test inserting a decimal value
            cursor.execute("INSERT INTO test_numeric (id, price) VALUES (1, 123.45)")
            cursor.execute("SELECT id, price FROM test_numeric")
            result = cursor.fetchone()
            print(f"✅ Insert and select worked: id={result[0]}, price={result[1]}")
            
            # Clean up
            cursor.execute("DROP TABLE test_numeric")
            print("✅ Cleanup completed")
            
        except Exception as e:
            print(f"❌ CREATE TABLE failed: {e}")
            return False
        
        cursor.close()
        conn.close()
        
        print()
        print("🎉 SUCCESS: NUMERIC(10, 2) type handled correctly!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Test NUMERIC type")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_numeric_type(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())