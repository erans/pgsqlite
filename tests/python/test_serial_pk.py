#!/usr/bin/env python3
"""
Test to isolate the SERIAL PRIMARY KEY issue
"""

import sys
import argparse

def test_serial_primary_key(port):
    """Test the specific SERIAL + PRIMARY KEY issue."""
    try:
        import psycopg2
        print("🧪 Testing SERIAL + PRIMARY KEY Issue")
        print("=====================================")
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
        
        # Test the failing CREATE TABLE statement (simplified version)
        print("🔍 Testing CREATE TABLE with SERIAL + explicit PRIMARY KEY...")
        
        # This is what SQLAlchemy generates that causes the error
        failing_sql = """
        CREATE TABLE test_users (
            id SERIAL NOT NULL, 
            username VARCHAR(50) NOT NULL, 
            PRIMARY KEY (id)
        )
        """
        
        try:
            cursor.execute(failing_sql)
            print("✅ CREATE TABLE with SERIAL + PRIMARY KEY succeeded")
            
            # Test that it works correctly
            cursor.execute("INSERT INTO test_users (username) VALUES ('test')")
            cursor.execute("SELECT id, username FROM test_users")
            result = cursor.fetchone()
            print(f"✅ Insert and select worked: id={result[0]}, username={result[1]}")
            
            # Clean up
            cursor.execute("DROP TABLE test_users")
            print("✅ Cleanup completed")
            
        except Exception as e:
            print(f"❌ CREATE TABLE failed: {e}")
            return False
        
        cursor.close()
        conn.close()
        
        print()
        print("🎉 SUCCESS: SERIAL + PRIMARY KEY issue resolved!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Test SERIAL + PRIMARY KEY issue")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_serial_primary_key(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())