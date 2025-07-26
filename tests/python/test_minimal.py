#!/usr/bin/env python3
"""
Minimal test to verify the SQLAlchemy issue is fixed.
This test only requires psycopg2 (not full SQLAlchemy stack).
"""

import sys
import argparse

def test_version_function(port):
    """Test that the version() function works with basic psycopg2."""
    try:
        import psycopg2
        print("✅ psycopg2 imported successfully")
        
        # Connect to pgsqlite
        conn = psycopg2.connect(
            host="localhost",
            port=port,
            database="main",
            user="postgres",
            password="postgres"
        )
        
        cursor = conn.cursor()
        
        # Test the specific queries that were failing
        print("🔍 Testing system functions...")
        
        # This was the original failing query
        cursor.execute("SELECT pg_catalog.version()")
        result = cursor.fetchone()
        print(f"✅ pg_catalog.version(): {result[0]}")
        
        # Test other system functions
        cursor.execute("SELECT version()")
        result = cursor.fetchone()
        print(f"✅ version(): {result[0]}")
        
        cursor.execute("SELECT current_database()")
        result = cursor.fetchone()
        print(f"✅ current_database(): {result[0]}")
        
        cursor.execute("SELECT current_user()")
        result = cursor.fetchone()
        print(f"✅ current_user(): {result[0]}")
        
        cursor.execute("SELECT current_schema()")
        result = cursor.fetchone()
        print(f"✅ current_schema(): {result[0]}")
        
        # Test basic table operations
        print("🔍 Testing basic table operations...")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_minimal (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value NUMERIC(10,2)
            )
        """)
        print("✅ Table created")
        
        cursor.execute("INSERT INTO test_minimal (id, name, value) VALUES (1, 'test', 42.50)")
        print("✅ Insert successful")
        
        cursor.execute("SELECT name, value FROM test_minimal WHERE id = 1")
        result = cursor.fetchone()
        print(f"✅ Select successful: {result}")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("🎉 All tests passed! SQLAlchemy compatibility issue is fixed!")
        return True
        
    except ImportError:
        print("❌ psycopg2 not available - install with: python3 -m pip install psycopg2-binary")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Minimal SQLAlchemy compatibility test")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    print("🧪 Minimal SQLAlchemy Compatibility Test")
    print("========================================")
    print("")
    
    success = test_version_function(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())