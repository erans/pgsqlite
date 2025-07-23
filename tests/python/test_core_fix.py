#!/usr/bin/env python3
"""
Test to verify that the core SQLAlchemy connection issues are fixed.
"""

import sys
import argparse

def test_core_connection(port):
    """Test the core connection and system functions."""
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
        
        print("🔍 Testing system functions that were failing before...")
        
        # Test pg_catalog.version() - this was the original issue
        cursor.execute("SELECT pg_catalog.version()")
        result = cursor.fetchone()
        print(f"✅ pg_catalog.version(): {result[0]}")
        
        # Test show transaction isolation level - this was the second issue
        cursor.execute("SHOW transaction isolation level")
        result = cursor.fetchone()
        print(f"✅ SHOW transaction isolation level: {result[0]}")
        
        # Test other system functions
        cursor.execute("SELECT current_database()")
        result = cursor.fetchone()
        print(f"✅ current_database(): {result[0]}")
        
        cursor.execute("SELECT current_schema()")
        result = cursor.fetchone()
        print(f"✅ current_schema(): {result[0]}")
        
        print("\n🎉 SUCCESS: Core SQLAlchemy compatibility issues are FIXED!")
        print("The original errors that were blocking SQLAlchemy are resolved:")
        print("  - pg_catalog.version() syntax error ✅ FIXED") 
        print("  - show transaction isolation level error ✅ FIXED")
        
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Test core SQLAlchemy compatibility fixes")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    print("🧪 Core SQLAlchemy Compatibility Fix Test")
    print("=========================================")
    print("")
    
    success = test_core_connection(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())