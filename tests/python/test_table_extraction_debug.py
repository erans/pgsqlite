#!/usr/bin/env python3
"""
Test to debug table name extraction by adding logging to pgsqlite
"""

import sys
import argparse

def test_table_extraction_debug(port):
    """Test table name extraction with debug logging."""
    try:
        import psycopg2
        
        print("🧪 Table Name Extraction Debug Test")
        print("===================================")
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
        
        # Create our test table
        print("🔍 Creating table...")
        cursor.execute("""
            CREATE TABLE extraction_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value NUMERIC(5,2)
            )
        """)
        
        # Test different query patterns that should extract table names
        test_queries = [
            "SELECT id, name, value FROM extraction_test",
            "SELECT * FROM extraction_test",
            "SELECT id FROM extraction_test WHERE id = 1",
            "SELECT id FROM extraction_test LIMIT 1",
            "SELECT extraction_test.id FROM extraction_test",  # This should work
        ]
        
        print("🔍 Testing queries (check pgsqlite log for debug output):")
        for i, query in enumerate(test_queries, 1):
            print(f"\n  Test {i}: {query}")
            try:
                cursor.execute(query)
                description = cursor.description
                print(f"    ✅ Success: {len(description)} columns")
                for j, desc in enumerate(description):
                    col_name = desc[0]
                    type_oid = desc[1]
                    type_name = {
                        16: "BOOLEAN", 23: "INTEGER", 25: "TEXT", 
                        1043: "VARCHAR", 1700: "NUMERIC"
                    }.get(type_oid, f"UNKNOWN({type_oid})")
                    expected_correct = (
                        (col_name == "id" and type_oid == 23) or  # INTEGER
                        (col_name == "name" and type_oid == 25) or  # TEXT
                        (col_name == "value" and type_oid == 1700)  # NUMERIC
                    )
                    status = "✅" if expected_correct else "❌"
                    print(f"      {status} Column '{col_name}' -> {type_name}")
            except Exception as e:
                print(f"    ❌ Failed: {e}")
        
        cursor.close()
        conn.close()
        
        print()
        print("🎉 SUCCESS: Table extraction debug test completed!")
        print("Check the pgsqlite log file for debug messages about table name extraction")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Debug table name extraction")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_table_extraction_debug(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())