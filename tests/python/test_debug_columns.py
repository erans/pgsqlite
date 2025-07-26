#!/usr/bin/env python3
"""
Test to debug exactly what column names are in response.columns vs schema table
"""

import sys
import argparse

def test_debug_columns(port):
    """Test to see column name differences."""
    try:
        import psycopg2
        
        print("🧪 Debug Column Names Test")
        print("=========================")
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
            CREATE TABLE debug_test (
                id INTEGER PRIMARY KEY,
                name TEXT,
                value NUMERIC(5,2)
            )
        """)
        
        # First, let's verify the schema table content
        print("\n🔍 Schema table content:")
        cursor.execute("SELECT table_name, column_name, pg_type FROM __pgsqlite_schema WHERE table_name = 'debug_test' ORDER BY column_name")
        schema_rows = cursor.fetchall()
        for row in schema_rows:
            table_name, column_name, pg_type = row
            print(f"  Schema: table='{table_name}', column='{column_name}', type='{pg_type}'")
        
        # Now try different types of queries to see the column names in results
        test_queries = [
            ("Simple SELECT", "SELECT id, name, value FROM debug_test"),
            ("SELECT *", "SELECT * FROM debug_test"),
            ("Qualified columns", "SELECT debug_test.id, debug_test.name FROM debug_test"),  
            ("Table alias", "SELECT t.id, t.name FROM debug_test t"),
            ("Column alias", "SELECT id AS test_id, name AS test_name FROM debug_test"),
        ]
        
        for test_name, query in test_queries:
            print(f"\n🔍 {test_name}: {query}")
            try:
                cursor.execute(query)
                description = cursor.description
                print(f"  Result columns:")
                for i, desc in enumerate(description):
                    col_name = desc[0] 
                    type_oid = desc[1]
                    print(f"    Column {i+1}: name='{col_name}', type_oid={type_oid}")
            except Exception as e:
                print(f"  ❌ Failed: {e}")
        
        cursor.close()
        conn.close()
        
        print()
        print("🎉 SUCCESS: Debug columns test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Debug column name matching")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_debug_columns(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())