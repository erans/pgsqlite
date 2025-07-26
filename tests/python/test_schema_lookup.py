#!/usr/bin/env python3
"""
Test to debug schema type lookup
"""

import sys
import argparse

def test_schema_lookup(port):
    """Test schema type lookup by querying the schema table directly."""
    try:
        import psycopg2
        
        print("🧪 Schema Type Lookup Debug Test")
        print("================================")
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
        print("🔍 Creating table with known types...")
        cursor.execute("""
            CREATE TABLE schema_test (
                id INTEGER PRIMARY KEY,
                text_col TEXT,
                numeric_col NUMERIC(10, 2),
                bool_col BOOLEAN
            )
        """)
        
        # Check what's stored in the __pgsqlite_schema table
        print("🔍 Checking __pgsqlite_schema table...")
        cursor.execute("SELECT table_name, column_name, pg_type FROM __pgsqlite_schema WHERE table_name = 'schema_test' ORDER BY column_name")
        
        schema_rows = cursor.fetchall()
        print(f"Found {len(schema_rows)} schema entries:")
        for row in schema_rows:
            table_name, column_name, pg_type = row
            print(f"  Table: {table_name}, Column: {column_name}, Type: {pg_type}")
        
        # Now do a simple SELECT and see what type OIDs we get
        print("\n🔍 Querying the table...")
        cursor.execute("SELECT id, text_col, numeric_col, bool_col FROM schema_test LIMIT 0")
        
        description = cursor.description
        print(f"Query returned {len(description)} columns:")
        for i, desc in enumerate(description):
            col_name = desc[0]
            type_code = desc[1]
            print(f"  Column {i+1}: '{col_name}' -> Type OID: {type_code}")
        
        cursor.close()
        conn.close()
        
        print()
        print("🎉 SUCCESS: Schema lookup test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Debug schema type lookup")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_schema_lookup(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())