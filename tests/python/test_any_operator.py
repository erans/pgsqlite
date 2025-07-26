#!/usr/bin/env python3
"""
Test to verify the ANY operator works with string literals
"""

import sys
import argparse

def test_any_operator(port):
    """Test the ANY operator with string literals."""
    try:
        import psycopg2
        print("🧪 Testing ANY Operator")
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
        
        # Create a test table
        print("🔍 Creating test table...")
        cursor.execute("""
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                relkind CHAR(1) NOT NULL
            )
        """)
        
        # Insert some test data
        cursor.execute("""
            INSERT INTO test_table (id, relkind) VALUES 
            (1, 'r'),
            (2, 'v'),
            (3, 't'),
            (4, 'p')
        """)
        
        # Test the ANY operator with string literal (SQLAlchemy pattern)
        print("🔍 Testing ANY operator with string literal...")
        query = """
            SELECT id, relkind FROM test_table 
            WHERE relkind = ANY('["r","p","f","v","m"]')
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"✅ ANY operator query returned {len(results)} rows:")
        for row in results:
            print(f"  - id={row[0]}, relkind={row[1]}")
        
        # Verify we got the expected results (should find 'r', 'v', 'p')
        expected_relkinds = {'r', 'v', 'p'}
        actual_relkinds = {row[1] for row in results}
        
        if expected_relkinds == actual_relkinds:
            print("✅ ANY operator returned correct results")
        else:
            print(f"❌ ANY operator returned wrong results. Expected {expected_relkinds}, got {actual_relkinds}")
            return False
        
        # Clean up
        cursor.execute("DROP TABLE test_table")
        
        cursor.close()
        conn.close()
        
        print()
        print("🎉 SUCCESS: ANY operator works correctly!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Test ANY operator")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_any_operator(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())