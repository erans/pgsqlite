#!/usr/bin/env python3
"""
Test INSERT...SELECT functionality in pgsqlite
"""

import sys
import argparse

def test_insert_select(port):
    """Test INSERT...SELECT statements."""
    try:
        import psycopg2
        print("🧪 Testing INSERT...SELECT Functionality")
        print("========================================")
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
        
        try:
            # Create source table
            print("🔍 Creating source table...")
            cursor.execute("""
                CREATE TABLE source_table (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50),
                    value NUMERIC(10,2)
                )
            """)
            
            # Create destination table
            print("🔍 Creating destination table...")
            cursor.execute("""
                CREATE TABLE dest_table (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(50),
                    value NUMERIC(10,2)
                )
            """)
            
            # Insert test data into source table
            print("🔍 Inserting test data...")
            cursor.execute("INSERT INTO source_table (id, name, value) VALUES (1, 'test1', 100.50)")
            cursor.execute("INSERT INTO source_table (id, name, value) VALUES (2, 'test2', 200.75)")
            
            # Test basic INSERT...SELECT
            print("🔍 Testing basic INSERT...SELECT...")
            cursor.execute("INSERT INTO dest_table SELECT * FROM source_table")
            
            # Verify the data was copied
            cursor.execute("SELECT COUNT(*) FROM dest_table")
            count = cursor.fetchone()[0]
            print(f"✅ Records copied: {count}")
            
            # Test INSERT...SELECT with WHERE clause
            print("🔍 Testing INSERT...SELECT with WHERE clause...")
            cursor.execute("DELETE FROM dest_table")  # Clear first
            cursor.execute("INSERT INTO dest_table SELECT * FROM source_table WHERE id = 1")
            
            cursor.execute("SELECT COUNT(*) FROM dest_table")
            count = cursor.fetchone()[0]
            print(f"✅ Conditional records copied: {count}")
            
            # Test INSERT...SELECT with column specification
            print("🔍 Testing INSERT...SELECT with specific columns...")
            cursor.execute("DELETE FROM dest_table")  # Clear first
            cursor.execute("INSERT INTO dest_table (id, name, value) SELECT id, name, value FROM source_table")
            
            cursor.execute("SELECT COUNT(*) FROM dest_table")
            count = cursor.fetchone()[0]
            print(f"✅ Column-specific records copied: {count}")
            
            # Test INSERT...SELECT with expressions
            print("🔍 Testing INSERT...SELECT with expressions...")
            cursor.execute("DELETE FROM dest_table")  # Clear first
            cursor.execute("INSERT INTO dest_table (id, name, value) SELECT id + 10, 'copy_' || name, value * 2 FROM source_table")
            
            cursor.execute("SELECT id, name, value FROM dest_table ORDER BY id")
            results = cursor.fetchall()
            print(f"✅ Expression results: {results}")
            
            # Clean up
            cursor.execute("DROP TABLE source_table")
            cursor.execute("DROP TABLE dest_table")
            print("✅ Cleanup completed")
            
        except Exception as e:
            print(f"❌ INSERT...SELECT test failed: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        cursor.close()
        conn.close()
        
        print()
        print("🎉 SUCCESS: INSERT...SELECT functionality working!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Test INSERT...SELECT functionality")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_insert_select(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())