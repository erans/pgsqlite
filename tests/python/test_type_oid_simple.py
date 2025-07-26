#!/usr/bin/env python3
"""
Simple test to verify PostgreSQL type OIDs are correctly returned
"""

import psycopg2
import sys

def test_type_oids(port):
    """Test that we get correct PostgreSQL type OIDs"""
    try:
        # Connect to pgsqlite
        conn = psycopg2.connect(
            host="localhost",
            port=port,
            database="main",
            user="postgres",
            password="postgres"
        )
        
        cur = conn.cursor()
        
        print("🔍 Testing PostgreSQL Type OIDs...")
        print("=" * 50)
        
        # Create a test table with various types
        cur.execute("""
            CREATE TABLE type_test (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                is_active BOOLEAN,
                created_at TIMESTAMP
            )
        """)
        
        # Insert a single row (avoid complex multi-row syntax)
        cur.execute("""
            INSERT INTO type_test (id, name, price, is_active, created_at) 
            VALUES (1, 'test', 123.45, true, '2023-01-01 12:00:00')
        """)
        
        # Query and check type OIDs
        cur.execute("SELECT * FROM type_test")
        
        # Get column descriptions with type OIDs
        descriptions = cur.description
        
        print("\nColumn Type OIDs:")
        expected_oids = {
            'id': 23,           # INTEGER
            'name': 1043,       # VARCHAR
            'price': 1700,      # NUMERIC  
            'is_active': 16,    # BOOLEAN
            'created_at': 1114  # TIMESTAMP
        }
        
        all_correct = True
        for i, desc in enumerate(descriptions):
            col_name = desc[0]
            type_oid = desc[1]
            expected_oid = expected_oids.get(col_name, 0)
            
            status = "✅" if type_oid == expected_oid else "❌"
            print(f"  {col_name}: {type_oid} (expected {expected_oid}) {status}")
            
            if type_oid != expected_oid:
                all_correct = False
        
        # Test information_schema.tables
        print("\n🔍 Testing information_schema.tables...")
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """)
        
        tables = cur.fetchall()
        print(f"Found {len(tables)} tables:")
        for table in tables:
            print(f"  - {table[0]}")
        
        cur.close()
        conn.close()
        
        if all_correct and len(tables) > 0:
            print("\n🎉 SUCCESS: All type OIDs correct and information_schema working!")
            return True
        else:
            print("\n❌ FAIL: Some issues found")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_type_oid_simple.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    success = test_type_oids(port)
    sys.exit(0 if success else 1)