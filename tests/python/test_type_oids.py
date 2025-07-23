#!/usr/bin/env python3
"""
Test script to verify PostgreSQL type OIDs are correctly returned
"""

import psycopg2
import sys

def test_type_oids(port):
    """Test that we get correct PostgreSQL type OIDs, not just TEXT (25)"""
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
        
        # Create a test table with various types
        cur.execute("""
            CREATE TABLE oid_test (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                is_active BOOLEAN,
                created_at TIMESTAMP
            )
        """)
        
        # Insert test data
        cur.execute("""
            INSERT INTO oid_test (id, name, price, is_active, created_at) 
            VALUES (1, 'test', 123.45, true, '2023-01-01 12:00:00')
        """)
        
        # Query and check type OIDs
        cur.execute("SELECT * FROM oid_test")
        
        # Get column descriptions with type OIDs
        descriptions = cur.description
        
        print("Column type OIDs:")
        expected_oids = {
            'id': 23,        # INTEGER
            'name': 1043,    # VARCHAR
            'price': 1700,   # NUMERIC  
            'is_active': 16, # BOOLEAN
            'created_at': 1114 # TIMESTAMP
        }
        
        success = True
        for i, desc in enumerate(descriptions):
            col_name = desc[0]
            type_oid = desc[1]
            expected_oid = expected_oids[col_name]
            
            print(f"  {col_name}: {type_oid} (expected {expected_oid})")
            
            if type_oid != expected_oid:
                print(f"    ❌ FAIL: Expected {expected_oid}, got {type_oid}")
                success = False
            else:
                print(f"    ✅ PASS")
        
        cur.close()
        conn.close()
        
        if success:
            print("\n🎉 All type OIDs are correct!")
            return True
        else:
            print("\n❌ Some type OIDs are incorrect")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_type_oids.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    success = test_type_oids(port)
    sys.exit(0 if success else 1)