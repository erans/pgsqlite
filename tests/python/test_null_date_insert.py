#!/usr/bin/env python3
"""Test NULL date handling in INSERT with RETURNING clause"""

import psycopg2
import sys

def test_null_date_insert(port):
    """Test INSERT with NULL date and RETURNING clause"""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=port,
            database="main",
            user="postgres",
            password="postgres"
        )
        
        cur = conn.cursor()
        
        print("🔍 Testing NULL date INSERT with RETURNING...")
        
        # Create test table
        cur.execute("""
            CREATE TABLE test_users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                birth_date DATE
            )
        """)
        
        # Test INSERT with NULL date and RETURNING clause
        cur.execute("""
            INSERT INTO test_users (name, birth_date) 
            VALUES (%s, %s) 
            RETURNING id
        """, ("Test User", None))
        
        result = cur.fetchone()
        print(f"✅ INSERT with NULL date succeeded! Returned ID: {result[0]}")
        
        # Verify the data
        cur.execute("SELECT name, birth_date FROM test_users WHERE id = %s", (result[0],))
        name, birth_date = cur.fetchone()
        print(f"✅ Verification: name='{name}', birth_date={birth_date}")
        
        cur.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_null_date_insert.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    success = test_null_date_insert(port)
    sys.exit(0 if success else 1)