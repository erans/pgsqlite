#!/usr/bin/env python3
"""Test RETURNING clause specifically"""

import psycopg
import subprocess
import time
import tempfile
import os
from datetime import datetime

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite=info'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15527',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Testing RETURNING clause...")
        
        conn = psycopg.connect('postgresql://postgres@localhost:15527/main', autocommit=True)
        cursor = conn.cursor()
        
        # Create table matching SQLAlchemy structure
        print("📝 Creating categories table...")
        cursor.execute("""
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(50) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Table created")
        
        # Test 1: Simple INSERT without RETURNING
        print("🔧 Test 1: INSERT without RETURNING...")
        try:
            cursor.execute("INSERT INTO categories (name, description) VALUES (%s, %s)", 
                          ("Tech", "Technology posts"))
            print("✅ INSERT without RETURNING works")
        except Exception as e:
            print(f"❌ INSERT without RETURNING failed: {e}")
            return 1
        
        # Test 2: INSERT with RETURNING (no datetime)
        print("🔧 Test 2: INSERT with RETURNING (no datetime)...")
        try:
            cursor.execute("INSERT INTO categories (name, description) VALUES (%s, %s) RETURNING id", 
                          ("Tech2", "Technology posts 2"))
            result = cursor.fetchone()
            print(f"✅ INSERT with RETURNING works: {result}")
        except Exception as e:
            print(f"❌ INSERT with RETURNING failed: {e}")
            return 1
        
        # Test 3: INSERT with datetime but no RETURNING
        print("🔧 Test 3: INSERT with datetime (no RETURNING)...")
        try:
            now = datetime.now()
            cursor.execute("INSERT INTO categories (name, description, created_at) VALUES (%s, %s, %s)", 
                          ("Tech3", "Technology posts 3", now))
            print("✅ INSERT with datetime works")
        except Exception as e:
            print(f"❌ INSERT with datetime failed: {e}")
            return 1
        
        # Test 4: The failing combination - datetime + RETURNING
        print("🔧 Test 4: INSERT with datetime + RETURNING...")
        try:
            now = datetime.now()
            cursor.execute("INSERT INTO categories (name, description, created_at) VALUES (%s, %s, %s) RETURNING id", 
                          ("Tech4", "Technology posts 4", now))
            result = cursor.fetchone()
            print(f"✅ INSERT with datetime + RETURNING works: {result}")
        except Exception as e:
            print(f"❌ INSERT with datetime + RETURNING failed: {e}")
            return 1
            
        # Test 5: The exact SQLAlchemy query structure
        print("🔧 Test 5: Exact SQLAlchemy structure...")
        try:
            now = datetime.now()
            query = "INSERT INTO categories (name, description, created_at) VALUES (%(name)s::VARCHAR, %(description)s::VARCHAR, %(created_at)s::TIMESTAMP WITHOUT TIME ZONE) RETURNING categories.id"
            params = {'name': 'Technology', 'description': 'Posts about technology and programming', 'created_at': now}
            cursor.execute(query, params)
            result = cursor.fetchone()
            print(f"✅ Exact SQLAlchemy structure works: {result}")
        except Exception as e:
            print(f"❌ Exact SQLAlchemy structure failed: {e}")
            return 1
        
        print('🎉 All RETURNING tests passed!')
        return 0
    
    except Exception as e:
        print(f'❌ Test failed: {e}')
        import traceback
        traceback.print_exc()
        return 1
    finally:
        pgsqlite_proc.terminate()
        try:
            output, _ = pgsqlite_proc.communicate(timeout=5)
            if output:
                print("\n--- pgsqlite debug output ---")
                lines = output.strip().split('\n')
                for line in lines[-10:]:
                    if 'error' in line.lower() or 'blob' in line.lower():
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())