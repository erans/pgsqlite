#!/usr/bin/env python3
"""Break down the SQLAlchemy query step by step"""

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
    env['RUST_LOG'] = 'pgsqlite=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15529',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Breaking down SQLAlchemy query...")
        
        conn = psycopg.connect('postgresql://postgres@localhost:15529/main', autocommit=True)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name VARCHAR(50) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        now = datetime.now()
        
        # Test increasingly complex versions of the query
        queries = [
            # 1. Just INSERT with single cast
            ("Single cast", 
             "INSERT INTO categories (name) VALUES (%(name)s::VARCHAR)", 
             {"name": "Tech"}),
            
            # 2. Multiple casts, no datetime
            ("Multiple casts no datetime", 
             "INSERT INTO categories (name, description) VALUES (%(name)s::VARCHAR, %(description)s::VARCHAR)", 
             {"name": "Tech", "description": "Tech posts"}),
            
            # 3. With datetime but no cast
            ("Datetime no cast", 
             "INSERT INTO categories (name, description, created_at) VALUES (%(name)s, %(description)s, %(created_at)s)", 
             {"name": "Tech", "description": "Tech posts", "created_at": now}),
            
            # 4. With datetime and cast
            ("Datetime with cast", 
             "INSERT INTO categories (name, description, created_at) VALUES (%(name)s::VARCHAR, %(description)s::VARCHAR, %(created_at)s::TIMESTAMP)", 
             {"name": "Tech", "description": "Tech posts", "created_at": now}),
            
            # 5. With RETURNING but no cast
            ("RETURNING no cast", 
             "INSERT INTO categories (name, description, created_at) VALUES (%(name)s, %(description)s, %(created_at)s) RETURNING id", 
             {"name": "Tech", "description": "Tech posts", "created_at": now}),
            
            # 6. The exact failing query
            ("Exact SQLAlchemy", 
             "INSERT INTO categories (name, description, created_at) VALUES (%(name)s::VARCHAR, %(description)s::VARCHAR, %(created_at)s::TIMESTAMP WITHOUT TIME ZONE) RETURNING categories.id", 
             {"name": "Tech", "description": "Tech posts", "created_at": now}),
        ]
        
        for test_name, query, params in queries:
            print(f"\n🔧 Testing {test_name}...")
            print(f"   Query: {query}")
            try:
                cursor.execute(query, params)
                if "RETURNING" in query:
                    result = cursor.fetchone()
                    print(f"   ✅ {test_name} works, returned: {result}")
                else:
                    print(f"   ✅ {test_name} works")
            except Exception as e:
                print(f"   ❌ {test_name} failed: {e}")
                # Don't return early, continue testing
        
        return 0
    
    except Exception as e:
        print(f'❌ Test failed: {e}')
        return 1
    finally:
        pgsqlite_proc.terminate()
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())