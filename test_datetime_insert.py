#!/usr/bin/env python3
"""Test just the failing datetime INSERT query"""

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
    env['RUST_LOG'] = 'pgsqlite::query::extended=debug,pgsqlite::query::extended_fast_path=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15527',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Testing datetime INSERT with TIMESTAMP cast...")
        
        conn = psycopg.connect('postgresql://postgres@localhost:15527/main', autocommit=True)
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
        
        # The exact failing query from SQLAlchemy
        query = "INSERT INTO categories (name, description, created_at) VALUES (%(name)s::VARCHAR, %(description)s::VARCHAR, %(created_at)s::TIMESTAMP WITHOUT TIME ZONE) RETURNING categories.id"
        params = {
            "name": "Technology", 
            "description": "Posts about technology and programming", 
            "created_at": datetime.now()
        }
        
        print(f"Query: {query}")
        print(f"Parameters: {params}")
        
        try:
            cursor.execute(query, params)
            result = cursor.fetchone()
            print(f"✅ Success! Returned: {result}")
        except Exception as e:
            print(f"❌ Failed: {e}")
        
        return 0
    
    except Exception as e:
        print(f'❌ Test failed: {e}')
        return 1
    finally:
        pgsqlite_proc.terminate()
        try:
            output, _ = pgsqlite_proc.communicate(timeout=5)
            if output:
                print("\n--- pgsqlite debug output ---")
                lines = output.strip().split('\n')
                for line in lines:
                    if ('fast path' in line.lower() or 'type inference' in line.lower() or 
                        'checking for pattern' in line.lower() or 'inferred parameter' in line.lower() or
                        'invalid function parameter' in line.lower() or 'blob' in line.lower()):
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait() 
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())