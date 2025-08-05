#!/usr/bin/env python3
"""Debug the specific INSERT query that's failing"""

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
    env['RUST_LOG'] = 'pgsqlite::query::extended_fast_path=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15525',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Testing the specific failing INSERT query...")
        
        # Create connection
        conn = psycopg.connect('postgresql://postgres@localhost:15525/main', autocommit=True)
        cursor = conn.cursor()
        
        # Create the table first
        print("📝 Creating categories table...")
        cursor.execute("""
            CREATE TABLE categories (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("✅ Table created successfully")
        
        # Test the specific failing INSERT query
        print("🔧 Testing INSERT with datetime parameter...")
        try:
            # This is the exact query that was failing
            query = "INSERT INTO categories (name, description, created_at) VALUES (%(name)s::VARCHAR, %(description)s::VARCHAR, %(created_at)s::TIMESTAMP WITHOUT TIME ZONE) RETURNING categories.id"
            params = {
                'name': 'Technology', 
                'description': 'Posts about technology and programming', 
                'created_at': datetime(2025, 8, 4, 21, 21, 13, 304163)
            }
            
            print(f"Query: {query}")
            print(f"Params: {params}")
            
            cursor.execute(query, params)
            result = cursor.fetchone()
            print(f"✅ INSERT successful, returned ID: {result}")
            
        except Exception as e:
            print(f"❌ INSERT failed: {e}")
            return 1
        
        # Clean up
        cursor.close()
        conn.close()
        
        print('🎉 Test completed successfully!')
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
                for line in lines[-20:]:
                    if 'debug' in line.lower() or 'error' in line.lower() or 'blob' in line.lower():
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())