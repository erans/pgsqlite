#!/usr/bin/env python3
"""Test CAST detection and parameter inference"""

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
    env['RUST_LOG'] = 'pgsqlite::query::extended=info'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15530',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Testing CAST detection...")
        
        conn = psycopg.connect('postgresql://postgres@localhost:15530/main', autocommit=True)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("""CREATE TABLE test_cast (name TEXT, created_at TIMESTAMP)""")
        
        # Test the problematic query to see what it gets translated to
        query = "INSERT INTO test_cast (name, created_at) VALUES (%(name)s::VARCHAR, %(created_at)s::TIMESTAMP)"
        params = {"name": "test", "created_at": datetime.now()}
        
        print(f"Original query: {query}")
        print(f"Parameters: {params}")
        
        try:
            cursor.execute(query, params)
            print("✅ Query executed successfully")
        except Exception as e:
            print(f"❌ Query failed: {e}")
        
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
                # Look for Parse and Bind messages to see the actual query structure
                for line in lines:
                    if ('parse' in line.lower() or 'bind' in line.lower() or 
                        'cast' in line.lower() or 'inferred' in line.lower() or
                        'translating' in line.lower()):
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait() 
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())