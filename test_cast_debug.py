#!/usr/bin/env python3
"""Debug cast translation"""

import psycopg
import subprocess
import time
import tempfile
import os

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite::translator=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15528',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Testing cast translation...")
        
        conn = psycopg.connect('postgresql://postgres@localhost:15528/main', autocommit=True)
        cursor = conn.cursor()
        
        # Create table
        cursor.execute("CREATE TABLE test_cast (id INTEGER PRIMARY KEY, name TEXT)")
        
        # Test different query formats
        queries = [
            # Simple format (works)
            ("Simple format", "INSERT INTO test_cast (name) VALUES (%s)", ("test1",)),
            
            # Named parameter without cast (works)  
            ("Named no cast", "INSERT INTO test_cast (name) VALUES (%(name)s)", {"name": "test2"}),
            
            # Named parameter with cast (fails)
            ("Named with cast", "INSERT INTO test_cast (name) VALUES (%(name)s::TEXT)", {"name": "test3"}),
        ]
        
        for test_name, query, params in queries:
            print(f"🔧 Testing {test_name}...")
            print(f"   Query: {query}")
            print(f"   Params: {params}")
            try:
                cursor.execute(query, params)
                print(f"   ✅ {test_name} works")
            except Exception as e:
                print(f"   ❌ {test_name} failed: {e}")
        
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
                for line in lines[-20:]:
                    if 'cast' in line.lower() or 'translat' in line.lower() or 'error' in line.lower():
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait() 
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())