#!/usr/bin/env python3
"""Test simple INSERT to isolate parameter handling issues"""

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
    env['RUST_LOG'] = 'pgsqlite::query::extended=info'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15526',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Testing simple INSERT queries...")
        
        conn = psycopg.connect('postgresql://postgres@localhost:15526/main', autocommit=True)
        cursor = conn.cursor()
        
        # Create simple table
        print("📝 Creating simple table...")
        cursor.execute("CREATE TABLE test_simple (id INTEGER PRIMARY KEY, name TEXT)")
        print("✅ Table created")
        
        # Test 1: Simple INSERT with single text parameter
        print("🔧 Test 1: Simple text parameter...")
        try:
            cursor.execute("INSERT INTO test_simple (name) VALUES (%s)", ("test_name",))
            print("✅ Simple text parameter works")
        except Exception as e:
            print(f"❌ Simple text parameter failed: {e}")
            return 1
        
        # Test 2: INSERT with named parameters  
        print("🔧 Test 2: Named parameters...")
        try:
            cursor.execute("INSERT INTO test_simple (name) VALUES (%(name)s)", {"name": "test_name2"})
            print("✅ Named parameters work")
        except Exception as e:
            print(f"❌ Named parameters failed: {e}")
            return 1
            
        # Test 3: INSERT with type casting
        print("🔧 Test 3: Type casting...")
        try:
            cursor.execute("INSERT INTO test_simple (name) VALUES (%(name)s::TEXT)", {"name": "test_name3"})
            print("✅ Type casting works")
        except Exception as e:
            print(f"❌ Type casting failed: {e}")
            return 1
        
        print('🎉 All simple tests passed!')
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
                for line in lines[-15:]:
                    if 'error' in line.lower() or 'blob' in line.lower() or 'param' in line.lower():
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())