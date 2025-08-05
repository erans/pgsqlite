#!/usr/bin/env python3
"""Test concurrent database connections to debug the bad parameter issue"""

import psycopg
import subprocess
import time
import tempfile
import os

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with session debugging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite::session=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15520',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Testing concurrent database connections...")
        
        # First connection - this should work
        print("🔗 Creating first connection...")
        conn1 = psycopg.connect('postgresql://postgres@localhost:15520/main', autocommit=True)
        cursor1 = conn1.cursor()
        cursor1.execute('SELECT 1 as first')
        result1 = cursor1.fetchone()
        print(f'✅ Connection 1: {result1}')
        
        # Keep connection 1 open and create connection 2
        print("🔗 Creating second connection while first is open...")
        try:
            conn2 = psycopg.connect('postgresql://postgres@localhost:15520/main', autocommit=True)
            cursor2 = conn2.cursor()
            cursor2.execute('SELECT 2 as second')
            result2 = cursor2.fetchone()  
            print(f'✅ Connection 2: {result2}')
            
            # Test both connections can execute queries
            print("🔧 Testing both connections can work...")
            cursor1.execute('SELECT 11')
            result1b = cursor1.fetchone()
            print(f'✅ Connection 1 again: {result1b}')
            
            cursor2.execute('SELECT 22')
            result2b = cursor2.fetchone()
            print(f'✅ Connection 2 again: {result2b}')
            
            # Close second connection
            cursor2.close()
            conn2.close()
            print("✅ Second connection closed successfully")
            
        except Exception as e:
            print(f"❌ Second connection failed: {e}")
            import traceback
            traceback.print_exc()
            return 1
        
        # Close first connection
        cursor1.close()
        conn1.close()
        print("✅ First connection closed successfully")
        
        print('🎉 Concurrent connections work fine!')
        return 0
    
    except Exception as e:
        print(f'❌ Concurrent connection test failed: {e}')
        import traceback
        traceback.print_exc()
        return 1
    finally:
        pgsqlite_proc.terminate()
        try:
            output, _ = pgsqlite_proc.communicate(timeout=5)
            if output:
                print("\n--- pgsqlite session debug output ---")
                lines = output.strip().split('\n')
                for line in lines[-20:]:
                    if ('connection' in line.lower() or 'session' in line.lower() or 
                        'bad parameter' in line.lower() or 'error' in line.lower()):
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())