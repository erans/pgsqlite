#!/usr/bin/env python3
"""Test the semicolon query that SQLAlchemy uses for ping"""

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
    env['RUST_LOG'] = 'pgsqlite=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15521',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        print("🔧 Testing SQLAlchemy's ping query pattern...")
        
        # Create first connection
        conn1 = psycopg.connect('postgresql://postgres@localhost:15521/main', autocommit=True)
        cursor1 = conn1.cursor()
        
        # Test normal queries
        cursor1.execute('SELECT 1')
        result1 = cursor1.fetchone()
        print(f'✅ SELECT 1: {result1}')
        
        # Test the semicolon query that SQLAlchemy uses for ping
        try:
            cursor1.execute(';')
            print('✅ Semicolon query works on first connection')
        except Exception as e:
            print(f'❌ Semicolon query failed on first connection: {e}')

        # Create second connection
        conn2 = psycopg.connect('postgresql://postgres@localhost:15521/main', autocommit=True)
        cursor2 = conn2.cursor()
        
        # Test semicolon on second connection
        try:
            cursor2.execute(';')
            print('✅ Semicolon query works on second connection')
        except Exception as e:
            print(f'❌ Semicolon query failed on second connection: {e}')
            import traceback
            traceback.print_exc()
            return 1
            
        # Test normal query on second after semicolon
        try:
            cursor2.execute('SELECT 2')
            result2 = cursor2.fetchone()
            print(f'✅ SELECT 2 after semicolon: {result2}')
        except Exception as e:
            print(f'❌ SELECT 2 after semicolon failed: {e}')
            import traceback
            traceback.print_exc()
            return 1
        
        # Clean up
        cursor1.close()
        conn1.close()
        cursor2.close()
        conn2.close()
        
        print('🎉 All semicolon tests passed!')
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
                    if 'bad parameter' in line.lower() or 'error' in line.lower():
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())