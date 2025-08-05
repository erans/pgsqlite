#!/usr/bin/env python3
import subprocess
import os
import sys
import time
import tempfile

def test_to_regtype():
    """Test direct to_regtype() function"""
    
    # Create a temporary database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15501',
        '--in-memory'
    ], env={
        **os.environ,
        'RUST_LOG': 'pgsqlite::catalog=debug'
    })
    
    time.sleep(1)  # Give server time to start
    
    try:
        # Test direct simple query
        result = subprocess.run([
            'psql',
            '-h', 'localhost',
            '-p', '15501',
            '-U', 'postgres',
            '-d', 'main',
            '-t',
            '-c', "SELECT to_regtype('integer')"
        ], env={'PGSSLMODE': 'disable'}, capture_output=True, text=True)
        
        print(f"Exit code: {result.returncode}")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
        
        if result.returncode == 0:
            print("✅ to_regtype('integer') succeeded")
            value = result.stdout.strip()
            if value == '23':
                print(f"✅ Got expected OID: {value}")
            else:
                print(f"❌ Unexpected result: {value}")
        else:
            print("❌ to_regtype('integer') failed")
            
    finally:
        pgsqlite_proc.terminate()
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == '__main__':
    test_to_regtype()