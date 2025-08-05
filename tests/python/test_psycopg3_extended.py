#!/usr/bin/env python3
"""Test psycopg3 with extended protocol tracing"""

import psycopg
import os
import subprocess
import time
import tempfile
import sys

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite::query::extended=debug,pgsqlite::catalog=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15502',
        '--in-memory'
    ], env=env)
    
    time.sleep(1)
    
    try:
        # Connect with text mode
        with psycopg.connect(
            "postgresql://postgres@localhost:15502/main",
            autocommit=True,
            options="-c default_int_size=4",
            cursor_factory=psycopg.cursor.Cursor  # Force text mode
        ) as conn:
            print("✅ Connected")
            
            with conn.cursor() as cur:
                # Test different ways of calling to_regtype
                try:
                    # Direct simple query
                    cur.execute("SELECT to_regtype('integer')")
                    result = cur.fetchone()
                    print(f"✅ Simple query to_regtype: {result}")
                except Exception as e:
                    print(f"❌ Simple query failed: {e}")
                
                try:
                    # With prepare=False (force simple protocol)
                    cur.execute("SELECT to_regtype('integer')", prepare=False)
                    result = cur.fetchone()
                    print(f"✅ Simple protocol to_regtype: {result}")
                except Exception as e:
                    print(f"❌ Simple protocol failed: {e}")
                    
                try:
                    # With prepare=True (force extended protocol)
                    cur.execute("SELECT to_regtype('integer')", prepare=True)
                    result = cur.fetchone()
                    print(f"✅ Extended protocol to_regtype: {result}")
                except Exception as e:
                    print(f"❌ Extended protocol failed: {e}")
                    
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return 1
    finally:
        pgsqlite_proc.terminate()
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())