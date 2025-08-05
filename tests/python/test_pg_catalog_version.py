#!/usr/bin/env python3
"""Test pg_catalog.version() specifically"""

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
    env['RUST_LOG'] = 'pgsqlite::catalog=debug,pgsqlite::query::executor=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15507',
        '--in-memory'
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(1)
    
    logs = []
    
    def read_logs():
        while True:
            line = pgsqlite_proc.stdout.readline()
            if not line:
                break
            logs.append(line.rstrip())
    
    import threading
    log_thread = threading.Thread(target=read_logs, daemon=True)
    log_thread.start()
    
    try:
        # Connect
        with psycopg.connect(
            "postgresql://postgres@localhost:15507/main",
            autocommit=True,
            cursor_factory=psycopg.cursor.Cursor  # Force text mode
        ) as conn:
            print("✅ Connected")
            
            with conn.cursor() as cur:
                # Test the exact query SQLAlchemy uses
                try:
                    cur.execute("select pg_catalog.version()")
                    result = cur.fetchone()
                    print(f"✅ pg_catalog.version() succeeded: {result}")
                except Exception as e:
                    print(f"❌ pg_catalog.version() failed: {e}")
                
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return 1
    finally:
        pgsqlite_proc.terminate()
        time.sleep(0.5)
        
        print("\nRelevant log lines:")
        for line in logs:
            if any(x in line for x in ['version', 'intercept', 'process', 'system', 'ERROR', 'Query']):
                print(line)
        
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())