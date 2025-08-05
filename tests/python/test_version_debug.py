#!/usr/bin/env python3
"""Debug version() function calls"""

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
    env['RUST_LOG'] = 'pgsqlite::catalog=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15505',
        '--in-memory'
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(1)
    
    try:
        # Connect
        with psycopg.connect(
            "postgresql://postgres@localhost:15505/main",
            autocommit=True,
            cursor_factory=psycopg.cursor.Cursor  # Force text mode
        ) as conn:
            print("✅ Connected")
            
            with conn.cursor() as cur:
                # Test different version queries
                test_queries = [
                    "SELECT version()",
                    "SELECT pg_catalog.version()",
                    "select pg_catalog.version()",
                    "SELECT version() AS server_version",
                ]
                
                for query in test_queries:
                    try:
                        cur.execute(query)
                        result = cur.fetchone()
                        print(f"✅ Query '{query}' succeeded: {result}")
                    except Exception as e:
                        print(f"❌ Query '{query}' failed: {e}")
                
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return 1
    finally:
        pgsqlite_proc.terminate()
        # Print last 50 lines of output
        if pgsqlite_proc.stdout:
            lines = []
            for line in pgsqlite_proc.stdout:
                lines.append(line.rstrip())
            print("\nLast log lines:")
            for line in lines[-50:]:
                print(line)
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())