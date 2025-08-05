#!/usr/bin/env python3
"""Debug SQLAlchemy configuration that causes bad parameter error"""

import psycopg
import subprocess
import time
import tempfile
import os
from sqlalchemy import create_engine, text

def test_exact_config():
    print("🔧 Testing exact SQLAlchemy ORM configuration...")
    try:
        # This is the exact config from the failing test
        engine = create_engine(
            'postgresql+psycopg://postgres@localhost:15518/main',
            echo=True,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            future=True,
            execution_options={"no_autoflush": False}
        )
        
        print("🔗 Testing basic connection...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            print(f"✅ Basic connection works: {result}")
            
        print("🔗 Testing second connection (pool)...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 2")).scalar()
            print(f"✅ Second connection works: {result}")
            
        print("🔗 Testing third connection (pool overflow)...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 3")).scalar()
            print(f"✅ Third connection works: {result}")
            
        return True
    except Exception as e:
        print(f"❌ Exact config failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite=info'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15518',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        success = test_exact_config()
        
        if success:
            print("\n✅ The SQLAlchemy configuration itself works fine")
            print("🔍 The issue must be elsewhere - perhaps in schema validation or complex queries")
        else:
            print("\n❌ Found the issue with the SQLAlchemy configuration")
            
        return 0 if success else 1
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1
    finally:
        pgsqlite_proc.terminate()
        try:
            output, _ = pgsqlite_proc.communicate(timeout=5)
            if output and not success:
                print("\n--- pgsqlite output ---")
                lines = output.strip().split('\n')
                for line in lines[-15:]:
                    if 'bad parameter' in line.lower() or 'error' in line.lower():
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())