#!/usr/bin/env python3
"""Debug the SQLAlchemy ping issue"""

import psycopg
import subprocess
import time
import tempfile
import os
from sqlalchemy import create_engine

def test_raw_psycopg():
    print("🔧 Testing raw psycopg connection...")
    try:
        conn = psycopg.connect('postgresql://postgres@localhost:15517/main', autocommit=True)
        cursor = conn.cursor()
        
        # Test simple query
        cursor.execute('SELECT 1')
        result = cursor.fetchone()
        print(f"✅ Raw psycopg SELECT 1: {result}")
        
        # Test with comment (SQLAlchemy's ping format)
        cursor.execute('/* ping */ SELECT 1')
        result = cursor.fetchone()
        print(f"✅ Raw psycopg ping query: {result}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ Raw psycopg failed: {e}")
        return False

def test_sqlalchemy_ping():
    print("🔧 Testing SQLAlchemy engine ping...")
    try:
        engine = create_engine(
            'postgresql+psycopg://postgres@localhost:15517/main',
            echo=False,
            pool_pre_ping=False  # Disable pre-ping to avoid the issue
        )
        
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT 1")).scalar()
            print(f"✅ SQLAlchemy connection: {result}")
            
        return True
    except Exception as e:
        print(f"❌ SQLAlchemy failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_sqlalchemy_with_ping():
    print("🔧 Testing SQLAlchemy with pool_pre_ping=True...")
    try:
        engine = create_engine(
            'postgresql+psycopg://postgres@localhost:15517/main',
            echo=False,
            pool_pre_ping=True  # This is what causes the issue
        )
        
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(text("SELECT 1")).scalar()
            print(f"✅ SQLAlchemy with ping: {result}")
            
        return True
    except Exception as e:
        print(f"❌ SQLAlchemy with ping failed: {e}")
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
        '--port', '15517',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        results = []
        results.append(test_raw_psycopg())
        results.append(test_sqlalchemy_ping())
        results.append(test_sqlalchemy_with_ping())
        
        print("\n=== Results ===")
        print(f"Raw psycopg: {'✅' if results[0] else '❌'}")
        print(f"SQLAlchemy without ping: {'✅' if results[1] else '❌'}")  
        print(f"SQLAlchemy with ping: {'✅' if results[2] else '❌'}")
        
        if not results[2]:
            print("\n🔍 The issue is with pool_pre_ping=True in SQLAlchemy")
            
        return 0 if all(results) else 1
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1
    finally:
        pgsqlite_proc.terminate()
        try:
            output, _ = pgsqlite_proc.communicate(timeout=5)
            if output and not all(results):
                print("\n--- pgsqlite output ---")
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