#!/usr/bin/env python3
"""Debug the bad parameter error in SQLAlchemy"""

import psycopg
import subprocess
import time
import tempfile
import os
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class TestTable(Base):
    __tablename__ = 'test_table'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15514',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            'postgresql+psycopg://postgres@localhost:15514/main',
            echo=True
        )
        
        print("🔧 Testing table creation...")
        try:
            Base.metadata.create_all(engine)
            print("✅ Table creation successful")
        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            
        return 0
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return 1
    finally:
        pgsqlite_proc.terminate()
        try:
            output, _ = pgsqlite_proc.communicate(timeout=5)
            if output:
                print("\n--- pgsqlite debug output (last 20 lines) ---")
                lines = output.strip().split('\n')[-20:]
                for line in lines:
                    if 'bad parameter' in line.lower() or 'error' in line.lower():
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())