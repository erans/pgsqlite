#!/usr/bin/env python3
"""Debug SQLAlchemy DDL operations with psycopg3-text"""

import psycopg
import subprocess
import time
import tempfile
import os
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class SimpleUser(Base):
    __tablename__ = 'simple_users'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite=info'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15512',
    ], env=env)
    
    time.sleep(2)
    
    try:
        # Test direct psycopg3 connection first
        print("🔗 Testing direct psycopg3 connection...")
        with psycopg.connect(
            "postgresql://postgres@localhost:15512/main",
            autocommit=True
        ) as conn:
            with conn.cursor() as cur:
                # Test basic operations
                try:
                    cur.execute("SELECT 1")
                    result = cur.fetchone()
                    print(f"✅ Basic query works: {result}")
                except Exception as e:
                    print(f"❌ Basic query failed: {e}")
                    return 1
                
                # Test CREATE TABLE
                try:
                    cur.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
                    print("✅ CREATE TABLE works")
                except Exception as e:
                    print(f"❌ CREATE TABLE failed: {e}")
                
                # Test DROP TABLE
                try:
                    cur.execute("DROP TABLE IF EXISTS test_table")
                    print("✅ DROP TABLE works")
                except Exception as e:
                    print(f"❌ DROP TABLE failed: {e}")
        
        # Test SQLAlchemy engine creation
        print("\n🏗️ Testing SQLAlchemy operations...")
        engine = create_engine(
            'postgresql+psycopg://postgres@localhost:15512/main',
            echo=False
        )
        
        # Test basic connection
        try:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1")).scalar()
                print(f"✅ SQLAlchemy connection works: {result}")
        except Exception as e:
            print(f"❌ SQLAlchemy connection failed: {e}")
            return 1
        
        # Test table creation
        try:
            print("📝 Testing table creation...")
            Base.metadata.create_all(engine)
            print("✅ Table creation successful")
        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            print(f"   Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            return 1
            
        # Test table listing
        try:
            print("📋 Testing table listing...")
            with engine.connect() as conn:
                result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table'")).fetchall()
                tables = [row[0] for row in result]
                print(f"✅ Found tables: {tables}")
        except Exception as e:
            print(f"❌ Table listing failed: {e}")
            
        # Test table drop
        try:
            print("🗑️  Testing table drop...")
            Base.metadata.drop_all(engine)
            print("✅ Table drop successful")
        except Exception as e:
            print(f"❌ Table drop failed: {e}")
            print(f"   Error type: {type(e)}")
            import traceback
            traceback.print_exc()
            
        return 0
        
    except Exception as e:
        print(f"❌ Connection error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        pgsqlite_proc.terminate()
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())