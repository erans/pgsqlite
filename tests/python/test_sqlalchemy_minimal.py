#!/usr/bin/env python3
"""Minimal SQLAlchemy test to isolate specific issues"""

from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import declarative_base, sessionmaker
import subprocess
import time
import tempfile
import os

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50))
    email = Column(String(100))

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite=debug'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15513',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            'postgresql+psycopg://postgres@localhost:15513/main',
            echo=True  # Show SQL queries
        )
        
        print("🔧 Step 1: Testing basic connection...")
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            print(f"✅ Basic connection works: {result}")
        
        print("\n🔧 Step 2: Testing table creation...")
        Base.metadata.create_all(engine)
        print("✅ Table creation successful")
        
        print("\n🔧 Step 3: Testing ORM session...")
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Test basic insert
        try:
            user = User(username='test_user', email='test@example.com')
            session.add(user)
            session.commit()
            print("✅ Insert successful")
        except Exception as e:
            print(f"❌ Insert failed: {e}")
            session.rollback()
            return 1
        
        # Test basic query  
        try:
            users = session.query(User).all()
            print(f"✅ Query successful: found {len(users)} users")
        except Exception as e:
            print(f"❌ Query failed: {e}")
            return 1
            
        # Test filtered query
        try:
            user = session.query(User).filter(User.username == 'test_user').first()
            if user:
                print(f"✅ Filtered query successful: {user.username}")
            else:
                print("❌ Filtered query returned no results")
        except Exception as e:
            print(f"❌ Filtered query failed: {e}")
            return 1
        
        session.close()
        
        print("\n🔧 Step 4: Testing table drop...")
        Base.metadata.drop_all(engine)
        print("✅ Table drop successful")
        
        print("\n🎉 All tests passed!")
        return 0
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        pgsqlite_proc.terminate()
        
        # Print last few lines of pgsqlite output for debugging
        try:
            output, _ = pgsqlite_proc.communicate(timeout=5)
            if output:
                lines = output.strip().split('\n')[-10:]  # Last 10 lines
                print("\n--- pgsqlite debug output (last 10 lines) ---")
                for line in lines:
                    if 'ERROR' in line or 'bad parameter' in line:
                        print(line)
        except:
            pass
            
        pgsqlite_proc.wait()
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    exit(main())