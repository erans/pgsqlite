#!/usr/bin/env python3
"""Debug complex DDL operations that cause bad parameter errors"""

import psycopg
import subprocess
import time
import tempfile
import os
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, ForeignKey, Numeric
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    posts = relationship("Post", back_populates="author")

class Category(Base):
    __tablename__ = "categories"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    posts = relationship("Post", back_populates="category")

class Post(Base):
    __tablename__ = "posts"
    
    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    content = Column(Text)
    author_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    category_id = Column(Integer, ForeignKey("categories.id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    view_count = Column(Integer, default=0)
    is_published = Column(Boolean, default=False)
    
    author = relationship("User", back_populates="posts")
    category = relationship("Category", back_populates="posts")

def main():
    # Create test database
    db_path = tempfile.mktemp(suffix='.db')
    
    # Start pgsqlite with debug logging
    env = os.environ.copy()
    env['RUST_LOG'] = 'pgsqlite=info'
    
    pgsqlite_proc = subprocess.Popen([
        '/home/eran/work/pgsqlite/target/release/pgsqlite',
        '--database', db_path,
        '--port', '15515',
    ], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    time.sleep(2)
    
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            'postgresql+psycopg://postgres@localhost:15515/main',
            echo=True,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            future=True
        )
        
        print("🔧 Testing complex table creation...")
        try:
            print("🧹 Dropping existing tables...")
            Base.metadata.drop_all(engine)
            
            print("🏗️ Creating tables...")
            Base.metadata.create_all(engine)
            print("✅ Complex table creation successful")
        except Exception as e:
            print(f"❌ Complex table creation failed: {e}")
            import traceback
            traceback.print_exc()
            
        return 0
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
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