#!/usr/bin/env python3
import subprocess
import time
import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, DECIMAL
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

# Start pgsqlite
pgsqlite_path = "../../target/debug/pgsqlite"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", ":memory:", "--port", "5437"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)

# Give it time to start
time.sleep(1)

try:
    # Create SQLAlchemy engine with psycopg3
    engine = create_engine(
        'postgresql+psycopg://postgres@localhost:5437/main',
        echo=True  # Enable SQL echo to see queries
    )
    
    Base = declarative_base()
    
    # Define a simple model
    class User(Base):
        __tablename__ = 'users'
        
        id = Column(Integer, primary_key=True)
        username = Column(String(50), nullable=False)
        email = Column(String(100))
        created_at = Column(DateTime, default=datetime.utcnow)
        balance = Column(DECIMAL(10, 2), default=0.00)
    
    # Create tables
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print("\n=== Testing psycopg3 + SQLAlchemy Integration ===")
    
    # Test 1: Simple INSERT with RETURNING
    print("\n1. Testing INSERT with RETURNING...")
    user = User(username='test_user', email='test@example.com', balance=100.50)
    session.add(user)
    session.commit()
    print(f"   ✅ Created user with ID: {user.id}")
    
    # Test 2: Query with parameter (triggers VARCHAR cast)
    print("\n2. Testing SELECT with parameter...")
    found_user = session.query(User).filter(User.username == 'test_user').first()
    assert found_user is not None
    assert found_user.email == 'test@example.com'
    print(f"   ✅ Found user: {found_user.username}")
    
    # Test 3: UPDATE with RETURNING
    print("\n3. Testing UPDATE...")
    found_user.balance = 200.00
    session.commit()
    print(f"   ✅ Updated balance to: {found_user.balance}")
    
    # Test 4: DELETE
    print("\n4. Testing DELETE...")
    session.delete(found_user)
    session.commit()
    count = session.query(User).count()
    assert count == 0
    print("   ✅ User deleted successfully")
    
    print("\n✅ All tests passed! psycopg3 + SQLAlchemy integration is working correctly")
    
    # Close session properly
    session.close()
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()