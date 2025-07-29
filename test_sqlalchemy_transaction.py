#!/usr/bin/env python3
"""Test SQLAlchemy transaction persistence issue"""

import sys
import time
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

Base = declarative_base()

class TestUser(Base):
    __tablename__ = "test_users"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

def test_transaction_persistence(port):
    print(f"🧪 Testing SQLAlchemy transaction persistence on port {port}")
    
    # Engine 1 - Insert and update
    print("📍 Creating first engine...")
    engine1 = create_engine(
        f"postgresql://postgres@localhost:{port}/main",
        echo=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    
    # Create table
    Base.metadata.drop_all(engine1)
    Base.metadata.create_all(engine1)
    
    Session1 = sessionmaker(bind=engine1)
    
    # Insert and update
    with Session1() as session:
        # Insert
        user = TestUser(id=1, name="original")
        session.add(user)
        session.commit()
        print(f"✅ Engine1: Inserted user with name: {user.name}")
        
        # Update
        user.name = "updated"
        print(f"📍 Before commit, object shows: {user.name}")
        session.commit()
        print(f"✅ Engine1: Updated user to name: {user.name}")
        
        # Verify in same session with fresh query
        user_fresh = session.query(TestUser).filter_by(id=1).first()
        print(f"📍 Engine1 same session fresh query sees: {user_fresh.name}")
        print(f"📍 Engine1 same session original object still shows: {user.name}")
    
    # Dispose engine1
    engine1.dispose()
    print("🗑️ Engine1 disposed")
    
    # Small delay
    time.sleep(0.1)
    
    # Engine 2 - Fresh connection
    print("📍 Creating second engine (new connection)...")
    engine2 = create_engine(
        f"postgresql://postgres@localhost:{port}/main",
        echo=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    
    Session2 = sessionmaker(bind=engine2)
    
    with Session2() as session:
        user = session.query(TestUser).filter_by(id=1).first()
        result_name = user.name if user else "NOT FOUND"
        print(f"📍 Engine2 new connection sees: {result_name}")
        
        if user and user.name == "updated":
            print("✅ SUCCESS: Transaction persistence works!")
            engine2.dispose()
            return True
        else:
            print(f"❌ FAILURE: Expected 'updated', got '{result_name}'")
            engine2.dispose()
            return False

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 15502
    success = test_transaction_persistence(port)
    sys.exit(0 if success else 1)