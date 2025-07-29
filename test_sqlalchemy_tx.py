#!/usr/bin/env python3
"""Isolated SQLAlchemy transaction test"""

from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import datetime
import sys

Base = declarative_base()

class User(Base):
    __tablename__ = "test_users"
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    full_name = Column(String(100))
    created_at = Column(DateTime, default=datetime.datetime.utcnow)

def test_transaction(port=15500):
    # Create engine
    engine = create_engine(
        f"postgresql://postgres:postgres@localhost:{port}/main",
        echo=True,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )
    
    # Create tables
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    
    # Insert user
    with Session() as session:
        user = User(username="test_user", full_name="Original Name")
        session.add(user)
        session.commit()
        user_id = user.id
        print(f"✅ Created user with id={user_id}, name='Original Name'")
    
    # Update user in new session
    with Session() as session:
        user = session.query(User).filter_by(id=user_id).first()
        print(f"📍 Before update: {user.full_name}")
        user.full_name = "Updated Name"
        session.commit()
        print("✅ Committed update")
    
    # Small delay to ensure commit is fully persisted
    import time
    time.sleep(0.5)
    
    # Check from completely new engine
    new_engine = create_engine(
        f"postgresql://postgres:postgres@localhost:{port}/main",
        echo=False,
    )
    NewSession = sessionmaker(bind=new_engine)
    
    with NewSession() as session:
        user = session.query(User).filter_by(id=user_id).first()
        result = user.full_name if user else "NOT FOUND"
        print(f"📍 New connection sees: {result}")
        
        # Cleanup
        session.query(User).delete()
        session.commit()
        
    # Drop table
    Base.metadata.drop_all(engine)
    
    return result == "Updated Name"

if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 15500
    success = test_transaction(port)
    print("✅ Test PASSED" if success else "❌ Test FAILED")
    sys.exit(0 if success else 1)