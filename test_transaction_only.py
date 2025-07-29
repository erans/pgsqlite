#!/usr/bin/env python3
"""Test only the transaction handling to isolate the issue."""

import sys
import argparse
from datetime import datetime, date
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, Date
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

Base = declarative_base()

class User(Base):
    """User model with basic information."""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    birth_date = Column(Date)

def main(port):
    # Create engine with same settings as main test
    engine = create_engine(
        f"postgresql://postgres:postgres@localhost:{port}/main",
        echo=True,
        poolclass=StaticPool,
        future=True,
        execution_options={"no_autoflush": False},
    )
    
    # Create tables
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    
    print("💾 Testing transaction handling...")
    
    # First, insert a test user
    try:
        with Session() as session:
            # Check if user already exists and delete
            existing = session.query(User).filter(User.username == "transaction_test_user").first()
            if existing:
                session.delete(existing)
                session.commit()
            
            # Insert new test user
            test_user = User(
                username="transaction_test_user",
                email="test@transaction.com", 
                full_name="Original Name"
            )
            session.add(test_user)
            print(f"Before commit - User in session.new: {test_user in session.new}")
            print(f"Before commit - User ID: {test_user.id}")
            session.commit()
            print(f"After commit - User ID: {test_user.id}")
            print(f"✅ Inserted test user with name: {test_user.full_name}")
    except Exception as e:
        print(f"❌ Error during insert: {e}")
        import traceback
        traceback.print_exc()
        return

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    main(args.port)