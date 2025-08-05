#!/usr/bin/env python
"""Test SQLAlchemy with psycopg3 text mode"""
import argparse
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), nullable=False, unique=True)
    email = Column(String(100), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

def test_sqlalchemy_text_mode():
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int, default=15502)
    args = parser.parse_args()
    
    # Use psycopg3 in text mode (psycopg)
    # Note: psycopg is psycopg3, psycopg2 is the old version
    engine = create_engine(f'postgresql+psycopg://postgres@localhost:{args.port}/main')
    
    print(f"Testing SQLAlchemy with psycopg3 text mode on port {args.port}...")
    
    # Create tables
    print("Creating tables...")
    Base.metadata.create_all(engine)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Add a user
        print("Adding user...")
        user = User(username='test_user', email='test@example.com')
        session.add(user)
        session.commit()
        print(f"  Created user with ID: {user.id}")
        
        # Query users
        print("\nQuerying users...")
        users = session.query(User).all()
        for u in users:
            print(f"  User: {u.username} ({u.email}) created at {u.created_at}")
        
        # Update user
        print("\nUpdating user...")
        user.email = 'updated@example.com'
        session.commit()
        
        # Query again
        updated_user = session.query(User).filter_by(username='test_user').first()
        print(f"  Updated email: {updated_user.email}")
        
        # Delete user
        print("\nDeleting user...")
        session.delete(user)
        session.commit()
        
        # Verify deletion
        count = session.query(User).count()
        print(f"  Remaining users: {count}")
        
        print("\n✅ All SQLAlchemy tests passed!")
        
    finally:
        session.close()
        
        # Drop tables
        print("\nCleaning up...")
        Base.metadata.drop_all(engine)

if __name__ == "__main__":
    test_sqlalchemy_text_mode()