#!/usr/bin/env python3
"""Test SQLAlchemy transaction persistence with journal mode fix"""

import psycopg2
import datetime
import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Date, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = 'users_test'  # Different table name to avoid conflicts
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    birth_date = Column(Date)

# Connect to pgsqlite
engine = create_engine('postgresql://postgres@localhost:5435/main')

try:
    # Drop table if exists
    print("Dropping existing table if any...")
    Base.metadata.drop_all(engine)
    
    # Create tables
    print("Creating tables...")
    Base.metadata.create_all(engine)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Test 1: Insert user
    print("\nTest 1: Inserting user...")
    user1 = User(
        username='alice',
        email='alice@example.com',
        full_name='Alice Smith',
        is_active=True,
        birth_date=datetime.date(1990, 5, 15),
        created_at=datetime.datetime(2025, 7, 27, 10, 30, 0)
    )
    
    session.add(user1)
    session.commit()
    print("✅ User inserted successfully")
    
    # Test 2: Transaction update
    print("\nTest 2: Transaction update...")
    alice = session.query(User).filter(User.username == 'alice').first()
    print(f"Current full_name: {alice.full_name}")
    alice.full_name = 'Alice Johnson'
    session.commit()
    print("✅ Update committed")
    
    # Test 3: Verify update persisted in new session
    print("\nTest 3: Verifying update persistence in new session...")
    session.close()
    session = Session()
    alice_updated = session.query(User).filter(User.username == 'alice').first()
    print(f"Full name after new session: {alice_updated.full_name}")
    
    if alice_updated.full_name == 'Alice Johnson':
        print("✅ Transaction update persisted correctly!")
        print(f"\nJournal mode used: {os.environ.get('PGSQLITE_JOURNAL_MODE', 'WAL (default)')}")
    else:
        print(f"❌ Transaction update failed: expected 'Alice Johnson', got '{alice_updated.full_name}'")
        print(f"\nJournal mode used: {os.environ.get('PGSQLITE_JOURNAL_MODE', 'WAL (default)')}")
    
    # Test 4: Try with explicit transaction
    print("\nTest 4: Testing with explicit transaction...")
    with session.begin():
        alice = session.query(User).filter(User.username == 'alice').first()
        alice.full_name = 'Alice Williams'
    # Transaction auto-commits when exiting the with block
    
    # Verify in new session
    session.close()
    session = Session()
    alice_final = session.query(User).filter(User.username == 'alice').first()
    print(f"Full name after explicit transaction: {alice_final.full_name}")
    
    if alice_final.full_name == 'Alice Williams':
        print("✅ Explicit transaction update persisted correctly!")
    else:
        print(f"❌ Explicit transaction update failed: expected 'Alice Williams', got '{alice_final.full_name}'")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    session.close()
    engine.dispose()