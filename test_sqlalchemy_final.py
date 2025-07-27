#!/usr/bin/env python3
"""Test SQLAlchemy with all fixes"""

import psycopg2
import datetime
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Date, DateTime, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
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
    # Create tables
    print("Creating tables...")
    Base.metadata.create_all(engine)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Test 1: Insert with datetime values
    print("\nTest 1: Inserting users with datetime values...")
    user1 = User(
        username='alice',
        email='alice@example.com',
        full_name='Alice Smith',
        is_active=True,
        birth_date=datetime.date(1990, 5, 15),
        created_at=datetime.datetime(2025, 7, 27, 10, 30, 0)
    )
    
    user2 = User(
        username='bob',
        email='bob@example.com',
        full_name='Bob Jones',
        is_active=False,
        birth_date=datetime.date(1985, 10, 22),
        created_at=datetime.datetime(2025, 7, 27, 11, 45, 0)
    )
    
    session.add(user1)
    session.add(user2)
    session.commit()
    print("✅ Users inserted successfully")
    
    # Test 2: Query with column aliases (like SQLAlchemy ORM does)
    print("\nTest 2: Querying with column aliases...")
    result = session.execute(text("""
        SELECT users.id AS users_id,
               users.username AS users_username,
               users.email AS users_email,
               users.full_name AS users_full_name,
               users.is_active AS users_is_active,
               users.created_at AS users_created_at,
               users.birth_date AS users_birth_date
        FROM users
        WHERE users.is_active = true
    """))
    
    for row in result:
        print(f"  ID: {row.users_id}, Username: {row.users_username}")
        print(f"  Birth Date: {row.users_birth_date} (type: {type(row.users_birth_date)})")
        print(f"  Created At: {row.users_created_at} (type: {type(row.users_created_at)})")
    print("✅ Query with aliases succeeded")
    
    # Test 3: SQLAlchemy ORM query
    print("\nTest 3: SQLAlchemy ORM query...")
    active_users = session.query(User).filter(User.is_active == True).all()
    print(f"Found {len(active_users)} active users")
    for user in active_users:
        print(f"  {user.username}: birth_date={user.birth_date}, created_at={user.created_at}")
    print("✅ ORM query succeeded")
    
    # Test 4: Transaction update
    print("\nTest 4: Transaction update...")
    alice = session.query(User).filter(User.username == 'alice').first()
    original_name = alice.full_name
    alice.full_name = 'Alice Johnson'
    session.commit()
    
    # Verify update persisted
    session.close()
    session = Session()
    alice_updated = session.query(User).filter(User.username == 'alice').first()
    if alice_updated.full_name == 'Alice Johnson':
        print("✅ Transaction update persisted correctly")
    else:
        print(f"❌ Transaction update failed: expected 'Alice Johnson', got '{alice_updated.full_name}'")
    
    print("\n✅ All tests passed!")
    
except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    session.close()
    engine.dispose()
