#!/usr/bin/env python3
"""Test SQLAlchemy with expire_on_commit settings"""

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = 'users_expire_test'
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True)
    full_name = Column(String(100))

# Create engine
engine = create_engine('postgresql://postgres@localhost:5435/main')

# Clean slate
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

print("=== Test 1: Default expire_on_commit (True) ===")
Session1 = sessionmaker(bind=engine, expire_on_commit=True)
session1 = Session1()

user1 = User(username='test1', full_name='Original')
session1.add(user1)
session1.commit()

user1.full_name = 'Updated'
session1.commit()

print(f"After commit (same session): {user1.full_name}")
session1.close()

# New session
session1b = Session1()
user1b = session1b.query(User).filter_by(username='test1').first()
print(f"New session: {user1b.full_name}")
session1b.close()

print("\n=== Test 2: expire_on_commit=False ===")
Session2 = sessionmaker(bind=engine, expire_on_commit=False)
session2 = Session2()

user2 = User(username='test2', full_name='Original')
session2.add(user2)
session2.commit()

user2.full_name = 'Updated'
session2.commit()

print(f"After commit (same session): {user2.full_name}")
session2.close()

# New session
session2b = Session2()
user2b = session2b.query(User).filter_by(username='test2').first()
print(f"New session: {user2b.full_name}")
session2b.close()

print("\n=== Test 3: Manual expire_all() ===")
Session3 = sessionmaker(bind=engine)
session3 = Session3()

user3 = User(username='test3', full_name='Original')
session3.add(user3)
session3.commit()

user3.full_name = 'Updated'
session3.commit()
session3.expire_all()  # Force expire all objects

print(f"After commit + expire_all: {user3.full_name}")
session3.close()

# New session
session3b = Session3()
user3b = session3b.query(User).filter_by(username='test3').first()
print(f"New session: {user3b.full_name}")
session3b.close()

print("\n=== Summary ===")
print("All tests should show 'Updated' in new sessions if transaction persistence works")