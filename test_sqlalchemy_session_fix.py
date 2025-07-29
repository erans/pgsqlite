#!/usr/bin/env python3
"""Test SQLAlchemy with different session configurations"""

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = 'session_test_users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

# Create engine
engine = create_engine('postgresql://postgres@localhost:5435/main')

# Create table
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

print("=== Test 1: Default Session Behavior ===")
Session = sessionmaker(bind=engine)
session = Session()
user = User(name='Alice')
session.add(user)
session.commit()
user_id = user.id

user.name = 'Bob'
session.commit()
session.close()

# Check in new session
session2 = Session()
user2 = session2.query(User).filter_by(id=user_id).first()
print(f"Default behavior result: {user2.name}")
session2.close()

print("\n=== Test 2: expire_on_commit=False ===")
Session2 = sessionmaker(bind=engine, expire_on_commit=False)
session = Session2()
user = session.query(User).filter_by(id=user_id).first()
user.name = 'Charlie'
session.commit()
session.close()

# Check in new session
session2 = Session2()
user2 = session2.query(User).filter_by(id=user_id).first()
print(f"expire_on_commit=False result: {user2.name}")
session2.close()

print("\n=== Test 3: Explicit expire() ===")
session = Session()
user = session.query(User).filter_by(id=user_id).first()
user.name = 'David'
session.commit()
session.expire_all()  # Force refresh from DB
user_refreshed = session.query(User).filter_by(id=user_id).first()
print(f"Same session after expire: {user_refreshed.name}")
session.close()

# New session check
session2 = Session()
user2 = session2.query(User).filter_by(id=user_id).first()
print(f"New session after expire test: {user2.name}")
session2.close()

print("\n=== Test 4: Direct SQL Verification ===")
import psycopg2
conn = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
cur = conn.cursor()
cur.execute(f"SELECT name FROM session_test_users WHERE id = {user_id}")
result = cur.fetchone()
print(f"Direct SQL sees: {result[0]}")

# Also check autocommit status
print(f"\npsycopg2 autocommit: {conn.autocommit}")
print(f"psycopg2 isolation level: {conn.isolation_level}")
conn.close()