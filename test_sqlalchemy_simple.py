#!/usr/bin/env python3
"""Simple SQLAlchemy ORM test"""

from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Enable SQL logging
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

Base = declarative_base()

class User(Base):
    __tablename__ = 'simple_users'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

# Create engine
engine = create_engine('postgresql://postgres@localhost:5435/main')

# Create table
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

# Test 1: Insert
print("\n=== Insert ===")
session = Session()
user = User(name='Alice')
session.add(user)
session.commit()
user_id = user.id
session.close()

# Test 2: Update
print("\n=== Update ===")
session = Session()
user = session.query(User).filter_by(id=user_id).first()
print(f"Before update: {user.name}")
user.name = 'Bob'
session.commit()
print(f"After commit: {user.name}")
session.close()

# Test 3: Verify
print("\n=== Verify ===")
session = Session()
user = session.query(User).filter_by(id=user_id).first()
print(f"In new session: {user.name}")
session.close()

# Direct check
print("\n=== Direct SQL Check ===")
import psycopg2
conn = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
cur = conn.cursor()
cur.execute(f"SELECT name FROM simple_users WHERE id = {user_id}")
result = cur.fetchone()
print(f"Direct SQL: {result[0] if result else 'Not found'}")
conn.close()