#!/usr/bin/env python3
"""Isolate the transaction update issue"""

import datetime
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Date, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class User(Base):
    __tablename__ = 'isolated_users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    birth_date = Column(Date)

# Create engine and session
engine = create_engine('postgresql://postgres@localhost:5435/main')
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

print("=== Test Case from test_sqlalchemy_final.py ===")

# First session - Insert
session = Session()
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
print("✅ User inserted")

# Same session - Update
alice = session.query(User).filter(User.username == 'alice').first()
print(f"Before update: {alice.full_name}")
alice.full_name = 'Alice Johnson'
session.commit()
print(f"After commit in same session: {alice.full_name}")

# Close and create new session
session.close()
session = Session()

# Query in new session
alice_updated = session.query(User).filter(User.username == 'alice').first()
print(f"In new session: {alice_updated.full_name}")

if alice_updated.full_name == 'Alice Johnson':
    print("✅ Update persisted!")
else:
    print("❌ Update NOT persisted!")
    
# Direct SQL check
print("\n=== Direct SQL Check ===")
import psycopg2
conn = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
cur = conn.cursor()
cur.execute("SELECT full_name FROM isolated_users WHERE username = 'alice'")
result = cur.fetchone()
print(f"Direct SQL sees: {result[0] if result else 'Not found'}")
conn.close()

# SQLite check
print("\n=== SQLite Direct Check ===")
import sqlite3
conn = sqlite3.connect('main.db')
cur = conn.cursor()
cur.execute("SELECT full_name FROM isolated_users WHERE username = 'alice'")
result = cur.fetchone()
print(f"SQLite sees: {result[0] if result else 'Not found'}")
conn.close()