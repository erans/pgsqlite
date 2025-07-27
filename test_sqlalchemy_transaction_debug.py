#!/usr/bin/env python3
"""Debug the exact SQLAlchemy transaction issue"""

import logging
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Enable detailed SQLAlchemy logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True)
    full_name = Column(String(100))

# Create engine
engine = create_engine('postgresql://postgres@localhost:5435/main')

# Ensure clean state
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

print("=== Reproducing the exact issue ===")

# Step 1: Insert user
session = Session()
user = User(username='transaction_test_user', full_name='Original Name')
session.add(user)
session.commit()
user_id = user.id
print(f"✅ User created with ID: {user_id}")

# Step 2: Update in same session (this is where the issue occurs)
print("\n--- Updating user ---")
user.full_name = 'Updated Name'
print(f"Before commit (same session): {user.full_name}")

# The commit that should persist the change
session.commit()
print(f"After commit (same session): {user.full_name}")

# This is where SQLAlchemy might read stale data
session.refresh(user)  # Force refresh from database
print(f"After refresh from database: {user.full_name}")

# Close the session (this triggers the ROLLBACK in the logs)
session.close()

# Step 3: New session (should see the committed change)
print("\n--- New session ---")
session2 = Session()
user2 = session2.query(User).filter_by(username='transaction_test_user').first()
print(f"New session sees: {user2.full_name}")

if user2.full_name == 'Updated Name':
    print("✅ Transaction update persisted")
else:
    print("❌ Transaction update not persisted")
    print(f"Expected: 'Updated Name', Got: '{user2.full_name}'")

session2.close()

# Step 4: Check what SQLite actually contains
print("\n--- Direct database check ---")
import sqlite3
conn = sqlite3.connect('main.db')
cur = conn.cursor()
cur.execute("SELECT full_name FROM users WHERE username = 'transaction_test_user'")
result = cur.fetchone()
print(f"SQLite database contains: {result[0] if result else 'Not found'}")
conn.close()

print("\n=== Test completed ===")