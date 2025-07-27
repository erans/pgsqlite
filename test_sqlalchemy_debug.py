#!/usr/bin/env python3
"""Debug SQLAlchemy transaction behavior"""

import psycopg2
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

# Enable SQLAlchemy logging
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.DEBUG)
logging.getLogger('sqlalchemy.pool').setLevel(logging.DEBUG)

print("=== Testing SQLAlchemy Session Behavior ===")

# Create engine with StaticPool to ensure same connection
engine = create_engine(
    'postgresql://postgres@localhost:5435/main',
    poolclass=StaticPool,  # Use same connection for all sessions
    echo=False  # We're using logging instead
)

# Log connection events
@event.listens_for(engine, "connect")
def receive_connect(dbapi_conn, connection_record):
    print(f"CONNECT event: {id(dbapi_conn)}")

@event.listens_for(engine, "checkout")
def receive_checkout(dbapi_conn, connection_record, connection_proxy):
    print(f"CHECKOUT event: {id(dbapi_conn)}")

# Create test table
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS test_sqlalchemy"))
    conn.execute(text("CREATE TABLE test_sqlalchemy (id INTEGER PRIMARY KEY, value TEXT)"))
    conn.execute(text("INSERT INTO test_sqlalchemy (id, value) VALUES (1, 'initial')"))
    conn.commit()

Session = sessionmaker(bind=engine)

print("\n=== Test 1: Basic Update ===")
session1 = Session()
print(f"Session 1 ID: {id(session1)}")

# Check initial value
result = session1.execute(text("SELECT value FROM test_sqlalchemy WHERE id = 1")).scalar()
print(f"Initial value: {result}")

# Update
session1.execute(text("UPDATE test_sqlalchemy SET value = 'updated' WHERE id = 1"))
print("Update executed")

# Check before commit
result = session1.execute(text("SELECT value FROM test_sqlalchemy WHERE id = 1")).scalar()
print(f"Value before commit (same session): {result}")

# Commit
session1.commit()
print("Committed")

# Check after commit in same session
result = session1.execute(text("SELECT value FROM test_sqlalchemy WHERE id = 1")).scalar()
print(f"Value after commit (same session): {result}")

# Close session
session1.close()

print("\n=== Test 2: New Session ===")
session2 = Session()
print(f"Session 2 ID: {id(session2)}")

# Check in new session
result = session2.execute(text("SELECT value FROM test_sqlalchemy WHERE id = 1")).scalar()
print(f"Value in new session: {result}")

session2.close()

print("\n=== Test 3: Direct Connection Check ===")
# Check directly with psycopg2
conn = psycopg2.connect(
    host='localhost',
    port=5435,
    dbname='main',
    user='postgres'
)
cur = conn.cursor()
cur.execute("SELECT value FROM test_sqlalchemy WHERE id = 1")
result = cur.fetchone()
print(f"Direct psycopg2 result: {result[0]}")
conn.close()

print("\n=== Test 4: Raw SQLite Check ===")
import sqlite3
conn = sqlite3.connect('main.db')
cur = conn.cursor()
cur.execute("SELECT value FROM test_sqlalchemy WHERE id = 1")
result = cur.fetchone()
print(f"Raw SQLite result: {result[0] if result else 'No row found'}")
conn.close()