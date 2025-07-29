#!/usr/bin/env python3
"""Mimic exact SQLAlchemy transaction pattern"""

import psycopg2
import sys

if len(sys.argv) < 2:
    print("Usage: test_sqlalchemy_mimic_exact.py <port>")
    sys.exit(1)

port = int(sys.argv[1])

# Connect to pgsqlite
conn = psycopg2.connect(
    host="localhost",
    port=port,
    database="main",
    user="postgres"
)
conn.autocommit = False  # SQLAlchemy uses explicit transaction management

print("=== Creating table ===")
cur = conn.cursor()
cur.execute("BEGIN")
cur.execute("""
    CREATE TABLE IF NOT EXISTS test_users (
        id SERIAL PRIMARY KEY,
        name TEXT
    )
""")
cur.execute("COMMIT")
cur.close()

print("\n=== Transaction 1: Insert ===")
cur = conn.cursor()
cur.execute("BEGIN")
cur.execute("INSERT INTO test_users (name) VALUES ('Original Name') RETURNING id")
user_id = cur.fetchone()[0]
print(f"Inserted user with id: {user_id}")
cur.execute("COMMIT")
cur.close()

print("\n=== Transaction 2: Update ===")
cur = conn.cursor()
cur.execute("BEGIN")
cur.execute(f"UPDATE test_users SET name = 'Updated Name' WHERE id = {user_id}")
print("Updated user name")
cur.execute("COMMIT")
cur.close()

print("\n=== Transaction 3: Check value (same connection) ===")
cur = conn.cursor()
cur.execute("BEGIN")
cur.execute(f"SELECT name FROM test_users WHERE id = {user_id}")
result = cur.fetchone()
print(f"Same connection sees: {result[0]}")
cur.execute("ROLLBACK")  # SQLAlchemy often uses ROLLBACK for read-only transactions
cur.close()

print("\n=== New connection check ===")
# Create a new connection
conn2 = psycopg2.connect(
    host="localhost",
    port=port,
    database="main",
    user="postgres"
)
cur2 = conn2.cursor()
cur2.execute(f"SELECT name FROM test_users WHERE id = {user_id}")
result = cur2.fetchone()
print(f"New connection sees: {result[0]}")
cur2.close()
conn2.close()

# Cleanup
conn.close()