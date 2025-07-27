#!/usr/bin/env python3
"""Debug SQLAlchemy transaction issues"""

import psycopg2
import sqlite3

print("=== Direct SQLite Test ===")
# First, let's test directly with SQLite to understand the expected behavior
conn = sqlite3.connect('main.db')
cur = conn.cursor()

# Check autocommit status
print(f"SQLite isolation level: {conn.isolation_level}")
print(f"SQLite in transaction: {conn.in_transaction}")

# Create a test table
cur.execute("DROP TABLE IF EXISTS test_tx")
cur.execute("CREATE TABLE test_tx (id INTEGER PRIMARY KEY, value TEXT)")
cur.execute("INSERT INTO test_tx (value) VALUES ('initial')")
conn.commit()

# Update in a transaction
cur.execute("BEGIN")
print(f"After BEGIN - in transaction: {conn.in_transaction}")
cur.execute("UPDATE test_tx SET value = 'updated' WHERE id = 1")
print(f"After UPDATE - in transaction: {conn.in_transaction}")
conn.commit()
print(f"After COMMIT - in transaction: {conn.in_transaction}")

# Verify the update
cur.execute("SELECT value FROM test_tx WHERE id = 1")
print(f"Direct SQLite result: {cur.fetchone()[0]}")

conn.close()

print("\n=== PostgreSQL Protocol Test ===")
# Now test through pgsqlite
conn = psycopg2.connect(
    host='localhost',
    port=5435,
    dbname='main',
    user='postgres'
)

cur = conn.cursor()

# Check transaction status
print(f"psycopg2 autocommit: {conn.autocommit}")
print(f"psycopg2 status: {conn.status}")
print(f"psycopg2 transaction status: {conn.get_transaction_status()}")

# Create table if not exists
cur.execute("DROP TABLE IF EXISTS test_tx2")
cur.execute("CREATE TABLE test_tx2 (id INTEGER PRIMARY KEY, value TEXT)")
cur.execute("INSERT INTO test_tx2 (value) VALUES ('initial')")
conn.commit()

print(f"\nAfter initial commit - transaction status: {conn.get_transaction_status()}")

# Update with explicit transaction
cur.execute("UPDATE test_tx2 SET value = 'updated' WHERE id = 1")
print(f"After UPDATE - transaction status: {conn.get_transaction_status()}")

conn.commit()
print(f"After COMMIT - transaction status: {conn.get_transaction_status()}")

# Create new cursor to simulate new session
cur2 = conn.cursor()
cur2.execute("SELECT value FROM test_tx2 WHERE id = 1")
result = cur2.fetchone()
print(f"Same connection, new cursor result: {result[0]}")

# Close and reopen connection
conn.close()

conn2 = psycopg2.connect(
    host='localhost',
    port=5435,
    dbname='main',
    user='postgres'
)
cur3 = conn2.cursor()
cur3.execute("SELECT value FROM test_tx2 WHERE id = 1")
result = cur3.fetchone()
print(f"New connection result: {result[0]}")

conn2.close()

print("\n=== Direct SQLite Check ===")
# Check what SQLite sees
conn = sqlite3.connect('main.db')
cur = conn.cursor()
cur.execute("SELECT value FROM test_tx2 WHERE id = 1")
result = cur.fetchone()
print(f"Direct SQLite sees: {result[0] if result else 'No row found'}")
conn.close()