#!/usr/bin/env python3
"""Test if shared connection is causing the issue"""

import sqlite3
import os

# Clean up any existing database
db_path = "test_shared_conn.db"
for ext in ["", "-shm", "-wal"]:
    if os.path.exists(db_path + ext):
        os.remove(db_path + ext)

# Simulate pgsqlite's single shared connection approach
shared_conn = sqlite3.connect(db_path)
shared_conn.execute("PRAGMA journal_mode=WAL")
shared_conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")

# Transaction 1: Insert
print("=== Transaction 1: Insert ===")
shared_conn.execute("BEGIN")
shared_conn.execute("INSERT INTO users (name) VALUES ('Original Name')")
shared_conn.execute("COMMIT")
print("Committed INSERT")

# Transaction 2: Update
print("\n=== Transaction 2: Update ===")
shared_conn.execute("BEGIN")
shared_conn.execute("UPDATE users SET name = 'Updated Name' WHERE id = 1")
shared_conn.execute("COMMIT")
print("Committed UPDATE")

# Force WAL checkpoint
shared_conn.execute("PRAGMA wal_checkpoint(RESTART)")
print("WAL checkpoint executed")

# Try to refresh connection state
shared_conn.execute("BEGIN IMMEDIATE")
shared_conn.execute("ROLLBACK")
print("Connection state refresh attempted")

# Check what the same connection sees
result = shared_conn.execute("SELECT name FROM users WHERE id = 1").fetchone()
print(f"\nShared connection sees: {result[0] if result else 'No data'}")

# Now test with explicit isolation level reset
print("\n=== Testing with isolation level reset ===")
shared_conn.isolation_level = None  # Reset to autocommit
shared_conn.isolation_level = ""    # Back to default
result = shared_conn.execute("SELECT name FROM users WHERE id = 1").fetchone()
print(f"After isolation reset: {result[0] if result else 'No data'}")

shared_conn.close()