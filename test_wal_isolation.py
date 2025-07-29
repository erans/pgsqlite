#!/usr/bin/env python3
"""Test WAL mode isolation issue with SQLite"""

import sqlite3
import os

# Clean up any existing database
db_path = "test_wal_isolation.db"
for ext in ["", "-shm", "-wal"]:
    if os.path.exists(db_path + ext):
        os.remove(db_path + ext)

# Create two connections
conn1 = sqlite3.connect(db_path)
conn1.execute("PRAGMA journal_mode=WAL")
conn1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
conn1.commit()

# First connection inserts data
print("=== Connection 1: Insert and commit ===")
conn1.execute("INSERT INTO users (name) VALUES ('Original Name')")
conn1.commit()
print("Committed INSERT")

# Second connection should see the data
conn2 = sqlite3.connect(db_path)
conn2.execute("PRAGMA journal_mode=WAL")
result = conn2.execute("SELECT name FROM users WHERE id = 1").fetchone()
print(f"Connection 2 sees: {result[0] if result else 'No data'}")

# First connection updates
print("\n=== Connection 1: Update and commit ===")
conn1.execute("UPDATE users SET name = 'Updated Name' WHERE id = 1")
conn1.commit()
print("Committed UPDATE")

# Force WAL checkpoint
conn1.execute("PRAGMA wal_checkpoint(RESTART)")
print("WAL checkpoint executed")

# Connection 2 should see updated data
result = conn2.execute("SELECT name FROM users WHERE id = 1").fetchone()
print(f"Connection 2 sees (before refresh): {result[0] if result else 'No data'}")

# Force connection 2 to refresh by starting a new transaction
conn2.execute("BEGIN")
conn2.rollback()
result = conn2.execute("SELECT name FROM users WHERE id = 1").fetchone()
print(f"Connection 2 sees (after refresh): {result[0] if result else 'No data'}")

# Test ROLLBACK behavior
print("\n=== Testing ROLLBACK after committed changes ===")
conn1.execute("BEGIN")
result = conn1.execute("SELECT name FROM users WHERE id = 1").fetchone()
print(f"Connection 1 in transaction sees: {result[0] if result else 'No data'}")
conn1.rollback()
result = conn1.execute("SELECT name FROM users WHERE id = 1").fetchone()
print(f"Connection 1 after ROLLBACK sees: {result[0] if result else 'No data'}")

conn1.close()
conn2.close()