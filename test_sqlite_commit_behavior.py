#!/usr/bin/env python3
"""Test SQLite commit behavior directly"""

import sqlite3

print("=== Testing SQLite COMMIT behavior ===")

# Test 1: Basic SQLite behavior
print("\nTest 1: Direct SQLite")
conn = sqlite3.connect('main.db')

# Check initial isolation level
print(f"Initial isolation level: {conn.isolation_level}")

conn.execute("DROP TABLE IF EXISTS commit_test")
conn.execute("CREATE TABLE commit_test (id INTEGER PRIMARY KEY, value TEXT)")
conn.execute("INSERT INTO commit_test (value) VALUES ('original')")
conn.commit()

# Start transaction and update
conn.execute("BEGIN")
print("Transaction started")
conn.execute("UPDATE commit_test SET value = 'updated' WHERE id = 1")
print("UPDATE executed")

# Check if we're in a transaction
print(f"In transaction: {conn.in_transaction}")

# Commit 
conn.execute("COMMIT")
print("COMMIT executed")
print(f"In transaction after COMMIT: {conn.in_transaction}")

# Check result
result = conn.execute("SELECT value FROM commit_test WHERE id = 1").fetchone()
print(f"Result after COMMIT: {result[0]}")

# Try to rollback (this should fail or be ignored)
try:
    conn.execute("ROLLBACK")
    print("ROLLBACK executed (unexpected!)")
except Exception as e:
    print(f"ROLLBACK failed as expected: {e}")

# Check result again
result = conn.execute("SELECT value FROM commit_test WHERE id = 1").fetchone()
print(f"Result after ROLLBACK attempt: {result[0]}")

conn.close()

# Test 2: New connection
print("\nTest 2: New SQLite connection")
conn2 = sqlite3.connect('main.db')
result = conn2.execute("SELECT value FROM commit_test WHERE id = 1").fetchone()
print(f"New connection sees: {result[0]}")
conn2.close()

print("\n=== Test completed ===")