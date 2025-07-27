#!/usr/bin/env python3
"""Mimic the exact SQLAlchemy pattern that's failing"""

import psycopg2

print("=== Mimicking SQLAlchemy Pattern ===")

# Setup
conn1 = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
conn1.autocommit = False
cur1 = conn1.cursor()

# Create test table and data
cur1.execute("DROP TABLE IF EXISTS sqlalchemy_mimic")
cur1.execute("CREATE TABLE sqlalchemy_mimic (id INTEGER PRIMARY KEY, value TEXT)")
cur1.execute("INSERT INTO sqlalchemy_mimic (value) VALUES ('original')")
conn1.commit()

# Step 1: Update (like SQLAlchemy ORM does)
print("\n--- Step 1: Update ---")
cur1.execute("UPDATE sqlalchemy_mimic SET value = 'updated' WHERE id = 1")
print("UPDATE executed")

# Step 2: Commit (like SQLAlchemy session.commit())
print("\n--- Step 2: Commit ---")
conn1.commit()
print("COMMIT executed")

# Step 3: Query in same connection (like SQLAlchemy checking the object)
print("\n--- Step 3: Query same connection ---")
cur1.execute("SELECT value FROM sqlalchemy_mimic WHERE id = 1")
result = cur1.fetchone()
print(f"Same connection sees: {result[0]}")

# Step 4: Close connection (this might trigger rollback)
print("\n--- Step 4: Close connection ---")
cur1.close()
conn1.close()
print("Connection closed")

# Step 5: New connection (like new SQLAlchemy session)
print("\n--- Step 5: New connection ---")
conn2 = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
conn2.autocommit = False
cur2 = conn2.cursor()

cur2.execute("SELECT value FROM sqlalchemy_mimic WHERE id = 1")
result = cur2.fetchone()
print(f"New connection sees: {result[0]}")

if result[0] == 'updated':
    print("✅ Update persisted correctly")
else:
    print("❌ Update was lost!")

cur2.close()
conn2.close()

print("\n=== Test completed ===")