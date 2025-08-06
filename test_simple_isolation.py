#!/usr/bin/env python3
import psycopg
import time

# Connect with autocommit to pgsqlite 
conn1 = psycopg.connect(
    "postgresql://postgres@localhost:15501/test",
    options="-c client_encoding=UTF8",
    autocommit=True
)
cur1 = conn1.cursor()

# Create table and insert data in connection 1
cur1.execute("DROP TABLE IF EXISTS test_table")
cur1.execute("""
    CREATE TABLE test_table (
        id INTEGER PRIMARY KEY,
        name TEXT
    )
""")
cur1.execute("INSERT INTO test_table (id, name) VALUES (1, 'Test')")
print("Connection 1: Inserted data")

# Query from the same connection
cur1.execute("SELECT * FROM test_table")
result = cur1.fetchone()
print(f"Connection 1: SELECT * = {result}")

# Now create a second connection
conn2 = psycopg.connect(
    "postgresql://postgres@localhost:15501/test",
    options="-c client_encoding=UTF8",
    autocommit=True
)
cur2 = conn2.cursor()

# Try different queries from connection 2
cur2.execute("SELECT * FROM test_table")
result = cur2.fetchone()
print(f"Connection 2: SELECT * = {result}")

# Try with WHERE clause but no parameters
cur2.execute("SELECT * FROM test_table WHERE id = 1")
result = cur2.fetchone()
print(f"Connection 2: SELECT with WHERE = {result}")

# Try with parameterized query
cur2.execute("SELECT * FROM test_table WHERE id = %s", (1,))
result = cur2.fetchone()
print(f"Connection 2: SELECT with param = {result}")

# Clean up
conn1.close()
conn2.close()