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
cur1.execute("DROP TABLE IF EXISTS users")
cur1.execute("""
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
cur1.execute("INSERT INTO users (name, created_at) VALUES (%s, %s)", 
            ("Test User", "2025-08-05 12:34:56"))
print("Connection 1: Inserted data")

# Now create a second connection
conn2 = psycopg.connect(
    "postgresql://postgres@localhost:15501/test",
    options="-c client_encoding=UTF8",
    autocommit=True
)
cur2 = conn2.cursor()

# Try to read from connection 2
cur2.execute("SELECT id, name, created_at FROM users WHERE id = %s", (1,))
result = cur2.fetchone()
print(f"Connection 2: Result = {result}")

if result:
    print(f"  created_at type: {type(result[2])}")
    print(f"  created_at value: {result[2]}")
else:
    print("  No data found!")

# Clean up
conn1.close()
conn2.close()