#!/usr/bin/env python3
import psycopg
from datetime import datetime

# Connect with autocommit to pgsqlite 
conn = psycopg.connect(
    "postgresql://postgres@localhost:15501/test",
    options="-c client_encoding=UTF8",
    autocommit=True
)
cur = conn.cursor()

# Create table with datetime column
cur.execute("DROP TABLE IF EXISTS users")
cur.execute("""
    CREATE TABLE users (
        id INTEGER PRIMARY KEY,
        name TEXT,
        created_at TIMESTAMP
    )
""")

# Insert with datetime value
dt = datetime(2025, 8, 5, 12, 34, 56, 123456)
cur.execute("INSERT INTO users (id, name, created_at) VALUES (%s, %s, %s)", 
            (1, "Test User", dt))
print(f"Inserted datetime: {dt}")

# Query back with parameter
cur.execute("SELECT id, name, created_at FROM users WHERE id = %s", (1,))
result = cur.fetchone()
print(f"Retrieved: {result}")
if result:
    print(f"  created_at type: {type(result[2])}")
    print(f"  created_at value: {result[2]}")

# Close connection
conn.close()