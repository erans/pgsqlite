#!/usr/bin/env python3
import psycopg

# Connect to pgsqlite
conn = psycopg.connect("postgresql://postgres@localhost:15501/test")
cur = conn.cursor()

# Create a simple table with timestamp
cur.execute("CREATE TABLE test_timestamps (id INTEGER PRIMARY KEY, created_at TIMESTAMP)")

# Insert data with timestamp
cur.execute("INSERT INTO test_timestamps (id, created_at) VALUES (1, '2025-08-05 10:30:45')")

# Select with parameters (should hit fast path)
cur.execute("SELECT * FROM test_timestamps WHERE id = %s", (1,))
result = cur.fetchone()
print(f"Result: {result}")

conn.close()