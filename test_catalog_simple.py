#!/usr/bin/env python3
"""Simple test for catalog query issue"""

import psycopg
import sys

# Connect to pgsqlite
conn = psycopg.connect(
    host="localhost",
    port=15432,
    dbname="main",
    user="postgres",
    password="dummy",
    options="-c client_encoding=UTF8"
)

# Create a test table first
try:
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)")
        conn.commit()
        print("Created test table")
except Exception as e:
    print(f"Error creating table: {e}")
    conn.rollback()

# Now test the catalog query that SQLAlchemy uses
query = """
SELECT 1 FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'test_table'
"""

try:
    with conn.cursor() as cur:
        cur.execute(query)
        result = cur.fetchall()
        print(f"Catalog query succeeded: {result}")
except Exception as e:
    print(f"Error with catalog query: {e}")

conn.close()