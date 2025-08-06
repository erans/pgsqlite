#!/usr/bin/env python3
import psycopg
from datetime import datetime

# Connect to pgsqlite
conn = psycopg.connect(
    "postgresql://postgres@localhost:15432/main",
    options="-c client_encoding=UTF8"
)

try:
    with conn.cursor() as cur:
        # Create a table with timestamp
        cur.execute("DROP TABLE IF EXISTS test_timestamps")
        cur.execute("""
            CREATE TABLE test_timestamps (
                id INTEGER PRIMARY KEY,
                created_at TIMESTAMP NOT NULL
            )
        """)
        
        # Insert with current timestamp
        now = datetime.now()
        print(f"Inserting timestamp: {now}")
        
        cur.execute(
            "INSERT INTO test_timestamps (id, created_at) VALUES (%s, %s)",
            (1, now)
        )
        
        # Query it back
        cur.execute("SELECT id, created_at FROM test_timestamps WHERE id = %s", (1,))
        row = cur.fetchone()
        print(f"Retrieved: id={row[0]}, created_at={row[1]} (type: {type(row[1])})")
        
        # Try another query without parameters
        cur.execute("SELECT * FROM test_timestamps")
        row = cur.fetchone()
        print(f"Retrieved (no params): id={row[0]}, created_at={row[1]} (type: {type(row[1])})")
        
        conn.commit()
finally:
    conn.close()