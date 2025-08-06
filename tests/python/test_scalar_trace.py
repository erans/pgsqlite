import psycopg
from datetime import datetime
import sys

# Enable tracing
import logging
logging.basicConfig(level=logging.DEBUG)

conn = psycopg.connect("host=localhost port=15432 dbname=main user=postgres", autocommit=True)

with conn.cursor() as cur:
    cur.execute('DROP TABLE IF EXISTS test_s')
    cur.execute('CREATE TABLE test_s (id INTEGER PRIMARY KEY, created_at TIMESTAMP NOT NULL)')
    cur.execute('INSERT INTO test_s (created_at) VALUES (%s)', (datetime.now(),))
    
    # Force a sync point
    conn.pgconn.flush()
    
    print("About to execute SELECT...")
    # Execute SELECT
    cur.execute("SELECT (SELECT MAX(created_at) FROM test_s) as max_created")
    print("SELECT executed")
    
    result = cur.fetchone()
    print(f"Result: {result[0]}")

conn.close()
