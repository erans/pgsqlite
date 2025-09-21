#!/usr/bin/env python3
import psycopg
import time

# Test pgsqlite binary mode performance
conn = psycopg.connect("host=localhost port=45000 dbname=test_overhead.db user=dummy password=dummy sslmode=disable")
# Binary mode (default for psycopg3)
cursor = conn.cursor()

start = time.perf_counter()
cursor.execute("CREATE TABLE IF NOT EXISTS test_pg_bin (id SERIAL PRIMARY KEY, name TEXT, value INTEGER)")
for i in range(100):
    cursor.execute("INSERT INTO test_pg_bin (name, value) VALUES (%s, %s) RETURNING id", (f"item{i}", i))
    cursor.fetchone()
for i in range(100):
    cursor.execute("SELECT * FROM test_pg_bin WHERE value > %s", (i // 2,))
    rows = cursor.fetchall()
conn.commit()
end = time.perf_counter()

print(f"pgsqlite binary mode total time: {(end-start)*1000:.3f}ms")
conn.close()