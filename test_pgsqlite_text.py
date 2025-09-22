#!/usr/bin/env python3
import psycopg
import time

# Test pgsqlite text mode performance
conn = psycopg.connect("host=localhost port=45000 dbname=test_overhead.db user=dummy password=dummy sslmode=disable")
# Force text mode
conn.prepare_threshold = None
cursor = conn.cursor()

start = time.perf_counter()
cursor.execute("CREATE TABLE IF NOT EXISTS test_pg (id SERIAL PRIMARY KEY, name TEXT, value INTEGER)")
for i in range(100):
    cursor.execute("INSERT INTO test_pg (name, value) VALUES (%s, %s) RETURNING id", (f"item{i}", i))
    cursor.fetchone()
for i in range(100):
    cursor.execute("SELECT * FROM test_pg WHERE value > %s", (i // 2,))
    rows = cursor.fetchall()
conn.commit()
end = time.perf_counter()

print(f"pgsqlite text mode total time: {(end-start)*1000:.3f}ms")
conn.close()