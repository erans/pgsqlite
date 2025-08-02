#!/usr/bin/env python3
import psycopg2
import time

print("Connecting to pgsqlite...")
conn = psycopg2.connect(
    host="localhost",
    port=5433,
    dbname="benchmark_test.db",
    user="dummy",
    password="dummy",
    sslmode="disable"
)
print("Connected!")

cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS simple_test (id INTEGER PRIMARY KEY, value INTEGER)")
conn.commit()

# Test INSERT
print("Testing INSERT...")
start = time.time()
cur.execute("INSERT INTO simple_test (id, value) VALUES (1, 42)")
conn.commit()
print(f"INSERT took: {(time.time() - start) * 1000:.2f}ms")

# Test SELECT  
print("Testing SELECT...")
start = time.time()
cur.execute("SELECT * FROM simple_test WHERE id = 1")
result = cur.fetchone()
print(f"SELECT took: {(time.time() - start) * 1000:.2f}ms, result: {result}")

conn.close()
print("Test completed!")