#!/usr/bin/env python3
import sqlite3
import time

# Test pure SQLite performance
conn = sqlite3.connect("test_overhead.db")
cursor = conn.cursor()

start = time.perf_counter()
cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)")
for i in range(100):
    cursor.execute("INSERT INTO test (name, value) VALUES (?, ?)", (f"item{i}", i))
for i in range(100):
    cursor.execute("SELECT * FROM test WHERE value > ?", (i // 2,))
    rows = cursor.fetchall()
conn.commit()
end = time.perf_counter()

print(f"SQLite total time: {(end-start)*1000:.3f}ms")
conn.close()