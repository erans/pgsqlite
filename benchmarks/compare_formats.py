#!/usr/bin/env python3
"""Compare text vs binary format performance with Unix sockets"""

import time
import psycopg2
import psycopg
import sys

# Configuration
SOCKET_DIR = "/tmp"
PORT = 5434
ITERATIONS = 100

print("=== Performance Comparison: Text vs Binary Format ===")
print(f"Using Unix socket at {SOCKET_DIR}/.s.PGSQL.{PORT}")
print(f"Running {ITERATIONS} operations each\n")

# Test 1: Text format with psycopg2
print("1. TEXT FORMAT (psycopg2)")
print("-" * 40)

try:
    conn = psycopg2.connect(
        host=SOCKET_DIR,
        port=PORT,
        dbname=":memory:",
        user="dummy",
        sslmode="disable"
    )
    cursor = conn.cursor()
    
    # Create table
    cursor.execute("DROP TABLE IF EXISTS text_test")
    cursor.execute("CREATE TABLE text_test (id INTEGER PRIMARY KEY, value INTEGER)")
    conn.commit()
    
    # Measure INSERT
    start = time.perf_counter()
    for i in range(ITERATIONS):
        cursor.execute("INSERT INTO text_test (id, value) VALUES (%s, %s)", (i, i * 10))
    conn.commit()
    text_insert = (time.perf_counter() - start) / ITERATIONS * 1000
    print(f"INSERT: {text_insert:.3f} ms/op")
    
    # Measure UPDATE
    start = time.perf_counter()
    for i in range(ITERATIONS):
        cursor.execute("UPDATE text_test SET value = %s WHERE id = %s", (i * 20, i))
    conn.commit()
    text_update = (time.perf_counter() - start) / ITERATIONS * 1000
    print(f"UPDATE: {text_update:.3f} ms/op")
    
    # Measure SELECT
    start = time.perf_counter()
    for i in range(ITERATIONS):
        cursor.execute("SELECT * FROM text_test WHERE id = %s", (i,))
        cursor.fetchone()
    text_select = (time.perf_counter() - start) / ITERATIONS * 1000
    print(f"SELECT: {text_select:.3f} ms/op")
    
    # Measure DELETE
    start = time.perf_counter()
    for i in range(ITERATIONS):
        cursor.execute("DELETE FROM text_test WHERE id = %s", (i,))
    conn.commit()
    text_delete = (time.perf_counter() - start) / ITERATIONS * 1000
    print(f"DELETE: {text_delete:.3f} ms/op")
    
    conn.close()
    
except Exception as e:
    print(f"Error with text format: {e}")
    sys.exit(1)

# Test 2: Binary format with psycopg3
print("\n2. BINARY FORMAT (psycopg3)")
print("-" * 40)

try:
    # Use autocommit to avoid transaction issues
    conn = psycopg.connect(
        host=SOCKET_DIR,
        port=PORT,
        dbname=":memory:",
        user="dummy",
        sslmode="disable",
        autocommit=True
    )
    
    # Create table with regular cursor
    with conn.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS binary_test")
        cursor.execute("CREATE TABLE binary_test (id INTEGER PRIMARY KEY, value INTEGER)")
    
    # Measure INSERT with binary cursor
    start = time.perf_counter()
    with conn.cursor(binary=True) as cursor:
        for i in range(ITERATIONS):
            cursor.execute("INSERT INTO binary_test (id, value) VALUES (%s, %s)", (i, i * 10))
    binary_insert = (time.perf_counter() - start) / ITERATIONS * 1000
    print(f"INSERT: {binary_insert:.3f} ms/op")
    
    # Measure UPDATE with binary cursor
    start = time.perf_counter()
    with conn.cursor(binary=True) as cursor:
        for i in range(ITERATIONS):
            cursor.execute("UPDATE binary_test SET value = %s WHERE id = %s", (i * 20, i))
    binary_update = (time.perf_counter() - start) / ITERATIONS * 1000
    print(f"UPDATE: {binary_update:.3f} ms/op")
    
    # Measure SELECT with binary cursor
    start = time.perf_counter()
    with conn.cursor(binary=True) as cursor:
        for i in range(ITERATIONS):
            cursor.execute("SELECT * FROM binary_test WHERE id = %s", (i,))
            cursor.fetchone()
    binary_select = (time.perf_counter() - start) / ITERATIONS * 1000
    print(f"SELECT: {binary_select:.3f} ms/op")
    
    # Measure DELETE with binary cursor
    start = time.perf_counter()
    with conn.cursor(binary=True) as cursor:
        for i in range(ITERATIONS):
            cursor.execute("DELETE FROM binary_test WHERE id = %s", (i,))
    binary_delete = (time.perf_counter() - start) / ITERATIONS * 1000
    print(f"DELETE: {binary_delete:.3f} ms/op")
    
    conn.close()
    
except Exception as e:
    print(f"Error with binary format: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Summary
print("\n3. PERFORMANCE COMPARISON")
print("-" * 40)
print(f"INSERT: Text={text_insert:.3f}ms, Binary={binary_insert:.3f}ms, Diff={((text_insert/binary_insert - 1) * 100):+.1f}%")
print(f"UPDATE: Text={text_update:.3f}ms, Binary={binary_update:.3f}ms, Diff={((text_update/binary_update - 1) * 100):+.1f}%")
print(f"SELECT: Text={text_select:.3f}ms, Binary={binary_select:.3f}ms, Diff={((text_select/binary_select - 1) * 100):+.1f}%")
print(f"DELETE: Text={text_delete:.3f}ms, Binary={binary_delete:.3f}ms, Diff={((text_delete/binary_delete - 1) * 100):+.1f}%")

print("\n4. VERDICT")
print("-" * 40)
if binary_insert < text_insert * 1.2 and binary_update < text_update * 1.2:
    print("✅ DML operations: Binary format performance is GOOD!")
else:
    print("❌ DML operations: Binary format still has regression")
    
if binary_select < text_select * 1.2:
    print("✅ SELECT operations: Binary format performance is GOOD!")
else:
    print("❌ SELECT operations: Binary format has regression")