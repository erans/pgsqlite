#!/usr/bin/env python3
"""Quick test of text vs binary format performance"""

import psycopg2
import psycopg
import time
import os

# Start server
print("Starting server...")
os.system("cd /home/eran/work/pgsqlite && ./target/release/pgsqlite --database test.db --port 5442 >/dev/null 2>&1 &")
time.sleep(3)

try:
    # Test text format
    print("\nTesting TEXT format (psycopg2)...")
    conn1 = psycopg2.connect(host="/tmp", port=5442, dbname="test.db", user="dummy")
    cur1 = conn1.cursor()
    
    cur1.execute("DROP TABLE IF EXISTS text_test")
    cur1.execute("CREATE TABLE text_test (id INTEGER PRIMARY KEY, val INTEGER)")
    conn1.commit()
    
    # Measure 10 INSERTs
    start = time.perf_counter()
    for i in range(10):
        cur1.execute("INSERT INTO text_test VALUES (%s, %s)", (i, i*10))
    conn1.commit()
    text_time = (time.perf_counter() - start) / 10 * 1000
    
    conn1.close()
    print(f"Text INSERT avg: {text_time:.3f} ms")
    
    # Test binary format
    print("\nTesting BINARY format (psycopg3)...")
    conn2 = psycopg.connect(host="/tmp", port=5442, dbname="test.db", user="dummy")
    
    with conn2.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS binary_test")
        cur.execute("CREATE TABLE binary_test (id INTEGER PRIMARY KEY, val INTEGER)")
    conn2.commit()
    
    # Measure 10 INSERTs with binary
    start = time.perf_counter()
    for i in range(10):
        with conn2.cursor(binary=True) as cur:
            cur.execute("INSERT INTO binary_test VALUES (%s, %s)", (i, i*10))
        conn2.commit()
    binary_time = (time.perf_counter() - start) / 10 * 1000
    
    conn2.close()
    print(f"Binary INSERT avg: {binary_time:.3f} ms")
    
    # Results
    print(f"\nRESULT: Binary is {(binary_time/text_time - 1)*100:+.1f}% vs Text")
    if binary_time < text_time * 1.5:
        print("✅ Binary format DML performance is FIXED!")
    else:
        print("❌ Binary format still has regression")
        
finally:
    os.system("pkill -9 pgsqlite")
    os.system("rm -f test.db")