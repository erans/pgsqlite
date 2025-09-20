#!/usr/bin/env python3
"""Simple test for array binary encoding."""

import psycopg
import subprocess
import time

# Start pgsqlite server
print("Starting pgsqlite server...")
server = subprocess.Popen(
    ["cargo", "run", "--bin", "pgsqlite", "--", "--database", "/tmp/simple_test.db", "--port", "5433"],
    env={**subprocess.os.environ, "RUST_LOG": "debug"},
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    cwd="/home/eran/work/pgsqlite"
)

# Give server time to start
time.sleep(5)

try:
    # Connect with psycopg3
    print("\nConnecting with binary cursor...")
    conn = psycopg.connect(
        host="127.0.0.1",
        port=5433,
        user="postgres",
        dbname="test"
    )
    cur = conn.cursor(binary=True)

    # Create table
    print("Creating table...")
    cur.execute("""
        CREATE TABLE simple_test (
            id INTEGER PRIMARY KEY,
            int_array INTEGER[]
        )
    """)
    conn.commit()

    # Insert array with PostgreSQL format
    print("\nInserting array [1, 2, 3]...")
    cur.execute(
        "INSERT INTO simple_test (id, int_array) VALUES (%s, %s)",
        (1, [1, 2, 3])
    )
    conn.commit()

    # Select with binary protocol
    print("Selecting array...")
    cur.execute("SELECT int_array FROM simple_test WHERE id = 1")
    result = cur.fetchone()[0]
    print(f"Result: {result}")
    print(f"✅ Test passed!")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Clean up
    if 'conn' in locals():
        conn.close()
    
    # Stop server and capture output
    print("\nStopping server...")
    server.terminate()
    output, _ = server.communicate(timeout=2)
    
    # Look for our debug messages
    print("\nServer debug output:")
    for line in output.split('\n'):
        if 'encode_array' in line or 'After conversion' in line:
            print(line)