#!/usr/bin/env python3
"""Test psycopg3 with binary arrays containing NULLs against pgsqlite."""

import psycopg
import subprocess
import time
import os
import tempfile
import signal

def test_arrays():
    # Create temp database
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "test_psycopg3.db")

    # Start pgsqlite server
    print("Starting pgsqlite server...")
    server = subprocess.Popen(
        ["cargo", "run", "--bin", "pgsqlite", "--", "--database", db_path, "--port", "5433"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd="/home/eran/work/pgsqlite"
    )

    # Give server time to start
    time.sleep(5)

    try:
        # Connect with psycopg3
        print("Connecting with psycopg3...")
        conn = psycopg.connect(
            host="127.0.0.1",
            port=5433,
            user="postgres",
            dbname="test"
        )
        # Create binary cursor
        cur = conn.cursor(binary=True)  # Force binary protocol for this cursor

        # Test 1: Create table with array column
        print("\nTest 1: Creating table with INTEGER[] column...")
        cur.execute("""
            CREATE TABLE test_arrays (
                id INTEGER PRIMARY KEY,
                int_array INTEGER[]
            )
        """)
        conn.commit()

        # Test 2: Insert array without NULLs
        print("\nTest 2: Inserting array without NULLs [1, 2, 3]...")
        cur.execute(
            "INSERT INTO test_arrays (id, int_array) VALUES (%s, %s)",
            (1, [1, 2, 3])
        )
        conn.commit()

        # Test 3: Select array without NULLs (binary)
        print("\nTest 3: Selecting array without NULLs (binary protocol)...")
        cur.execute("SELECT int_array FROM test_arrays WHERE id = 1")
        result = cur.fetchone()[0]
        print(f"  Result: {result}")
        assert result == [1, 2, 3], f"Expected [1, 2, 3], got {result}"
        print("  ✅ Non-NULL array works with binary protocol!")

        # Test 4: Insert array WITH NULLs
        print("\nTest 4: Inserting array with NULLs [1, None, 3]...")
        cur.execute(
            "INSERT INTO test_arrays (id, int_array) VALUES (%s, %s)",
            (2, [1, None, 3])
        )
        conn.commit()

        # Test 5: Select array with NULLs (binary)
        print("\nTest 5: Selecting array with NULLs (binary protocol)...")
        cur.execute("SELECT int_array FROM test_arrays WHERE id = 2")
        result = cur.fetchone()[0]
        print(f"  Result: {result}")
        assert result == [1, None, 3], f"Expected [1, None, 3], got {result}"
        print("  ✅ NULL array works with binary protocol!")

        # Test 6: Empty array
        print("\nTest 6: Testing empty array...")
        cur.execute(
            "INSERT INTO test_arrays (id, int_array) VALUES (%s, %s)",
            (3, [])
        )
        conn.commit()

        cur.execute("SELECT int_array FROM test_arrays WHERE id = 3")
        result = cur.fetchone()[0]
        print(f"  Result: {result}")
        assert result == [], f"Expected [], got {result}"
        print("  ✅ Empty array works with binary protocol!")

        # Test 7: Array with all NULLs
        print("\nTest 7: Testing array with all NULLs [None, None, None]...")
        cur.execute(
            "INSERT INTO test_arrays (id, int_array) VALUES (%s, %s)",
            (4, [None, None, None])
        )
        conn.commit()

        cur.execute("SELECT int_array FROM test_arrays WHERE id = 4")
        result = cur.fetchone()[0]
        print(f"  Result: {result}")
        assert result == [None, None, None], f"Expected [None, None, None], got {result}"
        print("  ✅ All-NULL array works with binary protocol!")

        print("\n🎉 All psycopg3 binary protocol tests passed!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        if 'conn' in locals():
            conn.close()

        # Stop server
        print("\nStopping server...")
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()

        # Clean up temp dir
        import shutil
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    test_arrays()