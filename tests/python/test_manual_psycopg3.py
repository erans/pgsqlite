#!/usr/bin/env python3
"""Test psycopg3 with binary arrays containing NULLs.
Run this after starting pgsqlite manually with:
cargo run --bin pgsqlite -- --database /tmp/test.db --port 5433
"""

import psycopg

def test_arrays():
    try:
        # Connect with psycopg3
        print("Connecting with psycopg3 to localhost:5433...")
        conn = psycopg.connect(
            host="127.0.0.1",
            port=5433,
            user="postgres",
            dbname="test"
        )
        # Create binary cursor
        cur = conn.cursor(binary=True)  # Force binary protocol for this cursor
        print("Connected with binary cursor!")

        # Test 1: Create table with array column
        print("\nTest 1: Creating table with INTEGER[] column...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_arrays (
                id INTEGER PRIMARY KEY,
                int_array INTEGER[]
            )
        """)
        conn.commit()

        # Clear any existing data
        cur.execute("DELETE FROM test_arrays")
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

        print("\n🎉 All psycopg3 binary protocol tests passed!")

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        if 'conn' in locals():
            conn.close()
            print("Connection closed.")

if __name__ == "__main__":
    test_arrays()