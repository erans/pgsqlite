#!/usr/bin/env python3
"""Final test of cleanup functionality."""

import psycopg2
import time

PORT = 15433

print("Testing cleanup with already running server...")

for i in range(3):
    print(f"\nTest {i+1}/3:")
    try:
        print("  Connecting...")
        conn = psycopg2.connect(host='/tmp', port=PORT, dbname='main', user='postgres')
        
        print("  Running query...")
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        print(f"  Result: {result}")
        
        print("  Closing...")
        cur.close()
        conn.close()
        
        print("  ✓ Success!")
        time.sleep(0.5)
        
    except Exception as e:
        print(f"  ✗ Error: {e}")
        import traceback
        traceback.print_exc()
        break

print("\n✅ Cleanup test completed!")