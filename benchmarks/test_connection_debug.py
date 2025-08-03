#!/usr/bin/env python3
"""Debug connection issues"""

import psycopg
import time

print("Testing connection...")

# Try simple connection first
print("1. Connecting...")
try:
    conn = psycopg.connect(
        host="127.0.0.1",
        port=5434,
        dbname="benchmark_test.db",
        user="dummy",
        password="dummy",
        sslmode="disable"
    )
    print("   Connected successfully")
    
    # Test regular cursor
    print("\n2. Creating regular cursor...")
    cursor = conn.cursor()
    print("   Regular cursor created")
    
    print("\n3. Executing SELECT 1 with regular cursor...")
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print(f"   Result: {result}")
    
    # Now test binary cursor
    print("\n4. Creating binary cursor...")
    binary_cursor = conn.cursor(binary=True)
    print("   Binary cursor created")
    
    print("\n5. About to execute SELECT 1 with binary cursor...")
    print("   This is where it might hang...")
    binary_cursor.execute("SELECT 1")
    print("   Execute completed!")
    
    result = binary_cursor.fetchone()
    print(f"   Result: {result}")
    
    conn.close()
    print("\nAll tests passed!")
    
except Exception as e:
    print(f"\nERROR: {e}")
    import traceback
    traceback.print_exc()