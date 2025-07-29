#!/usr/bin/env python3
"""Ultra-simple test for connection visibility"""

import psycopg2
import time
import sys

# Get port from command line
port = int(sys.argv[1]) if len(sys.argv) > 1 else 15505

# Connection 1 - Create and populate
conn1 = psycopg2.connect(f"host=localhost port={port} dbname=main user=postgres")
cur1 = conn1.cursor()

# Create table and insert data
cur1.execute("CREATE TABLE IF NOT EXISTS simple_test (id INT, value TEXT)")
cur1.execute("DELETE FROM simple_test")  # Clean up
cur1.execute("INSERT INTO simple_test VALUES (1, 'initial')")
conn1.commit()
print("✅ Conn1: Created table and inserted (1, 'initial')")

# Update the value
cur1.execute("UPDATE simple_test SET value = 'updated' WHERE id = 1")
conn1.commit()
print("✅ Conn1: Updated to 'updated' and committed")

# Verify from same connection
cur1.execute("SELECT value FROM simple_test WHERE id = 1")
result1 = cur1.fetchone()
print(f"📍 Conn1: Sees value = '{result1[0]}'")

# Close first connection completely
cur1.close()
conn1.close()
print("🔒 Conn1: Closed")

# Small delay
time.sleep(0.1)

# Connection 2 - New connection
conn2 = psycopg2.connect(f"host=localhost port={port} dbname=main user=postgres")
cur2 = conn2.cursor()
print("🔓 Conn2: New connection opened")

# Check what the new connection sees
cur2.execute("SELECT value FROM simple_test WHERE id = 1")
result2 = cur2.fetchone()
print(f"📍 Conn2: Sees value = '{result2[0]}'")

# Cleanup
cur2.execute("DROP TABLE simple_test")
conn2.commit()
cur2.close()
conn2.close()

if result2[0] == 'updated':
    print("✅ TEST PASSED: New connection sees committed update")
else:
    print(f"❌ TEST FAILED: Expected 'updated', got '{result2[0]}'")