#!/usr/bin/env python3
"""Simple transaction test to debug the issue"""

import psycopg2

print("=== Testing basic transaction behavior ===")

# Test 1: Basic transaction
print("\nTest 1: Basic BEGIN/COMMIT")
conn1 = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
cur1 = conn1.cursor()

try:
    cur1.execute("CREATE TABLE test_simple (id INTEGER PRIMARY KEY, value TEXT)")
    conn1.commit()
    print("✅ Table created")
    
    cur1.execute("BEGIN")
    print("✅ BEGIN executed")
    
    cur1.execute("INSERT INTO test_simple (value) VALUES ('test')")
    print("✅ INSERT executed")
    
    cur1.execute("COMMIT")
    print("✅ COMMIT executed")
    
except Exception as e:
    print(f"❌ Error: {e}")
    
finally:
    cur1.close()
    conn1.close()

# Test 2: New connection
print("\nTest 2: New connection after transaction")
conn2 = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
cur2 = conn2.cursor()

try:
    cur2.execute("BEGIN")
    print("✅ BEGIN on new connection succeeded")
    
    cur2.execute("SELECT value FROM test_simple WHERE id = 1")
    result = cur2.fetchone()
    print(f"Query result: {result}")
    
    cur2.execute("COMMIT")
    print("✅ COMMIT on new connection succeeded")
    
except Exception as e:
    print(f"❌ Error on new connection: {e}")
    
finally:
    cur2.close()
    conn2.close()

# Test 3: SQLAlchemy style
print("\nTest 3: SQLAlchemy style transactions")
conn3 = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
conn3.autocommit = False  # SQLAlchemy default

try:
    cur3 = conn3.cursor()
    
    # SQLAlchemy doesn't send explicit BEGIN, it relies on autocommit=False
    cur3.execute("UPDATE test_simple SET value = 'updated' WHERE id = 1")
    print("✅ UPDATE executed")
    
    # SQLAlchemy sends COMMIT
    conn3.commit()
    print("✅ COMMIT executed")
    
    # Close connection 
    cur3.close()
    conn3.close()
    
    # New connection (simulating new SQLAlchemy session)
    conn4 = psycopg2.connect(host='localhost', port=5435, dbname='main', user='postgres')
    conn4.autocommit = False
    cur4 = conn4.cursor()
    
    # This might trigger the "transaction within transaction" error
    cur4.execute("SELECT value FROM test_simple WHERE id = 1")
    result = cur4.fetchone()
    print(f"New connection result: {result}")
    
    cur4.close()
    conn4.close()
    
except Exception as e:
    print(f"❌ SQLAlchemy style error: {e}")

print("\n=== Test completed ===")