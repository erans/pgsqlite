#!/usr/bin/env python3
"""Debug the exact issue with empty result sets"""

import os
import tempfile
import subprocess
import time
from datetime import date, time as dt_time
from decimal import Decimal
import psycopg

# Start pgsqlite
db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
db_file.close()
db_path = db_file.name

port = 15446
print(f"Starting pgsqlite on port {port}")
env = os.environ.copy()
env['RUST_LOG'] = 'pgsqlite::query::extended_fast_path=debug'
pgsqlite_proc = subprocess.Popen([
    '../../target/release/pgsqlite',
    '--database', db_path,
    '--port', str(port)
], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)

time.sleep(1)

try:
    # Connect
    conn = psycopg.connect(f"postgresql://postgres@localhost:{port}/main")
    cur = conn.cursor()
    
    # Create the exact schema
    cur.execute("DROP TABLE IF EXISTS orders")
    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("""
        CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50),
            full_name VARCHAR(100)
        )
    """)
    cur.execute("""
        CREATE TABLE orders (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER REFERENCES users(id),
            order_date DATE,
            order_time TIME,
            total_amount NUMERIC(12, 2),
            status VARCHAR(20),
            notes TEXT
        )
    """)
    conn.commit()
    
    # Create user with NO orders
    cur.execute("INSERT INTO users (username, full_name) VALUES ('test_user', 'Test User') RETURNING id")
    user_id = cur.fetchone()[0]
    conn.commit()
    
    print(f"\nCreated user {user_id} with NO orders")
    
    # Test 1: Simple SELECT with no parameters (should work)
    print("\n=== Test 1: Simple SELECT (no params) ===")
    cur.execute("""
        SELECT orders.id AS orders_id, 
               orders.customer_id AS orders_customer_id, 
               orders.order_date AS orders_order_date,
               orders.order_time AS orders_order_time,
               orders.total_amount AS orders_total_amount,
               orders.status AS orders_status,
               orders.notes AS orders_notes
        FROM orders 
        WHERE customer_id = 999
    """)
    
    print("Column descriptions:")
    for desc in cur.description:
        print(f"  {desc.name}: type_code={desc.type_code}")
    
    results = cur.fetchall()
    print(f"Results (should be empty): {results}")
    
    # Test 2: Parameterized query (this is where the bug happens)
    print("\n=== Test 2: Parameterized query (BUG HERE) ===")
    
    # Execute the EXACT query that SQLAlchemy uses
    query = """
    SELECT orders.id AS orders_id, 
           orders.customer_id AS orders_customer_id, 
           orders.order_date AS orders_order_date,
           orders.order_time AS orders_order_time,
           orders.total_amount AS orders_total_amount,
           orders.status AS orders_status,
           orders.notes AS orders_notes
    FROM orders 
    WHERE %s::INTEGER = orders.customer_id
    """
    
    # This is the problematic query that returns wrong types
    cur.execute(query, (user_id,))
    
    print("Column descriptions after parameterized query:")
    for desc in cur.description:
        print(f"  {desc.name}: type_code={desc.type_code}")
        if desc.name == 'orders_total_amount' and desc.type_code == 25:
            print("    ^^^ BUG! Should be 1700 (NUMERIC) not 25 (TEXT)")
    
    results = cur.fetchall()
    print(f"Results (should be empty): {results}")
    
    # Test 3: Try with an actual order to see if it makes a difference
    print("\n=== Test 3: Create an order and query again ===")
    cur.execute("""
        INSERT INTO orders (customer_id, order_date, order_time, total_amount, status, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (user_id, date.today(), dt_time(12, 0), Decimal('123.45'), 'pending', 'test order'))
    conn.commit()
    
    # Query again with the order present
    cur.execute(query, (user_id,))
    
    print("Column descriptions with data present:")
    for desc in cur.description:
        print(f"  {desc.name}: type_code={desc.type_code}")
        if desc.name == 'orders_total_amount':
            if desc.type_code == 1700:
                print("    ✓ Correct: NUMERIC (1700)")
            else:
                print(f"    ✗ Wrong: Got {desc.type_code} instead of 1700")
    
    results = cur.fetchall()
    print(f"Results: {results}")
    
    # Look for key logs
    print("\n=== Checking pgsqlite logs for fast path behavior ===")
    
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    # Capture some logs
    pgsqlite_proc.terminate()
    output, _ = pgsqlite_proc.communicate(timeout=2)
    
    # Look for fast path related logs
    for line in output.splitlines()[-100:]:
        if any(keyword in line for keyword in [
            "send_row_desc",
            "field_descriptions.is_empty()",
            "ExtendedFastPath",
            "RowDescription",
            "Fast path"
        ]):
            print(line)
    
    os.unlink(db_path)