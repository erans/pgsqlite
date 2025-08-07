#!/usr/bin/env python3
"""Test the exact cascade delete scenario that fails in SQLAlchemy"""

import os
import tempfile
import subprocess
import time
from decimal import Decimal
from datetime import date, time as dt_time
import psycopg

# Start pgsqlite
db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
db_file.close()
db_path = db_file.name

port = 15447
print(f"Starting pgsqlite on port {port}")
env = os.environ.copy()
env['RUST_LOG'] = 'info'
pgsqlite_proc = subprocess.Popen([
    '../../target/release/pgsqlite',
    '--database', db_path,
    '--port', str(port)
], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)

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
            customer_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
            order_date DATE,
            order_time TIME,
            total_amount NUMERIC(12, 2),
            status VARCHAR(20),
            notes TEXT
        )
    """)
    conn.commit()
    
    print("Created tables with CASCADE delete")
    
    # Scenario 1: User with orders (should work)
    print("\n=== Scenario 1: User WITH orders ===")
    
    # Create user
    cur.execute("INSERT INTO users (username, full_name) VALUES ('user1', 'User One') RETURNING id")
    user1_id = cur.fetchone()[0]
    
    # Create orders
    cur.execute("""
        INSERT INTO orders (customer_id, order_date, order_time, total_amount, status, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (user1_id, date.today(), dt_time(10, 0), Decimal('100.00'), 'complete', 'order 1'))
    
    cur.execute("""
        INSERT INTO orders (customer_id, order_date, order_time, total_amount, status, notes)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (user1_id, date.today(), dt_time(11, 0), Decimal('200.00'), 'pending', 'order 2'))
    
    conn.commit()
    
    # Simulate SQLAlchemy's lazy loading query before delete
    print("Simulating lazy load query for user1's orders:")
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
    
    cur.execute(query, (user1_id,))
    print(f"  Found {cur.rowcount} orders")
    for desc in cur.description:
        if desc.name == 'orders_total_amount':
            print(f"  orders_total_amount type: {desc.type_code} (should be 1700)")
            if desc.type_code != 1700:
                print("    ERROR: Wrong type!")
    
    # Delete user (should cascade delete orders)
    cur.execute("DELETE FROM users WHERE id = %s", (user1_id,))
    print(f"  Deleted user1, affected rows: {cur.rowcount}")
    conn.commit()
    
    # Scenario 2: User with NO orders (this is where the bug might occur)
    print("\n=== Scenario 2: User with NO orders (BUG scenario) ===")
    
    # Create user
    cur.execute("INSERT INTO users (username, full_name) VALUES ('user2', 'User Two') RETURNING id")
    user2_id = cur.fetchone()[0]
    conn.commit()
    
    print(f"Created user2 with id {user2_id} and NO orders")
    
    # Simulate SQLAlchemy's lazy loading query before delete
    print("Simulating lazy load query for user2's orders (should be empty):")
    
    # This is the EXACT query that fails in SQLAlchemy
    cur.execute(query, (user2_id,))
    print(f"  Found {cur.rowcount} orders (should be 0)")
    
    print("  Column descriptions:")
    for desc in cur.description:
        print(f"    {desc.name}: type_code={desc.type_code}")
        if desc.name == 'orders_total_amount' and desc.type_code == 25:
            print("      ^^^ BUG REPRODUCED! Should be 1700 (NUMERIC) not 25 (TEXT)")
    
    results = cur.fetchall()
    print(f"  Results: {results}")
    
    # Try to delete user2
    print("Attempting to delete user2...")
    cur.execute("DELETE FROM users WHERE id = %s", (user2_id,))
    print(f"  Deleted user2, affected rows: {cur.rowcount}")
    conn.commit()
    
    print("\n=== Test completed successfully ===")
    
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    pgsqlite_proc.terminate()
    pgsqlite_proc.wait()
    os.unlink(db_path)