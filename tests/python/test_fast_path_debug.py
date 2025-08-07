#!/usr/bin/env python3
"""Debug fast path type inference"""

import os
import tempfile
import subprocess
import time
from decimal import Decimal
import psycopg

# Start pgsqlite
db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
db_file.close()
db_path = db_file.name

port = 15449
print(f"Starting pgsqlite on port {port}")
env = os.environ.copy()
env['RUST_LOG'] = 'pgsqlite::query::extended_fast_path=debug,pgsqlite::query::extended=debug'
pgsqlite_proc = subprocess.Popen([
    '../../target/debug/pgsqlite',  # Use debug build
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
            total_amount NUMERIC(12, 2)
        )
    """)
    conn.commit()
    
    # Create user with NO orders
    cur.execute("INSERT INTO users (username, full_name) VALUES ('test_user', 'Test User') RETURNING id")
    user_id = cur.fetchone()[0]
    conn.commit()
    
    print(f"\nCreated user {user_id} with NO orders")
    
    # Execute the exact query that fails
    query = """
    SELECT orders.id AS orders_id, 
           orders.customer_id AS orders_customer_id, 
           orders.total_amount AS orders_total_amount
    FROM orders 
    WHERE %s::INTEGER = orders.customer_id
    """
    
    print("\n=== Executing parameterized query ===")
    cur.execute(query, (user_id,))
    
    print("\nColumn descriptions:")
    for desc in cur.description:
        print(f"  {desc.name}: type_code={desc.type_code}")
        if desc.name == 'orders_total_amount' and desc.type_code != 1700:
            print(f"    ^^^ ERROR: Expected NUMERIC (1700) but got {desc.type_code}")
    
    results = cur.fetchall()
    print(f"Results: {results}")
    
    conn.close()
    
finally:
    # Capture logs
    pgsqlite_proc.terminate()
    output, _ = pgsqlite_proc.communicate(timeout=2)
    
    print("\n=== Fast path logs ===")
    for line in output.splitlines()[-50:]:
        if "Fast path:" in line:
            print(line)
    
    os.unlink(db_path)