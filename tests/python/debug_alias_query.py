#!/usr/bin/env python3
"""Debug the alias query that's causing type OID issues"""

import psycopg2

try:
    conn = psycopg2.connect(
        host="localhost",
        port=15516,
        database="main",
        user="postgres",
        password="postgres"
    )
    
    cur = conn.cursor()
    
    # Create table
    cur.execute("""
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100),
            price NUMERIC(10,2),
            is_active BOOLEAN
        )
    """)
    
    # Insert data
    cur.execute("""
        INSERT INTO products (id, name, price, is_active) 
        VALUES (1, 'Test Product', 123.45, true)
    """)
    
    print("Testing different query patterns:")
    
    # Test 1: Simple SELECT
    print("\n1. Simple SELECT:")
    cur.execute("SELECT * FROM products")
    for i, desc in enumerate(cur.description):
        print(f"  {desc[0]}: type_oid={desc[1]}")
    
    # Test 2: SELECT with aliases
    print("\n2. SELECT with AS aliases:")
    cur.execute("SELECT name AS product_name, price AS product_price FROM products")
    for i, desc in enumerate(cur.description):
        print(f"  {desc[0]}: type_oid={desc[1]}")
        
    # Test 3: SELECT with label-style aliases (like SQLAlchemy)
    print("\n3. SELECT with label aliases:")
    cur.execute("SELECT products.name AS products_name_1, products.price AS products_price_1 FROM products")
    for i, desc in enumerate(cur.description):
        print(f"  {desc[0]}: type_oid={desc[1]}")
        
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()