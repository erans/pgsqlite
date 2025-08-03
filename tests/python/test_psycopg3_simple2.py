#!/usr/bin/env python3
import psycopg

print("Testing psycopg3 with SQLAlchemy-style queries...")

try:
    # Connect to pgsqlite
    conn = psycopg.connect(
        host="localhost",
        port=5433,
        dbname="main",
        user="postgres",
        password=""
    )
    
    print("Connected successfully")
    
    with conn.cursor() as cur:
        # Create a table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS test_users (
                id INTEGER PRIMARY KEY,
                username VARCHAR(50),
                email VARCHAR(100),
                created_at TIMESTAMP WITHOUT TIME ZONE
            )
        """)
        print("Table created")
        
        # Insert with parameter casts (like SQLAlchemy does)
        import datetime
        now = datetime.datetime.now()
        
        cur.execute(
            """INSERT INTO test_users (username, email, created_at) 
               VALUES (%s::VARCHAR, %s::VARCHAR, %s::TIMESTAMP WITHOUT TIME ZONE)
               RETURNING id""",
            ("testuser", "test@example.com", now)
        )
        result = cur.fetchone()
        print(f"Inserted user with ID: {result}")
        
        # Select with parameter cast
        cur.execute(
            "SELECT * FROM test_users WHERE username = %s::VARCHAR",
            ("testuser",)
        )
        result = cur.fetchone()
        print(f"Selected user: {result}")
        
        # Test with integer parameter cast
        cur.execute(
            "SELECT * FROM test_users WHERE id = %s::INTEGER",
            (1,)
        )
        result = cur.fetchone()
        print(f"Selected by ID: {result}")
        
        # Test LIMIT with cast
        cur.execute(
            "SELECT * FROM test_users LIMIT %s::INTEGER",
            (5,)
        )
        results = cur.fetchall()
        print(f"Limited results: {len(results)} rows")
        
        # Test binary format
        with conn.cursor(binary=True) as binary_cur:
            binary_cur.execute("SELECT id, username FROM test_users WHERE id = 1")
            result = binary_cur.fetchone()
            print(f"Binary format result: {result}")
        
        # Clean up
        cur.execute("DROP TABLE test_users")
        print("Table dropped")
    
    conn.commit()
    conn.close()
    print("\nAll tests passed!")
    
except Exception as e:
    print(f"Error: {type(e).__name__}: {e}")
    import traceback
    traceback.print_exc()