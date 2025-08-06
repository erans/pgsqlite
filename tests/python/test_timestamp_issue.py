import psycopg
from datetime import datetime

conn = psycopg.connect("host=localhost port=15432 dbname=main user=postgres", autocommit=True)

with conn.cursor() as cur:
    # Create a simple test
    cur.execute('DROP TABLE IF EXISTS test_timestamp')
    cur.execute('''
        CREATE TABLE test_timestamp (
            id INTEGER PRIMARY KEY,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP
        )
    ''')
    
    # Insert data
    now = datetime.now()
    cur.execute('INSERT INTO test_timestamp (created_at, updated_at) VALUES (%s, %s)', 
                (now, now))
    
    # Test various queries
    print("Testing simple SELECT *...")
    cur.execute("SELECT * FROM test_timestamp")
    result = cur.fetchone()
    print(f"Result: id={result[0]}, created_at={result[1]}, updated_at={result[2]}")
    
    print("\nTesting SELECT with ORDER BY...")
    cur.execute("SELECT * FROM test_timestamp ORDER BY created_at")
    result = cur.fetchone()
    print(f"Result: id={result[0]}, created_at={result[1]}, updated_at={result[2]}")

conn.close()
