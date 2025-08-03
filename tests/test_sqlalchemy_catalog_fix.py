#!/usr/bin/env python3
import subprocess
import time
import os
import sys

# Start pgsqlite
pgsqlite_path = "../target/debug/pgsqlite"
db_path = "/tmp/test_sqlalchemy_catalog_fix.db"

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

env = os.environ.copy()
env["RUST_LOG"] = "info"

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", "5439"],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    env=env
)

# Give it time to start
time.sleep(1)

try:
    # Test with psycopg3 - this will fail if psycopg is not installed
    import psycopg
    
    conn = psycopg.connect(
        "host=localhost port=5439 user=postgres dbname=main",
        autocommit=True
    )
    
    print("=== Testing SQLAlchemy catalog query pattern ===")
    
    # Create a test table
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50)
            )
        """)
    
    # Test the exact query pattern that SQLAlchemy uses
    with conn.cursor() as cur:
        query = """
            SELECT pg_catalog.pg_class.relname 
            FROM pg_catalog.pg_class 
            JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
            WHERE pg_catalog.pg_class.relname = %s::VARCHAR 
            AND pg_catalog.pg_class.relkind = ANY (ARRAY[%s::VARCHAR, %s::VARCHAR, %s::VARCHAR, %s::VARCHAR, %s::VARCHAR]) 
            AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) 
            AND pg_catalog.pg_namespace.nspname != %s::VARCHAR
        """
        
        params = ('users', 'r', 'p', 'f', 'v', 'm', 'pg_catalog')
        
        try:
            cur.execute(query, params)
            results = cur.fetchall()
            print(f"✅ SUCCESS: Query executed successfully, found {len(results)} results")
            for row in results:
                print(f"   - {row[0]}")
        except Exception as e:
            print(f"❌ FAILED: {e}")
            # Get pgsqlite logs
            stdout, stderr = pgsqlite.communicate(timeout=1)
            if stderr:
                print("\npgsqlite stderr:")
                print(stderr.decode()[-1000:])  # Last 1000 chars
            sys.exit(1)
    
    # Now test with SQLAlchemy
    try:
        from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
        
        print("\n=== Testing with SQLAlchemy + psycopg3 ===")
        
        # Create engine with psycopg3
        engine = create_engine(
            "postgresql+psycopg://postgres@localhost:5439/main",
            echo=False
        )
        
        # Define a table
        metadata = MetaData()
        users_table = Table('users', metadata,
            Column('id', Integer, primary_key=True),
            Column('name', String(50))
        )
        
        # This will trigger the catalog query
        try:
            metadata.reflect(bind=engine)
            print("✅ SUCCESS: SQLAlchemy metadata reflection worked!")
            
            # List reflected tables
            print(f"   Reflected tables: {list(metadata.tables.keys())}")
        except Exception as e:
            print(f"❌ FAILED: SQLAlchemy error: {e}")
            sys.exit(1)
            
    except ImportError:
        print("\n⚠️  SQLAlchemy not installed, skipping SQLAlchemy test")
    
    print("\n✅ All tests passed!")
    
except ImportError:
    print("❌ psycopg (psycopg3) not installed. Install with: pip install psycopg")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()
    
    # Clean up database
    for ext in ['', '-shm', '-wal']:
        db_file = f"{db_path}{ext}"
        if os.path.exists(db_file):
            os.remove(db_file)