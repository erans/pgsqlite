#!/usr/bin/env python3
import sys
import argparse

def test_basic_connection(port):
    """Test basic SQLAlchemy connection without full ORM."""
    try:
        # Try to import required modules
        from sqlalchemy import create_engine, text
        print("✅ SQLAlchemy imported successfully")
        
        # Create connection
        connection_string = f"postgresql://postgres:postgres@localhost:{port}/main"
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as conn:
            # Test system functions
            result = conn.execute(text("SELECT version()")).fetchone()
            print(f"✅ version(): {result[0]}")
            
            result = conn.execute(text("SELECT current_database()")).fetchone()
            print(f"✅ current_database(): {result[0]}")
            
            result = conn.execute(text("SELECT current_user()")).fetchone()
            print(f"✅ current_user(): {result[0]}")
            
            # Test basic table creation
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            print("✅ Table created successfully")
            
            # Test insert
            conn.execute(text("""
                INSERT INTO test_table (id, name) 
                VALUES (1, 'Test Record') 
                ON CONFLICT(id) DO NOTHING
            """))
            print("✅ Insert successful")
            
            # Test select
            result = conn.execute(text("SELECT name FROM test_table WHERE id = 1")).fetchone()
            print(f"✅ Select successful: {result[0] if result else 'No data'}")
            
            # Test count
            count = conn.execute(text("SELECT COUNT(*) FROM test_table")).scalar()
            print(f"✅ Count query: {count} records")
            
            conn.commit()
        
        print("🎉 Basic SQLAlchemy test completed successfully!")
        return True
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Please install required packages: pip install sqlalchemy psycopg2-binary")
        return False
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Simple SQLAlchemy test")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_basic_connection(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())
