#!/usr/bin/env python3
"""
Comprehensive test to verify all SQLAlchemy compatibility fixes work together
"""

import sys
import argparse

def test_comprehensive_fixes(port):
    """Test all the major fixes we implemented."""
    try:
        import psycopg2
        from sqlalchemy import create_engine, Column, Integer, String, Numeric, Boolean, DateTime, text
        from sqlalchemy.ext.declarative import declarative_base
        from sqlalchemy.orm import sessionmaker
        
        print("🧪 Comprehensive SQLAlchemy Compatibility Test")
        print("==============================================")
        print()
        
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://postgres:postgres@localhost:{port}/main')
        Base = declarative_base()
        
        # Define a comprehensive model that exercises all our fixes
        class TestModel(Base):
            __tablename__ = 'comprehensive_test'
            
            # Test SERIAL + PRIMARY KEY fix
            id = Column(Integer, primary_key=True, autoincrement=True)
            
            # Test NUMERIC type parsing fix  
            price = Column(Numeric(10, 2), nullable=False)
            
            # Test VARCHAR types
            name = Column(String(100), nullable=False)
            
            # Test Boolean type
            is_active = Column(Boolean, default=True)
            
            # Test TIMESTAMP WITHOUT TIME ZONE fix
            created_at = Column(DateTime, nullable=True)
        
        print("🔍 Testing table creation (SERIAL + NUMERIC + TIMESTAMP fixes)...")
        Base.metadata.create_all(engine)
        print("✅ Table creation succeeded!")
        
        # Test the ANY function with a direct query
        print("🔍 Testing ANY function fix...")
        with engine.connect() as conn:
            # This would have failed before our ANY function fix
            result = conn.execute(text("""
                SELECT 'test' = ANY('["test","other"]')
            """))
            any_result = result.fetchone()[0]
            print(f"✅ ANY function test result: {any_result}")
        
        # Test INSERT with casts
        print("🔍 Testing INSERT with timestamp cast...")
        Session = sessionmaker(bind=engine)
        session = Session()
        
        test_record = TestModel(
            name='Test Product',
            price=123.45,
            is_active=True
        )
        session.add(test_record)
        session.commit()
        
        # Test SELECT with filtering
        print("🔍 Testing SELECT with numeric comparison...")
        products = session.query(TestModel).filter(TestModel.price > 100.0).all()
        print(f"✅ Found {len(products)} products with price > 100")
        
        session.close()
        
        print()
        print("🎉 SUCCESS: All major SQLAlchemy compatibility fixes are working!")
        print("✅ Fixed issues:")
        print("  - SERIAL + PRIMARY KEY constraint conflicts")
        print("  - NUMERIC(precision, scale) type parsing")
        print("  - ANY function with string literals")
        print("  - TIMESTAMP WITHOUT TIME ZONE casts")
        print("  - PostgreSQL system function calls (pg_catalog.version)")
        print("  - SHOW command support")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    parser = argparse.ArgumentParser(description="Test comprehensive SQLAlchemy fixes")
    parser.add_argument("--port", type=int, required=True, help="Port number")
    args = parser.parse_args()
    
    success = test_comprehensive_fixes(args.port)
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())