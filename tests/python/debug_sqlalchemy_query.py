#!/usr/bin/env python3
"""Debug what exact query SQLAlchemy generates"""

import sys
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Boolean, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Enable query logging
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

def test_sqlalchemy_query_debug(port):
    """Debug the exact query SQLAlchemy generates"""
    try:
        print("🔍 Debugging SQLAlchemy Query Generation")
        print("=" * 50)
        
        # Create SQLAlchemy engine
        engine = create_engine(f'postgresql://postgres:postgres@localhost:{port}/main')
        Base = declarative_base()
        
        # Define a simple model
        class Product(Base):
            __tablename__ = 'products'
            
            id = Column(Integer, primary_key=True, autoincrement=True)
            name = Column(String(100), nullable=False)
            price = Column(Numeric(10, 2), nullable=False)
            is_active = Column(Boolean, default=True)
        
        Base.metadata.create_all(engine)
        
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Insert test data
        product = Product(name='Test Product', price=123.45, is_active=True)
        session.add(product)
        session.commit()
        
        print("\n🔍 Testing query that fails...")
        
        # This is the query that's failing
        query = session.query(
            Product.name.label('product_name'),
            Product.price.label('product_price'),
            Product.is_active.label('active_status')
        )
        
        print(f"Generated SQL: {query}")
        
        # Try to execute it
        try:
            result = query.first()
            print(f"✅ Query succeeded: {result}")
        except Exception as e:
            print(f"❌ Query failed: {e}")
        
        session.close()
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python debug_sqlalchemy_query.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    success = test_sqlalchemy_query_debug(port)
    sys.exit(0 if success else 1)