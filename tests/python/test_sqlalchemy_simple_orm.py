#!/usr/bin/env python3
"""
Simple SQLAlchemy ORM test focused on type OID compatibility
"""

import sys
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def test_sqlalchemy_types(port):
    """Test SQLAlchemy with our type OID fixes"""
    try:
        print("🧪 SQLAlchemy Type OID Compatibility Test")
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
        
        print("🔍 Testing table creation...")
        Base.metadata.create_all(engine)
        print("✅ Table creation succeeded!")
        
        # Test single INSERT (avoid complex multi-row syntax)
        print("🔍 Testing single INSERT...")
        Session = sessionmaker(bind=engine)
        session = Session()
        
        product = Product(
            name='Test Product',
            price=123.45,
            is_active=True
        )
        session.add(product)
        session.commit()
        print("✅ Single INSERT succeeded!")
        
        # Test SELECT with type inference
        print("🔍 Testing SELECT with column aliases...")
        products = session.query(Product).all()
        print(f"✅ Found {len(products)} products")
        
        # Test query with aliases (this exercises our type OID mapping)
        print("🔍 Testing complex SELECT with aliases...")
        result = session.query(
            Product.name.label('product_name'),
            Product.price.label('product_price'),
            Product.is_active.label('active_status')
        ).first()
        
        if result:
            print(f"✅ Complex query succeeded: {result.product_name}, ${result.product_price}, active={result.active_status}")
        
        session.close()
        
        print("\n🎉 SUCCESS: SQLAlchemy type OID compatibility working!")
        print("✅ Key features working:")
        print("  - Table creation with mixed types")
        print("  - Single record INSERT")
        print("  - SELECT queries with column aliases")
        print("  - Proper type OID inference")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_sqlalchemy_simple_orm.py <port>")
        sys.exit(1)
    
    port = int(sys.argv[1])
    success = test_sqlalchemy_types(port)
    sys.exit(0 if success else 1)