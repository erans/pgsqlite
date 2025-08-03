#!/usr/bin/env python3
import subprocess
import time
import os
import signal
from sqlalchemy import create_engine, Column, Integer, String, DateTime, DECIMAL, Boolean, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Start pgsqlite
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_psycopg3_sqlalchemy_complete.db"

# Remove old database files
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", "5435"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)

# Give it time to start
time.sleep(1)

try:
    # Create SQLAlchemy engine with psycopg3
    engine = create_engine(
        'postgresql+psycopg://postgres@localhost:5435/main',
        echo=False
    )
    
    Base = declarative_base()
    
    # Define models
    class User(Base):
        __tablename__ = 'users'
        
        id = Column(Integer, primary_key=True)
        username = Column(String(50), nullable=False, unique=True)
        email = Column(String(100), nullable=False)
        is_active = Column(Boolean, default=True)
        created_at = Column(DateTime, default=datetime.utcnow)
        balance = Column(DECIMAL(10, 2), default=0.00)
        
        __table_args__ = (
            UniqueConstraint('email', name='uq_user_email'),
        )
    
    class Product(Base):
        __tablename__ = 'products'
        
        id = Column(Integer, primary_key=True)
        name = Column(String(100), nullable=False)
        price = Column(DECIMAL(10, 2), nullable=False)
        in_stock = Column(Boolean, default=True)
    
    # Create tables
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    session = Session()
    
    print("=== Testing psycopg3 + SQLAlchemy ===")
    
    # Test 1: Basic INSERT with RETURNING
    print("\n1. Testing INSERT with RETURNING...")
    user1 = User(
        username='john_doe',
        email='john@example.com',
        balance=100.50
    )
    session.add(user1)
    session.commit()
    print(f"   Created user with ID: {user1.id}")
    assert user1.id is not None
    
    # Test 2: Query with parameter casts (this triggers $1::VARCHAR internally)
    print("\n2. Testing query with parameter casts...")
    found_user = session.query(User).filter(User.username == 'john_doe').first()
    assert found_user is not None
    assert found_user.email == 'john@example.com'
    print(f"   Found user: {found_user.username} with email: {found_user.email}")
    
    # Test 3: Decimal operations
    print("\n3. Testing decimal operations...")
    product = Product(name='Widget', price=29.99)
    session.add(product)
    session.commit()
    
    # Query with decimal comparison
    expensive_products = session.query(Product).filter(Product.price > 20.00).all()
    assert len(expensive_products) == 1
    print(f"   Found {len(expensive_products)} expensive products")
    
    # Test 4: DateTime operations
    print("\n4. Testing datetime operations...")
    recent_users = session.query(User).filter(
        User.created_at >= datetime(2025, 1, 1)
    ).all()
    assert len(recent_users) == 1
    print(f"   Found {len(recent_users)} recent users")
    
    # Test 5: UPDATE with RETURNING
    print("\n5. Testing UPDATE with RETURNING...")
    user1.balance = 200.75
    session.commit()
    assert user1.balance == 200.75
    print(f"   Updated balance to: {user1.balance}")
    
    # Test 6: Complex query with joins (if we add relationships)
    print("\n6. Testing complex queries...")
    active_users_with_balance = session.query(User).filter(
        User.is_active == True,
        User.balance > 100
    ).all()
    assert len(active_users_with_balance) == 1
    print(f"   Found {len(active_users_with_balance)} active users with balance > 100")
    
    # Test 7: Bulk operations
    print("\n7. Testing bulk operations...")
    users = [
        User(username=f'user{i}', email=f'user{i}@example.com', balance=i * 10.0)
        for i in range(2, 5)
    ]
    session.bulk_save_objects(users)
    session.commit()
    
    total_users = session.query(User).count()
    assert total_users == 4
    print(f"   Total users after bulk insert: {total_users}")
    
    print("\n✅ All tests passed! psycopg3 + SQLAlchemy is working correctly")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Close session if it exists
    try:
        session.close()
    except:
        pass
    
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()
    
    # Clean up
    for ext in ['', '-shm', '-wal']:
        db_file = f"{db_path}{ext}"
        if os.path.exists(db_file):
            os.remove(db_file)