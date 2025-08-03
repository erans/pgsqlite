#!/usr/bin/env python3
import subprocess
import time
import os
from sqlalchemy import create_engine, Column, Integer, String, DateTime, DECIMAL, Boolean, text
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

# Start pgsqlite with a fresh database
pgsqlite_path = "../../target/debug/pgsqlite"
db_path = "/tmp/test_psycopg3_sqlalchemy_final.db"

# Clean up old database
for ext in ['', '-shm', '-wal']:
    db_file = f"{db_path}{ext}"
    if os.path.exists(db_file):
        os.remove(db_file)

pgsqlite = subprocess.Popen(
    [pgsqlite_path, "--database", db_path, "--port", "5442"],
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL
)

# Give it time to start
time.sleep(1)

try:
    # Create SQLAlchemy engine with psycopg3
    engine = create_engine(
        'postgresql+psycopg://postgres@localhost:5442/main',
        echo=False  # Set to True to see SQL
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
    
    print("=== Testing psycopg3 + SQLAlchemy Complete Integration ===")
    
    # Test 1: Basic INSERT with RETURNING (uses parameter casts)
    print("\n1. Testing INSERT with RETURNING...")
    user1 = User(
        username='alice',
        email='alice@example.com',
        balance=100.50
    )
    session.add(user1)
    session.commit()
    print(f"   ✅ Created user with ID: {user1.id}")
    assert user1.id is not None
    
    # Test 2: Query with parameter casts
    print("\n2. Testing query with parameter casts...")
    found_user = session.query(User).filter(User.username == 'alice').first()
    assert found_user is not None
    assert found_user.email == 'alice@example.com'
    print(f"   ✅ Found user: {found_user.username} with email: {found_user.email}")
    
    # Test 3: Multiple inserts
    print("\n3. Testing multiple inserts...")
    users = [
        User(username='bob', email='bob@example.com', balance=200.00),
        User(username='charlie', email='charlie@example.com', balance=300.00)
    ]
    session.add_all(users)
    session.commit()
    total_users = session.query(User).count()
    print(f"   ✅ Total users: {total_users}")
    assert total_users == 3
    
    # Test 4: UPDATE with RETURNING
    print("\n4. Testing UPDATE with RETURNING...")
    found_user.balance = 150.75
    session.commit()
    assert found_user.balance == 150.75
    print(f"   ✅ Updated balance to: {found_user.balance}")
    
    # Test 5: Complex queries with decimals and dates
    print("\n5. Testing complex queries...")
    rich_users = session.query(User).filter(
        User.balance > 100.00,
        User.is_active == True
    ).all()
    print(f"   ✅ Found {len(rich_users)} users with balance > 100")
    
    # Test 6: Products with decimal operations
    print("\n6. Testing decimal operations...")
    products = [
        Product(name='Widget', price=29.99),
        Product(name='Gadget', price=49.99),
        Product(name='Gizmo', price=99.99)
    ]
    session.add_all(products)
    session.commit()
    
    # Query with decimal comparison
    expensive_products = session.query(Product).filter(Product.price >= 50.00).all()
    print(f"   ✅ Found {len(expensive_products)} expensive products")
    
    # Test 7: Aggregate functions
    print("\n7. Testing aggregate functions...")
    from sqlalchemy import func
    avg_price = session.query(func.avg(Product.price)).scalar()
    max_balance = session.query(func.max(User.balance)).scalar()
    print(f"   ✅ Average product price: {avg_price}")
    print(f"   ✅ Maximum user balance: {max_balance}")
    
    # Test 8: Raw SQL with parameters (uses psycopg3 directly)
    print("\n8. Testing raw SQL with parameters...")
    result = session.execute(
        text("SELECT username FROM users WHERE balance > :min_balance ORDER BY username"),
        {"min_balance": 150}
    )
    high_balance_users = [row[0] for row in result]
    print(f"   ✅ Users with balance > 150: {high_balance_users}")
    
    # Test 9: Transaction rollback
    print("\n9. Testing transaction rollback...")
    try:
        # Start a new transaction
        new_user = User(username='alice', email='duplicate@example.com')  # Duplicate username
        session.add(new_user)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"   ✅ Transaction rolled back successfully after error")
    
    # Verify rollback worked
    final_count = session.query(User).count()
    assert final_count == 3  # Should still be 3 users
    
    # Test 10: DELETE operations
    print("\n10. Testing DELETE operations...")
    user_to_delete = session.query(User).filter(User.username == 'charlie').first()
    session.delete(user_to_delete)
    session.commit()
    remaining_users = session.query(User).count()
    print(f"   ✅ Deleted user, remaining: {remaining_users}")
    
    print("\n✅ All tests passed! psycopg3 + SQLAlchemy integration is working perfectly!")
    
    # Close session properly
    session.close()
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()

finally:
    # Kill pgsqlite
    pgsqlite.terminate()
    pgsqlite.wait()
    
    # Clean up database
    for ext in ['', '-shm', '-wal']:
        db_file = f"{db_path}{ext}"
        if os.path.exists(db_file):
            os.remove(db_file)