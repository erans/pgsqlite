#!/usr/bin/env python3
"""Debug the exact SQLAlchemy transaction test failure"""

import os
import tempfile
import subprocess
import time
from decimal import Decimal
from datetime import date, time as dt_time

# Start pgsqlite
db_file = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
db_file.close()
db_path = db_file.name

port = 15448
print(f"Starting pgsqlite on port {port}")
env = os.environ.copy()
env['RUST_LOG'] = 'pgsqlite::query::extended=info'
pgsqlite_proc = subprocess.Popen([
    '../../target/release/pgsqlite',
    '--database', db_path,
    '--port', str(port)
], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)

time.sleep(1)

try:
    from sqlalchemy import create_engine, Column, Integer, String, DateTime, DECIMAL, Date, Time, Text, ForeignKey
    from sqlalchemy.orm import declarative_base, sessionmaker, relationship
    from datetime import datetime
    
    # Create engine with psycopg3 in text mode
    engine = create_engine(f'postgresql+psycopg://postgres@localhost:{port}/main')
    Base = declarative_base()
    
    # Define the exact models from the test
    class User(Base):
        __tablename__ = 'users'
        id = Column(Integer, primary_key=True)
        username = Column(String(50), unique=True, nullable=False)
        full_name = Column(String(100))
        orders = relationship('Order', back_populates='customer', cascade='all, delete-orphan')
    
    class Order(Base):
        __tablename__ = 'orders'
        id = Column(Integer, primary_key=True)
        customer_id = Column(Integer, ForeignKey('users.id'), nullable=False)
        order_date = Column(Date)
        order_time = Column(Time)
        total_amount = Column(DECIMAL(12, 2))
        status = Column(String(20))
        notes = Column(Text)
        customer = relationship('User', back_populates='orders')
    
    print("Creating tables...")
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    
    print("\n=== Test 1: User with orders ===")
    session = Session()
    
    # Create user with orders
    user1 = User(username='user1', full_name='User One')
    order1 = Order(
        order_date=date.today(),
        order_time=dt_time(10, 0),
        total_amount=Decimal('100.00'),
        status='complete',
        notes='order 1'
    )
    order2 = Order(
        order_date=date.today(),
        order_time=dt_time(11, 0),
        total_amount=Decimal('200.00'),
        status='pending',
        notes='order 2'
    )
    user1.orders.append(order1)
    user1.orders.append(order2)
    
    session.add(user1)
    session.commit()
    
    print(f"Created user1 with {len(user1.orders)} orders")
    
    # Delete user (should cascade delete orders)
    session.delete(user1)
    session.commit()
    print("Deleted user1 successfully")
    
    session.close()
    
    print("\n=== Test 2: User with NO orders (BUG scenario) ===")
    session = Session()
    
    # Create user with NO orders
    user2 = User(username='user2', full_name='User Two')
    session.add(user2)
    session.commit()
    
    print(f"Created user2 with {len(user2.orders)} orders")
    
    # This is where the bug happens - when SQLAlchemy tries to lazy load
    # the orders relationship during delete to handle cascade
    try:
        print("Attempting to delete user2...")
        session.delete(user2)
        session.commit()
        print("Deleted user2 successfully")
    except Exception as e:
        print(f"ERROR during delete: {e}")
        import traceback
        traceback.print_exc()
    
    session.close()
    
    print("\n=== Test completed ===")
    
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    # Capture relevant logs
    pgsqlite_proc.terminate()
    output, _ = pgsqlite_proc.communicate(timeout=2)
    
    # Look for field description related logs
    print("\n=== Relevant pgsqlite logs ===")
    for line in output.splitlines()[-50:]:
        if any(keyword in line for keyword in [
            "field descriptions",
            "orders_total_amount",
            "type OID",
            "NUMERIC",
            "TEXT",
            "25",
            "1700"
        ]):
            print(line)
    
    os.unlink(db_path)