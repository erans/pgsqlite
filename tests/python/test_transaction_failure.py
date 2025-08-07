#!/usr/bin/env python3
"""Reproduce the exact SQLAlchemy transaction test failure"""

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

port = 15450
print(f"Starting pgsqlite on port {port}")
env = os.environ.copy()
env['RUST_LOG'] = 'pgsqlite::query::extended_fast_path=debug,pgsqlite::query::extended=info'
pgsqlite_proc = subprocess.Popen([
    '../../target/debug/pgsqlite',
    '--database', db_path,
    '--port', str(port)
], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)

time.sleep(1)

try:
    from sqlalchemy import create_engine, Column, Integer, String, DECIMAL, Date, Time, Text, ForeignKey
    from sqlalchemy.orm import declarative_base, sessionmaker, relationship
    
    # Create engine with psycopg3
    engine = create_engine(f'postgresql+psycopg://postgres@localhost:{port}/main')
    Base = declarative_base()
    
    # Define models exactly as in test
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
    session = Session()
    
    # Create user with NO orders - this is where the bug happens
    user = User(username='testuser', full_name='Test User')
    session.add(user)
    session.commit()
    
    print(f"Created user {user.id} with NO orders")
    
    # This is where the failure happens - when deleting a user with no orders
    print("\nAttempting to delete user with cascade...")
    try:
        session.delete(user)
        session.commit()
        print("SUCCESS: User deleted without error")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    session.close()
    
except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()
    
finally:
    # Capture logs
    pgsqlite_proc.terminate()
    output, _ = pgsqlite_proc.communicate(timeout=2)
    
    print("\n=== Relevant logs ===")
    for line in output.splitlines()[-100:]:
        if any(keyword in line for keyword in [
            "Fast path:",
            "orders_total_amount",
            "field_descriptions.is_empty()",
            "send_row_desc",
            "Building field descriptions",
            "No type found",
            "defaulting to TEXT"
        ]):
            print(line)
    
    os.unlink(db_path)