#!/usr/bin/env python3
"""Debug SQLAlchemy ORM transaction behavior"""

from sqlalchemy import create_engine, Column, Integer, String, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

Base = declarative_base()

class TestModel(Base):
    __tablename__ = 'test_orm'
    
    id = Column(Integer, primary_key=True)
    value = Column(String(50))

# Enable SQLAlchemy logging
import logging
logging.basicConfig()
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

print("=== Testing SQLAlchemy ORM Behavior ===")

# Create engine with StaticPool
engine = create_engine(
    'postgresql://postgres@localhost:5435/main',
    poolclass=StaticPool,
)

# Drop and create table
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

print("\n=== Test 1: ORM Insert and Update ===")
session1 = Session()

# Insert
obj = TestModel(value='initial')
session1.add(obj)
session1.commit()
print(f"Inserted with id: {obj.id}")

# Update in same session
obj.value = 'updated'
print(f"Object dirty: {obj in session1.dirty}")
session1.commit()
print("Update committed")

# Close session
session1.close()

print("\n=== Test 2: Check in New Session ===")
session2 = Session()
obj2 = session2.query(TestModel).filter_by(id=obj.id).first()
print(f"Value in new session: {obj2.value}")

# Try another update
obj2.value = 'updated_again'
print(f"Object dirty: {obj2 in session2.dirty}")
session2.commit()
session2.close()

print("\n=== Test 3: Check Again ===")
session3 = Session()
obj3 = session3.query(TestModel).filter_by(id=obj.id).first()
print(f"Value in third session: {obj3.value}")
session3.close()

print("\n=== Test 4: Expunge and Merge Test ===")
session4 = Session()
obj4 = session4.query(TestModel).filter_by(id=obj.id).first()
print(f"Initial value: {obj4.value}")

# Detach from session
session4.expunge(obj4)
obj4.value = 'detached_update'

# Merge back
merged = session4.merge(obj4)
print(f"Merged object dirty: {merged in session4.dirty}")
session4.commit()
session4.close()

# Check result
session5 = Session()
obj5 = session5.query(TestModel).filter_by(id=obj.id).first()
print(f"Value after merge: {obj5.value}")
session5.close()

print("\n=== Test 5: Raw SQL Check ===")
with engine.connect() as conn:
    result = conn.execute("SELECT value FROM test_orm WHERE id = {}".format(obj.id)).scalar()
    print(f"Raw SQL result: {result}")