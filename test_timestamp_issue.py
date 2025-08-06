#!/usr/bin/env python3
"""Debug timestamp issues in SQLAlchemy queries"""

from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

Base = declarative_base()

class TestModel(Base):
    __tablename__ = 'test_model'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    created_at = Column(DateTime, nullable=False)

# Connect with psycopg3 text mode
engine = create_engine(
    'postgresql+psycopg://postgres@localhost:15432/main',
    echo=True
)

# Drop and create tables
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Create session
Session = sessionmaker(bind=engine)
session = Session()

# Insert a record
print("\n=== INSERTING RECORD ===")
test_record = TestModel(
    id=1,
    name="Test",
    created_at=datetime.now()
)
session.add(test_record)
session.commit()
print(f"Inserted: {test_record.created_at}")

# Query it back
print("\n=== QUERYING RECORD ===")
try:
    record = session.query(TestModel).filter_by(id=1).first()
    print(f"Retrieved: {record.created_at} (type: {type(record.created_at)})")
except Exception as e:
    print(f"ERROR: {e}")

# Try a direct query
print("\n=== DIRECT QUERY ===")
try:
    result = session.execute("SELECT id, name, created_at FROM test_model WHERE id = 1")
    row = result.fetchone()
    print(f"Direct query result: {row}")
except Exception as e:
    print(f"ERROR: {e}")

session.close()