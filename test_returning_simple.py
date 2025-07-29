#!/usr/bin/env python3
"""Simple test to debug RETURNING clause issue with SQLAlchemy."""

import sys
from sqlalchemy import create_engine, Column, Integer, String, text
from sqlalchemy.orm import declarative_base, sessionmaker

Base = declarative_base()

class TestUser(Base):
    __tablename__ = "test_returning_users"
    
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

def main(port):
    # Create engine with echo enabled
    engine = create_engine(
        f"postgresql://postgres:postgres@localhost:{port}/main",
        echo=True,
        pool_pre_ping=True,
    )
    
    # Create table
    Base.metadata.create_all(engine)
    
    print("\n=== Test 1: Raw SQL with RETURNING ===")
    with engine.connect() as conn:
        # Test raw SQL with RETURNING
        result = conn.execute(
            text("INSERT INTO test_returning_users (name) VALUES (:name) RETURNING id"),
            {"name": "Test User 1"}
        )
        returned_id = result.scalar()
        print(f"Returned ID from raw SQL: {returned_id}")
        conn.commit()
    
    print("\n=== Test 2: ORM insert with RETURNING ===")
    Session = sessionmaker(bind=engine)
    with Session() as session:
        user = TestUser(name="Test User 2")
        session.add(user)
        try:
            session.commit()
            print(f"User ID after commit: {user.id}")
        except Exception as e:
            print(f"Error during ORM insert: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_returning_simple.py <port>")
        sys.exit(1)
    
    main(int(sys.argv[1]))