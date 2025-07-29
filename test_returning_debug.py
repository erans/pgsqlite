#!/usr/bin/env python3
"""Debug test for RETURNING clause issue with SQLAlchemy ORM."""

import sys
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

Base = declarative_base()

class User(Base):
    """User model matching the one in test_sqlalchemy_orm.py"""
    __tablename__ = "debug_users"
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False) 
    full_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

def main(port):
    # Create engine with same options as main test
    engine = create_engine(
        f"postgresql://postgres:postgres@localhost:{port}/main",
        echo=True,
        poolclass=StaticPool,  # Same as main test
        future=True,  # Same as main test
    )
    
    # Create table
    Base.metadata.create_all(engine)
    
    Session = sessionmaker(bind=engine)
    
    print("\n=== Test: ORM insert with complex model ===")
    try:
        with Session() as session:
            user = User(
                username="test_user_debug",
                email="debug@test.com",
                full_name="Debug User"
            )
            session.add(user)
            print(f"Before commit - User ID: {user.id}")
            session.commit()
            print(f"After commit - User ID: {user.id}")
            print("✅ SUCCESS: Complex model INSERT with RETURNING worked!")
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        
        # Try to debug what's happening
        print("\n=== Debugging the issue ===")
        with engine.connect() as conn:
            # Try raw SQL
            result = conn.execute("SELECT 1")
            print(f"Raw SELECT works: {result.scalar()}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python test_returning_debug.py <port>")
        sys.exit(1)
    
    main(int(sys.argv[1]))