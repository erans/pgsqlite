#\!/usr/bin/env python3
"""Test MAX aggregate with DECIMAL returns correct type"""

from sqlalchemy import create_engine, Column, Integer, String, DECIMAL, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from decimal import Decimal

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    username = Column(String(50))
    balance = Column(DECIMAL(10, 2))

# Connect to pgsqlite
engine = create_engine('postgresql://postgres@localhost:5434/test_aggregate.db', echo=False)
Session = sessionmaker(bind=engine)
session = Session()

# Setup
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

# Add test data
users = [
    User(username=f"user{i}", balance=Decimal(str(100*i)))
    for i in range(1, 6)
]
session.add_all(users)
session.commit()

print("Testing MAX(balance) aggregate type fix...")
try:
    # Test that caused "Unknown PG numeric type: 25" error
    max_balance = session.query(func.max(User.balance)).scalar()
    print(f"✅ MAX(balance) = {max_balance} (type: {type(max_balance)})")
    assert max_balance == Decimal('500')
    print("✅ Assertion passed\!")
    
    # Test with alias (SQLAlchemy's actual pattern)
    result = session.query(func.max(User.balance).label('max_1')).first()
    print(f"✅ With alias: max_1 = {result.max_1} (type: {type(result.max_1)})")
    assert result.max_1 == Decimal('500')
    print("✅ Alias test passed\!")
    
    print("\n✅ All aggregate type tests passed\!")
except Exception as e:
    print(f"❌ FAILED: {e}")
    import traceback
    traceback.print_exc()

session.close()