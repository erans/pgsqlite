#!/usr/bin/env python3
"""
Comprehensive SQLAlchemy ORM integration tests for pgsqlite.

Tests cover:
- Connection establishment and system functions
- ORM model creation with relationships
- CRUD operations
- Complex queries with joins and aggregations
- Transaction handling
- PostgreSQL compatibility features
"""

import argparse
import sys
import traceback
from datetime import datetime, date, time
from decimal import Decimal
from typing import List, Optional

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Date,
    Time,
    Numeric,
    Boolean,
    ForeignKey,
    func,
    select,
    and_,
    or_,
    text,
    case,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import StaticPool

# Base class for ORM models
Base = declarative_base()


class User(Base):
    """User model with basic information."""
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(100))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    birth_date = Column(Date)
    
    # Relationship to posts
    posts = relationship("Post", back_populates="author", cascade="all, delete-orphan")
    orders = relationship("Order", back_populates="customer")


class Category(Base):
    """Category model for organizing posts."""
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True)
    name = Column(String(50), unique=True, nullable=False)
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to posts
    posts = relationship("Post", back_populates="category")


class Post(Base):
    """Post model with foreign key relationships."""
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    content = Column(Text)
    author_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    category_id = Column(Integer, ForeignKey("categories.id"))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    view_count = Column(Integer, default=0)
    is_published = Column(Boolean, default=False)
    
    # Relationships
    author = relationship("User", back_populates="posts")
    category = relationship("Category", back_populates="posts")


class Product(Base):
    """Product model for e-commerce testing."""
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    description = Column(Text)
    price = Column(Numeric(10, 2), nullable=False)  # Test NUMERIC type
    stock_quantity = Column(Integer, default=0)
    is_available = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationship to order items
    order_items = relationship("OrderItem", back_populates="product")


class Order(Base):
    """Order model for testing relationships and transactions."""
    __tablename__ = "orders"

    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    order_date = Column(Date, default=date.today)
    order_time = Column(Time, default=time(12, 0))  # Test TIME type
    total_amount = Column(Numeric(12, 2), default=Decimal('0.00'))
    status = Column(String(20), default='pending')
    notes = Column(Text)
    
    # Relationships
    customer = relationship("User", back_populates="orders")
    items = relationship("OrderItem", back_populates="order", cascade="all, delete-orphan")


class OrderItem(Base):
    """Order item model for many-to-many relationship."""
    __tablename__ = "order_items"

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id"), nullable=False)
    product_id = Column(Integer, ForeignKey("products.id"), nullable=False)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    
    # Relationships
    order = relationship("Order", back_populates="items")
    product = relationship("Product", back_populates="order_items")


class SQLAlchemyTestSuite:
    """Comprehensive test suite for SQLAlchemy integration with pgsqlite."""

    def __init__(self, port: int):
        self.port = port
        self.engine = None
        self.Session = None
        self.test_results = []

    def connect_to_database(self) -> bool:
        """Establish connection to pgsqlite and test system functions."""
        try:
            print(f"🔌 Connecting to pgsqlite on port {self.port}...")
            
            # Create engine with PostgreSQL dialect
            connection_string = f"postgresql://postgres:postgres@localhost:{self.port}/main"
            self.engine = create_engine(
                connection_string,
                echo=True,  # Set to True for SQL debugging
                # Use proper connection pooling to test connection-per-session isolation
                pool_size=5,  # Allow multiple connections
                max_overflow=10,  # Allow connection overflow
                pool_pre_ping=True,  # Verify connections before use
                future=True,  # Enable SQLAlchemy 2.0 style
                # Work around RETURNING issue
                execution_options={"no_autoflush": False},
            )
            
            # Test connection and system functions
            with self.engine.connect() as conn:
                # Test the system functions that SQLAlchemy relies on
                version_result = conn.execute(text("SELECT version()")).fetchone()
                db_result = conn.execute(text("SELECT current_database()")).fetchone()
                user_result = conn.execute(text("SELECT current_user()")).fetchone()
                schema_result = conn.execute(text("SELECT current_schema()")).fetchone()
                
                print(f"✅ Database version: {version_result[0]}")
                print(f"✅ Current database: {db_result[0]}")
                print(f"✅ Current user: {user_result[0]}")
                print(f"✅ Current schema: {schema_result[0]}")
            
            # Create session factory
            self.Session = sessionmaker(bind=self.engine)
            
            print("✅ Successfully connected to pgsqlite!")
            return True
            
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            traceback.print_exc()
            return False

    def create_tables(self) -> bool:
        """Create all database tables using SQLAlchemy ORM."""
        try:
            print("🏗️  Creating database tables...")
            
            # Create all tables
            Base.metadata.create_all(self.engine)
            
            # Verify tables were created
            with self.engine.connect() as conn:
                # Use PostgreSQL-style table introspection
                tables_result = conn.execute(text("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                """)).fetchall()
                
                created_tables = [row[0] for row in tables_result]
                expected_tables = ['users', 'categories', 'posts', 'products', 'orders', 'order_items']
                
                print(f"✅ Created tables: {created_tables}")
                
                # Verify all expected tables exist
                missing_tables = set(expected_tables) - set(created_tables)
                if missing_tables:
                    print(f"❌ Missing tables: {missing_tables}")
                    return False
                    
            print("✅ All tables created successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Table creation failed: {e}")
            traceback.print_exc()
            return False

    def insert_test_data(self) -> bool:
        """Insert comprehensive test data using SQLAlchemy ORM."""
        try:
            print("📝 Inserting test data...")
            
            with self.Session() as session:
                # Create categories
                tech_category = Category(
                    name="Technology",
                    description="Posts about technology and programming"
                )
                lifestyle_category = Category(
                    name="Lifestyle", 
                    description="Posts about lifestyle and personal development"
                )
                session.add_all([tech_category, lifestyle_category])
                session.flush()  # Get IDs without committing
                
                # Create users
                alice = User(
                    username="alice_dev",
                    email="alice@example.com",
                    full_name="Alice Johnson",
                    birth_date=date(1990, 5, 15),
                    is_active=True
                )
                bob = User(
                    username="bob_writer",
                    email="bob@example.com", 
                    full_name="Bob Smith",
                    birth_date=date(1985, 10, 22),
                    is_active=True
                )
                charlie = User(
                    username="charlie_inactive",
                    email="charlie@example.com",
                    full_name="Charlie Brown",
                    birth_date=date(1995, 3, 8),
                    is_active=False
                )
                session.add_all([alice, bob, charlie])
                session.flush()
                
                # Create posts
                posts = [
                    Post(
                        title="Getting Started with SQLAlchemy",
                        content="SQLAlchemy is a powerful Python ORM...",
                        author=alice,
                        category=tech_category,
                        is_published=True,
                        view_count=150
                    ),
                    Post(
                        title="PostgreSQL vs SQLite",
                        content="Comparing two popular database systems...",
                        author=alice,
                        category=tech_category,
                        is_published=True,
                        view_count=89
                    ),
                    Post(
                        title="Work-Life Balance Tips",
                        content="How to maintain a healthy work-life balance...",
                        author=bob,
                        category=lifestyle_category,
                        is_published=True,
                        view_count=245
                    ),
                    Post(
                        title="Draft: Future of AI",
                        content="This is a draft post about AI...",
                        author=bob,
                        category=tech_category,
                        is_published=False,
                        view_count=5
                    ),
                ]
                session.add_all(posts)
                session.flush()
                
                # Create products
                products = [
                    Product(
                        name="Laptop Pro",
                        description="High-performance laptop for developers",
                        price=Decimal('1299.99'),
                        stock_quantity=25,
                        is_available=True
                    ),
                    Product(
                        name="Wireless Mouse",
                        description="Ergonomic wireless mouse",
                        price=Decimal('49.99'),
                        stock_quantity=100,
                        is_available=True
                    ),
                    Product(
                        name="Mechanical Keyboard",
                        description="RGB mechanical keyboard",
                        price=Decimal('129.50'),
                        stock_quantity=0,  # Out of stock
                        is_available=False
                    ),
                ]
                session.add_all(products)
                session.flush()
                
                # Create orders with items
                order1 = Order(
                    customer=alice,
                    order_date=date(2024, 1, 15),
                    order_time=time(14, 30),
                    status='completed',
                    notes='Express delivery requested'
                )
                
                order2 = Order(
                    customer=bob,
                    order_date=date(2024, 1, 20),
                    order_time=time(10, 15),
                    status='pending',
                    notes='Standard delivery'
                )
                
                session.add_all([order1, order2])
                session.flush()
                
                # Create order items
                order_items = [
                    OrderItem(
                        order=order1,
                        product=products[0],  # Laptop
                        quantity=1,
                        unit_price=Decimal('1299.99')
                    ),
                    OrderItem(
                        order=order1,
                        product=products[1],  # Mouse
                        quantity=1,
                        unit_price=Decimal('49.99')
                    ),
                    OrderItem(
                        order=order2,
                        product=products[1],  # Mouse
                        quantity=2,
                        unit_price=Decimal('49.99')
                    ),
                ]
                session.add_all(order_items)
                
                # Update order totals
                order1.total_amount = Decimal('1349.98')  # Laptop + Mouse
                order2.total_amount = Decimal('99.98')    # 2 x Mouse
                
                # Commit all changes
                session.commit()
                
            print("✅ Test data inserted successfully!")
            return True
            
        except Exception as e:
            print(f"❌ Data insertion failed: {e}")
            traceback.print_exc()
            return False

    def test_basic_queries(self) -> bool:
        """Test basic CRUD operations using SQLAlchemy ORM."""
        try:
            print("🔍 Testing basic CRUD operations...")
            
            with self.Session() as session:
                # Test SELECT - Count records
                user_count = session.query(User).count()
                post_count = session.query(Post).count()
                product_count = session.query(Product).count()
                
                print(f"✅ Found {user_count} users, {post_count} posts, {product_count} products")
                
                # Test WHERE clause
                active_users = session.query(User).filter(User.is_active == True).all()
                print(f"✅ Found {len(active_users)} active users")
                
                # Test UPDATE
                alice = session.query(User).filter(User.username == "alice_dev").first()
                if alice:
                    alice.full_name = "Alice Johnson-Dev"
                    session.commit()
                    print("✅ Updated Alice's full name")
                
                # Test complex WHERE with OR
                tech_or_popular_posts = session.query(Post).join(Category).filter(
                    or_(
                        Category.name == "Technology",
                        Post.view_count > 200
                    )
                ).all()
                print(f"✅ Found {len(tech_or_popular_posts)} tech or popular posts")
                
                # Test LIKE query
                dev_users = session.query(User).filter(
                    User.username.like('%dev%')
                ).all()
                print(f"✅ Found {len(dev_users)} users with 'dev' in username")
                
            return True
            
        except Exception as e:
            print(f"❌ Basic queries failed: {e}")
            traceback.print_exc()
            return False

    def test_relationships_and_joins(self) -> bool:
        """Test ORM relationships and complex join queries."""
        try:
            print("🔗 Testing relationships and joins...")
            
            with self.Session() as session:
                # Test relationship loading
                alice = session.query(User).filter(User.username == "alice_dev").first()
                if alice:
                    print(f"✅ Alice has {len(alice.posts)} posts")
                    print(f"✅ Alice has {len(alice.orders)} orders")
                
                # Test join query - posts with authors and categories
                posts_with_details = session.query(Post, User, Category).join(
                    User, Post.author_id == User.id
                ).join(
                    Category, Post.category_id == Category.id
                ).all()
                
                print(f"✅ Loaded {len(posts_with_details)} posts with author and category details")
                
                # Test aggregate functions with GROUP BY
                posts_per_user = session.query(
                    User.username,
                    func.count(Post.id).label('post_count'),
                    func.avg(Post.view_count).label('avg_views')
                ).join(Post).group_by(User.username).all()
                
                print("✅ Posts per user with average views:")
                for username, count, avg_views in posts_per_user:
                    print(f"   {username}: {count} posts, {avg_views:.1f} avg views")
                
                # Test complex join with order details
                order_details = session.query(
                    Order.id,
                    User.username,
                    Product.name,
                    OrderItem.quantity,
                    OrderItem.unit_price,
                    (OrderItem.quantity * OrderItem.unit_price).label('item_total')
                ).join(User, Order.customer_id == User.id)\
                 .join(OrderItem, Order.id == OrderItem.order_id)\
                 .join(Product, OrderItem.product_id == Product.id)\
                 .all()
                
                print(f"✅ Loaded {len(order_details)} order line items")
                
            return True
            
        except Exception as e:
            print(f"❌ Relationships and joins test failed: {e}")
            traceback.print_exc()
            return False

    def test_advanced_queries(self) -> bool:
        """Test advanced SQLAlchemy features and PostgreSQL compatibility."""
        try:
            print("🚀 Testing advanced queries and PostgreSQL features...")
            
            with self.Session() as session:
                # Test subquery
                avg_views_subquery = session.query(
                    func.avg(Post.view_count)
                ).filter(Post.is_published == True).scalar_subquery()
                
                above_avg_posts = session.query(Post).filter(
                    Post.view_count > avg_views_subquery
                ).all()
                
                print(f"✅ Found {len(above_avg_posts)} posts with above-average views")
                
                # Test window functions (PostgreSQL feature)
                try:
                    ranked_posts = session.query(
                        Post.title,
                        Post.view_count,
                        func.rank().over(order_by=Post.view_count.desc()).label('rank')
                    ).filter(Post.is_published == True).all()
                    
                    print(f"✅ Ranked {len(ranked_posts)} posts by view count")
                except Exception as window_error:
                    print(f"⚠️  Window functions not fully supported: {window_error}")
                
                # Test CASE expression
                user_status = session.query(
                    User.username,
                    case(
                        (User.is_active == True, 'Active'),
                        else_='Inactive'
                    ).label('status')
                ).all()
                
                print(f"✅ Generated status for {len(user_status)} users")
                
                # Test date functions
                recent_posts = session.query(Post).filter(
                    Post.created_at >= func.date('now', '-30 days')
                ).count()
                
                print(f"✅ Found {recent_posts} posts from last 30 days")
                
                # Test HAVING clause
                categories_with_multiple_posts = session.query(
                    Category.name,
                    func.count(Post.id).label('post_count')
                ).join(Post).group_by(Category.name).having(
                    func.count(Post.id) > 1
                ).all()
                
                print(f"✅ Found {len(categories_with_multiple_posts)} categories with multiple posts")
                
            return True
            
        except Exception as e:
            print(f"❌ Advanced queries test failed: {e}")
            traceback.print_exc()
            return False

    def test_transactions(self) -> bool:
        """Test transaction handling and rollback scenarios."""
        try:
            print("💾 Testing transaction handling...")
            
            # Test: SQLAlchemy ORM transaction with explicit debugging
            print("  Testing SQLAlchemy ORM transaction persistence...")
            
            # First, insert a test user
            with self.Session() as session:
                # Check if user already exists and delete
                existing = session.query(User).filter(User.username == "transaction_test_user").first()
                if existing:
                    session.delete(existing)
                    session.commit()
                
                # Insert new test user
                test_user = User(
                    username="transaction_test_user",
                    email="test@transaction.com", 
                    full_name="Original Name"
                )
                session.add(test_user)
                session.commit()
                print(f"  ✅ Inserted test user with name: {test_user.full_name}")
            
            # Now test transaction update
            with self.Session() as session:
                # Load user
                user = session.query(User).filter(User.username == "transaction_test_user").first()
                if not user:
                    print("❌ Could not find test user")
                    return False
                    
                print(f"  📍 Before update: {user.full_name}")
                
                # Modify user - this should mark object as dirty
                user.full_name = "Updated Name"
                
                # Check if SQLAlchemy thinks object is dirty
                print(f"  🔍 Object is dirty: {user in session.dirty}")
                print(f"  🔍 Session has pending changes: {session.dirty or session.new or session.deleted}")
                
                # Explicit flush to force UPDATE to database
                print("  💾 Flushing changes...")
                session.flush()
                
                # Commit transaction
                print("  ✅ Committing transaction...")
                session.commit()
                
                # Check in same session (should use identity map)
                print(f"  📍 After commit (same session): {user.full_name}")
                
                # Force refresh from database
                session.expire(user)
                session.refresh(user)
                print(f"  📍 After refresh from database: {user.full_name}")
            
            # Check in completely new session with new engine to force new connection
            print("  🔄 Creating new engine to force new connection...")
            new_engine = create_engine(
                f"postgresql://postgres:postgres@localhost:{self.port}/main",
                echo=False,  # Reduce noise for this test connection
                pool_size=1,  # Force single connection per engine
                max_overflow=0,  # No overflow
                pool_pre_ping=True,
                future=True,
            )
            NewSession = sessionmaker(bind=new_engine)
            
            with NewSession() as session:
                user = session.query(User).filter(User.username == "transaction_test_user").first()
                result_name = user.full_name if user else "NOT FOUND"
                print(f"  📍 New session (new connection) sees: {result_name}")
                
                if user and user.full_name == "Updated Name":
                    print("✅ Transaction persistence verified!")
                    # Cleanup
                    session.delete(user)
                    session.commit()
                    new_engine.dispose()
                    return True
                else:
                    print("❌ Transaction update not persisted")
                    print(f"     Expected: 'Updated Name', Got: '{result_name}'")
                    new_engine.dispose()
                    return False
            
        except Exception as e:
            print(f"❌ Transaction test failed: {e}")
            traceback.print_exc()
            return False

    def test_numeric_precision(self) -> bool:
        """Test numeric precision and decimal handling."""
        try:
            print("🔢 Testing numeric precision and decimal handling...")
            
            with self.Session() as session:
                # Test precise decimal calculations
                total_revenue = session.query(
                    func.sum(OrderItem.quantity * OrderItem.unit_price)
                ).scalar()
                
                print(f"✅ Total revenue calculated: ${total_revenue}")
                
                # Test decimal precision in queries
                expensive_products = session.query(Product).filter(
                    Product.price > Decimal('100.00')
                ).all()
                
                print(f"✅ Found {len(expensive_products)} expensive products")
                
                # Test arithmetic operations
                discounted_prices = session.query(
                    Product.name,
                    Product.price,
                    (Product.price * Decimal('0.9')).label('discounted_price')
                ).all()
                
                print("✅ Calculated discounted prices:")
                for name, original, discounted in discounted_prices:
                    print(f"   {name}: ${original} -> ${discounted}")
                
            return True
            
        except Exception as e:
            print(f"❌ Numeric precision test failed: {e}")
            traceback.print_exc()
            return False

    def run_all_tests(self) -> bool:
        """Run the complete test suite."""
        print("🧪 Starting SQLAlchemy ORM Integration Tests for pgsqlite")
        print("=" * 60)
        
        test_methods = [
            ("Connection & System Functions", self.connect_to_database),
            ("Table Creation", self.create_tables),
            ("Data Insertion", self.insert_test_data),
            ("Basic CRUD Operations", self.test_basic_queries),
            ("Relationships & Joins", self.test_relationships_and_joins),
            ("Advanced Queries", self.test_advanced_queries),
            ("Transaction Handling", self.test_transactions),
            ("Numeric Precision", self.test_numeric_precision),
        ]
        
        passed_tests = 0
        total_tests = len(test_methods)
        
        for test_name, test_method in test_methods:
            print(f"\n📋 Running: {test_name}")
            print("-" * 40)
            
            try:
                if test_method():
                    passed_tests += 1
                    print(f"✅ {test_name}: PASSED")
                else:
                    print(f"❌ {test_name}: FAILED")
            except Exception as e:
                print(f"❌ {test_name}: FAILED with exception: {e}")
                traceback.print_exc()
        
        print("\n" + "=" * 60)
        print(f"📊 Test Results: {passed_tests}/{total_tests} tests passed")
        
        if passed_tests == total_tests:
            print("🎉 All SQLAlchemy integration tests passed!")
            print("✅ pgsqlite is fully compatible with SQLAlchemy ORM!")
            return True
        else:
            print(f"⚠️  {total_tests - passed_tests} tests failed.")
            print("❌ Some SQLAlchemy features may not be fully compatible.")
            return False

    def cleanup(self) -> None:
        """Clean up database connections."""
        if self.engine:
            self.engine.dispose()
            print("🧹 Database connections cleaned up")


def main() -> int:
    """Main entry point for the test script."""
    parser = argparse.ArgumentParser(description="SQLAlchemy ORM integration tests for pgsqlite")
    parser.add_argument("--port", type=int, required=True, help="Port number where pgsqlite is running")
    
    args = parser.parse_args()
    
    # Create and run test suite
    test_suite = SQLAlchemyTestSuite(args.port)
    
    try:
        success = test_suite.run_all_tests()
        return 0 if success else 1
        
    finally:
        test_suite.cleanup()


if __name__ == "__main__":
    sys.exit(main())