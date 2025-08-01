#!/usr/bin/env python3
"""
Comprehensive SQLAlchemy compatibility test for pgsqlite
Tests common ORM patterns and operations
"""

import time
import sys
from datetime import datetime, date, timedelta
from decimal import Decimal
from typing import List, Optional
import json

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, Date, Text, ForeignKey, JSON, DECIMAL, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, backref
from sqlalchemy.sql import func
from sqlalchemy import and_, or_, not_
from colorama import init, Fore, Style

init(autoreset=True)

Base = declarative_base()

# Define test models
class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    email = Column(String(100), unique=True, nullable=False)
    full_name = Column(String(100))
    age = Column(Integer)
    balance = Column(DECIMAL(10, 2), default=Decimal('0.00'))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=func.now())
    birth_date = Column(Date)
    bio = Column(Text)
    metadata_json = Column(JSON)
    
    # Relationships
    posts = relationship("Post", back_populates="author", cascade="all, delete-orphan")
    
    # Indexes
    __table_args__ = (
        Index('idx_username_email', 'username', 'email'),
        UniqueConstraint('username', 'email', name='unique_user_constraint'),
    )

class Post(Base):
    __tablename__ = 'posts'
    
    id = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    content = Column(Text)
    author_id = Column(Integer, ForeignKey('users.id'))
    view_count = Column(Integer, default=0)
    rating = Column(Float)
    published = Column(Boolean, default=False)
    published_at = Column(DateTime)
    tags = Column(JSON)
    
    # Relationships
    author = relationship("User", back_populates="posts")
    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")

class Comment(Base):
    __tablename__ = 'comments'
    
    id = Column(Integer, primary_key=True)
    post_id = Column(Integer, ForeignKey('posts.id'))
    user_id = Column(Integer, ForeignKey('users.id'))
    content = Column(Text)
    created_at = Column(DateTime, default=func.now())
    
    # Relationships
    post = relationship("Post", back_populates="comments")
    user = relationship("User")

class TestRunner:
    def __init__(self, connection_string: str):
        self.engine = create_engine(connection_string, echo=False)
        self.Session = sessionmaker(bind=self.engine)
        self.results = []
        self.failed_tests = []
        
    def setup(self):
        """Create all tables"""
        print(f"{Fore.YELLOW}Setting up database schema...{Style.RESET_ALL}")
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)
        
    def teardown(self):
        """Clean up"""
        Base.metadata.drop_all(self.engine)
        
    def run_test(self, test_name: str, test_func):
        """Run a single test and record results"""
        print(f"\n{Fore.CYAN}Running: {test_name}{Style.RESET_ALL}")
        try:
            start = time.perf_counter()
            result = test_func()
            elapsed = (time.perf_counter() - start) * 1000
            self.results.append((test_name, "PASS", elapsed, None))
            print(f"  {Fore.GREEN}✓ PASS{Style.RESET_ALL} ({elapsed:.2f}ms)")
            return True
        except Exception as e:
            import traceback
            full_error = traceback.format_exc()
            self.results.append((test_name, "FAIL", 0, str(e)))
            self.failed_tests.append((test_name, str(e)))
            print(f"  {Fore.RED}✗ FAIL: {e}{Style.RESET_ALL}")
            print(f"  {Fore.YELLOW}Full traceback:{Style.RESET_ALL}")
            print(full_error)
            return False
    
    def test_basic_crud(self):
        """Test basic CRUD operations"""
        session = self.Session()
        try:
            # CREATE
            user = User(
                username="john_doe",
                email="john@example.com",
                full_name="John Doe",
                age=30,
                balance=Decimal('1000.50'),
                birth_date=date(1993, 5, 15),
                bio="Software developer",
                metadata_json={"interests": ["coding", "gaming"]}
            )
            session.add(user)
            session.commit()
            
            # READ
            retrieved = session.query(User).filter_by(username="john_doe").first()
            assert retrieved is not None
            assert retrieved.email == "john@example.com"
            assert retrieved.balance == Decimal('1000.50')
            
            # UPDATE
            retrieved.age = 31
            retrieved.metadata_json = {"interests": ["coding", "gaming", "reading"]}
            session.commit()
            
            updated = session.query(User).filter_by(id=retrieved.id).first()
            assert updated.age == 31
            assert len(updated.metadata_json["interests"]) == 3
            
            # DELETE
            session.delete(updated)
            session.commit()
            
            deleted = session.query(User).filter_by(username="john_doe").first()
            assert deleted is None
            
        finally:
            session.close()
    
    def test_relationships(self):
        """Test relationship operations"""
        session = self.Session()
        try:
            # Create user with posts
            user = User(username="blogger", email="blogger@example.com")
            post1 = Post(title="First Post", content="Hello World", author=user)
            post2 = Post(title="Second Post", content="More content", author=user)
            
            session.add(user)
            session.commit()
            
            # Test relationship loading
            loaded_user = session.query(User).filter_by(username="blogger").first()
            assert len(loaded_user.posts) == 2
            
            # Test backref
            loaded_post = session.query(Post).filter_by(title="First Post").first()
            assert loaded_post.author.username == "blogger"
            
            # Test cascade delete
            session.delete(loaded_user)
            session.commit()
            
            orphaned_posts = session.query(Post).filter_by(author_id=loaded_user.id).all()
            assert len(orphaned_posts) == 0
            
        finally:
            session.close()
    
    def test_complex_queries(self):
        """Test complex query patterns"""
        session = self.Session()
        try:
            # Setup test data
            # ages will be: 21, 22, 23, 24, 25 (avg = 23)
            users = [
                User(username=f"user{i}", email=f"user{i}@example.com", age=20+i, balance=Decimal(str(100*i)))
                for i in range(1, 6)
            ]
            session.add_all(users)
            session.commit()
            
            # Test AND/OR conditions
            result = session.query(User).filter(
                or_(
                    User.age > 22,
                    and_(User.age == 21, User.username == "user1")
                )
            ).all()
            assert len(result) == 4
            
            # Test NOT condition
            result = session.query(User).filter(
                not_(User.username.in_(["user1", "user2"]))
            ).all()
            assert len(result) == 3
            
            # Test LIKE pattern
            result = session.query(User).filter(
                User.email.like("%user%")
            ).all()
            assert len(result) == 5
            
            # Test aggregates
            avg_age = session.query(func.avg(User.age)).scalar()
            # SQLite returns Decimal for AVG
            assert float(avg_age) == 23.0  # (21+22+23+24+25)/5
            
            max_balance = session.query(func.max(User.balance)).scalar()
            assert max_balance == Decimal('400')
            
            # Test GROUP BY
            result = session.query(
                User.age,
                func.count(User.id)
            ).group_by(User.age).all()
            assert len(result) == 5
            
        finally:
            session.close()
    
    def test_joins(self):
        """Test various join operations"""
        session = self.Session()
        try:
            # Create test data
            user1 = User(username="author1", email="author1@example.com")
            user2 = User(username="author2", email="author2@example.com")
            
            post1 = Post(title="Post 1", author=user1, published=True)
            post2 = Post(title="Post 2", author=user1, published=False)
            post3 = Post(title="Post 3", author=user2, published=True)
            
            comment1 = Comment(post=post1, user=user2, content="Nice post!")
            comment2 = Comment(post=post1, user=user1, content="Thanks!")
            
            session.add_all([user1, user2, post1, post2, post3, comment1, comment2])
            session.commit()
            
            # Test INNER JOIN
            result = session.query(User, Post).join(Post).filter(Post.published == True).all()
            assert len(result) == 2
            
            # Test LEFT JOIN
            result = session.query(User).outerjoin(Post).filter(
                or_(Post.id == None, Post.published == True)
            ).distinct().all()
            # We should have all users since we're doing LEFT JOIN
            # But we filter to only users with no posts OR published posts
            # So we expect user1 and user2
            assert len(result) >= 2  # At least the two authors
            
            # Test multiple joins
            result = session.query(Comment).join(Post).join(User).filter(
                User.username == "author1"
            ).all()
            assert len(result) == 2
            
        finally:
            session.close()
    
    def test_transactions(self):
        """Test transaction handling"""
        session = self.Session()
        try:
            # Test rollback
            user = User(username="rollback_test", email="rollback@example.com")
            session.add(user)
            session.flush()  # Get ID without committing
            user_id = user.id
            
            session.rollback()
            
            # User should not exist after rollback
            result = session.query(User).filter_by(id=user_id).first()
            assert result is None
            
            # Test commit
            user = User(username="commit_test", email="commit@example.com")
            session.add(user)
            session.commit()
            
            # User should exist after commit
            result = session.query(User).filter_by(username="commit_test").first()
            assert result is not None
            
        finally:
            session.close()
    
    def test_bulk_operations(self):
        """Test bulk insert and update operations"""
        session = self.Session()
        try:
            # Bulk insert
            users = [
                User(username=f"bulk{i}", email=f"bulk{i}@example.com", age=20+i)
                for i in range(100)
            ]
            session.bulk_save_objects(users)
            session.commit()
            
            count = session.query(User).filter(User.username.like("bulk%")).count()
            assert count == 100
            
            # Bulk update
            session.query(User).filter(User.username.like("bulk%")).update(
                {User.is_active: False},
                synchronize_session=False
            )
            session.commit()
            
            inactive_count = session.query(User).filter(
                and_(User.username.like("bulk%"), User.is_active == False)
            ).count()
            assert inactive_count == 100
            
            # Bulk delete
            session.query(User).filter(User.username.like("bulk%")).delete(
                synchronize_session=False
            )
            session.commit()
            
            remaining = session.query(User).filter(User.username.like("bulk%")).count()
            assert remaining == 0
            
        finally:
            session.close()
    
    def test_datetime_operations(self):
        """Test datetime handling"""
        session = self.Session()
        try:
            now = datetime.now()
            
            user = User(
                username="datetime_test",
                email="datetime@example.com",
                created_at=now,
                birth_date=date(1990, 1, 1)
            )
            session.add(user)
            session.commit()
            
            # Test datetime filtering
            result = session.query(User).filter(
                User.created_at >= now - timedelta(minutes=1)
            ).first()
            assert result is not None
            
            # Test date operations
            result = session.query(User).filter(
                User.birth_date < date(2000, 1, 1)
            ).first()
            assert result is not None
            
        finally:
            session.close()
    
    def test_subqueries(self):
        """Test subquery operations"""
        session = self.Session()
        try:
            # Clean up from previous tests
            session.query(Post).delete()
            session.query(User).delete()
            session.commit()
            
            # Create test data
            for i in range(5):
                user = User(username=f"sub{i}", email=f"sub{i}@example.com", age=20+i*5)
                for j in range(i):
                    post = Post(title=f"Post {i}-{j}", author=user)
                    session.add(post)
                session.add(user)
            session.commit()
            
            # Subquery for users with posts
            subq = session.query(Post.author_id).distinct().subquery()
            users_with_posts = session.query(User).filter(
                User.id.in_(subq)
            ).all()
            assert len(users_with_posts) == 4  # user0 has no posts
            
            # Correlated subquery
            users_with_many_posts = session.query(User).filter(
                session.query(func.count(Post.id)).filter(
                    Post.author_id == User.id
                ).scalar_subquery() >= 2
            ).all()
            assert len(users_with_many_posts) == 3
            
        finally:
            session.close()
    
    def run_all_tests(self):
        """Run all tests"""
        print(f"\n{Fore.YELLOW}{'='*60}")
        print(f"SQLAlchemy Compatibility Test Suite")
        print(f"{'='*60}{Style.RESET_ALL}")
        
        self.setup()
        
        tests = [
            ("Basic CRUD Operations", self.test_basic_crud),
            ("Relationship Operations", self.test_relationships),
            ("Complex Queries", self.test_complex_queries),
            ("Join Operations", self.test_joins),
            ("Transaction Handling", self.test_transactions),
            ("Bulk Operations", self.test_bulk_operations),
            ("DateTime Operations", self.test_datetime_operations),
            ("Subquery Operations", self.test_subqueries),
        ]
        
        for test_name, test_func in tests:
            self.run_test(test_name, test_func)
        
        self.print_summary()
        self.teardown()
        
        return len(self.failed_tests) == 0
    
    def print_summary(self):
        """Print test summary"""
        print(f"\n{Fore.YELLOW}{'='*60}")
        print(f"Test Summary")
        print(f"{'='*60}{Style.RESET_ALL}")
        
        passed = sum(1 for _, status, _, _ in self.results if status == "PASS")
        failed = sum(1 for _, status, _, _ in self.results if status == "FAIL")
        total = len(self.results)
        
        print(f"\nTotal Tests: {total}")
        print(f"{Fore.GREEN}Passed: {passed}{Style.RESET_ALL}")
        print(f"{Fore.RED}Failed: {failed}{Style.RESET_ALL}")
        
        if self.failed_tests:
            print(f"\n{Fore.RED}Failed Tests:{Style.RESET_ALL}")
            for test_name, error in self.failed_tests:
                print(f"  - {test_name}: {error}")
        
        # Performance summary
        total_time = sum(time for _, status, time, _ in self.results if status == "PASS")
        if passed > 0:
            avg_time = total_time / passed
            print(f"\n{Fore.CYAN}Performance:{Style.RESET_ALL}")
            print(f"  Total Time: {total_time:.2f}ms")
            print(f"  Average Time: {avg_time:.2f}ms")
        
        if failed == 0:
            print(f"\n{Fore.GREEN}✓ All tests passed!{Style.RESET_ALL}")
        else:
            print(f"\n{Fore.RED}✗ {failed} test(s) failed!{Style.RESET_ALL}")

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Test SQLAlchemy compatibility with pgsqlite')
    parser.add_argument('--port', type=int, default=5432, help='pgsqlite port')
    parser.add_argument('--host', default='localhost', help='pgsqlite host')
    parser.add_argument('--database', default=':memory:', help='Database name')
    args = parser.parse_args()
    
    # Test with pgsqlite
    connection_string = f"postgresql://postgres@{args.host}:{args.port}/{args.database}"
    print(f"{Fore.CYAN}Testing with pgsqlite at {connection_string}{Style.RESET_ALL}")
    
    runner = TestRunner(connection_string)
    success = runner.run_all_tests()
    
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()