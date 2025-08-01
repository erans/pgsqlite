#!/usr/bin/env python3
"""
Fixed SQLAlchemy test - properly handles foreign key deletion order
"""

from test_sqlalchemy import Base, User, Post, Comment, TestRunner
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class FixedTestRunner(TestRunner):
    def test_subqueries(self):
        """Test subquery operations with proper cleanup"""
        session = self.Session()
        try:
            # Clean up from previous tests - proper order for foreign keys
            session.query(Comment).delete()  # Delete Comments first
            session.query(Post).delete()     # Then Posts
            session.query(User).delete()     # Finally Users
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

if __name__ == '__main__':
    import sys
    from sqlalchemy import func
    
    # Test with fixed subquery test
    connection_string = "postgresql://postgres@localhost:5434/test_fixed.db"
    print(f"Testing fixed subquery with pgsqlite at {connection_string}")
    
    runner = FixedTestRunner(connection_string)
    runner.setup()
    
    try:
        runner.run_test("Subquery Operations (Fixed)", runner.test_subqueries)
        print("\n✅ Fixed subquery test passed!")
    except Exception as e:
        print(f"\n❌ Fixed subquery test failed: {e}")
        import traceback
        traceback.print_exc()
    
    runner.teardown()