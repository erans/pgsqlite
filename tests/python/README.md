# SQLAlchemy Integration Tests for pgsqlite

This directory contains comprehensive integration tests for pgsqlite using SQLAlchemy ORM.

## Setup

The tests use Poetry for dependency management with a local `.venv`:

```bash
# Run the test script (it handles all setup)
./run_sqlalchemy_tests.sh

# Or manually:
poetry install
poetry run python test_sqlalchemy_orm.py --port 15400
```

## Test Coverage

- SQLAlchemy ORM model creation and relationships
- Database initialization and connection
- CRUD operations (Create, Read, Update, Delete)
- Complex queries with joins and aggregations
- Transaction handling
- PostgreSQL-specific features compatibility
- System function compatibility (`version()`, `current_database()`, etc.)

## Dependencies

- SQLAlchemy 2.0+ for modern ORM features
- psycopg2-binary for PostgreSQL driver compatibility
- pytest for test framework
- alembic for database migrations (future use)