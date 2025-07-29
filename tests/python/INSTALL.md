# Installation Guide for SQLAlchemy Tests

## Prerequisites

### Option 1: Using Poetry (Recommended)

1. Install Poetry:
   ```bash
   curl -sSL https://install.python-poetry.org | python3 -
   # Add Poetry to PATH (follow instructions shown)
   ```

2. Run the comprehensive tests:
   ```bash
   cd tests/python
   ./run_sqlalchemy_tests.sh
   ```

### Option 2: Using pip

1. Install dependencies:
   ```bash
   python3 -m pip install --user sqlalchemy psycopg2-binary
   ```

2. Run the simple test:
   ```bash
   cd tests/python
   ./run_simple_test.sh
   ```

### Option 3: Minimal Test (Verification Only)

1. Install minimal dependency:
   ```bash
   python3 -m pip install --user psycopg2-binary
   ```

2. Run minimal test:
   ```bash
   cd tests/python
   ./run_minimal_test.sh
   ```

## Test Scripts

- **`run_sqlalchemy_tests.sh`** - Full comprehensive ORM test suite with Poetry
- **`run_simple_test.sh`** - Basic SQLAlchemy test with pip
- **`run_minimal_test.sh`** - Minimal compatibility verification

## What the Tests Cover

### Comprehensive Test (`test_sqlalchemy_orm.py`)
- ✅ SQLAlchemy ORM model creation with relationships
- ✅ Connection establishment and system functions
- ✅ CRUD operations (Create, Read, Update, Delete)
- ✅ Complex queries with joins and aggregations
- ✅ Transaction handling and rollback
- ✅ Numeric precision and decimal handling
- ✅ PostgreSQL compatibility features

### Simple Test (`simple_sqlalchemy_test.py`)
- ✅ Basic connection and system functions
- ✅ Table creation and basic operations
- ✅ Essential compatibility verification

### Minimal Test (`test_minimal.py`)
- ✅ System function compatibility (`version()`, `current_database()`, etc.)
- ✅ Basic database operations
- ✅ Verification that the original SQLAlchemy error is fixed

## Expected Output

When tests pass, you should see:
```
🎉 SUCCESS: All SQLAlchemy integration tests passed!
✅ pgsqlite is fully compatible with SQLAlchemy ORM
```

This confirms that the original `psycopg2.errors.SyntaxErrorOrAccessRuleViolation: Query execution failed: SQLite error: near "(": syntax error in SELECT pg_catalog.version()` error has been resolved.