# SQLAlchemy Tests Usage Guide

## Quick Start

The tests are now properly configured to use Poetry with a local `.venv` in the `tests/python` directory.

### Option 1: Full Integration Test (Recommended)

```bash
cd tests/python
./run_sqlalchemy_tests.sh
```

This script will:
1. ✅ Check prerequisites (Poetry, Rust, Python)
2. ✅ Build pgsqlite in release mode
3. ✅ Start pgsqlite server on port 15500
4. ✅ Create local `.venv` and install dependencies
5. ✅ Run comprehensive SQLAlchemy ORM tests
6. ✅ Clean up automatically

### Option 2: Manual Setup

```bash
cd tests/python

# Set up Poetry environment
poetry config virtualenvs.in-project true --local
poetry install --only main

# Test the environment
poetry run python test_poetry_setup.py

# Run the full test suite (after starting pgsqlite manually)
poetry run python test_sqlalchemy_orm.py --port 15500
```

### Option 3: Quick Test

```bash
cd tests/python
./quick_test.sh
```

## Files Overview

### Core Test Files
- **`test_sqlalchemy_orm.py`** - Comprehensive SQLAlchemy ORM test suite
- **`test_poetry_setup.py`** - Environment verification test
- **`test_minimal.py`** - Basic compatibility test (psycopg2 only)

### Bash Scripts
- **`run_sqlalchemy_tests.sh`** - Full automated test runner (Poetry-based)
- **`quick_test.sh`** - Quick Poetry environment test
- **`run_simple_test.sh`** - Fallback script using pip
- **`run_minimal_test.sh`** - Basic functionality test

### Configuration
- **`pyproject.toml`** - Poetry configuration with SQLAlchemy dependencies
- **`.gitignore`** - Excludes `.venv/`, `*.db`, `*.log`, etc.

## Dependencies

The Poetry configuration includes:
- **SQLAlchemy 2.0+** - Modern ORM features
- **psycopg2-binary** - PostgreSQL driver compatibility

## Test Coverage

### System Functions (Fixed SQLAlchemy Error)
- ✅ `version()` - Returns PostgreSQL-compatible version string  
- ✅ `current_database()`, `current_schema()`, `current_user()`
- ✅ Process and network information functions
- ✅ Privilege checking functions

### ORM Features
- ✅ Model creation with relationships (Users, Posts, Products, Orders)
- ✅ CRUD operations with complex queries
- ✅ Joins, aggregations, and subqueries
- ✅ Transaction handling with rollback
- ✅ Numeric precision (DECIMAL/NUMERIC types)
- ✅ Date/time handling

## Expected Output

When successful, you'll see:
```
🎉 SUCCESS: All SQLAlchemy integration tests passed!
✅ pgsqlite is fully compatible with SQLAlchemy ORM
```

This confirms the original SQLAlchemy compatibility issue has been resolved.

## Troubleshooting

### Poetry Installation Issues
If Poetry installation times out, you can manually install dependencies:
```bash
cd tests/python
python3 -m venv .venv
source .venv/bin/activate
pip install sqlalchemy psycopg2-binary
```

### Port Conflicts
If port 15500 is in use, the scripts will automatically detect this and exit. You can manually specify a different port in the test scripts.

### Dependencies Not Found
Make sure Poetry is properly installed and accessible in your PATH:
```bash
poetry --version  # Should show Poetry version
```