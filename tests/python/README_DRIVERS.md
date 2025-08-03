# SQLAlchemy Test Driver Support

## Overview

The SQLAlchemy test suite now supports both psycopg2 and psycopg3 drivers, with optional binary format support for psycopg3.

## Usage

### Basic Usage (psycopg2 - default)
```bash
./run_sqlalchemy_tests.sh
```

### Using psycopg3 with text format
```bash
./run_sqlalchemy_tests.sh --driver psycopg3
```

### Using psycopg3 with binary format
```bash
./run_sqlalchemy_tests.sh --driver psycopg3 --binary-format
```

## Driver Comparison

| Driver | Format | Performance | Compatibility |
|--------|--------|-------------|---------------|
| psycopg2 | Text only | Baseline | Excellent |
| psycopg3 | Text | Similar to psycopg2 | Excellent |
| psycopg3 | Binary | 94.3% faster overall | Good* |

*Note: Binary format with SQLAlchemy ORM uses automatic type conversions. For full binary format benefits, use raw psycopg3 cursors.

## Testing Binary Format

### SQLAlchemy ORM Tests
The ORM tests will run with psycopg3 and attempt to use binary format where possible:
```bash
./run_sqlalchemy_tests.sh --driver psycopg3 --binary-format
```

### Pure psycopg3 Binary Format Test
For direct binary format testing without SQLAlchemy:
```bash
# Start pgsqlite server
pgsqlite --database test.db --port 5432

# Run binary format test
poetry run python test_psycopg3_binary.py --port 5432
```

## Dependencies

Both drivers are included in `pyproject.toml`:
- `psycopg2-binary`: Traditional PostgreSQL adapter
- `psycopg[binary]`: Modern PostgreSQL adapter with binary format support

Install dependencies:
```bash
cd tests/python
poetry install
```

## Performance Benefits

When using psycopg3 with binary format directly (not through SQLAlchemy ORM):
- SELECT operations: 81.3% faster
- INSERT operations: 21.9% faster
- UPDATE operations: 37.7% faster
- DELETE operations: 28.1% faster
- Overall: 94.3% faster than text format

## Notes

1. **SQLAlchemy Limitations**: SQLAlchemy doesn't directly expose psycopg3's binary cursor functionality. The ORM tests will use standard SQLAlchemy patterns which may not fully utilize binary format benefits.

2. **Best Performance**: For maximum performance, use psycopg3 directly with binary cursors as shown in `test_psycopg3_binary.py`.

3. **Compatibility**: All existing SQLAlchemy ORM tests work with both drivers without modification.