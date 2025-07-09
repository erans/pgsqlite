# VARCHAR/CHAR Length Constraints Implementation

## Overview

This document describes the implementation of PostgreSQL-compatible VARCHAR, CHAR, and NVARCHAR length constraints in pgsqlite. The implementation ensures that string length constraints specified in table definitions are properly enforced during INSERT and UPDATE operations.

## Architecture

### Components

1. **Schema Storage**: Extended `__pgsqlite_schema` table to store type modifiers
2. **Type Parser**: Enhanced `CreateTableTranslator` to extract length constraints
3. **Validation Layer**: New `StringConstraintValidator` module for constraint checking
4. **Error Handling**: PostgreSQL-compatible error messages for constraint violations
5. **Query Integration**: Validation hooks in query execution pipeline

### Data Flow

```
CREATE TABLE → Parse Types → Store Constraints → __pgsqlite_schema
                                                          ↓
INSERT/UPDATE → Validate Constraints → Execute or Error
```

## Implementation Details

### 1. Schema Migration (v6)

The migration adds a `type_modifier` column to store length constraints:

```sql
ALTER TABLE __pgsqlite_schema ADD COLUMN type_modifier INTEGER;
```

This column stores:
- For `VARCHAR(255)`: type_modifier = 255
- For `CHAR(10)`: type_modifier = 10
- For unbounded types: type_modifier = NULL

### 2. Type Parsing

The `CreateTableTranslator` now extracts length constraints from type definitions:

- `VARCHAR(n)` → pg_type="varchar", type_modifier=n
- `CHARACTER VARYING(n)` → pg_type="varchar", type_modifier=n
- `CHAR(n)` → pg_type="char", type_modifier=n
- `CHARACTER(n)` → pg_type="char", type_modifier=n
- `NVARCHAR(n)` → pg_type="varchar", type_modifier=n

### 3. Constraint Validation

The `StringConstraintValidator` performs these checks:

1. **Character Length**: Uses UTF-8 aware character counting (not byte length)
2. **NULL Handling**: NULL values bypass constraint checking
3. **CHAR Padding**: CHAR types are right-padded with spaces to match PostgreSQL

### 4. Error Handling

PostgreSQL-compatible errors are generated:

- **Error Code**: `22001` (string_data_right_truncation)
- **Message Format**: `value too long for type character varying(n)`
- **Details**: Include column name, actual length, and maximum allowed length

Example:
```
ERROR: value too long for type character varying(10)
DETAIL: Failing row contains (column_name) with 15 characters, maximum is 10.
```

### 5. Performance Considerations

- Constraints are cached in memory after first table access
- Tables without string constraints bypass validation entirely
- Fast path optimization for simple queries remains intact
- Validation only occurs for columns with defined constraints

## Usage Examples

### Creating Tables with Constraints

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(255),
    code CHAR(10),
    description TEXT  -- No constraint
);
```

### Insert Validation

```sql
-- Valid insert
INSERT INTO users (username, email, code) 
VALUES ('john_doe', 'john@example.com', 'ABC123');

-- Invalid insert (username too long)
INSERT INTO users (username, email, code) 
VALUES ('this_username_is_way_too_long_and_exceeds_fifty_characters', 'john@example.com', 'ABC123');
-- ERROR: value too long for type character varying(50)

-- CHAR padding behavior
INSERT INTO users (code) VALUES ('ABC');
-- Stored as 'ABC       ' (padded to 10 characters)
```

### Update Validation

```sql
-- Invalid update
UPDATE users SET username = 'this_is_also_too_long_for_the_varchar_fifty_constraint' 
WHERE id = 1;
-- ERROR: value too long for type character varying(50)
```

## Character vs Byte Length

PostgreSQL counts characters, not bytes. This implementation follows the same behavior:

```sql
CREATE TABLE test (name VARCHAR(5));

-- These are all valid (5 characters each):
INSERT INTO test VALUES ('hello');
INSERT INTO test VALUES ('café☕');  -- 5 characters, but more bytes
INSERT INTO test VALUES ('你好世界了');  -- 5 Chinese characters

-- This fails (6 characters):
INSERT INTO test VALUES ('hello!');
-- ERROR: value too long for type character varying(5)
```

## Compatibility Notes

1. **PostgreSQL Compatibility**: Behavior matches PostgreSQL 14+ for string constraints
2. **SQLite Storage**: Constraints are enforced at the pgsqlite layer, not by SQLite
3. **Case Sensitivity**: Type names are case-insensitive (VARCHAR = varchar)
4. **Default Lengths**: VARCHAR without length specification has no constraint

## Testing

The implementation includes comprehensive tests:

- Basic constraint validation
- Multi-byte character handling
- NULL value handling
- CHAR padding behavior
- Error message format validation
- Performance impact benchmarks

See `tests/varchar_constraints_test.rs` for complete test coverage.

## Future Enhancements

1. **NUMERIC(p,s)**: Precision and scale constraints
2. **Custom Domains**: User-defined types with constraints
3. **Check Constraints**: General-purpose value validation
4. **Array Types**: Length constraints for array elements