# PostgreSQL ENUM Type Implementation in pgsqlite

## Overview

This document describes the actual implementation of PostgreSQL ENUM type support in pgsqlite. ENUMs are custom types that define a static, ordered set of values. The implementation uses a trigger-based validation approach that allows dynamic updates to ENUM values, including support for `ALTER TYPE ADD VALUE` on existing tables.

## Key Design Decision: Trigger-Based Validation

The implementation uses **database triggers** instead of CHECK constraints for ENUM validation. This critical design decision enables:

- `ALTER TYPE ADD VALUE` to work correctly with existing tables
- Dynamic validation against the current set of ENUM values
- Better PostgreSQL compatibility

## Architecture Components

### 1. Metadata Storage

ENUM metadata is stored in two system tables:

```sql
-- Track ENUM type definitions
CREATE TABLE __pgsqlite_enum_types (
    type_oid INTEGER PRIMARY KEY,
    type_name TEXT NOT NULL UNIQUE,
    namespace_oid INTEGER DEFAULT 2200,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Track ENUM values with ordering
CREATE TABLE __pgsqlite_enum_values (
    value_oid INTEGER PRIMARY KEY,
    type_oid INTEGER NOT NULL,
    label TEXT NOT NULL,
    sort_order REAL NOT NULL,
    FOREIGN KEY (type_oid) REFERENCES __pgsqlite_enum_types(type_oid),
    UNIQUE (type_oid, label)
);

-- Track ENUM column usage for dependency checking
CREATE TABLE __pgsqlite_enum_usage (
    table_name TEXT NOT NULL,
    column_name TEXT NOT NULL,
    enum_type TEXT NOT NULL,
    PRIMARY KEY (table_name, column_name)
);
```

### 2. Validation Triggers

For each ENUM column, the system creates INSERT and UPDATE triggers that validate values at runtime:

```sql
-- Example trigger for mood column in test_enums table
CREATE TRIGGER IF NOT EXISTS "__pgsqlite_enum_insert_test_enums_user_mood"
BEFORE INSERT ON "test_enums"
FOR EACH ROW
WHEN NEW."user_mood" IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM __pgsqlite_enum_values ev
    JOIN __pgsqlite_enum_types et ON ev.type_oid = et.type_oid
    WHERE et.type_name = 'mood' AND ev.label = NEW."user_mood"
)
BEGIN
    SELECT RAISE(ABORT, 'invalid input value for enum mood: "' || NEW."user_mood" || '"');
END
```

### 3. Core Modules

#### EnumMetadata (`src/metadata/enum_metadata.rs`)
- Manages ENUM type creation, modification, and deletion
- Handles value ordering and OID generation
- Provides caching through `EnumCache` for performance

#### EnumTriggers (`src/metadata/enum_triggers.rs`)
- Creates and manages validation triggers
- Records ENUM usage in `__pgsqlite_enum_usage`
- Handles trigger cleanup on DROP TYPE CASCADE

#### EnumDdlHandler (`src/ddl/enum_ddl_handler.rs`)
- Intercepts and processes ENUM-related DDL statements
- Supports CREATE TYPE, ALTER TYPE ADD VALUE, and DROP TYPE
- Handles CASCADE operations for dependent objects

### 4. DDL Support

#### CREATE TYPE
```sql
CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral');
```
- Creates entries in `__pgsqlite_enum_types` and `__pgsqlite_enum_values`
- Generates stable OIDs using hash functions

#### ALTER TYPE ADD VALUE
```sql
ALTER TYPE mood ADD VALUE 'excited' AFTER 'happy';
ALTER TYPE mood ADD VALUE 'confused' BEFORE 'sad';
```
- Adds new values with proper sort ordering
- Works immediately on existing tables due to trigger-based validation
- Supports BEFORE/AFTER positioning

#### DROP TYPE
```sql
DROP TYPE mood;                    -- Fails if type is in use
DROP TYPE mood CASCADE;            -- Drops triggers and cleans up usage
```
- Checks dependencies via `__pgsqlite_enum_usage`
- CASCADE option removes all triggers and usage records

### 5. Table Integration

When creating tables with ENUM columns:

```sql
CREATE TABLE test_enums (
    id SERIAL PRIMARY KEY,
    user_mood mood,
    task_status status DEFAULT 'pending'
);
```

The system:
1. Stores ENUM columns as TEXT in SQLite
2. Records column metadata in `__pgsqlite_schema`
3. Creates validation triggers for each ENUM column
4. Records usage in `__pgsqlite_enum_usage`

### 6. Query Support

#### Type Resolution
- ENUM types are recognized during Parse phase
- Always transmitted as TEXT (OID 25) in wire protocol
- Type OIDs are mapped correctly for client compatibility

#### Value Operations
- INSERT/UPDATE: Validated by triggers
- SELECT: Works directly with TEXT values
- Comparisons: Standard TEXT comparison (alphabetical)
- Type casting: Supports both `::` and `CAST()` syntax

### 7. System Catalog Integration

#### pg_type
- ENUM types appear with `typtype = 'e'`
- Proper OIDs are assigned from `__pgsqlite_enum_types`

#### pg_enum
- Returns all ENUM values with their sort order
- Fully compatible with PostgreSQL's pg_enum structure

#### pg_attribute
- ENUM columns show correct type OIDs
- Integrates with `__pgsqlite_schema` for type information

### 8. Error Handling

PostgreSQL-compatible error messages:
```
invalid input value for enum mood: "invalid_value"
Type 'nonexistent' does not exist
cannot drop type mood because other objects depend on it
```

## Performance Optimizations

1. **EnumCache**: In-memory cache for ENUM metadata
2. **Prepared Statements**: Trigger queries are optimized by SQLite
3. **Indexed Lookups**: Proper indexes on metadata tables

## Testing

Comprehensive test coverage includes:
- Unit tests for all ENUM operations
- Integration tests with psql client (`test_queries.sql`)
- Error handling tests
- Binary protocol compatibility tests

## Limitations

1. **Ordering**: ENUMs are ordered alphabetically, not by definition order
2. **Arrays**: ENUM arrays (e.g., `mood[]`) are not yet supported
3. **Functions**: PostgreSQL ENUM functions not implemented

## Migration from CHECK Constraints

The system automatically uses triggers for new ENUM columns. Existing databases using CHECK constraints would need migration (not automated).

## Conclusion

The trigger-based implementation provides full PostgreSQL ENUM compatibility while working within SQLite's constraints. The key innovation is using triggers instead of CHECK constraints, enabling dynamic ENUM value updates that work correctly with existing tables.