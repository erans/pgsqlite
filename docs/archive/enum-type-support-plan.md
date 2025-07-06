# PostgreSQL ENUM Type Support Implementation Plan for pgsqlite

## Overview

This document outlines the implementation plan for supporting PostgreSQL ENUM types in pgsqlite. ENUMs are custom types that define a static, ordered set of values. They present unique challenges because SQLite doesn't have native ENUM support, and PostgreSQL's implementation involves complex metadata management.

## Current State

- pgsqlite currently has no ENUM support
- Unknown types fall back to TEXT representation
- Missing pg_enum system catalog table
- No CREATE TYPE or ALTER TYPE statement handling

## Implementation Strategy

### Phase 1: Metadata Storage Infrastructure

#### 1.1 Extend __pgsqlite_schema

Add tables to track ENUM types and their values:

```sql
-- Track ENUM type definitions
CREATE TABLE __pgsqlite_enum_types (
    type_oid INTEGER PRIMARY KEY,
    type_name TEXT NOT NULL UNIQUE,
    namespace_oid INTEGER DEFAULT 2200, -- public schema
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

-- Index for efficient lookups
CREATE INDEX idx_enum_values_type ON __pgsqlite_enum_values(type_oid);
CREATE INDEX idx_enum_values_label ON __pgsqlite_enum_values(type_oid, label);
```

#### 1.2 OID Generation Strategy

- Type OIDs: Use hash of type name + fixed offset (e.g., 10000) to avoid conflicts
- Value OIDs: Use hash of type_oid + label + fixed offset (e.g., 20000)
- Ensure OIDs don't conflict with PostgreSQL built-in types (< 10000)

### Phase 2: DDL Statement Handling

#### 2.1 CREATE TYPE Statement

Intercept and handle:
```sql
CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry');
```

Implementation steps:
1. Parse CREATE TYPE statement to extract type name and values
2. Generate type OID and insert into __pgsqlite_enum_types
3. Generate value OIDs and insert into __pgsqlite_enum_values with sort_order
4. Return success to client

#### 2.2 ALTER TYPE Statement

Support these operations:
- `ALTER TYPE name ADD VALUE 'new_value' [BEFORE|AFTER 'existing_value']`
- `ALTER TYPE name RENAME VALUE 'old_value' TO 'new_value'`
- `ALTER TYPE name RENAME TO new_name`

Implementation:
1. Parse ALTER TYPE statement
2. Update __pgsqlite_enum_values or __pgsqlite_enum_types accordingly
3. For ADD VALUE with BEFORE/AFTER, calculate appropriate sort_order

#### 2.3 DROP TYPE Statement

```sql
DROP TYPE mood;
```

Implementation:
1. Check if type is used in any tables (query sqlite_master)
2. If not used, delete from both __pgsqlite_enum_types and __pgsqlite_enum_values
3. If used, return error unless CASCADE is specified

### Phase 3: Table Column Support

#### 3.1 CREATE TABLE with ENUM Columns

When creating tables with ENUM columns:
```sql
CREATE TABLE person (
    id INTEGER PRIMARY KEY,
    current_mood mood
);
```

Implementation:
1. Store as TEXT in SQLite
2. Add CHECK constraint using enum values:
   ```sql
   CHECK (current_mood IN (SELECT label FROM __pgsqlite_enum_values WHERE type_oid = ?))
   ```
3. Store type mapping in __pgsqlite_schema

#### 3.2 Storage Strategy

- Store ENUM values as TEXT (labels) in SQLite
- Convert to/from OIDs at the protocol layer
- This simplifies queries and maintains readability

### Phase 4: System Catalog Implementation

#### 4.1 pg_type Enhancement

Modify pg_type handler to include ENUM types:
- Set typtype = 'e' for ENUM types
- Return proper OID from __pgsqlite_enum_types
- Handle joins with pg_enum

#### 4.2 pg_enum Implementation

Create new catalog handler for pg_enum:
```rust
// src/catalog/pg_enum.rs
pub struct PgEnumHandler;

impl PgEnumHandler {
    pub async fn handle_query(sql: &str, db: &DbHandler) -> Result<QueryResult> {
        // Return columns: oid, enumtypid, enumsortorder, enumlabel
        // Query from __pgsqlite_enum_values
    }
}
```

### Phase 5: Query Execution Support

#### 5.1 Type Resolution

- In Parse phase: Look up ENUM type OIDs from __pgsqlite_enum_types
- In ParameterDescription: Return correct ENUM type OID
- Update type_mappings.rs to recognize ENUM types

#### 5.2 Value Conversion

Text Protocol:
- Input: Accept label strings, validate against __pgsqlite_enum_values
- Output: Return label strings

Binary Protocol:
- Input: Accept OID (4 bytes), convert to label for storage
- Output: Convert label to OID for transmission

#### 5.3 Comparison and Ordering

Implement custom collation for ENUM columns:
```rust
// Register custom collation when database opens
fn register_enum_collation(conn: &Connection, type_oid: i32) -> Result<()> {
    conn.create_collation(
        &format!("ENUM_{}", type_oid),
        |a, b| {
            // Look up sort_order for both values
            // Compare based on sort_order, not alphabetically
        }
    )?;
}
```

### Phase 6: WHERE Clause Support

#### 6.1 Query Rewriting

Rewrite queries to handle ENUM comparisons:
```sql
-- Original
SELECT * FROM person WHERE current_mood = 'happy';

-- Rewritten (validation added)
SELECT * FROM person 
WHERE current_mood = 'happy' 
  AND 'happy' IN (SELECT label FROM __pgsqlite_enum_values WHERE type_oid = ?);
```

#### 6.2 Operator Support

Support all comparison operators (<, >, =, !=, etc.) using sort_order:
```sql
-- For ORDER BY
SELECT * FROM person ORDER BY current_mood;
-- Needs to use custom collation or join with __pgsqlite_enum_values
```

### Phase 7: Type Casting

Support explicit casting:
```sql
SELECT 'happy'::mood;
SELECT mood('happy');
```

Implementation:
1. Intercept cast expressions
2. Validate value exists in __pgsqlite_enum_values
3. Return with proper type OID

### Phase 8: Error Handling

Implement PostgreSQL-compatible error messages:
- Invalid enum value: "invalid input value for enum mood: 'invalid'"
- Type doesn't exist: "type 'mood' does not exist"
- Cannot drop type in use: "cannot drop type mood because other objects depend on it"

## Testing Strategy

1. **Unit Tests**:
   - Test ENUM type creation/alteration/deletion
   - Test value validation and conversion
   - Test comparison operations

2. **Integration Tests**:
   - Test with real PostgreSQL clients (psql, pgAdmin)
   - Test binary and text protocols
   - Test prepared statements with ENUM parameters

3. **Compatibility Tests**:
   - Compare behavior with real PostgreSQL
   - Test edge cases (empty enums, single value, many values)
   - Test transaction rollback behavior

## Performance Considerations

1. **Caching**:
   - Cache ENUM type metadata in memory
   - Invalidate on ALTER TYPE operations
   - Use prepared statements for validation queries

2. **Indexing**:
   - Ensure proper indexes on __pgsqlite_enum_values
   - Consider composite index on (type_oid, label)

3. **Batch Operations**:
   - Optimize bulk inserts with ENUM validation
   - Consider temporary disabling of CHECK constraints

## Migration and Compatibility

1. **Existing Databases**:
   - Provide migration tool to convert TEXT columns to ENUMs
   - Detect potential ENUM columns by CHECK constraints

2. **PostgreSQL Compatibility**:
   - Match PostgreSQL error codes
   - Support same SQL syntax
   - Handle client library expectations

## Implementation Priority

1. **High Priority**:
   - Basic CREATE TYPE support
   - Column storage and retrieval
   - pg_type and pg_enum catalogs
   - Text protocol support

2. **Medium Priority**:
   - ALTER TYPE support
   - Binary protocol support
   - Comparison operators
   - ORDER BY support

3. **Low Priority**:
   - Complex ALTER TYPE operations
   - Performance optimizations
   - Migration tools

## Open Questions

1. Should we support custom ENUM types in `:memory:` databases?
2. How to handle ENUM arrays (mood[])?
3. Should we implement enum_range() and enum_first()/enum_last() functions?
4. How to handle ENUM types in COPY operations?

## Estimated Effort

- Phase 1-2 (Infrastructure & DDL): 2-3 days
- Phase 3-4 (Storage & Catalogs): 3-4 days
- Phase 5-6 (Query Support): 4-5 days
- Phase 7-8 (Casting & Errors): 2-3 days
- Testing & Refinement: 3-4 days

**Total Estimate**: 15-20 days for full implementation

## Alternative Approaches Considered

1. **Store OIDs directly**: Rejected because it complicates SQLite queries
2. **Use INTEGER with mapping table**: Rejected because it breaks text visibility
3. **CHECK constraints only**: Rejected because it doesn't support ordering
4. **Virtual tables**: Rejected as overly complex for this use case

## Conclusion

Supporting ENUM types in pgsqlite requires significant infrastructure but would greatly improve PostgreSQL compatibility. The proposed approach balances compatibility with SQLite's constraints while maintaining good performance and usability.