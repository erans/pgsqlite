# PGSQLite Type Mapping PRD

## Overview
This document outlines the design for a type-mapping system for a service that speaks the PostgreSQL wire protocol on top of a SQLite storage backend. The core challenge is ensuring consistent bidirectional type conversion between PostgreSQL and SQLite, especially when SQLite does not natively support the full range of PostgreSQL types.

## Goals
- Preserve PostgreSQL type information when SQLite lacks native equivalents
- Enable correct wire protocol behavior for `CREATE`, `INSERT`, `SELECT`, `UPDATE`, and `DELETE`
- Avoid reliance on column names alone
- Enable inference of types for functions such as `COUNT`, `AVG`, `MAX`, etc.

---

## Type Mapping

### Postgres to SQLite Mapping
The following table maps supported PostgreSQL types to SQLite storage representations:

| PostgreSQL Type | SQLite Type | Custom Type | Notes |
|-----------------|-------------|-------------|-------|
| BOOLEAN         | INTEGER     | -           | Stored as 0/1 |
| SMALLINT        | INTEGER     | -           | Integer range |
| INTEGER         | INTEGER     | -           | Default |
| BIGINT          | INTEGER     | -           | 64-bit signed |
| REAL            | TEXT        | DECIMAL     | 32-bit float stored as decimal for precision |
| DOUBLE PRECISION| TEXT        | DECIMAL     | 64-bit float stored as decimal for precision |
| NUMERIC/DECIMAL | TEXT        | DECIMAL     | Custom type using rust_decimal for precision |
| CHAR/VARCHAR/TEXT| TEXT       | -           | Length ignored |
| UUID            | TEXT        | -           | Format validation in code |
| DATE            | TEXT        | -           | ISO8601 format |
| TIMESTAMP       | TEXT        | -           | ISO8601 format |
| TIME            | TEXT        | -           | HH:MM[:SS[.fff]] |
| BYTEA           | BLOB        | -           | Binary |
| JSON/JSONB      | TEXT        | -           | Validated/serialized in code |
| ENUM            | TEXT        | ENUM        | Full PostgreSQL ENUM support with CHECK constraints |
| ARRAY           | TEXT        | -           | JSON string |
| SERIAL/BIGSERIAL| INTEGER     | -           | Use AUTOINCREMENT |
| MONEY           | TEXT        | -           | Currency values with validation |
| INT4RANGE       | TEXT        | -           | Integer ranges stored as strings |
| INT8RANGE       | TEXT        | -           | Big integer ranges stored as strings |
| NUMRANGE        | TEXT        | -           | Numeric ranges stored as strings |
| CIDR            | TEXT        | -           | Network addresses with prefix |
| INET            | TEXT        | -           | IP addresses with optional prefix |
| MACADDR         | TEXT        | -           | 6-byte MAC addresses |
| MACADDR8        | TEXT        | -           | 8-byte MAC addresses |
| BIT             | TEXT        | -           | Fixed-length bit strings |
| BIT VARYING     | TEXT        | -           | Variable-length bit strings |

### Custom Types

PGSQLite implements custom SQLite types using user-defined functions to provide better PostgreSQL compatibility:

#### DECIMAL Type
The DECIMAL custom type provides arbitrary precision decimal arithmetic using the rust_decimal library. This ensures accurate calculations without floating-point errors.

**Functions provided:**
- `decimal_from_text(text)` - Convert text to DECIMAL
- `decimal_to_text(decimal)` - Convert DECIMAL to text
- `decimal_add(a, b)` - Addition
- `decimal_sub(a, b)` - Subtraction
- `decimal_mul(a, b)` - Multiplication
- `decimal_div(a, b)` - Division
- `decimal_sum()` - Aggregate SUM
- `decimal_avg()` - Aggregate AVG
- `decimal_min()` - Aggregate MIN
- `decimal_max()` - Aggregate MAX

#### ENUM Type
PostgreSQL ENUM types are fully supported with automatic CHECK constraint generation:

**DDL Support:**
- `CREATE TYPE name AS ENUM ('value1', 'value2', ...)` - Create new ENUM type
- `ALTER TYPE name ADD VALUE 'new_value' [BEFORE|AFTER 'existing_value']` - Add enum values
- `DROP TYPE name [IF EXISTS]` - Drop ENUM type with dependency checking

**Implementation:**
- ENUM values are stored as TEXT in SQLite
- CHECK constraints enforce valid values at the database level
- Metadata stored in `__pgsqlite_enum_types` and `__pgsqlite_enum_values` tables
- Full system catalog integration (pg_type, pg_enum)
- Type casting support with both `::` and `CAST()` syntax

### Unmapped PostgreSQL Types
The following PostgreSQL native types are not yet mapped to SQLite equivalents:

#### Geometric Types
| PostgreSQL Type | Suggested SQLite Type | Notes |
|-----------------|----------------------|-------|
| POINT           | TEXT                 | Store as JSON: {"x": x, "y": y} |
| LINE            | TEXT                 | Store as JSON: {"A": a, "B": b, "C": c} |
| LSEG            | TEXT                 | Store as JSON: [{"x": x1, "y": y1}, {"x": x2, "y": y2}] |
| BOX             | TEXT                 | Store as JSON: {"upper_right": {...}, "lower_left": {...}} |
| PATH            | TEXT                 | Store as JSON array of points |
| POLYGON         | TEXT                 | Store as JSON array of points |
| CIRCLE          | TEXT                 | Store as JSON: {"center": {...}, "radius": r} |

#### Text Search Types
| PostgreSQL Type | Suggested SQLite Type | Notes |
|-----------------|----------------------|-------|
| TSVECTOR        | TEXT                 | Store as serialized format |
| TSQUERY         | TEXT                 | Store as serialized format |

#### Date/Time Types
| PostgreSQL Type | Suggested SQLite Type | Notes |
|-----------------|----------------------|-------|
| INTERVAL        | TEXT                 | Store as ISO 8601 duration |
| TSRANGE         | TEXT                 | Store as JSON with timestamps |
| TSTZRANGE       | TEXT                 | Store as JSON with timestamps |
| DATERANGE       | TEXT                 | Store as JSON with dates |

#### Other Types
| PostgreSQL Type | Suggested SQLite Type | Notes |
|-----------------|----------------------|-------|
| XML             | TEXT                 | Validate XML structure |
| PG_LSN          | TEXT                 | Store as string |
| TXID_SNAPSHOT   | TEXT                 | Store as string |
| Composite Types | TEXT                 | Store as JSON |
| Domain Types    | (base type)          | Map to underlying base type |
| OID Types       | INTEGER/TEXT         | Depends on specific OID type |

---

## CREATE TABLE Handling
When receiving a `CREATE TABLE` statement via the PostgreSQL protocol, the original column types should be parsed and stored in a metadata registry. This registry serves as the source of truth for type mapping.

### Metadata Tables
To persist type information across restarts, pgsqlite uses special metadata tables:

#### Schema Metadata
```sql
CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
  table_name TEXT NOT NULL,
  column_name TEXT NOT NULL,
  pg_type TEXT NOT NULL,
  sqlite_type TEXT NOT NULL,
  PRIMARY KEY (table_name, column_name)
);
```

#### ENUM Type Metadata
```sql
CREATE TABLE IF NOT EXISTS __pgsqlite_enum_types (
  type_name TEXT PRIMARY KEY,
  type_oid INTEGER NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS __pgsqlite_enum_values (
  type_name TEXT NOT NULL,
  enum_value TEXT NOT NULL,
  enum_label_oid INTEGER NOT NULL UNIQUE,
  sort_order INTEGER NOT NULL,
  PRIMARY KEY (type_name, enum_value),
  FOREIGN KEY (type_name) REFERENCES __pgsqlite_enum_types(type_name)
);
```

### Example
```sql
CREATE TABLE users (
  id UUID PRIMARY KEY,
  profile JSONB,
  active BOOLEAN,
  balance NUMERIC(10,2)
);
```
Would store the following in `__pgsqlite_schema`:

| table_name | column_name | pg_type | sqlite_type |
|------------|-------------|---------|-------------|
| users      | id          | UUID    | TEXT        |
| users      | profile     | JSONB   | TEXT        |
| users      | active      | BOOLEAN | INTEGER     |
| users      | balance     | NUMERIC | DECIMAL     |

---

## SELECT and PRAGMA Support
SQLite's `PRAGMA table_info(table)` provides column types such as `TEXT`, `INTEGER`, etc. To reverse map correctly to PostgreSQL types:

1. First consult the `__pgsqlite_schema` registry.
2. If unavailable, fall back to `PRAGMA`-based inference.

Example fallback:
```sql
PRAGMA table_info('users');
```

---

## INSERT / UPDATE / DELETE Handling
When handling inserts or updates:
- Use type OIDs (PostgreSQL protocol) to map values
- Validate or convert values (e.g., parse UUIDs, validate JSON)
- Use registry to resolve expected Postgres type

---

## Aggregation Type Inference
When handling expressions like `COUNT(*)`, `AVG(col)`, etc., infer the result type using known input types from the registry:

| Function | Inferred PG Type |
|----------|------------------|
| COUNT    | BIGINT           |
| SUM      | Depends on input |
| AVG      | DOUBLE           |
| MAX/MIN  | Same as column   |

---

## JSON and UUID Support
Because SQLite stores JSON/JSONB and UUID as `TEXT`, conversion must be performed in code:

### JSON Example
```rust
let value: serde_json::Value = serde_json::from_str(&text_column)?;
```

### UUID Example
```rust
let uuid: Uuid = Uuid::parse_str(&text_column)?;
```
