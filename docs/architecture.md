# Architecture Overview

This document provides a technical deep dive into how pgsqlite works internally.

## High-Level Architecture

```
┌─────────────────┐     PostgreSQL      ┌──────────────────┐
│                 │     Wire Protocol    │                  │
│  PostgreSQL     │ ◄─────────────────► │     pgsqlite     │
│    Client       │        (v3)         │                  │
│                 │                     │ ┌──────────────┐ │
└─────────────────┘                     │ │              │ │
                                        │ │   SQLite     │ │
                                        │ │   Database   │ │
                                        │ │              │ │
                                        │ └──────────────┘ │
                                        └──────────────────┘
```

## Core Components

### 1. Protocol Layer (`src/protocol/`)

Implements PostgreSQL Wire Protocol v3:

- **Message Codec**: Encodes/decodes PostgreSQL protocol messages
- **Authentication**: Handles client authentication (currently accepts all)
- **SSL Negotiation**: Manages SSL/TLS handshake for secure connections

### 2. Session Management (`src/session/`)

Maintains client connection state:

- **Session State**: Tracks prepared statements, parameters, transaction status
- **Connection Pooling**: Manages SQLite connections efficiently
- **Database Handler**: Thread-safe wrapper around SQLite connection

### 3. Query Processing Pipeline

```
Client Query → Parse → Analyze → Translate → Execute → Format → Response
```

#### Parse Stage
- Extracts query type (SELECT, INSERT, etc.)
- Identifies prepared statement placeholders
- Validates basic SQL syntax

#### Analyze Stage
- Resolves table and column references
- Infers data types from schema
- Builds query metadata

#### Translate Stage
- Converts PostgreSQL-specific syntax to SQLite
- Handles type casting (e.g., `::int4`)
- Rewrites unsupported features

#### Execute Stage
- Runs query against SQLite
- Manages transactions
- Handles prepared statements

#### Format Stage
- Converts SQLite values to PostgreSQL format
- Applies type-specific formatting
- Builds protocol response messages

### 4. Type System (`src/types/`)

#### Type Mapping
Maps PostgreSQL's 100+ types to SQLite's 5 storage classes:

```
PostgreSQL          SQLite Storage    Metadata
----------          --------------    --------
SMALLINT      →     INTEGER          type_oid: 21
INTEGER       →     INTEGER          type_oid: 23
BIGINT        →     INTEGER          type_oid: 20
REAL          →     REAL             type_oid: 700
VARCHAR(n)    →     TEXT             type_oid: 1043
TIMESTAMP     →     INTEGER          type_oid: 1114
DECIMAL       →     TEXT             type_oid: 1700
```

#### Value Conversion
- **Inbound**: PostgreSQL format → SQLite storage
- **Outbound**: SQLite storage → PostgreSQL format
- **Special handling**: BOOLEAN (0/1 ↔ t/f), DECIMAL (rust_decimal)

### 5. SQL Translation (`src/translator/`)

Handles PostgreSQL-specific features:

#### CREATE TABLE Translation
```sql
-- PostgreSQL
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255)
);

-- Translated to SQLite
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT
);
```

#### RETURNING Clause Simulation
```sql
-- PostgreSQL
INSERT INTO users (email) VALUES ('test@example.com') RETURNING id;

-- Simulated in SQLite
INSERT INTO users (email) VALUES ('test@example.com');
SELECT id FROM users WHERE rowid = last_insert_rowid();
```

### 6. Performance Optimizations

#### Multi-Level Caching
```
┌─────────────────┐
│  Result Cache   │  Complete query results (100 entries, 60s TTL)
├─────────────────┤
│  Query Cache    │  Parsed query plans (1000 entries, 600s TTL)
├─────────────────┤
│ Statement Pool  │  Prepared statements (100 statements, LRU)
├─────────────────┤
│  Schema Cache   │  Table metadata (bulk loaded, 300s TTL)
└─────────────────┘
```

#### Fast Path Execution
For simple queries, bypasses full parsing:
- Pattern matching for INSERT/UPDATE/DELETE
- Direct SQLite execution
- Minimal overhead (1.0-1.5x vs raw SQLite)

#### Zero-Copy Architecture
- Memory-mapped values for large data
- Reusable buffer pool
- Direct socket writes

## Data Flow Example

### SELECT Query

```
1. Client sends: SELECT * FROM users WHERE id = 1

2. Protocol decode: Parse message type, extract query

3. Query analysis:
   - Identify SELECT query
   - Find table "users"
   - Check schema cache

4. Translation: (minimal for simple SELECT)

5. Execution:
   - Check result cache
   - If miss, execute on SQLite
   - Fetch results

6. Value conversion:
   - Map SQLite values to PostgreSQL types
   - Format timestamps, booleans, etc.

7. Protocol encode:
   - RowDescription message
   - DataRow messages
   - CommandComplete message

8. Cache results for future queries
```

## Metadata Storage

### Internal Tables

pgsqlite maintains metadata in special tables:

#### `__pgsqlite_schema`
Stores PostgreSQL type information for each column:
```sql
CREATE TABLE __pgsqlite_schema (
    table_name TEXT,
    column_name TEXT,
    data_type TEXT,
    type_oid INTEGER,
    PRIMARY KEY (table_name, column_name)
);
```

#### `__pgsqlite_enums`
Tracks ENUM type definitions:
```sql
CREATE TABLE __pgsqlite_enums (
    enum_name TEXT PRIMARY KEY,
    enum_values TEXT  -- JSON array
);
```

## Transaction Management

- **BEGIN/COMMIT/ROLLBACK**: Direct pass-through to SQLite
- **Savepoints**: Supported via SQLite's savepoint mechanism
- **Isolation**: SQLite's default (SERIALIZABLE)

## Connection Handling

### TCP Connections
```rust
TcpListener → Accept → Spawn Task → Handle Client
                           ↓
                    Session Loop → Process Messages
```

### Unix Socket Connections
Same as TCP but using Unix domain sockets for lower latency.

## Error Handling

Errors are translated to PostgreSQL error codes:

| Condition | PostgreSQL Code | Example |
|-----------|----------------|---------|
| Syntax Error | 42601 | Invalid SQL |
| Constraint Violation | 23505 | Unique constraint |
| Type Mismatch | 42804 | Type casting error |
| No Such Table | 42P01 | Table not found |

## Security Considerations

- **Authentication**: Currently accepts all connections (implement as needed)
- **SSL/TLS**: Full support for encrypted connections
- **SQL Injection**: Prepared statements prevent injection
- **File Access**: Limited to configured database file

## Future Architecture Considerations

1. **Multi-Database Support**: Route to different SQLite files
2. **Read Replicas**: Multiple SQLite connections for reads
3. **Write-Ahead Logging**: Better concurrency with WAL mode
4. **Plugin System**: Extensible function support
5. **Clustering**: Distributed SQLite with consensus

## Development Guidelines

When extending pgsqlite:

1. **Maintain Type Safety**: Always preserve PostgreSQL type semantics
2. **Cache Aggressively**: But invalidate correctly
3. **Fail Gracefully**: Return PostgreSQL-compatible errors
4. **Test Thoroughly**: Both unit and integration tests
5. **Document Changes**: Update this architecture doc