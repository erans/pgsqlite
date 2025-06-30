# pgsqlite TODO List

## ✅ Performance Optimization - COMPLETED (2025-06-30)

### Background
Investigated replacing the channel-based DbHandler with a direct multi-threaded implementation using SQLite's FULLMUTEX mode.

### Performance Findings
Benchmark results comparing implementations (1000 operations each):

| Implementation | INSERT | SELECT | UPDATE | DELETE | Notes |
|----------------|--------|--------|--------|--------|-------|
| Raw SQLite | 0.005ms | 0.006ms | 0.005ms | 0.004ms | Baseline |
| Mutex Handler | 0.036ms | 0.046ms | 0.040ms | 0.038ms | 7.7-9.6x overhead (CHOSEN) |
| Direct Executor | 0.038ms | 0.050ms | 0.043ms | 0.042ms | 8.1-10.7x overhead |
| Simple Executor | 0.036ms | 0.047ms | 0.040ms | 0.039ms | 7.7-9.9x overhead |
| Channel-based | 0.094ms | 0.159ms | 0.092ms | 0.083ms | 20-27x overhead |

**Key Achievement**: Mutex-based implementation provides 2.2-3.5x performance improvement over channels.

### Final Implementation
[x] Implemented and deployed **Mutex-based DbHandler** as the sole database handler:
- Uses `parking_lot::Mutex` for efficient synchronization
- Single SQLite connection with `SQLITE_OPEN_FULL_MUTEX` flag
- Thread-safe and Send+Sync compatible
- Maintains all features: schema cache, fast path optimization, transaction support

### Work Completed
- [x] Benchmarked multiple implementations (channel, direct, simple, mutex)
- [x] Created mutex-based implementation with best performance characteristics
- [x] Removed all experimental implementations (direct_handler, simple_executor, etc.)
- [x] Updated session module to use single DbHandler implementation
- [x] Documented architectural decision in CLAUDE.md
- [x] Cleaned up codebase to remove unused modules

### Architectural Decision Rationale
Chose mutex-based approach because:
1. Best performance (7.7-9.6x overhead vs 20-27x for channels)
2. Simplest implementation (single connection, no pool complexity)
3. Thread-safe through parking_lot::Mutex + SQLite FULLMUTEX
4. Minimal code changes required
5. Trade-offs acceptable (no parallel reads, potential mutex contention under extreme load)

# pgsqlite TODO List

## How to Use This TODO List

This file tracks all future development tasks for the pgsqlite project. It serves as a comprehensive roadmap for features, improvements, and fixes that need to be implemented.

### Adding New Tasks
- Add new tasks under the appropriate section or create a new section if needed
- Use the checkbox format: `- [ ] Task description`
- Be specific and actionable in task descriptions
- Include technical details when helpful (e.g., "Store in __pgsqlite_schema table")
- Group related tasks under subsections for better organization

### Marking Tasks as Complete
- Change `- [ ]` to `- [x]` when a task is fully implemented and tested
- Only mark as complete when the feature is:
  - Fully implemented with all edge cases handled
  - Tested and working correctly
  - Integrated with the existing codebase
  - Documentation updated if needed

### Removing Completed Tasks
- Remove tasks from this list ONLY when they are completely done
- Do not remove tasks that are partially complete or have known issues
- Keep completed tasks marked with `[x]` temporarily for tracking, then remove during periodic cleanup
- If a task reveals additional subtasks during implementation, add those subtasks before removing the parent task

### Task Priority
- Tasks are roughly organized by importance and logical implementation order
- High-priority items that affect core functionality are listed first
- Consider dependencies between tasks when planning implementation

## Type System Enhancements

### Code Quality - Magic Numbers
- [ ] Replace OID type magic numbers with PgType enum values (e.g., replace hardcoded 1700 for Numeric with proper enum references)

### Schema Validation and Drift Detection
- [ ] Implement schema drift detection between __pgsqlite_schema and actual SQLite tables
- [ ] Check for mismatches on connection startup/first query
- [ ] Return appropriate PostgreSQL error when drift is detected
- [ ] Handle cases where columns exist in SQLite but not in __pgsqlite_schema
- [ ] Handle cases where __pgsqlite_schema has columns missing from SQLite table
- [ ] Validate column types match between schema metadata and SQLite PRAGMA table_info

### VARCHAR/NVARCHAR Length Constraints
- [ ] Store VARCHAR(n) and NVARCHAR(n) length constraints in __pgsqlite_schema
- [ ] Validate string lengths on INSERT/UPDATE operations
- [ ] Return proper PostgreSQL error when length constraints are violated
- [ ] Handle character vs byte length for multi-byte encodings

### NUMERIC/DECIMAL Precision and Scale
- [ ] Store NUMERIC(p,s) precision and scale in __pgsqlite_schema
- [ ] Enforce precision and scale constraints on INSERT/UPDATE
- [ ] Format decimal values according to specified scale before returning results
- [ ] Handle rounding/truncation according to PostgreSQL behavior

### Decimal Query Rewriting - Cast Detection
- [ ] Implement implicit cast detection in decimal query rewriting
- [ ] Handle implicit casts in comparisons (e.g., `integer_column = '123.45'`)
- [ ] Detect function parameter implicit casts to decimal types
- [ ] Support type promotion in arithmetic operations (integer + decimal -> decimal)
- [ ] Handle assignment casts in INSERT/UPDATE statements
- [ ] Implement full PostgreSQL-style implicit cast analysis in ExpressionTypeResolver

### Decimal Query Rewriting - Context Handling
- [x] Fixed correlated subquery context inheritance (outer table columns now properly recognized in subqueries)
- [x] Improved aggregate function decimal wrapping (only wrap NUMERIC types, not FLOAT types)
- [x] Enhanced derived table decimal type propagation for WHERE clause rewriting
- [x] Fixed recursive CTE decimal rewriting (arithmetic operations in recursive part of UNION now properly rewritten)
- [ ] Optimize context merging performance for deeply nested subqueries

### CHAR Type Support
- [ ] Implement CHAR(n) with proper blank-padding behavior
- [ ] Store fixed length in __pgsqlite_schema
- [ ] Pad values to specified length on storage
- [ ] Handle comparison semantics (trailing space handling)

## Data Type Improvements

### Date/Time Types
- [ ] Implement INTERVAL type support
- [ ] Add TIME WITH TIME ZONE support
- [ ] Implement proper timezone handling for TIMESTAMP WITH TIME ZONE
- [ ] Support PostgreSQL date/time functions (date_trunc, extract, etc.)

### Array Types
- [ ] Complete array type implementation for all base types
- [ ] Support multi-dimensional arrays
- [ ] Implement array operators and functions
- [ ] Handle array literals in queries

### Geometric Types
- [ ] Implement POINT, LINE, LSEG, BOX, PATH, POLYGON, CIRCLE types
- [ ] Add geometric operators and functions
- [ ] Store as JSON or custom format in SQLite

### JSON/JSONB
- [ ] Implement JSONB type (binary JSON)
- [ ] Add JSON operators (->, ->>, @>, etc.)
- [ ] Support JSON path expressions
- [ ] Implement JSON aggregation functions

## Protocol Features

### Connection Methods
- [x] Unix domain socket support
  - [x] Add --socket-dir command line option
  - [x] Create socket file as .s.PGSQL.{port} in specified directory
  - [x] Handle socket file cleanup on shutdown
  - [x] Support both TCP and Unix socket listeners simultaneously
  - [x] Implement proper socket permissions

### Prepared Statements
- [ ] Full support for prepared statement lifecycle
- [ ] Parameter type inference improvements
- [ ] Named prepared statements
- [ ] DEALLOCATE support

### Copy Protocol
- [ ] Implement COPY TO for data export
- [ ] Implement COPY FROM for bulk data import
- [ ] Support both text and binary formats
- [ ] Handle CSV format options

### Extended Query Protocol
- [ ] Portal management (multiple portals per session)
- [ ] Cursor support with FETCH
- [ ] Row count limits in Execute messages

## Query Features

### CTEs and Subqueries
- [x] Recursive CTE decimal rewriting support (fixed table alias resolution for recursive parts)
- [ ] Materialized CTEs
- [ ] Lateral joins
- [x] Correlated subqueries with decimal operations (fixed context inheritance for outer table references)

### Window Functions
- [ ] Implement missing window functions
- [ ] Support all frame specifications
- [ ] Handle EXCLUDE clause
- [ ] Optimize performance for large windows

### Full Text Search
- [ ] Implement tsvector and tsquery types
- [ ] Add text search operators
- [ ] Support text search configurations
- [ ] Implement ts_rank and ts_headline

## Performance and Storage

### Caching and Optimization
- [x] Schema metadata caching to avoid repeated PRAGMA table_info queries
- [x] Query plan caching for parsed INSERT statements
- [x] SQLite WAL mode + multi-threaded support with connection pooling
  - [x] Separate read/write connection pools
  - [x] Connection affinity for transactions
  - [x] Shared cache for in-memory databases
  - [x] Fix concurrent access test failures (implemented RAII connection return)
  - [x] Optimize connection pool management
- [ ] Batch INSERT support for multi-row inserts
- [ ] Fast path for simple INSERTs that don't need decimal rewriting
- [ ] Cache SQLite prepared statements for reuse
- [ ] Remove debug logging from hot paths
- [ ] Direct read-only access optimization (bypass channels for SELECT)

### Indexing
- [ ] Support for expression indexes
- [ ] Partial index support
- [ ] Multi-column index statistics
- [ ] Index-only scans where applicable

### Query Optimization
- [ ] Cost-based query planning
- [ ] Join order optimization
- [ ] Subquery unnesting
- [ ] Common subexpression elimination

### Storage Optimization
- [ ] Compression for large text/blob values
- [ ] Efficient storage for sparse columns
- [ ] Table partitioning support
- [ ] Vacuum and analyze equivalents

## Compatibility and Standards

### SQL Compliance
- [ ] LATERAL joins
- [ ] GROUPING SETS, CUBE, ROLLUP
- [ ] VALUES lists as tables
- [ ] Full MERGE statement support

### PostgreSQL Compatibility
- [ ] System catalogs (pg_class, pg_attribute, etc.)
- [ ] Information schema views
- [ ] PostgreSQL-specific functions
- [ ] Extension mechanism (CREATE EXTENSION)

### Error Handling
- [ ] Complete PostgreSQL error code mapping
- [ ] Detailed error positions in queries
- [ ] HINT and DETAIL in error messages
- [ ] Proper constraint violation messages

## Administrative Features

### Security
- [ ] Row-level security policies
- [ ] Column-level permissions
- [ ] SSL/TLS connection support
  - [ ] Implement SSL negotiation in protocol handler
  - [ ] Support sslmode options (disable, allow, prefer, require, verify-ca, verify-full)
  - [ ] Certificate-based authentication
  - [ ] Configure SSL cert/key paths via command line or config
  - [ ] Support PostgreSQL SSL protocol flow
- [ ] Authentication methods (md5, scram-sha-256)

### Monitoring
- [ ] Query statistics collection
- [ ] Connection pooling stats
- [ ] Performance metrics export
- [ ] Slow query logging

### Configuration
- [ ] Runtime parameter system (SET/SHOW)
- [ ] Configuration file support
- [ ] Per-database settings
- [ ] Connection limits and timeouts

## Testing and Quality

### Test Coverage
- [ ] Comprehensive type conversion tests
- [ ] Protocol compliance test suite
- [ ] Performance benchmarks
- [ ] Stress testing for concurrent connections

### Documentation
- [ ] API documentation
- [ ] Migration guide from PostgreSQL
- [ ] Performance tuning guide
- [ ] Troubleshooting guide