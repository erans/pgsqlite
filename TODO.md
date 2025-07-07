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

---

## 🚀 HIGH PRIORITY - Core Functionality & Performance

### Type System Enhancements

#### Schema Validation and Drift Detection
- [ ] Implement schema drift detection between __pgsqlite_schema and actual SQLite tables
- [ ] Check for mismatches on connection startup/first query
- [ ] Return appropriate PostgreSQL error when drift is detected
- [ ] Handle cases where columns exist in SQLite but not in __pgsqlite_schema
- [ ] Handle cases where __pgsqlite_schema has columns missing from SQLite table
- [ ] Validate column types match between schema metadata and SQLite PRAGMA table_info

#### VARCHAR/NVARCHAR Length Constraints
- [ ] Store VARCHAR(n) and NVARCHAR(n) length constraints in __pgsqlite_schema
- [ ] Validate string lengths on INSERT/UPDATE operations
- [ ] Return proper PostgreSQL error when length constraints are violated
- [ ] Handle character vs byte length for multi-byte encodings

#### NUMERIC/DECIMAL Precision and Scale
- [ ] Store NUMERIC(p,s) precision and scale in __pgsqlite_schema
- [ ] Enforce precision and scale constraints on INSERT/UPDATE
- [ ] Format decimal values according to specified scale before returning results
- [ ] Handle rounding/truncation according to PostgreSQL behavior

#### CHAR Type Support
- [ ] Implement CHAR(n) with proper blank-padding behavior
- [ ] Store fixed length in __pgsqlite_schema
- [ ] Pad values to specified length on storage
- [ ] Handle comparison semantics (trailing space handling)

### Query Optimization

#### Decimal Query Rewriting - Cast Detection
- [ ] Implement implicit cast detection in decimal query rewriting
- [ ] Handle implicit casts in comparisons (e.g., `integer_column = '123.45'`)
- [ ] Detect function parameter implicit casts to decimal types
- [ ] Support type promotion in arithmetic operations (integer + decimal -> decimal)
- [ ] Handle assignment casts in INSERT/UPDATE statements
- [ ] Implement full PostgreSQL-style implicit cast analysis in ExpressionTypeResolver

#### Decimal Query Rewriting - Context Handling
- [ ] Optimize context merging performance for deeply nested subqueries

#### Performance Enhancements
- [x] Profile protocol serialization overhead - COMPLETED (2025-07-06)
  - Identified protocol framing (20-30%), type conversions (30-40%) as main bottlenecks
  - Added itoa for 21% faster integer formatting
  - Fixed unnecessary clones in batch sending
  - Determined ryu float formatting is slower than stdlib
- [x] Implement small value optimization to avoid heap allocations - COMPLETED (2025-07-06)
  - Added SmallValue enum for zero-allocation handling of common values
  - Achieved 8% improvement in cached SELECT queries
  - 3% improvement in UPDATE/DELETE operations
- [ ] Consider lazy schema loading for better startup performance
- [ ] Implement connection pooling with warm statement caches
- [ ] Add query pattern recognition for automatic optimization hints
- [ ] Batch INSERT support for multi-row inserts
- [ ] Fast path for simple INSERTs that don't need decimal rewriting
- [ ] Cache SQLite prepared statements for reuse
- [ ] Direct read-only access optimization (bypass channels for SELECT)

### Protocol Features

#### Prepared Statements
- [ ] Full support for prepared statement lifecycle
- [x] Parameter type inference improvements - COMPLETED (2025-07-03)
  - Fixed explicit type specification via prepare_typed
  - Proper handling of binary format parameters
  - Correct type inference for simple parameter queries
- [ ] Named prepared statements
- [ ] DEALLOCATE support

#### Copy Protocol
- [ ] Implement COPY TO for data export
- [ ] Implement COPY FROM for bulk data import
- [ ] Support both text and binary formats
- [ ] Handle CSV format options

#### Extended Query Protocol
- [ ] Portal management (multiple portals per session)
- [ ] Cursor support with FETCH
- [ ] Row count limits in Execute messages

---

## 📊 MEDIUM PRIORITY - Feature Completeness

### Data Type Improvements

#### Date/Time Types - IN PROGRESS
- [x] **Phase 1: Type Mapping and Storage (High Priority)** - COMPLETED (2025-07-07)
  - [x] Add TIMETZ (1266) and INTERVAL (1186) to PgType enum
  - [x] Update type mappings to use REAL (Unix timestamps) for all datetime types
  - [x] Create migration v3 to add datetime_format and timezone_offset columns to __pgsqlite_schema
  - [x] Implement storage format: Unix timestamps with fractional seconds for microsecond precision
- [ ] **Phase 2: Value Conversion Layer (High Priority)**
  - [x] Implement text protocol conversion (PostgreSQL format ↔ Unix timestamp) - COMPLETED (2025-07-07)
  - [ ] Implement binary protocol conversion (PostgreSQL binary ↔ Unix timestamp)
  - [ ] Handle special values (infinity, -infinity)
  - [x] Support microsecond precision with fractional seconds - COMPLETED (2025-07-07)
- [ ] **Phase 3: Query Translation (Medium Priority)**
  - [ ] Map PostgreSQL datetime functions to SQLite equivalents
  - [ ] Implement EXTRACT, DATE_TRUNC, AGE functions
  - [ ] Handle AT TIME ZONE operator
  - [ ] Support interval arithmetic with timestamps
- [ ] **Phase 4: Advanced Features (Medium Priority)**
  - [ ] Session timezone management
  - [ ] Complex interval handling (months/years)
  - [ ] Timezone database support for common timezones
  - [ ] Performance optimization with caching
- [ ] **Phase 5: Testing and Documentation (High Priority)**
  - [ ] Unit tests for all conversions
  - [ ] Integration tests with PostgreSQL clients
  - [ ] Migration guide for existing users
  - [ ] Performance benchmarks

#### Array Types
- [ ] Complete array type implementation for all base types
- [ ] Support multi-dimensional arrays
- [ ] Implement array operators and functions
- [ ] Handle array literals in queries

#### ENUM Types
- [x] Phase 1: Metadata Storage Infrastructure - COMPLETED (2025-07-05)
  - Created __pgsqlite_enum_types and __pgsqlite_enum_values tables
  - Implemented EnumMetadata module with full CRUD operations
  - Added EnumCache for performance optimization
  - Stable OID generation for types and values
  - Comprehensive unit tests
- [x] Phase 2: DDL Statement Handling - COMPLETED (2025-07-05)
  - Implemented CREATE TYPE AS ENUM interception
  - Support ALTER TYPE ADD VALUE with BEFORE/AFTER positioning
  - Handle DROP TYPE with IF EXISTS support
  - Regex-based parsing for ENUM DDL statements
  - Integration with query executor in execute_ddl method
- [x] Phase 3: Table Column Support - COMPLETED (2025-07-05)
  - Modified CREATE TABLE translator to recognize ENUM columns
  - Generate CHECK constraints automatically for ENUM validation
  - Store ENUM type mappings in __pgsqlite_schema
  - Support multiple ENUM columns in same table
  - Handle ENUM values with quotes properly
- [x] Phase 4: System Catalog Implementation - COMPLETED (2025-07-05)
  - Created pg_enum handler for catalog queries
  - Enhanced pg_type to include ENUM types (OID assignment)
  - Updated pg_attribute for ENUM columns with proper type OIDs
  - Full integration with catalog interceptor
- [x] Phase 5: Query Execution Support - COMPLETED (2025-07-05)
  - Type resolution in Parse phase with OID mapping
  - Text/binary protocol conversion working correctly
  - Parameter type inference for ENUMs in extended protocol
  - Always send ENUMs as TEXT OID (25) in wire protocol
- [x] Phase 6: WHERE Clause Support - COMPLETED (2025-07-05)
  - WHERE clauses work natively through CHECK constraints
  - No query rewriting needed - SQLite handles via CHECK
  - Equality, IN/NOT IN, and NULL comparisons all working
  - Ordering works alphabetically by default
- [x] Phase 7: Type Casting - COMPLETED (2025-07-05)
  - Explicit casting support for both :: and CAST() syntax
  - CastTranslator handles PostgreSQL cast syntax translation
  - Integration with both simple and extended protocol
  - CHECK constraints validate cast values at runtime
- [x] Phase 8: Error Handling & Polish - COMPLETED (2025-07-06)
  - PostgreSQL-compatible error messages for constraint violations
  - Better error formatting for invalid enum values (e.g., "invalid input value for enum mood: 'angry'")
  - DROP TYPE dependency checking with proper error messages
  - Automatic conversion of SQLite CHECK constraint errors to PostgreSQL format
- [x] Phase 9: ALTER TYPE ADD VALUE Support - COMPLETED (2025-07-06)
  - Replaced CHECK constraints with trigger-based validation
  - Triggers dynamically validate against __pgsqlite_enum_values table
  - ALTER TYPE ADD VALUE now works correctly with existing tables
  - Created __pgsqlite_enum_usage table to track ENUM column usage
  - Added EnumTriggers module for managing validation triggers

#### JSON/JSONB
- [ ] Implement JSONB type (binary JSON)
- [ ] Add JSON operators (->, ->>, @>, etc.)
- [ ] Support JSON path expressions
- [ ] Implement JSON aggregation functions

#### Geometric Types
- [ ] Implement POINT, LINE, LSEG, BOX, PATH, POLYGON, CIRCLE types
- [ ] Add geometric operators and functions
- [ ] Store as JSON or custom format in SQLite

### Query Features

#### CTEs and Advanced Queries
- [ ] Materialized CTEs
- [ ] Lateral joins

#### Window Functions
- [ ] Implement missing window functions
- [ ] Support all frame specifications
- [ ] Handle EXCLUDE clause
- [ ] Optimize performance for large windows

#### Full Text Search
- [ ] Implement tsvector and tsquery types
- [ ] Add text search operators
- [ ] Support text search configurations
- [ ] Implement ts_rank and ts_headline

### Storage & Optimization

#### Schema Migration System - COMPLETED (2025-07-06)
- [x] Implement internal schema migration framework
  - [x] Create migration module with runner and registry
  - [x] Implement Migration and MigrationAction structs with SHA256 checksums
  - [x] Build migration registry with lazy_static for embedded migrations
  - [x] Create MigrationRunner with transaction-based execution
  - [x] Add migration locking to prevent concurrent migrations
  - [x] Integrate migrations into DbHandler initialization
  - [x] Support for SQL, SqlBatch, Function, and Combined migration types
  - [x] Version detection for pre-migration databases (v1 recognition)
  - [x] Comprehensive test coverage for all migration scenarios
  - [x] Migration history tracking in __pgsqlite_migrations table
  - [x] Idempotent migrations - can run multiple times safely
  - [x] Explicit migration mode - requires --migrate flag, errors if schema outdated
  - [x] Current migrations:
    - v1: Initial schema (__pgsqlite_schema, metadata tables)
    - v2: ENUM support (enum types, values, usage tracking)

#### Indexing
- [ ] Support for expression indexes
- [ ] Partial index support
- [ ] Multi-column index statistics
- [ ] Index-only scans where applicable

#### Query Optimization
- [x] SQL comment stripping (single-line -- and multi-line /* */) - COMPLETED (2025-07-03)
  - Implemented strip_sql_comments function in query/comment_stripper.rs
  - Integrated into QueryExecutor and ExtendedQueryHandler
  - Preserves string literals correctly
  - Handles empty queries after comment stripping with proper error
  - Full test coverage with test_comment_stripping.rs
- [ ] Cost-based query planning
- [ ] Join order optimization
- [ ] Subquery unnesting
- [ ] Common subexpression elimination

#### Storage Optimization
- [ ] Compression for large text/blob values
- [ ] Efficient storage for sparse columns
- [ ] Table partitioning support
- [ ] Vacuum and analyze equivalents

---

## 🔒 LOW PRIORITY - Advanced Features

### Security & Administration

#### Security
- [ ] Row-level security policies
- [ ] Column-level permissions
- [x] SSL/TLS connection support - COMPLETED (2025-07-03)
  - [x] Implement SSL negotiation in protocol handler
  - [x] Support basic sslmode (enabled/disabled via --ssl flag)
  - [x] Certificate generation and management
  - [x] Configure SSL cert/key paths via command line or config
  - [x] Support PostgreSQL SSL protocol flow
  - [ ] Full sslmode options support (allow, prefer, require, verify-ca, verify-full)
  - [ ] Client certificate authentication
  - [ ] Certificate rotation without restart
- [ ] Authentication methods (md5, scram-sha-256)

#### Monitoring
- [ ] Query statistics collection
- [ ] Connection pooling stats
- [ ] Performance metrics export
- [ ] Slow query logging

#### Configuration
- [ ] Runtime parameter system (SET/SHOW)
- [ ] Configuration file support
- [ ] Per-database settings
- [ ] Connection limits and timeouts

### Compatibility & Standards

#### SQL Compliance
- [ ] LATERAL joins
- [ ] GROUPING SETS, CUBE, ROLLUP
- [ ] VALUES lists as tables
- [ ] Full MERGE statement support

#### PostgreSQL Compatibility - System Catalogs (Partial - 2025-07-03)
- [ ] System catalogs (pg_class, pg_attribute, etc.)
  - [x] Basic CatalogInterceptor framework - COMPLETED (2025-07-03)
  - [x] Implement pg_class queries for table/relation listing - COMPLETED (2025-07-03)
    - Returns all tables and indexes from SQLite
    - Generates stable OIDs from names
    - Maps SQLite metadata to PostgreSQL format
    - **UPDATED (2025-07-05)**: Now returns all 33 columns per PostgreSQL 14+ specification
    - Added missing columns: reloftype, relallvisible, relacl, reloptions, relpartbound
  - [x] Implement pg_attribute queries for column details - COMPLETED (2025-07-03)
    - Maps PRAGMA table_info to pg_attribute format
    - Integrates with __pgsqlite_schema for type information
    - Supports type modifiers (VARCHAR length, NUMERIC precision/scale)
    - **UPDATED (2025-07-05)**: PRIMARY KEY columns are now correctly marked as NOT NULL
  - [x] **Column Projection Support** - CRITICAL FOR PSQL - COMPLETED (2025-07-05)
    - Implemented column projection for pg_attribute handler
    - Parses SELECT clauses and returns only requested columns
    - Handles column aliases and wildcard (*) selection
    - **UPDATED (2025-07-05)**: pg_class handler now also has column projection support
  - [x] **WHERE Clause Filtering** - CRITICAL FOR PSQL - COMPLETED (2025-07-04)
    - Implemented WhereEvaluator module for evaluating WHERE clauses
    - Added WHERE clause support to pg_class and pg_attribute handlers
    - Supports common operators: =, !=, <, >, <=, >=, IN, LIKE, ILIKE, IS NULL, IS NOT NULL
    - Evaluates WHERE conditions against catalog data before returning rows
  - [ ] **JOIN Query Support** - CRITICAL FOR PSQL
    - psql \d commands use complex JOINs between catalog tables
    - Need to handle joins between pg_class, pg_namespace, pg_attribute, etc.
    - Current implementation only handles single-table queries
  - [ ] **Enhance pg_namespace implementation**
    - Currently returns minimal hardcoded data
    - Need to map SQLite schemas/databases if available
    - Support namespace visibility checks
  - [ ] Implement pg_index queries for index information
    - Map PRAGMA index_list and index_info to pg_index format
    - Include index expressions and predicate information
    - Support unique, primary key, and exclusion constraints
  - [ ] Implement pg_constraint queries for constraint details
    - Extract PRIMARY KEY constraints from PRAGMA table_info
    - Map FOREIGN KEY constraints from PRAGMA foreign_key_list
    - Parse CHECK constraints from sqlite_master.sql
    - Support UNIQUE constraints from indexes
  - [x] **PostgreSQL System Functions** - REQUIRED FOR PSQL (2025-07-04)
    - [x] pg_table_is_visible(oid) - Check if table is in search path
    - [x] pg_get_userbyid(oid) - Return user name for OID
    - [x] format_type(oid, typmod) - Format type name with modifiers
    - [x] pg_get_indexdef(oid) - Return CREATE INDEX statement
    - [x] pg_get_constraintdef(oid) - Return constraint definition
    - [x] pg_get_expr(node, relation) - Return expression from node tree
    - [ ] regclass type casting support (e.g., 'tablename'::regclass)
    - **Note**: System functions are detected and processed in query interceptor, replaced with their results before execution
  - [ ] **Additional System Catalogs**
    - [ ] pg_am (access methods) - Required for index queries
    - [ ] pg_proc (functions) - For \df command
    - [ ] pg_type enhancements - Support for all PostgreSQL types
    - [ ] pg_database - Database information
    - [ ] pg_roles/pg_user - User information
    - [ ] pg_tablespace - Tablespace information
  - [ ] **Query Optimization for Catalog Queries**
    - Catalog queries should bypass normal query processing
    - Implement specialized handlers for common patterns
    - Cache catalog data for repeated access
  - [ ] **psql Slash Command Support**
    - [ ] \d - List all relations (partially works, formatting issues)
    - [ ] \dt - List tables only (partially works)
    - [ ] \di - List indexes
    - [ ] \dv - List views
    - [ ] \ds - List sequences
    - [ ] \df - List functions
    - [ ] \d tablename - Describe specific table (needs multiple queries)
    - [ ] \l - List databases
    - [ ] \dn - List schemas
    - [ ] \du - List users/roles
  - [ ] Add comprehensive tests for catalog query compatibility
    - Test all common psql queries
    - Test edge cases (empty tables, special characters, etc.)
    - Performance tests for catalog queries
- [ ] Information schema views
- [ ] PostgreSQL-specific functions
- [ ] Extension mechanism (CREATE EXTENSION)

#### Error Handling
- [ ] Complete PostgreSQL error code mapping
- [ ] Detailed error positions in queries
- [ ] HINT and DETAIL in error messages
- [ ] Proper constraint violation messages

### Testing & Documentation

#### Test Coverage
- [ ] Comprehensive type conversion tests
- [ ] Protocol compliance test suite
- [ ] Performance benchmarks
- [ ] Stress testing for concurrent connections

#### Documentation
- [ ] API documentation
- [ ] Migration guide from PostgreSQL
- [ ] Performance tuning guide
- [ ] Troubleshooting guide

---

## ✅ COMPLETED TASKS

### 🚀 Performance Optimization Phase 1 - COMPLETED (2025-06-30)

#### Background
Investigated replacing the channel-based DbHandler with a direct multi-threaded implementation using SQLite's FULLMUTEX mode.

#### Performance Findings
Benchmark results comparing implementations (1000 operations each):

| Implementation | INSERT | SELECT | UPDATE | DELETE | Notes |
|----------------|--------|--------|--------|--------|-------|
| Raw SQLite | 0.005ms | 0.006ms | 0.005ms | 0.004ms | Baseline |
| Mutex Handler | 0.036ms | 0.046ms | 0.040ms | 0.038ms | 7.7-9.6x overhead (CHOSEN) |
| Direct Executor | 0.038ms | 0.050ms | 0.043ms | 0.042ms | 8.1-10.7x overhead |
| Simple Executor | 0.036ms | 0.047ms | 0.040ms | 0.039ms | 7.7-9.9x overhead |
| Channel-based | 0.094ms | 0.159ms | 0.092ms | 0.083ms | 20-27x overhead |

**Key Achievement**: Mutex-based implementation provides 2.2-3.5x performance improvement over channels.

#### Final Implementation
[x] Implemented and deployed **Mutex-based DbHandler** as the sole database handler:
- Uses `parking_lot::Mutex` for efficient synchronization
- Single SQLite connection with `SQLITE_OPEN_FULL_MUTEX` flag
- Thread-safe and Send+Sync compatible
- Maintains all features: schema cache, fast path optimization, transaction support

#### Work Completed
- [x] Benchmarked multiple implementations (channel, direct, simple, mutex)
- [x] Created mutex-based implementation with best performance characteristics
- [x] Removed all experimental implementations (direct_handler, simple_executor, etc.)
- [x] Updated session module to use single DbHandler implementation
- [x] Documented architectural decision in CLAUDE.md
- [x] Cleaned up codebase to remove unused modules

### 🚀 Performance Optimization Phase 2 - SELECT Query Optimization - COMPLETED

#### High Priority - Query Plan Cache - COMPLETED (2025-06-30)
- [x] Design query plan cache structure with LRU eviction
- [x] Implement cache key normalization for query text
- [x] Cache parsed AST and analysis results
- [x] Store column types and table metadata with plans
- [x] Add cache hit/miss metrics for monitoring
- [x] Benchmark impact on repeated queries
- [x] Create cache effectiveness benchmark (benchmark_cache_effectiveness.rs)
- [x] Add cache metrics logging with debug/info level
- [x] Implement pgsqlite_cache_status virtual table for monitoring
- [x] Add periodic cache status logging (every 5 minutes)

#### High Priority - Enhanced Fast Path - COMPLETED (2025-07-01)
- [x] Extend fast path to handle simple WHERE clauses (=, >, <, >=, <=, !=, <>)
- [x] Add support for single-table queries with basic predicates
- [x] Implement fast path for parameterized queries ($1, $2, etc.)
- [x] Skip decimal rewriting for non-decimal tables
- [x] Add fast path detection for common patterns
- [x] Optimize boolean conversion in fast path
- [x] Integrate with extended protocol to avoid parameter substitution overhead

#### Medium Priority - Prepared Statement Optimization - COMPLETED (2025-07-01)
- [x] Improve SQLite prepared statement reuse
- [x] Cache statement metadata between executions
- [x] Implement statement pool with size limits (100 statements, LRU eviction)
- [x] Optimize parameter binding process
- [x] Add prepared statement metrics and statistics
- [x] Integrate with DbHandler for transparent statement reuse
- [x] Support both parameterized and non-parameterized queries

#### Medium Priority - Schema Cache Improvements - COMPLETED (2025-07-01)
- [x] Implemented bulk schema preloading on first table access
- [x] Created HashMap-based efficient column type lookup
- [x] Added HashSet bloom filter for decimal table detection
- [x] Eliminated per-query __pgsqlite_schema lookups
- [x] Schema cache integrated with query parsing

#### Low Priority - Protocol and Processing Optimization - COMPLETED (2025-07-01)
- [x] Implemented query fingerprinting with execution cache
- [x] Created pre-computed type converter lookup tables
- [x] Optimized boolean conversion with specialized fast paths
- [x] Implemented batch row processing with pre-allocated buffers
- [x] Added fast paths for common value types

#### High Priority - Binary Protocol and Advanced Optimization - COMPLETED (2025-07-01)
- [x] Implement binary protocol support for common PostgreSQL types
- [x] Create zero-copy message construction for protocol responses
- [x] Add result set caching for frequently executed identical queries
- [x] Optimize extended protocol parameter handling - COMPLETED (2025-07-02)

### 🎉 Zero-Copy Protocol Architecture - FULLY COMPLETED (2025-07-01)

#### Phase 1: Memory-Mapped Value Access - COMPLETED
- [x] Implemented `MappedValue` enum for zero-copy data access (Memory/Mapped/Reference variants)
- [x] Created `MappedValueFactory` for automatic threshold-based memory mapping
- [x] Built `ValueHandler` system for smart SQLite-to-PostgreSQL value conversion
- [x] Integrated with existing query executors for seamless operation

#### Phase 2: Enhanced Protocol Writer System - COMPLETED
- [x] Migrated all query executors to use `ProtocolWriter` trait
- [x] Implemented `DirectWriter` for direct socket communication bypassing tokio-util framing
- [x] Created connection adapters for seamless integration with existing handlers
- [x] Added comprehensive message batching for DataRow messages

#### Phase 3: Stream Splitting and Connection Management - COMPLETED
- [x] Implemented proper async stream splitting for concurrent read/write operations
- [x] Enhanced `DirectConnection` for zero-copy operation modes
- [x] Integrated with existing connection handling infrastructure
- [x] Added comprehensive error handling and connection lifecycle management

#### Phase 4: Memory-Mapped Value Integration - COMPLETED
- [x] Enhanced memory-mapped value system with configurable thresholds
- [x] Implemented `MemoryMappedExecutor` for optimized query processing
- [x] Added smart value slicing and reference management
- [x] Integrated temporary file management for large value storage

#### Phase 5: Reusable Message Buffers - COMPLETED
- [x] Implemented thread-safe `BufferPool` with automatic recycling and size management
- [x] Created `MemoryMonitor` with configurable pressure thresholds and cleanup callbacks
- [x] Built `PooledDirectWriter` using buffer pooling for reduced allocations
- [x] Added intelligent message batching with configurable flush triggers
- [x] Implemented comprehensive monitoring and statistics tracking

### ✅ Protocol Flush Fix - COMPLETED (2025-07-02)
- [x] Added `framed.flush().await?` after ReadyForQuery in simple query protocol (main.rs:276)
- [x] Added `framed.flush().await?` after ReadyForQuery in Sync handling (lib.rs:228)

### 🚧 SELECT Query Optimization - Logging Reduction - COMPLETED (2025-07-02)
- [x] Profiled SELECT query execution to identify logging bottlenecks
- [x] Changed error! and warn! logging to debug! level for missing metadata
- [x] Reduced logging overhead for user tables without schema metadata
- [x] Benchmark impact of logging reduction on SELECT performance - 33% improvement achieved
- [x] Implement RowDescription caching to avoid repeated field generation - 41% improvement achieved

### RowDescription Cache Implementation - COMPLETED (2025-07-02)
- [x] Created RowDescriptionCache with LRU eviction and TTL support
- [x] Integrated cache into all query executors (simple, v2, extended protocol)
- [x] Cache key includes query, table name, and column names for accuracy
- [x] Added environment variables for cache configuration

### ✅ Performance Optimization Phase 6 - INSERT Operation Optimization - COMPLETED (2025-07-02)

#### Fast Path for INSERT
- [x] Implemented regex-based fast path detection for simple INSERT statements
- [x] Support INSERT INTO table (cols) VALUES (...) pattern
- [x] Bypass full SQL parsing for detected patterns
- [x] Skip decimal rewriting for non-decimal tables
- [x] Cache table schema for fast lookups
- [x] Integrated with DbHandler execute method

#### Statement Pool Integration
- [x] Extended statement pool to cache INSERT statements
- [x] Implemented prepared statement reuse for repeated INSERTs
- [x] Added parameter binding optimization
- [x] Cache column type information with statements
- [x] Track and log statement pool hit rates
- [x] Global statement pool with 100 entry LRU cache

### ✅ Extended Fast Path Optimization for Special Types - COMPLETED (2025-07-02)
- [x] Added `original_types` tracking in parameter cache to preserve PostgreSQL types before TEXT mapping
- [x] Implemented proper parameter conversion for MONEY and other special types
- [x] Added proper DataRow and CommandComplete message sending for SELECT queries
- [x] Added intelligent fallback to normal path for binary result formats
- [x] Fixed all 10 failing binary protocol tests
- [x] **Query Type Detection**: Replaced `to_uppercase()` with byte comparison - **400,000x speedup**
- [x] **Binary Format Check**: Moved after parameter conversion, only for SELECT queries
- [x] **Early Exit**: Skip fast path entirely for binary SELECT queries
- [x] **Direct Array Access**: Check only first element for uniform format queries

### ✅ Executor Consolidation and Architecture Simplification - COMPLETED (2025-07-03)

#### Phase 1: Cleanup and Consolidation
- [x] Removed `zero-copy-protocol` feature flag from Cargo.toml
- [x] Deleted 7 redundant executor files (~1,800 lines of code)
- [x] Integrated static string optimizations for command tags (0/1 row cases)
- [x] Cleaned up all conditional compilation and module exports
- [x] Updated mod.rs to remove zero-copy exports

#### Phase 2: Performance Optimization
- [x] Added optimized command tag creation with static strings for common cases
- [x] Achieved 5-7% DML performance improvement
- [x] Maintained full compatibility with existing functionality

#### Phase 3: Intelligent Batch Optimization
- [x] Implemented dynamic batch sizing based on result set size
- [x] Added periodic flushing for timely delivery
- [x] Optimized for both latency and throughput scenarios

### 🧹 Dead Code Cleanup - COMPLETED (2025-07-03)

#### Cleanup Work Completed
- [x] Removed 13 files of unused protocol implementations
- [x] Updated protocol module exports
- [x] ~3,000+ lines of dead code removed
- [x] Zero performance regression confirmed via benchmarks
- [x] All 75 unit tests continue to pass

### ✅ Extended Protocol Parameter Type Inference - COMPLETED (2025-07-03)

#### Parameter Type Handling Fixed
- [x] Fixed parameter type inference to respect explicitly specified TEXT types
- [x] Modified `needs_inference` check to only trigger for empty or unknown (0) param types
- [x] Added proper handling for simple parameter SELECT queries (e.g., SELECT $1)
- [x] Fixed regex for PostgreSQL type casts to avoid matching IPv6 addresses (::1)

### ✅ CTE Query Support Fixed - COMPLETED (2025-07-03)

#### CTE (WITH) Query Recognition
- [x] Updated QueryTypeDetector to recognize queries starting with "WITH" as SELECT queries
- [x] Fixed "Execute returned results - did you mean to call query?" error for CTE queries
- [x] Added support for WITH RECURSIVE queries
- [x] Added comprehensive test coverage for CTE query detection
- [x] Verified complex CTE queries with JOINs now work correctly
- [x] Added `inferred_param_types` field to Portal for better type tracking
- [x] Resolved issue where 'SELECT $1' with TEXT parameter incorrectly interpreted 4-byte strings as INT4
- [x] Full test coverage with improved test_comment_stripping.rs
- [x] No performance regression - benchmarks show consistent results

### Type System Enhancements

#### Code Quality - Magic Numbers - COMPLETED (2025-07-02)
- [x] Replace OID type magic numbers with PgType enum values

### Data Type Improvements

#### Decimal Query Rewriting - Context Handling - COMPLETED
- [x] Fixed correlated subquery context inheritance (outer table columns now properly recognized in subqueries)
- [x] Improved aggregate function decimal wrapping (only wrap NUMERIC types, not FLOAT types)
- [x] Enhanced derived table decimal type propagation for WHERE clause rewriting
- [x] Fixed recursive CTE decimal rewriting (arithmetic operations in recursive part of UNION now properly rewritten)

### Protocol Features

#### Connection Methods - COMPLETED
- [x] Unix domain socket support
  - [x] Add --socket-dir command line option
  - [x] Create socket file as .s.PGSQL.{port} in specified directory
  - [x] Handle socket file cleanup on shutdown
  - [x] Support both TCP and Unix socket listeners simultaneously
  - [x] Implement proper socket permissions

### Query Features

#### CTEs and Subqueries - COMPLETED
- [x] Recursive CTE decimal rewriting support (fixed table alias resolution for recursive parts)
- [x] Correlated subqueries with decimal operations (fixed context inheritance for outer table references)

### Performance and Storage

#### Caching and Optimization - COMPLETED
- [x] Schema metadata caching to avoid repeated PRAGMA table_info queries
- [x] Query plan caching for parsed INSERT statements
- [x] SQLite WAL mode + multi-threaded support with connection pooling
  - [x] Separate read/write connection pools
  - [x] Connection affinity for transactions
  - [x] Shared cache for in-memory databases
  - [x] Fix concurrent access test failures (implemented RAII connection return)
  - [x] Optimize connection pool management

### Testing and Quality - COMPLETED
- [x] Skip test_flush_performance in CI due to long execution time (marked with #[ignore])
- [x] Skip test_logging_reduced in CI due to server startup requirement (marked with #[ignore])
- [x] Skip test_row_description_cache in CI due to server startup requirement (marked with #[ignore])

### CI/CD Integration Testing - COMPLETED (2025-07-03)
- [x] Added PostgreSQL client installation to GitHub Actions workflow
- [x] Created multi-mode test runner script (run_ssl_tests.sh)
  - [x] TCP with SSL mode (in-memory database)
  - [x] TCP without SSL mode (in-memory database)
  - [x] Unix socket mode (in-memory database)
  - [x] File database with SSL mode
  - [x] File database without SSL mode
- [x] Integrated comprehensive SQL test suite (test_queries.sql) into CI pipeline
- [x] Proper error handling - any SQL query failure causes build to fail
- [x] Resource cleanup for all modes (sockets, certificates, databases)
- [x] Renamed workflow from rust.yml to ci.yml for clarity

### 🗄️ PostgreSQL System Catalog Foundation - PARTIAL IMPLEMENTATION (2025-07-03)

#### Background
Started implementation of PostgreSQL system catalogs to support psql \d commands and other PostgreSQL tools that query catalog tables.

#### Work Completed
- [x] Created comprehensive research document (docs/pg_catalog_research.md)
  - Documented PostgreSQL catalog table structures
  - Analyzed psql \d command queries
  - Mapped SQLite metadata to PostgreSQL catalogs
- [x] Implemented pg_class handler (src/catalog/pg_class.rs)
  - Maps SQLite tables and indexes to pg_class format
  - Generates stable OIDs from object names
  - Queries SQLite metadata for relnatts, relhasindex
  - Returns all 28 pg_class columns with appropriate values
- [x] Implemented pg_attribute handler (src/catalog/pg_attribute.rs)
  - Maps PRAGMA table_info to pg_attribute format
  - Integrates with __pgsqlite_schema for PostgreSQL types
  - Falls back to intelligent type inference for unmapped types
  - Handles type modifiers (VARCHAR length, NUMERIC precision/scale)
- [x] Updated CatalogInterceptor to be async and accept DbHandler
  - Routes pg_class and pg_attribute queries to handlers
  - Maintains existing pg_type, pg_namespace support

#### Current Limitations
1. **No Column Projection** - SELECT relname returns all columns
2. **No WHERE Filtering** - WHERE relkind = 'r' returns all rows
3. **No JOIN Support** - Cannot handle psql's complex JOIN queries
4. **Missing System Functions** - COMPLETED (2025-07-04)
5. **Incomplete Catalog Tables** - Need pg_index, pg_constraint, pg_am, etc.

#### Testing Results
- Basic pg_class queries work (returns tables and indexes)
- Basic pg_attribute queries work (returns column information)
- psql can connect and query catalogs but \d commands show raw data
- Need proper query processing for full psql compatibility

### ✅ System Catalog Extended Protocol Support - COMPLETED (2025-07-05)

#### Background
Catalog queries were failing with UnexpectedMessage errors when using the extended protocol (prepared statements). This affected tools that use prepared statements to query system catalogs.

#### Issues Fixed
1. **pg_class Column Count**: Updated from 28 to 33 columns per PostgreSQL 14+ specification
   - Added missing columns: reloftype, relallvisible, relacl, reloptions, relpartbound
   - Updated all related type mappings and handlers

2. **Extended Protocol Field Descriptions**: Fixed UnexpectedMessage errors
   - Field descriptions generated during Describe phase are now properly stored in prepared statements
   - Available during Execute phase for correct protocol handling
   - Catalog queries now work correctly with both simple and extended protocols

3. **Binary Encoding Support**: Fixed "invalid buffer size" errors
   - Catalog data is now properly formatted for binary result encoding
   - Added special handling for numeric columns (attnum, attlen, etc.)
   - PRIMARY KEY columns are correctly identified as NOT NULL

4. **Column Projection**: Implemented for pg_attribute handler
   - SELECT specific columns now returns only those columns
   - Handles wildcard (*) and column aliases correctly
   - Fixed test failures related to column index mismatches

5. **Test Infrastructure**: Improved diagnostic test handling
   - Trace tests that intentionally panic are now marked with #[ignore]
   - Can still be run manually for debugging with --ignored flag