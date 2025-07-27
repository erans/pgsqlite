# pgsqlite Project Context

## Overview
pgsqlite is a PostgreSQL protocol adapter for SQLite databases. It allows PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

## Project Structure
- `src/` - Main source code directory
  - `lib.rs` - Main library entry point
  - `protocol/` - PostgreSQL wire protocol implementation
  - `session/` - Session state management
  - `query/` - Query execution handlers
- `tests/` - Test files and infrastructure
  - `runner/` - Test runner scripts
    - `run_ssl_tests.sh` - Main integration test runner
  - `sql/` - SQL test files organized by category
    - `core/` - Core functionality tests (`test_queries.sql`)
    - `meta/` - Meta-command tests (`test_meta_commands*.sql`)
    - `features/` - Feature-specific tests (JSON, arrays, datetime)
    - `debug/` - Debug and diagnostic tests
  - `output/` - Test output files (logs, temporary databases)
- `Cargo.toml` - Rust project configuration
- `TODO.md` - Comprehensive task list for future development

## Build Commands
- `cargo build` - Build the project
- `cargo test` - Run unit tests
- `cargo run` - Run the project
- `tests/runner/run_ssl_tests.sh` - Run integration tests (must be executed from project root)

## Development Workflow
- After implementing any feature, always run the full test suite with `cargo test` to ensure nothing is broken
- **ALWAYS update TODO.md when completing work or discovering new tasks**:
  - Mark completed tasks with `[x]`
  - Add new discovered tasks or subtasks
  - Document partial progress with detailed notes
  - Update task descriptions if implementation reveals complexity
- Check TODO.md for prioritized tasks when planning development work
- Use TODO.md as the authoritative source for tracking all future work
- **NEVER commit code before ensuring ALL of the following pass**:
  - `cargo check` - No compilation errors or warnings
  - `cargo build` - Successfully builds the project
  - `cargo test` - All tests pass
  - If any of these fail, fix the issues before committing

## Code Style
- Follow Rust conventions
- Use existing imports and patterns
- Avoid adding comments unless necessary
- Keep code concise and idiomatic

## Schema Migration System
- **In-memory databases**: Migrations are run automatically on startup (since they always start fresh)
- **New file-based databases**: Migrations are run automatically when creating a new database file
- **Existing file-based databases**: Schema version is checked on startup
- **Error on outdated schema**: If an existing database schema is outdated, pgsqlite will exit with an error message
- **Explicit migration**: Use `--migrate` command line flag to run pending migrations and exit

### Usage
```bash
# In-memory databases (auto-migrate on startup)
pgsqlite --in-memory

# New database file (auto-migrate on first run)
pgsqlite --database newdb.db

# Run migrations on an existing file-based database
pgsqlite --database existingdb.db --migrate

# Normal operation with existing database (will fail if schema is outdated)
pgsqlite --database existingdb.db
```

### Current Migrations
- **v1**: Initial schema (creates __pgsqlite_schema, metadata tables)
- **v2**: ENUM support (creates enum types, values, and usage tracking tables)
- **v3**: DateTime support (adds datetime_format and timezone_offset columns to __pgsqlite_schema, creates datetime cache and session settings tables)
- **v4**: DateTime INTEGER storage (converts all datetime types to INTEGER microseconds/days for perfect precision)
- **v5**: PostgreSQL catalog tables (creates pg_class, pg_namespace, pg_am, pg_type, pg_attribute views; pg_constraint, pg_attrdef, pg_index tables)
- **v6**: VARCHAR/CHAR constraints (adds type_modifier to __pgsqlite_schema, creates __pgsqlite_string_constraints table)
- **v7**: NUMERIC/DECIMAL constraints (creates __pgsqlite_numeric_constraints table for precision/scale validation)
- **v8**: Array support (creates __pgsqlite_array_types table, updates pg_type view with typarray field)
- **v9**: Full-Text Search support (creates __pgsqlite_fts_tables, __pgsqlite_fts_columns tables for PostgreSQL tsvector/tsquery with SQLite FTS5 backend)

### Creating New Migrations
**IMPORTANT**: When modifying internal pgsqlite tables (any table starting with `__pgsqlite_`), you MUST create a new migration:

1. **Add migration to registry** in `src/migration/registry.rs`:
   ```rust
   register_vX_your_feature(&mut registry);
   ```

2. **Define the migration function**:
   ```rust
   fn register_vX_your_feature(registry: &mut BTreeMap<u32, Migration>) {
       registry.insert(X, Migration {
           version: X,
           name: "your_feature_name",
           description: "Description of what this migration does",
           up: MigrationAction::Sql(r#"
               ALTER TABLE __pgsqlite_schema ADD COLUMN new_column TEXT;
               -- Other schema changes
           "#),
           down: Some(MigrationAction::Sql(r#"
               -- Rollback SQL if possible
           "#)),
           dependencies: vec![X-1], // Previous migration version
       });
   }
   ```

3. **For complex migrations** that need data transformation, use `MigrationAction::Combined` or `MigrationAction::Function`

4. **Update this file** to list the new migration in the "Current Migrations" section above

## Important Design Decisions
- **Type Inference**: NEVER use column names to infer types. Types should be determined from:
  - Explicit PostgreSQL type declarations in CREATE TABLE statements
  - SQLite schema information via PRAGMA table_info
  - Explicit type casts in queries (e.g., $1::int4)
  - Value-based inference only when schema information is unavailable

- **Decimal Query Rewriting**: 
  - Only NUMERIC types (stored as DECIMAL in SQLite) require decimal_from_text wrapping for aggregates
  - FLOAT types (REAL, DOUBLE PRECISION, FLOAT4, FLOAT8) should NOT be wrapped as they're already decimal-compatible
  - Correlated subqueries must inherit outer context to recognize outer table columns
  - Context merging is essential for proper type resolution in nested queries

- **DateTime Storage (INTEGER Microseconds)**:
  - All datetime types use INTEGER storage for perfect precision (no floating point errors)
  - Storage formats:
    - DATE: INTEGER days since epoch (1970-01-01)
    - TIME/TIMETZ: INTEGER microseconds since midnight
    - TIMESTAMP/TIMESTAMPTZ: INTEGER microseconds since epoch
    - INTERVAL: INTEGER microseconds
  - Microsecond precision matches PostgreSQL's maximum precision
  - Conversion implementation:
    - InsertTranslator converts datetime literals to INTEGER during INSERT/UPDATE
    - Fast path value converters transform INTEGER back to datetime strings during SELECT
    - Supports both single-row and multi-row INSERT statements
    - **INSERT SELECT Support**: Full support for INSERT SELECT with datetime literal translation
    - No triggers needed - all conversion happens in the query pipeline
  - Clients see proper PostgreSQL datetime formats via wire protocol

- **INSERT SELECT Translation**:
  - Full support for INSERT SELECT statements with automatic datetime/array translation
  - Pattern recognition for both explicit and implicit column specifications:
    - `INSERT INTO table (cols) SELECT literal_values, existing_cols FROM source`
    - `INSERT INTO table SELECT literal_values, functions FROM source`
  - SELECT clause analysis with proper expression parsing and parentheses handling
  - Position-based mapping of SELECT expressions to target table column types
  - Literal datetime conversion: `'2024-01-15'` → `19737` (INTEGER days), timestamps → microseconds
  - PostgreSQL function handling: `NOW()` → `CURRENT_TIMESTAMP`, preserves runtime evaluation
  - Data integrity guarantee: INSERT SELECT now behaves identically to INSERT VALUES
  - Zero performance impact: maintains all existing optimizations and ultra-fast paths

## Quality Standards
- Write tests that actually verify functionality, not tests that are designed to pass easily
- Only mark tasks as complete when they are actually finished and working
- Test edge cases and error conditions, not just happy paths
- Verify implementations work end-to-end, not just in isolation
- Don't claim something works without actually testing it

## Performance Characteristics
### Current Performance (as of 2025-07-27) - POST UUID/NOW() FIXES AND LOGGING OPTIMIZATION
- **✅ COMPREHENSIVE QUERY OPTIMIZATION SYSTEM**: Complete optimization infrastructure with read-only optimizer
- **SELECT**: ~674.9x overhead (0.669ms) - higher than baseline due to parameter queries preventing ultra-fast path
- **SELECT (cached)**: ~17.2x overhead (0.046ms) - excellent cache performance, better than expected 50x
- **UPDATE**: ~50.9x overhead (0.059ms) - excellent performance maintained
- **DELETE**: ~35.8x overhead (0.034ms) - excellent performance maintained  
- **INSERT**: ~36.6x overhead (0.060ms) - excellent performance
- **Cache Effectiveness**: 14.7x speedup for cached queries (0.669ms → 0.046ms)
- **Overall Operations**: 5,251 total operations with advanced optimization active
- **Read-Only Optimizer**: Successfully intercepting SELECT queries as confirmed by benchmarks
- **Enhanced Statement Caching**: 200+ cached query plans with priority-based eviction

### Key Optimizations Implemented
- **Phase 5 - Connection Pooling Infrastructure (2025-07-20)**: Read/write separation foundation
  - **ReadOnlyDbHandler**: Connection pool for SELECT queries using SQLite WAL mode
  - **QueryRouter**: Intelligent query classification and routing (SELECT vs DML)
  - **Transaction Affinity**: Ensures transactions stay on single connection for consistency
  - **Performance Baseline**: 95,961 QPS single-thread, 124,380 QPS with 8 concurrent tasks
  - **Architecture Ready**: Infrastructure complete, integration with main pipeline pending
  - **Test Coverage**: 300/300 unit tests passing, extended protocol timeout issues fixed
- **Phase 4 - Comprehensive Query Optimization System (2025-07-18)**: Complete optimization infrastructure
  - **Read-Only Optimizer**: Direct execution path for SELECT queries with query plan caching
  - **Enhanced Statement Caching**: Intelligent caching with priority-based eviction (200+ cached plans)
  - **Query Plan Caching**: Complexity classification (Simple/Medium/Complex) and type conversion caching
  - **Type Conversion Caching**: Optimized boolean, datetime, and numeric type handling
  - **LRU Eviction**: Priority scoring based on access patterns and success rates
  - **Performance Validation**: 2.4x speedup for cached queries, all 279 tests passing with zero warnings
- **Phase 3 - Query Optimization System (2025-07-17)**: Comprehensive optimization infrastructure
  - **Context Merging Optimization**: Efficient context handling for deeply nested subqueries with TTL-based caching
  - **Lazy Schema Loading**: Deferred schema loading until needed with thread-safe duplicate work prevention
  - **Query Pattern Recognition**: 14 distinct query patterns with pre-compiled regex and optimization hints
  - **Integrated Management**: OptimizationManager coordinates all optimization features with effectiveness metrics
  - **Zero Performance Impact**: All benchmarks maintained or improved, 706 tests passing with zero warnings
- **Phase 2 - Regex Caching**: Pre-compiled regex patterns in array translator
  - 20 pre-compiled patterns for array function detection
  - Eliminated runtime regex compilation overhead
  - Simplified type inference with match expressions
- **Phase 1 - Logging Fix**: Changed high-volume info!() to debug!() level
  - Fixed 2,842+ excessive log calls per benchmark in query executor
  - Array translation metadata, type hints, and conversion logging

### Historical Baseline (2025-07-08)
- **Overall System**: ~134x overhead vs raw SQLite (comprehensive benchmark results)
- **SELECT**: ~294x overhead (protocol translation overhead)
- **SELECT (cached)**: ~39x overhead (excellent caching performance)
- **INSERT (single-row)**: ~332x overhead (use batch INSERTs for better performance)
- **UPDATE**: ~48x overhead (excellent)
- **DELETE**: ~44x overhead (excellent)

### Performance Optimization Results (2025-07-08)
- **Cached SELECT performance**: 39x overhead (0.156ms) - excellent caching effectiveness
- **UPDATE/DELETE performance**: 44-48x overhead (0.044-0.048ms) - excellent
- **Cache effectiveness**: 1.9x speedup for repeated queries (0.294ms → 0.156ms)
- **Multi-row INSERT**: Dramatic performance improvements with batch operations
- **DateTime conversion**: Complete bidirectional conversion with minimal performance impact
- **Detection**: Regex-based patterns identify simple SELECT/INSERT/UPDATE/DELETE queries
- **Coverage**: Queries without PostgreSQL casts (::), datetime functions, JOINs, or complex expressions

### Batch INSERT Performance
Multi-row INSERT syntax provides dramatic improvements:
```sql
INSERT INTO table (col1, col2) VALUES 
  (val1, val2),
  (val3, val4),
  (val5, val6);
```
- 10-row batches: 11.5x speedup over single-row
- 100-row batches: 51.3x speedup
- 1000-row batch: 76.4x speedup

#### Best Practices for Batch INSERTs
1. **Optimal Batch Size**: 100-1000 rows per INSERT statement provides best performance
2. **Fast Path Optimization**: Simple batch INSERTs without datetime/decimal values use the ultra-fast path
3. **Prepared Statement Caching**: Batch INSERTs with same column structure share cached metadata
4. **Error Handling**: Batch operations are atomic - all rows succeed or all fail
5. **DateTime Values**: Use standard formats (YYYY-MM-DD, HH:MM:SS) to avoid conversion errors
6. **Memory Usage**: Very large batches (>10,000 rows) may require more memory
7. **Network Efficiency**: Reduces round trips between client and server

## Recent Major Features
- **UUID Generation and NOW() Function Fixes (2025-07-27)**: Critical fixes for non-deterministic functions
  - **UUID Generation Fix**: Fixed gen_random_uuid() returning duplicate values due to wire protocol caching
  - **Caching Updates**: All cache layers now properly exclude non-deterministic functions (UUID, random, now)
  - **NOW() Function Fix**: Fixed NOW() returning epoch time '1970-01-01 00:00:00' instead of current timestamp
  - **Type Converter Enhancement**: Execution cache converters now handle both INTEGER and Text datetime values
  - **Performance Optimization**: Changed excessive info!() logging to debug!() to reduce overhead
  - **Performance Results**: SELECT cached improved to 17.2x overhead (better than 50x baseline)
  - **Production Ready**: All non-deterministic functions now work correctly with caching enabled
- **SQLAlchemy INSERT SELECT VALUES Fix (2025-07-25)**: Critical compatibility fix for SQLAlchemy ORM
  - **Bug Fixed**: SQLAlchemy multi-row INSERT statements using VALUES clause failed with "Mapper[Category(categories)]" error
  - **Root Cause**: SQLAlchemy generates complex INSERT SELECT statements with PostgreSQL VALUES syntax not supported by SQLite
  - **Pattern Example**: `INSERT INTO table SELECT p0::TYPE FROM (VALUES (val1, val2, 0)) AS imp_sen(p0, p1, sen_counter)`
  - **Solution**: Detect SQLAlchemy VALUES pattern and convert to UNION ALL syntax for SQLite compatibility
  - **Implementation**: Added `is_sqlalchemy_values_pattern()` and `convert_sqlalchemy_values_to_union()` in InsertTranslator
  - **Technical Details**: VALUES clause with column aliases converted to SELECT UNION ALL while preserving datetime conversions
  - **Impact**: SQLAlchemy ORM bulk inserts now work correctly with pgsqlite, resolving data insertion failures
  - **Zero Performance Impact**: Fix maintains all existing optimizations and fast-path behaviors
- **PostgreSQL Full-Text Search Support (2025-07-23)**: Complete tsvector/tsquery implementation with SQLite FTS5 backend
  - **Migration v9**: FTS schema tables (__pgsqlite_fts_tables, __pgsqlite_fts_columns) for metadata tracking
  - **Type System**: Full tsvector and tsquery type support with proper PostgreSQL wire protocol integration
  - **CREATE TABLE**: Automatic FTS5 virtual table creation for tsvector columns with SQLite FTS5 backend
  - **Search Functions**: to_tsvector(), to_tsquery(), plainto_tsquery() with comprehensive text processing
  - **Query Operations**: @@ operator translation to SQLite FTS5 MATCH with complex query support
  - **Data Operations**: INSERT, UPDATE, DELETE with automatic tsvector population and FTS index maintenance
  - **Advanced Query Support**: Complex tsquery to FTS5 syntax conversion (AND, OR, NOT, phrase matching)
  - **Table Alias Resolution**: Proper handling of table aliases in FTS queries (d.search_vector @@ query)
  - **SQL Parser Compatibility**: Custom pgsqlite_fts_match() function to avoid MATCH syntax parser conflicts
  - **Comprehensive Testing**: Full integration test suite covering all FTS operations and edge cases
  - **Production Ready**: Zero performance impact on non-FTS queries, full PostgreSQL FTS compatibility
- **CREATE TABLE DEFAULT now() Fix (2025-07-23)**: Fixed CREATE TABLE statements with DEFAULT NOW() clauses
  - **CreateTableTranslator Enhancement**: Added datetime translation support for DEFAULT clauses
  - **SQLite Compatibility**: Proper translation of DEFAULT NOW() to DEFAULT datetime('now') for SQLite
  - **Parser Integration**: Fixed SQL syntax errors in CREATE TABLE statements with datetime defaults
  - **Unit Test Coverage**: Added test_translate_default_now() to validate translation works correctly
  - **Code Quality**: Fixed all compilation warnings (6 warnings across benchmark test files.
- **Portal Management Support (2025-01-22)**: Complete Extended Query Protocol enhancement with proven performance benefits
  - **Enhanced Portal Architecture**: PortalManager with configurable limits (default: 100 concurrent portals)
  - **Partial Result Fetching**: Execute messages respect max_rows parameter with portal suspension/resumption
  - **Resource Management**: LRU eviction, stale portal cleanup, and memory-efficient result caching
  - **Extended Protocol Integration**: Enhanced Bind/Execute/Close message handling with state tracking
  - **Thread-Safe Implementation**: parking_lot::RwLock for concurrent access across multiple portals
  - **Comprehensive Testing**: 6 integration tests covering lifecycle, concurrency, limits, and cleanup
  - **Production Ready**: Zero performance impact, all 324 tests passing with full PostgreSQL compatibility
  - **Performance Validation**: 90% memory reduction (1.50MB → 0.15MB), 439K portals/sec creation, 1.8M lookups/sec
  - **Benchmarks**: Direct API validation shows 5% throughput overhead for massive memory efficiency gains
- **Connection Pooling with Read/Write Separation (2025-07-20)**: Complete production-ready implementation
  - **Architecture Components**: ReadOnlyDbHandler with connection pool, QueryRouter for intelligent routing
  - **SQLite WAL Mode**: Enabled for multi-reader support with single writer
  - **Query Classification**: Automatic routing of SELECT to pool, DML to single connection
  - **Transaction Safety**: Transaction affinity ensures consistency across operations
  - **QueryExecutor Integration**: Complete integration with all query execution paths
  - **Health Checks & Recovery**: Background monitoring, automatic stale connection cleanup, failure recovery
  - **Configuration**: Pool size, timeouts, and health check intervals configurable via environment variables
  - **Environment Control**: Enable via PGSQLITE_USE_POOLING=true environment variable (opt-in by default)
  - **Performance Benchmarks**: Baseline metrics established (3,402 QPS reads, 2,197 mixed ops/sec)
  - **Concurrent Testing**: 100% transaction consistency, comprehensive benchmark suite
  - **Production Status**: Ready for deployment with comprehensive testing, health monitoring, and zero regressions
- **BIT Type Cast Performance Fix (2025-07-20)**: Major prepared statement compatibility improvement
  - **PostgreSQL BIT Type Support**: Fixed prepared statements with BIT type casts returning empty strings
  - **SQL Parser Enhancement**: Fixed SQL parser errors with parameterized BIT types like `::bit(8)`, `::varbit(10)`
  - **Performance Optimization**: Eliminated 34% performance regression through fast-path optimizations
  - **Cast Translator Enhancement**: Enhanced `find_type_end()` function to properly handle parentheses in type names
  - **Type Recognition**: Added explicit BIT and VARBIT type recognition in PostgreSQL-to-SQLite type mapping
  - **Comprehensive Testing**: All 706+ tests passing, 5,251 benchmark operations validated
  - **Performance Results**: SELECT ~283x overhead (4% better than baseline), cache effectiveness maintained
  - **Zero Regressions**: BIT cast functionality preserved across all query types with no performance impact
- **PostgreSQL Function Completions (2025-07-19)**: Major PostgreSQL compatibility milestone
  - **String Functions**: Implemented 11 PostgreSQL string functions including split_part(), string_agg(), translate(), ascii(), chr(), repeat(), reverse(), left(), right(), lpad(), rpad()
  - **Math Functions**: Implemented 25+ PostgreSQL math functions including trunc() with precision, round() with precision, trigonometric functions, logarithms, and random()
  - **Rust 2024 Compatibility**: Fixed random() function to use new rand::rng() API instead of deprecated thread_rng()
  - **Test Suite Fixes**: Fixed all commented-out tests including string_agg, array operators (@>, <@, &&), ANY/ALL operators, and array concatenation
  - All functions have comprehensive unit tests and integration test coverage
  - Zero performance impact - maintains existing optimization levels
- **Constraint Tables Population (2025-07-19)**: Enhanced psql \d tablename support
  - **pg_constraint**: Populated with PRIMARY KEY, UNIQUE, NOT NULL constraints with proper conkey arrays
  - **pg_attrdef**: Populated with column default expressions from SQLite schema
  - **pg_index**: Populated with all index metadata including proper indkey arrays
  - **psql Compatibility**: \d tablename command now works perfectly, showing full table descriptions
  - Constraint information properly integrated with catalog query system
- **DateTime Roundtrip Fixes (2025-07-18)**: Complete wire protocol datetime compatibility
  - Fixed TIME value binary encoding where microseconds were incorrectly treated as seconds
  - Enhanced BinaryEncoder::encode_time() and encode_timestamp() for proper microsecond precision
  - Fixed NOW() and CURRENT_TIMESTAMP() type inference from TEXT to TIMESTAMPTZ
  - Updated extended query protocol to handle both string and integer datetime representations
  - Resolved "time not drained" error in GitHub Actions tests
  - All datetime roundtrip tests now pass with proper PostgreSQL protocol compliance
  - Zero performance impact - maintains system performance characteristics
- **INSERT SELECT Translation Bug Fix (2025-07-23)**: Critical data integrity issue resolved
  - **Bug Fixed**: INSERT SELECT with literal datetime values stored as TEXT instead of INTEGER microseconds
  - **Root Cause**: InsertTranslator only handled INSERT VALUES patterns, bypassing INSERT SELECT entirely
  - **Impact**: Prevented silent data corruption affecting SQLAlchemy ORM users and ETL operations
  - **Enhanced Architecture**: Added INSERT_SELECT_PATTERN recognition and SELECT clause analysis
  - **Translation Logic**: Implemented `translate_select_clause()` with expression parsing and column type mapping
  - **Technical Results**: Date literals `'2024-01-15'` → `19737` (INTEGER), timestamps → microseconds (INTEGER)
  - **Pattern Coverage**: Both explicit/implicit column lists, mixed expressions, PostgreSQL functions
  - **Comprehensive Testing**: 7 new unit tests, multiple integration scenarios, SQLite storage validation
  - **Production Ready**: Zero performance regression, backward compatible, resolves data corruption
- **SQLAlchemy ORM Support (2025-07-26)**: Complete compatibility with SQLAlchemy 2.0+
  - **Multi-Row INSERT Fix**: SQLAlchemy VALUES pattern `INSERT INTO table SELECT p0::TYPE FROM (VALUES (...)) AS alias` now converts to UNION ALL
  - **JOIN Type Inference**: Created join_type_inference module to properly map columns to source tables in multi-table queries
  - **Type OID Mapping**: Fixed issue where all columns returned TEXT (OID 25) instead of proper PostgreSQL types
  - **Test Suite**: All 8 SQLAlchemy ORM tests pass including relationships, joins, transactions, and advanced queries
  - **Pattern Support**: Handles complex SQLAlchemy patterns like `order_items.unit_price AS order_items_unit_price`
- **Boolean Conversion Fix (2025-07-17)**: Complete PostgreSQL boolean protocol compliance
  - Fixed psycopg2 compatibility issue where boolean values were returned as strings '0'/'1' instead of 't'/'f'
  - Root cause: Ultra-fast path in simple query protocol was not converting boolean values
  - Implemented schema-aware boolean conversion with performance optimization
  - Added boolean column cache (`BOOLEAN_COLUMNS_CACHE`) to avoid repeated database queries
  - Boolean conversion now works correctly across all query types and protocols
  - Performance maintained: SELECT ~417x overhead, cached SELECT ~77x overhead
  - Fixed all release build warnings for clean compilation
- **Comprehensive Query Optimization System (2025-07-17)**: Advanced query optimization infrastructure implemented
  - **Context Merging Optimization**: ContextOptimizer with 300s TTL caching for deeply nested subqueries
  - **Lazy Schema Loading**: LazySchemaLoader with 600s TTL, thread-safe duplicate work prevention, and PostgreSQL type inference
  - **Query Pattern Recognition**: QueryPatternOptimizer with 14 distinct patterns, pre-compiled regex, and complexity analysis
  - **Integrated Optimization Manager**: OptimizationManager coordinates all optimization features with effectiveness metrics
  - **Performance Impact**: Zero regression - SELECT ~337x overhead, cached SELECT ~37x overhead, 706 tests passing
- **Array Enhancement Completion (2025-07-16)**: Final array support features implemented
  - ARRAY[1,2,3] literal syntax translation to JSON format
  - ALL operator syntax fixes with proper balanced parentheses parser
  - Enhanced unnest() WITH ORDINALITY support (PostgreSQL-compatible 1-based indexing)
  - Simple query detector fixes to ensure array queries use translation pipeline
  - Complete unit test coverage (228/228 tests passing)
  - Zero performance impact - all benchmarks maintained or improved
- **PostgreSQL Type Support**: 40+ types including ranges, network types, binary types
- **ENUM Types**: Full PostgreSQL ENUM implementation with CREATE/ALTER/DROP TYPE
- **Zero-Copy Architecture**: Achieved 67% improvement in cached SELECT queries
- **System Catalog Support**: Full pg_class, pg_namespace, pg_am views and catalog tables for psql compatibility
- **SSL/TLS Support**: Available for TCP connections with automatic certificate management
- **Ultra-Fast Path Optimization (2025-07-08)**: 19% SELECT performance improvement via translation bypass
- **DateTime/Timezone Support (2025-07-07)**: INTEGER microsecond storage with full PostgreSQL compatibility
- **DateTime Value Conversion (2025-07-08)**: Complete bidirectional conversion between text and INTEGER storage
- **Multi-row INSERT Support (2025-07-08)**: Enhanced InsertTranslator to handle multi-row VALUES with datetime conversion
- **Comprehensive Performance Profiling (2025-07-08)**: Detailed pipeline metrics and optimization monitoring
- **Arithmetic Type Inference (2025-07-08)**: Smart type propagation for aliased arithmetic expressions
  - Enhanced to handle complex nested parentheses expressions like ((a + b) * c) / d
  - Improved regex patterns to properly match complex arithmetic operations
  - Fixed type inference for float columns in arithmetic operations
- **Performance Optimization (2025-07-14)**: Major performance restoration and improvements
  - Phase 1: Fixed high-volume logging causing 2,842+ calls per benchmark
  - Phase 2: Implemented regex compilation caching in array translator
  - Restored SELECT performance to 272x overhead (exceeds 294x baseline target)
  - Enhanced array translator with early exit optimization
- **psql \d Command Support (2025-07-08)**: Full support for psql meta-commands \d and \dt through enhanced catalog system
- **Array Type Support (2025-07-16)**: Complete PostgreSQL array implementation with JSON storage
  - Support for 30+ array types (INTEGER[], TEXT[][], BOOLEAN[], etc.)
  - JSON-based storage with automatic validation constraints
  - Array literal conversion (ARRAY[1,2,3] and '{1,2,3}' formats) - COMPLETED
  - Wire protocol array support with proper type OIDs
  - Multi-row INSERT with array values fully supported
  - Comprehensive test coverage in CI/CD pipeline
  - Fixed wire protocol conversion: JSON arrays now properly convert to PostgreSQL format
  - ALL operator fixes with proper nested parentheses handling - COMPLETED
  - Enhanced unnest() WITH ORDINALITY support - COMPLETED
  - Array support now 95% complete for common PostgreSQL use cases
- **JSON/JSONB Support (2025-07-12)**: Complete operator and function support with robust error handling
  - All major operators: ->, ->>, @>, <@, #>, #>>
  - Core functions: json_valid, json_typeof, json_array_length, jsonb_object_keys
  - Manipulation functions: jsonb_set, json_extract_path, json_strip_nulls
  - **JSON Path Operator Fix**: Resolved SQL parser $ character conflicts in path expressions
  - Custom SQLite functions eliminate json_extract dependency and $ character issues
  - Enhanced type handling supports chained operations (data->'items'->1->>'name')
  - Automatic operator translation in query pipeline
  - Full test coverage for operators, functions, and edge cases
- **JSON Key Existence Operators (2025-07-15)**: Complete implementation of PostgreSQL ? operators
  - ? operator: json_col ? 'key' - checks if key exists in JSON object
  - ?| operator: json_col ?| ARRAY['key1', 'key2'] - checks if any key exists
  - ?& operator: json_col ?& ARRAY['key1', 'key2'] - checks if all keys exist
  - Custom SQLite functions: pgsqlite_json_has_key, pgsqlite_json_has_any_key, pgsqlite_json_has_all_keys
  - Unit tests pass completely, integration tests have known SQL parser limitations
- **JSON Aggregation Functions (2025-07-15)**: Complete json_agg and jsonb_agg implementation
  - json_agg(expression): aggregates values into JSON array
  - jsonb_agg(expression): identical to json_agg for PostgreSQL compatibility
  - Proper NULL handling and empty result set behavior (returns "[]")
  - Uses SQLite's Aggregate trait for efficient aggregation
  - Comprehensive test coverage including multi-row scenarios and NULL values
- **JSON Object Aggregation Functions (2025-07-15)**: Complete json_object_agg and jsonb_object_agg implementation
  - json_object_agg(key, value): aggregates key-value pairs into JSON object
  - jsonb_object_agg(key, value): attempts JSON parsing of text values, otherwise treats as strings
  - HashMap-based accumulation for optimal performance
  - Handles all SQLite data types (NULL, INTEGER, REAL, TEXT, BLOB)
  - Returns empty object "{}" for empty result sets
  - Duplicate key handling with last-value-wins semantics
  - Enhanced schema type mapper for PostgreSQL wire protocol compatibility
- **JSON Table-Valued Functions (2025-07-15)**: Complete json_each/jsonb_each implementation
  - json_each(json_data): expands JSON object to key-value pairs as table rows
  - jsonb_each(json_data): identical behavior to json_each
  - JsonEachTranslator converts PostgreSQL calls to SQLite json_each() equivalents
  - Handles both FROM clause and SELECT clause patterns
  - PostgreSQL-compatible column selection (key, value only, hides SQLite's type column)
  - Integrated into query execution pipeline with metadata support
- **Decimal Query Rewriting Enhancements (2025-07-14)**: Complete nested arithmetic decomposition
  - Fixed complex nested arithmetic expressions like `(quantity * 2 + 5) * price / 100`
  - Added performance regression fix with SchemaCache optimization
  - Fixed arithmetic aliasing test failures for float vs decimal handling
  - Resolved arithmetic edge case with int * float literal operations
  - All implicit cast tests (9/9), arithmetic aliasing tests (5/5), and edge case tests (7/7) now pass
  - Maintained backwards compatibility with existing decimal functionality
- **Array Function Completion (2025-07-14)**: Full unnest() and enhanced array_agg support
  - unnest() function translates PostgreSQL unnest() calls to SQLite json_each() equivalents
  - Enhanced array_agg with DISTINCT support via array_agg_distinct() function
  - ArrayAggTranslator handles ORDER BY and DISTINCT clauses in array_agg
  - Performance optimization: fast-path checks eliminate expensive string operations for non-array queries
  - Results: SELECT performance improved from 318x to 305x overhead, cached SELECT exceeds baseline by 44%
- **Array Concatenation Operator Enhancement (2025-07-14)**: Improved || operator with ARRAY[] syntax detection
  - Enhanced to detect ARRAY[] syntax patterns (e.g., ARRAY[1,2] || ARRAY[3,4])
  - Custom character-based parser for proper balanced bracket matching
  - Fixed early exit optimization bug by detecting || operator in contains_array_functions
  - All 6 integration tests and 23 unit tests pass
  - Note: ARRAY literal translation (ARRAY[1,2,3] → JSON) requires separate implementation
- **JSON Record Conversion Functions (2025-07-16)**: Complete json_populate_record and json_to_record implementation
  - json_populate_record(base_record, json_data): populates record from JSON object with PostgreSQL semantics
  - json_to_record(json_data): converts JSON objects to record-like string representations
  - Simplified implementations acknowledging SQLite's lack of native RECORD type support
  - Comprehensive error handling for invalid JSON and non-object inputs
  - Full integration with PostgreSQL wire protocol and CI/CD test suite
  - Brings pgsqlite JSON functionality to 100% completion for common PostgreSQL use cases
- **JSON Each Text Functions (2025-07-15)**: Complete json_each_text() and jsonb_each_text() implementation
  - Implemented json_each_text_value() custom SQLite function for proper text conversion
  - Enhanced JsonEachTranslator to handle both regular and _text variants
  - Comprehensive text conversion: booleans to "true"/"false", numbers to text, arrays/objects to JSON strings
  - Supports both FROM clause and cross join patterns with proper PostgreSQL compatibility
  - 5 integration tests and 6 unit tests with comprehensive coverage
  - Zero performance impact - maintains system performance characteristics
- **JSON Manipulation Functions (2025-07-15)**: Complete jsonb_delete, jsonb_insert, and jsonb_pretty implementation
  - jsonb_insert(target, path, new_value, insert_after): inserts values into JSON objects/arrays
  - jsonb_delete(target, path): deletes values from JSON objects/arrays by path
  - jsonb_delete_path(target, path): alias for jsonb_delete for PostgreSQL compatibility
  - jsonb_pretty(jsonb): pretty-prints JSON with 2-space indentation for readability
  - Supports nested JSON operations with PostgreSQL-compatible path syntax ({key1,key2})
  - Handles object key insertion/deletion and array element insertion/deletion
  - Error handling for invalid paths and non-existent keys (returns original JSON)
  - Comprehensive unit tests (26 test cases) and integration tests (11 test cases)
  - Zero performance impact on system - all benchmarks maintained or improved
- **Row to JSON Conversion (2025-07-16)**: Complete row_to_json() function implementation
  - RowToJsonTranslator converts PostgreSQL subquery patterns to SQLite json_object() calls
  - Pattern matching for `SELECT row_to_json(t) FROM (SELECT ...) t` syntax with alias validation
  - Column extraction supporting both explicit (AS) and implicit aliases from SELECT clauses
  - SQLite function registration for simple value conversion cases
  - Integration with both simple and extended query protocols with proper type inference
  - Comprehensive test coverage across all scenarios (subqueries, aliases, multiple rows)
- **Complete JSON Function Test Coverage (2025-07-16)**: Comprehensive CI/CD validation suite
  - All JSON functions included in test_queries.sql for CI/CD pipeline validation
  - Test coverage: aggregation (json_agg, json_object_agg), table functions (json_each), manipulation (jsonb_insert, jsonb_delete, jsonb_pretty), existence checks
  - Fixed compatibility issues with row_to_json subquery patterns and JSON existence operators
  - 100% test success rate across all connection modes (TCP+SSL, TCP-only, Unix sockets, file databases)
  - Production-ready validation ensures reliable deployment across all supported configurations
  - Comprehensive test coverage: basic subqueries, multiple data types, column aliases, multiple rows
  - Full PostgreSQL compatibility for converting table rows to JSON objects

## Known Issues
- **Array function limitations**: 
  - ORDER BY in array_agg relies on outer query ORDER BY
  - Multi-array unnest support (advanced edge case)

## Database Handler Architecture
Uses a Mutex-based implementation for thread safety:
- Single `rusqlite::Connection` with `SQLITE_OPEN_FULL_MUTEX`
- `parking_lot::Mutex` for efficient synchronization
- Schema cache for performance
- Fast path optimization for simple queries

## Connection Pooling Configuration

Connection pooling provides improved performance for concurrent workloads by maintaining a pool of reusable SQLite connections. The system automatically routes read-only queries (SELECT, EXPLAIN) to the read pool while directing write operations to the primary connection.

### Features
- **Read/Write Separation**: Automatic query routing based on operation type
- **Health Checks & Recovery**: Background monitoring, automatic stale connection cleanup, failure recovery  
- **Transaction Affinity**: Ensures all operations within a transaction use the same connection for consistency
- **Configuration**: Pool size, timeouts, and health check intervals configurable via environment variables
- **Environment Control**: Enable via PGSQLITE_USE_POOLING=true environment variable (opt-in by default)

### Environment Variables
- `PGSQLITE_USE_POOLING=true` - Enable connection pooling (default: false)
- `PGSQLITE_POOL_SIZE=N` - Maximum connections in read pool (default: 5)
- `PGSQLITE_POOL_TIMEOUT=N` - Connection acquisition timeout in seconds (default: 30)
- `PGSQLITE_POOL_IDLE_TIMEOUT=N` - Idle connection timeout in seconds (default: 300)
- `PGSQLITE_POOL_HEALTH_INTERVAL=N` - Health check interval in seconds (default: 60)

### Usage Examples
```bash
# Enable pooling with default settings
PGSQLITE_USE_POOLING=true pgsqlite --database mydb.db

# Custom pool configuration
PGSQLITE_USE_POOLING=true \
PGSQLITE_POOL_SIZE=10 \
PGSQLITE_POOL_TIMEOUT=60 \
pgsqlite --database mydb.db

# Production configuration with health monitoring
PGSQLITE_USE_POOLING=true \
PGSQLITE_POOL_SIZE=8 \
PGSQLITE_POOL_IDLE_TIMEOUT=600 \
PGSQLITE_POOL_HEALTH_INTERVAL=30 \
pgsqlite --database production.db
```

### Query Routing Rules
- **SELECT, EXPLAIN**: Routed to read-only connection pool
- **INSERT, UPDATE, DELETE**: Routed to primary write connection
- **DDL (CREATE, ALTER, DROP)**: Routed to primary write connection
- **Transaction blocks**: All queries routed to write connection for consistency
- **Read-only PRAGMA**: Routed to read pool (e.g., `PRAGMA table_info`)
- **Write PRAGMA**: Routed to write connection (e.g., `PRAGMA journal_mode=WAL`)

### Performance Characteristics
- **Best for**: Concurrent read-heavy workloads, multiple client connections
- **Resource overhead**: Additional memory per pooled connection (~1-2MB each)
- **Latency**: Slight increase for connection acquisition (~0.1ms)
- **Throughput**: Significant improvement for concurrent SELECT operations

### When to Use Connection Pooling
- **Enable for**: TCP connections with multiple concurrent clients
- **Enable for**: Read-heavy workloads with frequent SELECT queries
- **Enable for**: Applications with sustained connection patterns
- **Disable for**: Single-client applications or simple scripts
- **Disable for**: Memory-constrained environments
- **Disable for**: Unix socket connections with low concurrency

## SSL/TLS Configuration
Enable via command line or environment variables:
- `--ssl` / `PGSQLITE_SSL=true` - Enable SSL support
- `--ssl-cert` / `PGSQLITE_SSL_CERT` - Path to SSL certificate
- `--ssl-key` / `PGSQLITE_SSL_KEY` - Path to SSL private key
- `--ssl-ca` / `PGSQLITE_SSL_CA` - Path to CA certificate (optional)
- `--ssl-ephemeral` / `PGSQLITE_SSL_EPHEMERAL` - Generate ephemeral certificates

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.