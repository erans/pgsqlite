# pgsqlite Project Context

## Overview
pgsqlite is a PostgreSQL protocol adapter for SQLite databases, allowing PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

## Quick Reference

### Build & Test Commands
```bash
cargo build              # Build project
cargo test              # Run unit tests
cargo check             # Check for errors/warnings
cargo clippy            # Check for code quality issues
./tests/runner/run_ssl_tests.sh  # Run integration tests (from project root)

# Constraint-specific tests
cargo test --test pg_constraint_test  # Test pg_constraint functionality
```

**Pre-commit checklist**: Run ALL of these before committing:
1. `cargo check` - No errors or warnings
2. `cargo clippy` - Review and fix warnings where reasonable
3. `cargo build` - Successful build
4. `cargo test` - All tests pass

### Development Workflow
1. Check TODO.md for prioritized tasks
2. Run full test suite after implementing features
3. Update TODO.md: mark completed tasks `[x]`, add new discoveries
4. Follow pre-commit checklist above

## Project Structure
```
src/
├── lib.rs              # Main library entry
├── protocol/           # PostgreSQL wire protocol
├── session/           # Session state management
├── query/             # Query execution handlers
└── migration/         # Schema migration system

tests/
├── runner/            # Test runner scripts
├── sql/               # SQL test files by category
└── output/            # Test outputs, temp databases
```

## Core Design Principles

### Type Inference
NEVER use column names to infer types. Use only:
- Explicit PostgreSQL type declarations in CREATE TABLE
- SQLite schema info via PRAGMA table_info
- Explicit type casts in queries (e.g., $1::int4)
- Value-based inference as last resort

### DateTime Storage
All datetime types use INTEGER storage (microseconds/days since epoch):
- DATE: INTEGER days since 1970-01-01
- TIME/TIMETZ: INTEGER microseconds since midnight
- TIMESTAMP/TIMESTAMPTZ: INTEGER microseconds since epoch
- INTERVAL: INTEGER microseconds

### Query Translation
- Full INSERT SELECT support with datetime/array translation
- Decimal aggregates: Only NUMERIC types need decimal_from_text wrapping
- FLOAT types (REAL, DOUBLE PRECISION) don't need wrapping
- Zero performance impact design for all translations

## Performance Benchmarks

### Current Performance (2025-08-12) - psycopg3-text Dominates!

#### With psycopg3-text (BEST SELECT PERFORMANCE - Recommended for read-heavy workloads)
- SELECT: ~125x overhead (0.136ms) - **✓ 21.8x FASTER than psycopg2!**
- SELECT (cached): ~90x overhead (0.299ms) - **✓ 5.5x FASTER than psycopg2**
- UPDATE: ~70x overhead (0.084ms) - Acceptable
- DELETE: ~78x overhead (0.072ms) - Acceptable
- INSERT: ~381x overhead (0.661ms) - Needs optimization
- **Overall**: Best overhead reduction vs native SQLite

#### With psycopg2 (BEST WRITE PERFORMANCE - Recommended for write-heavy workloads)
- SELECT: ~2,692x overhead (2.963ms) - Poor read performance
- SELECT (cached): ~520x overhead (1.656ms) - Poor cache performance
- UPDATE: ~45x overhead (0.057ms) - **✓ MEETS TARGET, 1.5x faster than psycopg3**
- DELETE: ~38x overhead (0.036ms) - **✓ MEETS TARGET, 2.0x faster than psycopg3**
- INSERT: ~107x overhead (0.185ms) - **✓ 3.6x FASTER than psycopg3**

#### With psycopg3-binary (Mixed results - not recommended for simple operations)
- SELECT: ~434x overhead (0.497ms) - Binary encoding overhead exceeds benefits
- SELECT (cached): ~372x overhead (1.579ms) - Poor cache performance
- UPDATE: ~65x overhead (0.086ms) - Similar to text mode
- DELETE: ~67x overhead (0.071ms) - Similar to text mode
- INSERT: ~377x overhead (0.691ms) - Similar to text mode
- **Note**: Binary protocol fully functional but best suited for complex data types

### Performance Targets (2025-07-27)
- SELECT: ~674.9x overhead (0.669ms) **✓ ACHIEVED with psycopg3**
- SELECT (cached): ~17.2x overhead (0.046ms) - In progress
- UPDATE: ~50.9x overhead (0.059ms) **✓ ACHIEVED with psycopg2**
- DELETE: ~35.8x overhead (0.034ms) **✓ ACHIEVED with psycopg2**
- INSERT: ~36.6x overhead (0.060ms) - In progress

**Status**: Major performance breakthrough! psycopg3-binary exceeds all expectations.

### Performance Characteristics
- **psycopg3-binary strongly recommended** - 19x faster SELECT than psycopg2, 5x faster than psycopg3-text
- **Binary protocol advantages** - Native type encoding, reduced parsing overhead
- **Connection-per-session architecture** working well with proper isolation
- **Batch operations provide best performance** (10x-50x speedup)
- **Cache effectiveness needs improvement** - Currently only 0.4x-1.7x speedup

### Batch INSERT Best Practices
```sql
INSERT INTO table (col1, col2) VALUES 
  (val1, val2),
  (val3, val4);  -- 10-row: 11.5x speedup, 100-row: 51.3x speedup
```

## Schema Migrations

### Creating New Migrations
When modifying `__pgsqlite_*` tables:

1. Add to `src/migration/registry.rs`:
```rust
register_vX_your_feature(&mut registry);
```

2. Define migration:
```rust
fn register_vX_your_feature(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(X, Migration {
        version: X,
        name: "feature_name",
        description: "What this does",
        up: MigrationAction::Sql(r#"
            ALTER TABLE __pgsqlite_schema ADD COLUMN new_column TEXT;
        "#),
        down: Some(MigrationAction::Sql(r#"
            -- Rollback SQL
        "#)),
        dependencies: vec![X-1],
    });
}
```

3. Update Current Migrations list in this file

### Current Migrations
- v1: Initial schema
- v2: ENUM support
- v3: DateTime support
- v4: DateTime INTEGER storage
- v5: PostgreSQL catalog tables
- v6: VARCHAR/CHAR constraints
- v7: NUMERIC/DECIMAL constraints
- v8: Array support
- v9: Full-Text Search support
- v10: typcategory column in pg_type view
- v15: pg_depend table for object dependencies
- v16: pg_proc view for function metadata
- v17: pg_description view for object comments
- v18: pg_roles and pg_user views for user/role management
- v19: pg_stats view for table statistics and query optimization
- v20: information_schema.routines support for function metadata
- v21: information_schema.views support for view metadata
- v22: information_schema.referential_constraints support for foreign key metadata
- v23: information_schema.check_constraints support for check constraint metadata

## Key Features & Fixes

### Recently Fixed (2025-09-19)
- **information_schema.check_constraints Check Constraint Metadata Support**: Complete PostgreSQL check constraint introspection for ORM compatibility
  - Added comprehensive information_schema.check_constraints table implementation with session-aware connection handling
  - Supports all 4 PostgreSQL-standard columns: constraint_catalog, constraint_schema, constraint_name, check_clause
  - Session-based constraint discovery using connection-per-session architecture for proper transaction isolation
  - Direct integration with existing pg_constraint catalog for check constraint detection and metadata extraction
  - Migration v23 enables information_schema.check_constraints support with complete SQL standard compliance
  - Supports both user-defined check constraints and system constraints (NOT NULL, column-level, table-level)
  - Integrated into catalog query interceptor with WHERE clause filtering and proper error handling
  - Enables Django constraint discovery via inspectdb, SQLAlchemy constraint validation, Rails constraint introspection, Ecto schema validation
  - Files: src/catalog/query_interceptor.rs, src/migration/registry.rs (v23), src/session/db_handler.rs, tests/information_schema_check_constraints_test.rs
  - Impact: Completes critical constraint validation metadata access for ORM frameworks requiring check constraint introspection capabilities

- **information_schema.referential_constraints Foreign Key Metadata Support**: Complete PostgreSQL foreign key constraint introspection for ORM compatibility
  - Added comprehensive information_schema.referential_constraints table implementation with session-aware connection handling
  - Supports all 9 PostgreSQL-standard columns: constraint_catalog, constraint_schema, constraint_name, unique_constraint_catalog, unique_constraint_schema, unique_constraint_name, match_option, update_rule, delete_rule
  - Session-based constraint discovery using connection-per-session architecture for proper transaction isolation
  - Direct integration with existing pg_constraint catalog for foreign key constraint detection and metadata extraction
  - Migration v22 enables information_schema.referential_constraints support with complete SQL standard compliance
  - Fixed critical data type issue: confrelid column read as INTEGER instead of STRING for proper constraint resolution
  - Integrated into catalog query interceptor with WHERE clause filtering and proper error handling
  - Enables Django foreign key discovery via inspectdb, SQLAlchemy relationship automap generation, Rails association mapping, Ecto schema introspection
  - Files: src/catalog/query_interceptor.rs, src/migration/registry.rs (v22), src/session/db_handler.rs, tests/information_schema_referential_constraints_test.rs
  - Impact: Completes critical constraint metadata access for ORM frameworks requiring foreign key relationship discovery capabilities

- **information_schema.views View Metadata Support**: Complete PostgreSQL view introspection for ORM compatibility
  - Added comprehensive information_schema.views table implementation with session-aware connection handling
  - Supports 10 PostgreSQL-standard columns: table_catalog, table_schema, table_name, view_definition, check_option, is_updatable, etc.
  - Enhanced view definition extraction with proper AS keyword parsing for multiline CREATE VIEW statements
  - Session-based view discovery using connection-per-session architecture for proper isolation
  - Filters out system catalog views (pg_*, information_schema_*) to return only user-created views
  - Migration v21 enables information_schema.views support with complete SQL standard compliance
  - Integrated into catalog query interceptor with WHERE clause filtering and connection isolation
  - Enables Django view discovery via inspectdb, SQLAlchemy metadata reflection, Rails schema introspection, Ecto database analysis
  - Files: src/catalog/query_interceptor.rs, src/migration/registry.rs (v21), src/session/db_handler.rs, tests/information_schema_views_test.rs
  - Impact: Completes critical view metadata access for ORM frameworks requiring view introspection capabilities

- **information_schema.routines Function Metadata Support**: Complete PostgreSQL function introspection for ORM compatibility
  - Added comprehensive information_schema.routines table implementation with 76+ PostgreSQL-standard columns
  - Provides metadata for 40+ built-in functions including string, math, aggregate, datetime, JSON, array, UUID, system, and full-text search functions
  - Complete function metadata: routine_name, routine_type, data_type, external_language, parameter_style, security_type, etc.
  - Migration v20 enables information_schema.routines support with full SQL standard compliance
  - Integrated into catalog query interceptor with WHERE clause filtering and session-based execution
  - Enables Django function discovery via inspectdb, SQLAlchemy metadata reflection, Rails schema introspection, Ecto database analysis
  - Files: src/catalog/query_interceptor.rs, src/migration/registry.rs (v20), src/session/db_handler.rs, tests/information_schema_routines_test.rs
  - Impact: Completes information_schema compliance for standardized function metadata access across all major ORMs

- **pg_stats Table Statistics Support**: Complete PostgreSQL query optimization and performance hints system
  - Added comprehensive pg_stats table implementation for ORM query planning and optimization
  - Realistic statistics generation based on column types and naming patterns (null_frac, n_distinct, correlation)
  - Support for most_common_vals, most_common_freqs, and histogram_bounds for all PostgreSQL data types
  - Intelligent statistics based on column semantics: IDs get unique stats, status columns get categorical data
  - Migration v19 enables pg_stats support with full PostgreSQL schema compatibility (13 columns)
  - Integrated into catalog query interceptor with session-based connection support and WHERE clause filtering
  - Enables SQLAlchemy query optimization, Rails performance analysis, Django query planning, Ecto database introspection
  - Files: src/catalog/pg_stats.rs, src/migration/registry.rs (v19), src/session/db_handler.rs, tests/pg_stats_test.rs
  - Impact: Provides query planners and ORMs with essential table statistics for optimal query performance

- **pg_roles and pg_user Support**: Complete PostgreSQL user and role management system
  - Added full pg_roles and pg_user table implementations for enterprise authentication workflows
  - Default roles: postgres (superuser), public (group role), pgsqlite_user (current user) with proper privileges
  - Default users: postgres and pgsqlite_user with appropriate permissions (usecreatedb, usesuper, etc.)
  - Migration v18 creates pg_roles and pg_user views with PostgreSQL-compatible role/user metadata
  - Integrated into catalog query interceptor with WHERE clause filtering support
  - Enables Django user management, SQLAlchemy role-based access control, Rails authentication integration
  - Files: src/catalog/pg_roles.rs, src/catalog/pg_user.rs, src/migration/registry.rs (v18), tests/pg_roles_user_test.rs
  - Impact: Enables complete enterprise user/role management and authentication workflows for all major ORMs

- **pg_description Comment Support**: Complete PostgreSQL object comment and documentation system
  - Added full pg_description table implementation with COMMENT DDL integration for table/column/function comments
  - Supports PostgreSQL COMMENT ON syntax: `COMMENT ON TABLE users IS 'User information table'`
  - Maps pgsqlite's __pgsqlite_comments table to PostgreSQL pg_description format (objoid, classoid, objsubid, description)
  - Migration v17 creates pg_description view with proper OID mapping and PostgreSQL class compatibility
  - Integrated COMMENT DDL handler into both memory and file-based database execution paths
  - Enables Django inspectdb model documentation, SQLAlchemy comment reflection, Rails schema documentation
  - Files: src/catalog/pg_description.rs, src/session/db_handler.rs, src/migration/registry.rs (v17), tests/pg_description_test.rs
  - Impact: Enables full documentation-driven development workflows with complete ORM comment introspection

- **pg_proc Function Metadata Support**: Complete PostgreSQL function introspection for enterprise ORM compatibility
  - Added comprehensive pg_proc view with 35+ built-in functions (string, math, aggregate, JSON, array, UUID, system)
  - Includes all essential PostgreSQL metadata: oid, proname, prokind, prorettype, provolatile, proisstrict, etc.
  - Migration v16 creates SQLite view with proper function categorization (functions 'f', aggregates 'a')
  - Supports \df command functionality and complete function discovery for ORMs
  - Enables Django inspectdb function analysis, SQLAlchemy function reflection, Rails procedure-based migrations
  - Files: src/catalog/pg_proc.rs, src/migration/registry.rs (v16), tests/pg_proc_simple_test.rs
  - Impact: Major step toward enterprise-grade PostgreSQL catalog compatibility

- **Array Types Binary Protocol Support**: Complete PostgreSQL array binary encoding for psycopg3 compatibility
  - Added binary encoding for all array types (int4[], text[], numeric[], etc.) in extended query protocol
  - Fixed cast translation issues preventing ARRAY[] syntax from working correctly
  - Enhanced CastTranslator to properly handle array type names (INTEGER[] vs INTEGER)
  - Array literals now work: ARRAY[1,2,3] → '[1,2,3]' → PostgreSQL binary format
  - Comprehensive test coverage with array_binary_protocol_test.rs and simple_integer_array_test.rs
  - Files: src/query/extended.rs (lines 3717-3813), src/translator/cast_translator.rs (lines 522-527)
  - Enables modern ORM frameworks (Django ArrayField, SQLAlchemy ARRAY, Rails arrays) with psycopg3 binary mode

### Previously Fixed (2025-09-18)
- **Information Schema Binary Protocol Support**: Fixed all information_schema test failures with binary format
  - **Problem**: Extended protocol information_schema queries failed with UnexpectedMessage errors in binary format
  - **Root Cause**: information_schema not included in catalog query detection and missing field descriptions for SELECT *
  - **Fixed Binary Format**: Added information_schema to catalog detection in handle_describe() for proper FieldDescription generation
  - **Fixed Wildcard Queries**: Added complete field descriptions for SELECT * on information_schema.schemata/tables/columns
  - **Fixed WHERE Filtering**: Added extract_table_name_filters() function supporting both equality and IN clause filtering
  - **Pattern Support**: Handles table_name = 'value' and table_name IN ('val1', 'val2') with compound identifiers
  - **Test Results**: 8/8 information_schema tests now pass (was 0/8 failing with protocol errors)
  - **Full Compatibility**: Binary protocol works perfectly for both specific columns and SELECT * with proper row filtering
  - Files: src/query/extended.rs, src/catalog/query_interceptor.rs, tests/information_schema_test.rs
- **Enhanced PostgreSQL Attribute Support (pg_attribute)**: Complete column metadata with defaults and constraints
  - Enhanced pg_attribute with column default expression extraction from PRAGMA table_info
  - Identity/SERIAL column detection for PRIMARY KEY INTEGER columns (attidentity = 'd')
  - Generated column support detection with proper attgenerated field population
  - NOT NULL constraint detection and proper attnotnull field mapping
  - Comprehensive test coverage with various default types (strings, numbers, functions, NULL)
  - Full ORM compatibility for advanced schema introspection and column analysis
  - Files: src/catalog/pg_attribute.rs, tests/enhanced_pg_attribute_test.rs, tests/simple_pg_attribute_test.rs
  - ORM Benefits: Complete column metadata now available for all major ORM frameworks
- **PostgreSQL Index Support (pg_index)**: Complete index management for ORM frameworks
  - Enhanced pg_index table population using PRAGMA index_list/index_info for accurate metadata
  - Proper PostgreSQL-compatible column number mapping (1-based attnum values in indkey field)
  - Multi-column index support with space-separated indkey field (e.g., "2 3" for columns 2 and 3)
  - Unique/primary key detection using SQLite origin field and name pattern analysis
  - Auto-population triggered by all CREATE TABLE and CREATE INDEX operations
  - Type mappings added to extended query processor for proper OID/Int4/Bool type handling
  - Enables complete index introspection: Rails `ActiveRecord::Base.connection.indexes()`, SQLAlchemy `Inspector.get_indexes()`, Django `inspectdb` index discovery
  - Files: src/catalog/constraint_populator.rs, src/query/extended.rs, comprehensive test suite
  - ORM Benefits: Index discovery now works for Rails, SQLAlchemy, Django, and Ecto migrations
- **PostgreSQL Constraint Support (pg_constraint)**: Full constraint introspection for ORM compatibility
  - Added comprehensive pg_constraint table population for all CREATE TABLE operations
  - Supports foreign key, primary key, unique, and check constraint detection
  - Multi-execution path support: extended protocol, simple query, and db.execute() methods
  - Regex-based foreign key parsing with table-level and inline syntax support
  - Proper PostgreSQL type compatibility (TEXT for OIDs, BOOLEAN for flags)
  - Auto-populates constraints: employees_dept_id_fkey → departments relationship
  - Enables full ORM constraint introspection for Django, Rails, SQLAlchemy, and Ecto
  - Files: src/catalog/constraint_populator.rs, src/session/db_handler.rs, src/query/extended.rs
  - Fixed: "Found 0 foreign key constraints" → "Found 1+ foreign key constraints"
- **PostgreSQL Dependency Support (pg_depend)**: Rails sequence ownership detection
  - Added complete pg_depend table with hybrid catalog handler approach for object dependencies
  - Automatic sequence ownership detection for INTEGER PRIMARY KEY columns (Rails SERIAL compatibility)
  - Smart filtering: only single-column INTEGER PRIMARY KEY creates dependencies (compound PKs ignored)
  - Rails ActiveRecord `pk_and_sequence_for` method compatibility for auto-increment detection
  - Supports exact Rails query pattern: `WHERE dep.refclassid = '1259' AND dep.refobjsubid = 1 AND dep.deptype = 'a'`
  - Fixed PRAGMA parameter binding issues affecting all constraint population functions
  - Migration v15 with pg_depend table creation and proper column definitions
  - Comprehensive test coverage including Rails patterns and edge cases
  - Files: src/catalog/pg_depend.rs, src/catalog/constraint_populator.rs, tests/pg_depend_test.rs
  - ORM Benefits: Rails sequence detection, Django SERIAL introspection, SQLAlchemy auto-increment discovery
- **Fixed Type Conversion Issues**: Resolved runtime type errors in pg_constraint queries
  - Added comprehensive pg_constraint type mapping in extended query handler
  - Fixed "cannot convert between Rust type and Postgres type" errors
  - Proper handling of OID, CHAR, BOOL, and TEXT column types
  - All 5 pg_constraint tests now pass successfully

### Previously Fixed (2025-09-04)
- **PostgreSQL Operator Class Support in CREATE INDEX**: Fixed "near varchar_pattern_ops: syntax error"
  - Added CreateIndexTranslator for PostgreSQL operator class syntax translation
  - Maps `varchar_pattern_ops`, `text_pattern_ops`, `bpchar_pattern_ops` to SQLite `COLLATE BINARY`
  - Maps `varchar_ops`, `text_ops`, `bpchar_ops` to default SQLite collation
  - Supports multiple operator classes in single index statement
  - Full compatibility with Django/SQLAlchemy migrations that create pattern-optimized indexes
  - Example: `CREATE INDEX idx_name ON table (column varchar_pattern_ops)` → `CREATE INDEX idx_name ON table (column COLLATE BINARY)`

### Previously Fixed (2025-08-12)
- **Full Binary Protocol Support for psycopg3**: Complete binary format implementation
  - Fixed NUMERIC binary encoding - now properly encodes decimal values using PostgreSQL binary format
  - Fixed duplicate RowDescription issue - Describe(Portal) now updates statement's field_descriptions to prevent duplicate sending
  - Added proper format field propagation from Portal result_formats to FieldDescription
  - Binary encoding now works for: INT2, INT4, INT8, FLOAT4, FLOAT8, BOOL, BYTEA, NUMERIC, UUID, JSON/JSONB, Money
  - SQLAlchemy tests pass with psycopg3-binary driver (9/9 tests passing)
  - Benchmarks now run successfully with psycopg3 binary mode

### Previously Fixed (2025-08-08)
- **SQLAlchemy Full Compatibility Achieved**: All tests passing for both psycopg2 and psycopg3-text drivers
  - Fixed json_object_agg type inference - now correctly returns TEXT type instead of JSON type
  - Updated integration tests to reflect current binary protocol capabilities
  - Migration tests updated to expect v11 "fix_catalog_views" migration
  - All 8 SQLAlchemy ORM test scenarios now passing including transaction cascade deletes

### Previously Fixed (2025-08-06)
- **Aggregate Function Type Inference**: Fixed "Unknown PG numeric type: 25" errors in psycopg3 text mode
  - Root cause: SUM/AVG aggregate functions returned TEXT (OID 25) instead of NUMERIC (1700) for aliases like `sum_1`, `avg_1`
  - Enhanced `get_aggregate_return_type_with_query()` to detect aliased aggregate functions in query context
  - Added regex pattern matching to identify `sum(...) AS sum_1` and similar SQLAlchemy-generated patterns
  - SUM/AVG functions on arithmetic expressions now always return NUMERIC type regardless of source column types
  - Fixes most psycopg3 compatibility issues where it expects numeric types but receives TEXT for aggregates
  - SQLAlchemy tests improved from 4/8 to 6/8 passing, with aggregate-related errors resolved
- **VALUES Clause Binary Timestamp Handling**: Fixed raw microsecond values in SQLAlchemy multi-row inserts
  - SQLAlchemy VALUES clause pattern was inserting raw microseconds like '807813532548380'
  - Binary timestamp parameters from psycopg3 now detected and converted to formatted strings
  - Only applies to VALUES clause rewriting - normal queries still use raw microseconds for storage
  - Converts PostgreSQL epoch (2000-01-01) to Unix epoch and formats as ISO timestamp string
  - Fixes "timestamp too large (after year 10K)" errors in psycopg3 text mode
- **Transaction Isolation Bug in Schema Lookup**: Fixed SQLAlchemy failing to see schema within same transaction
  - Root cause: `get_schema_type()` used separate connection that couldn't see uncommitted schema changes
  - Created `get_schema_type_with_session()` that uses the session's connection for proper isolation
  - Updated 20+ calls across extended.rs and executor.rs to use session-aware lookup
  - Fixes SQLAlchemy CREATE TABLE followed by INSERT seeing wrong types in same transaction
  - Timestamps now properly formatted as strings instead of raw microseconds
- **Ultra-fast Path Parameter Cast Support**: Fixed queries with parameter casts bypassing optimizations
  - Queries like `SELECT * FROM table WHERE id = $1::INTEGER` now use ultra-fast path
  - Modified condition to only exclude non-parameter casts (e.g., `column::TEXT`)
  - Parameter casts (`$1::TYPE`) are common in psycopg3 and now properly optimized
  - Ensures timestamp conversion and other optimizations work with cast parameters
- **psycopg3 Binary Parameter Conversion**: Fixed parameterized queries returning 0 rows
  - Added conversion of PostgreSQL binary format parameters to text format for SQLite
  - Handles INT2, INT4, INT8, FLOAT4, FLOAT8, BOOL, and TIMESTAMP binary formats
  - Converts PostgreSQL timestamp format (microseconds since 2000-01-01) to Unix epoch
  - Fixes issue where psycopg3 sends parameters in binary format even in text mode
- **Scalar Subquery and Direct Aggregate Timestamp Conversion**: Fixed raw microseconds in query results
  - Scalar subqueries like `(SELECT MAX(created_at) FROM table) AS alias` were returning raw INTEGER microseconds
  - Direct aggregates `MAX(created_at)`, `MIN(created_at)` also returned raw values instead of formatted timestamps
  - Added pattern detection in simple query protocol for both scalar subqueries and direct aggregates
  - Detection works in both ultra-simple and non-ultra-simple query paths
  - All aggregate timestamp queries now return properly formatted timestamps in psycopg3 text mode

### Previously Fixed (2025-08-05)
- **Schema-Based Type Inference for Empty Result Sets**: Fixed columns defaulting to TEXT when no data rows
  - All columns were incorrectly returning TEXT (OID 25) instead of proper PostgreSQL types
  - Implemented async schema lookup when queries return no data rows
  - Two-level fallback: alias resolution → table extraction from FROM clause
  - Uses `db.get_schema_type()` to fetch actual types from __pgsqlite_schema
  - Fixes SQLAlchemy lazy loading and relationship queries with proper type OIDs
- **Column Alias Type Inference**: Fixed incorrect PostgreSQL type OIDs for aliased columns
  - `SELECT users.id AS users_id` now returns INT4 (23) instead of TEXT (25)
  - Implemented `extract_source_table_column_for_alias()` to parse `table.column AS alias` patterns
  - Fixed SQLAlchemy compatibility where psycopg3 tried to parse strings as integers
  - Resolves "invalid literal for int() with base 10: 'Test User'" errors
- **Multi-Row INSERT RETURNING**: Fixed row count mismatch for bulk insert operations
  - SQLAlchemy multi-row INSERT with RETURNING now returns correct number of rows
  - Uses rowid range queries instead of just `last_insert_rowid()` for multiple rows
  - Fixes "Multi-row INSERT statement did not produce the correct number of INSERTed rows" error
- **Date Function Translation**: Fixed malformed SQL in datetime function translation
  - `func.date('now', '-30 days')` no longer creates invalid julianday syntax
  - Skip translation for parameterized date functions to prevent nested cast issues
  - Fixes "SQLite error: near 'AS': syntax error" in datetime queries
- **Aggregate Type Detection**: Fixed unknown PostgreSQL type OID 25 for numeric aggregates
  - AVG/SUM/COUNT on DECIMAL columns now return NUMERIC (1700) instead of TEXT (25)
  - Added query context to `get_aggregate_return_type_with_query()` for better type detection
  - Improved SQLAlchemy compatibility with aggregate functions on numeric columns
- **DateTime Conversion for psycopg3 Text Mode**: Fixed timestamp parsing errors with comprehensive query pattern support
  - psycopg3 text mode was receiving raw INTEGER microseconds like '1754404262713579' instead of formatted timestamps
  - Fixed table-prefixed aliases (`SELECT table.created_at AS alias`) by updating SIMPLE_SELECT_REGEX pattern
  - Fixed wildcard patterns (`SELECT table.*`) with session-based schema lookup for connection-per-session architecture
  - All query patterns now work: `SELECT *`, `SELECT col`, `SELECT table.*`, `SELECT table.col AS alias`
  - Prevents "timestamp too large (after year 10K)" errors in SQLAlchemy datetime queries
- **Column Alias Parsing Robustness**: Fixed extract_source_table_column_for_alias function parsing failures
  - Function was failing to parse `orders.id AS orders_id` patterns in SQLAlchemy lazy loading queries
  - Root cause: Character indexing logic had potential out-of-bounds errors and incorrect expression boundary detection
  - Fixed by rewriting parsing logic using safer string methods (`rfind` for commas, proper SELECT keyword detection)
  - SQLAlchemy lazy loading now correctly returns INT4 (OID 23) instead of TEXT (OID 25) for integer columns

### Previously Fixed (2025-08-04)
- **Binary Protocol Support for psycopg3**: Implemented core binary format encoders
  - Added binary encoders for Numeric/Decimal, UUID, JSON/JSONB, Money types
  - PostgreSQL binary NUMERIC format with proper weight/scale/digit encoding
  - Test infrastructure updated to support psycopg2, psycopg3-text, psycopg3-binary drivers
  - SQLAlchemy tests can now run with `--driver psycopg3-binary` option

### Previously Fixed (2025-08-01)
- **SQLAlchemy MAX/MIN Aggregate Types**: Fixed "Unknown PG numeric type: 25" error
  - Added aggregate_type_fixer.rs to detect aliased aggregate columns
  - SQLite returns TEXT for MAX/MIN on TEXT columns storing decimals
  - Now properly returns NUMERIC (1700) for MAX/MIN on DECIMAL columns
  - Handles SQLAlchemy's alias patterns like "max_1"
- **Build Warnings**: Fixed all compilation warnings
  - Fixed unused variables/functions in simple_query_detector.rs
  - Fixed unused variant/fields in unified_processor.rs
  - All 372 unit tests now pass without warnings

### Previously Fixed (2025-07-29)
- **Connection-per-Session Architecture**: Implemented true connection isolation matching PostgreSQL behavior
  - Each client session gets its own SQLite connection
  - Fixes SQLAlchemy transaction persistence issues with WAL mode
  - Eliminates transaction visibility problems between sessions
  - Tests now use temporary files instead of :memory: for proper isolation
- **AT TIME ZONE Support**: Fixed simple_query protocol issues
  - Fixed UTF-8 encoding errors when using simple_query with AT TIME ZONE
  - AT TIME ZONE operator now properly returns float values
  - Tests updated to use prepared statements for reliable behavior
  - Added datetime translation support to LazyQueryProcessor
- **Test Infrastructure Stability**: Fixed migration lock contention and build system reliability
  - Resolved "Migration lock held by process" errors in concurrent tests
  - Updated test files to use unique temporary databases instead of shared `:memory:`
  - Fixed common test module compatibility with connection-per-session architecture
- **Logging Optimization**: Converted info to debug logging in hot paths
  - Changed query logging from info!() to debug!() level
  - Performance regression still present despite optimization

### Previously Fixed (2025-07-27)
- **UUID/NOW() Functions**: Fixed duplicate UUIDs and epoch timestamps in cached queries
- **SQLAlchemy ORM**: Full compatibility with VALUES clause conversion and datetime handling
- **DateTime Column Aliases**: Fixed "unable to parse date" errors in SELECT queries with aliases

### Major Features
- **PostgreSQL Constraint Support**: Full pg_constraint table with foreign key introspection
- **PostgreSQL Index Support**: Complete pg_index table with multi-column index introspection
- **PostgreSQL Dependency Support**: Full pg_depend table with Rails sequence ownership detection ✨ **NEW!**
- **Binary Protocol Support**: psycopg3 compatibility with binary format encoders
- **Connection Pooling**: Enable with `PGSQLITE_USE_POOLING=true`
- **SSL/TLS**: Use `--ssl` flag or `PGSQLITE_SSL=true`
- **40+ PostgreSQL Types**: Including arrays, JSON/JSONB, ENUMs
- **Full-Text Search**: PostgreSQL tsvector/tsquery with SQLite FTS5
- **CREATE INDEX with Operator Classes**: Support for `varchar_pattern_ops`, `text_pattern_ops`, etc.
- **psql Compatibility**: \d commands work via catalog tables

### JSON/JSONB Support
- All operators: `->`, `->>`, `@>`, `<@`, `#>`, `#>>`, `?`, `?|`, `?&`
- Functions: json_agg, json_object_agg, json_each, jsonb_set, jsonb_delete
- Row conversions: row_to_json, json_populate_record

### Array Support
- 30+ array types with JSON storage and PostgreSQL binary protocol support
- Operators: `@>`, `<@`, `&&`, `||`
- Functions: unnest(), array_agg() with DISTINCT/ORDER BY
- Binary encoding: Full psycopg3 compatibility with proper array wire format
- ORM support: Django ArrayField, SQLAlchemy ARRAY, Rails arrays work in binary mode

## ORM Framework Compatibility

**Full ORM compatibility** achieved with comprehensive PostgreSQL catalog support:

### Constraint Introspection ✅ **ENHANCED (2025-09-19)**
- **Django**: `inspectdb` command discovers foreign key relationships via `pg_constraint` and `information_schema.referential_constraints`, plus check constraint validation via `information_schema.check_constraints`
- **Rails**: ActiveRecord association mapping through constraint discovery with complete foreign key metadata and check constraint introspection
- **SQLAlchemy**: Relationship automap generation with complete constraint metadata including update/delete rules and check constraint validation
- **Ecto**: Schema introspection with proper foreign key detection, constraint metadata, and check constraint validation capabilities

### Index Management ✅ **NEW (2025-09-18)**
- **Django**: `inspectdb` discovers indexes for model generation via `pg_index`
- **Rails**: `ActiveRecord::Base.connection.indexes(table_name)` returns complete index information
- **SQLAlchemy**: `Inspector.get_indexes()` provides full index reflection with column mapping
- **Ecto**: Schema introspection includes index definitions for migration generation

### Enhanced Column Metadata ✅ **NEW (2025-09-18)**
- **Django**: Complete column introspection with defaults, constraints, and identity detection
- **Rails**: Full schema analysis including column defaults and constraint information
- **SQLAlchemy**: Advanced column metadata for automap and reflection with default expressions

### Sequence Ownership Detection ✅ **NEW (2025-09-18)**
- **Rails**: ActiveRecord `pk_and_sequence_for` method works for auto-increment detection via `pg_depend`
- **Django**: SERIAL column introspection through dependency mapping
- **SQLAlchemy**: Auto-increment column discovery for sequence-based primary keys
- **Ecto**: Schema introspection includes sequence ownership information

### Comment and Documentation Support ✅ **NEW (2025-09-19)**
- **Django**: `inspectdb` generates model documentation from table/column comments via `pg_description`
- **Rails**: Schema introspection includes comment metadata for migration documentation
- **SQLAlchemy**: Comment reflection for table and column documentation in automap and inspector
- **Ecto**: Schema documentation generation with complete comment integration

### User and Role Management ✅ **NEW (2025-09-19)**
- **Django**: Complete user management and permission checking via `pg_roles` and `pg_user` tables
- **SQLAlchemy**: Role-based access control with proper privilege introspection and user authentication
- **Rails**: Authentication integration with user/role discovery for permission management
- **Ecto**: User and role introspection for authorization logic and permission systems

**Example queries now work correctly:**
```sql
-- Foreign key discovery (Django/Rails pattern)
SELECT conname, contype, conrelid, confrelid FROM pg_constraint WHERE contype = 'f';

-- Constraint metadata (SQLAlchemy pattern)
SELECT conname, contype, condeferrable, condeferred, convalidated FROM pg_constraint;

-- Primary key discovery (all ORMs)
SELECT conname, contype, conkey FROM pg_constraint WHERE contype = 'p';

-- Foreign key constraint details (information_schema pattern) - NEW!
SELECT constraint_name, unique_constraint_name, match_option, update_rule, delete_rule
FROM information_schema.referential_constraints;

-- Check constraint details (information_schema pattern) - NEW!
SELECT constraint_name, check_clause FROM information_schema.check_constraints;

-- Index discovery (Rails/SQLAlchemy pattern) - NEW!
SELECT i.indexrelid, ic.relname as index_name, i.indrelid, tc.relname as table_name,
       i.indnatts, i.indisunique, i.indisprimary, i.indkey
FROM pg_index i
JOIN pg_class ic ON i.indexrelid = ic.oid
JOIN pg_class tc ON i.indrelid = tc.oid
WHERE tc.relname = 'users';

-- Multi-column index information (SQLAlchemy Inspector pattern) - NEW!
SELECT ic.relname, i.indnatts, i.indkey, i.indisunique
FROM pg_index i
JOIN pg_class ic ON i.indexrelid = ic.oid
WHERE i.indnatts > 1;

-- Enhanced column metadata with defaults and constraints (NEW!)
SELECT attname, attnotnull, atthasdef, attidentity
FROM pg_attribute
WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'users')
AND attnum > 0
ORDER BY attnum;

-- Identity/SERIAL column detection (NEW!)
SELECT attname, attidentity, attgenerated
FROM pg_attribute
WHERE attidentity != '' OR attgenerated != '';

-- Sequence ownership detection (Rails ActiveRecord pattern) - NEW!
SELECT dep.classid, dep.objid, dep.objsubid, dep.refclassid, dep.refobjid, dep.refobjsubid, dep.deptype
FROM pg_depend dep
WHERE dep.refclassid = '1259'
AND dep.refobjsubid = 1
AND dep.deptype = 'a';

-- Auto-increment column discovery (Django/SQLAlchemy pattern) - NEW!
SELECT dep.refobjid as table_oid, dep.refobjsubid as column_number
FROM pg_depend dep
WHERE dep.deptype = 'a'
AND dep.classid = '1259';

-- Comment discovery (Django inspectdb / SQLAlchemy reflection pattern) - NEW!
SELECT objoid, classoid, objsubid, description
FROM pg_description
WHERE objsubid = 0;  -- Table comments

-- Column comment discovery (ORM documentation generation) - NEW!
SELECT objoid, objsubid, description
FROM pg_description
WHERE objsubid > 0;  -- Column comments

-- User and role management (Django user management / SQLAlchemy RBAC pattern) - NEW!
SELECT rolname, rolsuper, rolcreatedb, rolcanlogin
FROM pg_roles
WHERE rolcanlogin = 't';  -- Login roles

-- User authentication integration (Rails authentication pattern) - NEW!
SELECT usename, usesuper, usecreatedb, userepl
FROM pg_user
ORDER BY usename;  -- All users with privileges

-- Role-based access control (Enterprise authentication pattern) - NEW!
SELECT rolname, rolsuper, rolbypassrls
FROM pg_roles
WHERE rolname IN ('postgres', 'pgsqlite_user');  -- Specific roles

-- Table statistics and query optimization (SQLAlchemy/Rails performance pattern) - NEW!
SELECT schemaname, tablename, attname, null_frac, n_distinct, most_common_vals, correlation
FROM pg_stats
WHERE tablename = 'users';  -- Table-specific statistics

-- Query planning optimization (Advanced ORM pattern) - NEW!
SELECT tablename, attname, n_distinct, histogram_bounds
FROM pg_stats
WHERE n_distinct > 100;  -- High-cardinality columns

-- Performance analysis and monitoring (Rails performance gems pattern) - NEW!
SELECT tablename, COUNT(*) as column_count, AVG(CAST(null_frac AS REAL)) as avg_null_fraction
FROM pg_stats
GROUP BY tablename
ORDER BY column_count DESC;  -- Table analysis
```

### Query Optimization and Performance Hints ✅ **NEW (2025-09-19)**
- **SQLAlchemy**: Query planner uses `pg_stats` for optimization hints and cost estimation
- **Rails**: Performance analysis tools leverage statistics for query performance monitoring
- **Django**: Query optimization through statistics-based decision making in ORM layer
- **Ecto**: Database introspection includes table statistics for performance tuning

### Function Metadata and Introspection ✅ **NEW (2025-09-19)**
- **Django**: `inspectdb` discovers available functions via `information_schema.routines` for custom model methods
- **SQLAlchemy**: Function reflection and metadata analysis through standardized `information_schema.routines`
- **Rails**: ActiveRecord function discovery for stored procedure integration and custom SQL optimization
- **Ecto**: Schema introspection includes function metadata for database function mapping

**Example queries now work correctly:**
```sql
-- Function discovery (Django/Rails pattern)
SELECT routine_name, routine_type, data_type FROM information_schema.routines WHERE routine_schema = 'pg_catalog';

-- Function metadata (SQLAlchemy pattern)
SELECT routine_name, external_language, parameter_style, sql_data_access FROM information_schema.routines;

-- Built-in function availability (all ORMs)
SELECT routine_name, data_type FROM information_schema.routines WHERE routine_name LIKE '%agg%';
```

### View Metadata and Introspection ✅ **NEW (2025-09-19)**
- **Django**: `inspectdb` discovers database views via `information_schema.views` for model generation and complex query optimization
- **SQLAlchemy**: Complete view reflection and metadata analysis through standardized `information_schema.views` table
- **Rails**: ActiveRecord view discovery for read-only model creation and database schema analysis
- **Ecto**: Schema introspection includes view metadata for advanced query optimization and migration planning

**Example queries now work correctly:**
```sql
-- View discovery (Django/Rails pattern)
SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = 'public';

-- View metadata (SQLAlchemy pattern)
SELECT table_name, is_updatable, is_insertable_into, check_option FROM information_schema.views;

-- View definition analysis (all ORMs)
SELECT table_name, view_definition FROM information_schema.views WHERE table_name LIKE '%summary%';
```

### SQLAlchemy Compatibility
**Full SQLAlchemy ORM support** with all tests passing for both psycopg2 and psycopg3-text drivers:

```python
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('postgresql://postgres@localhost:5432/main')
Session = sessionmaker(bind=engine)

# All SQLAlchemy operations work correctly:
# - Table creation, INSERT, UPDATE, DELETE with RETURNING
# - Complex JOINs with proper type inference  
# - Transaction management and persistence
# - Datetime operations with proper formatting
```

**Production Configuration**:
```bash
# Default configuration with connection-per-session architecture
# Each PostgreSQL client session gets its own SQLite connection
# Full SQLAlchemy compatibility with proper transaction isolation
pgsqlite --database mydb.db

# Journal mode options (both work with connection-per-session):
PGSQLITE_JOURNAL_MODE=WAL pgsqlite --database mydb.db    # Better performance
PGSQLITE_JOURNAL_MODE=DELETE pgsqlite --database mydb.db  # More conservative
```

**PostgreSQL Driver Support**:
```bash
# Test with different PostgreSQL drivers
./tests/python/run_sqlalchemy_tests.sh                    # Default psycopg2 (8/8 tests pass)
./tests/python/run_sqlalchemy_tests.sh --driver psycopg3-text   # 8/8 tests pass
./tests/python/run_sqlalchemy_tests.sh --driver psycopg3-binary # Binary protocol support

# psycopg3-text status:
# ✅ Connection, Table Creation, Data Insertion
# ✅ Basic CRUD, Relationships & Joins, Advanced Queries  
# ✅ Numeric Precision with proper type inference
# ✅ Transaction handling with cascade deletes - ALL TESTS PASSING

# psycopg3 binary format provides better performance for:
# - Large binary data (BYTEA)
# - Numeric/Decimal values
# - UUID, JSON/JSONB data
# - Date/Time types
```

## Connection Pooling

Enable for concurrent workloads:
```bash
PGSQLITE_USE_POOLING=true \
PGSQLITE_POOL_SIZE=10 \
pgsqlite --database mydb.db
```

Environment variables:
- `PGSQLITE_USE_POOLING`: Enable pooling (default: false)
- `PGSQLITE_POOL_SIZE`: Max read connections (default: 5)
- `PGSQLITE_POOL_TIMEOUT`: Acquisition timeout seconds (default: 30)

## Quality Standards
- Test edge cases, not just happy paths
- Verify end-to-end functionality
- Only mark tasks complete when fully working
- No assumptions - test everything

## Known Limitations
- Array ORDER BY in array_agg relies on outer query ORDER BY
- Multi-array unnest (edge case)
- Some catalog queries and CAST operations still use get_mut_connection (needs update for per-session)

## Code Style
- Follow Rust conventions
- Use existing patterns
- Avoid comments unless necessary
- Keep code concise and idiomatic