# pgsqlite Project Context

## Overview
pgsqlite is a PostgreSQL protocol adapter for SQLite databases. It allows PostgreSQL clients to connect to and query SQLite databases using the PostgreSQL wire protocol.

## Project Structure
- `src/` - Main source code directory
  - `lib.rs` - Main library entry point
  - `protocol/` - PostgreSQL wire protocol implementation
  - `session/` - Session state management
  - `query/` - Query execution handlers
- `tests/` - Test files
- `Cargo.toml` - Rust project configuration
- `TODO.md` - Comprehensive task list for future development

## Build Commands
- `cargo build` - Build the project
- `cargo test` - Run tests
- `cargo run` - Run the project

## Development Workflow
- After implementing any feature, always run the full test suite with `cargo test` to ensure nothing is broken
- **ALWAYS update TODO.md when completing work or discovering new tasks**:
  - Mark completed tasks with `[x]`
  - Add new discovered tasks or subtasks
  - Document partial progress with detailed notes
  - Update task descriptions if implementation reveals complexity
- Check TODO.md for prioritized tasks when planning development work
- Use TODO.md as the authoritative source for tracking all future work

## Code Style
- Follow Rust conventions
- Use existing imports and patterns
- Avoid adding comments unless necessary
- Keep code concise and idiomatic

## Recent Work
- Fixed unused import warning in src/lib.rs:63 (removed unused `info` import)
- Added support for new PostgreSQL types: MONEY, INT4RANGE, INT8RANGE, NUMRANGE, CIDR, INET, MACADDR, MACADDR8, BIT, BIT VARYING
- Refactored CreateTableTranslator to use TypeMapper instead of duplicating type mapping logic
- Enhanced TypeMapper with pg_to_sqlite_for_create_table() method for handling SERIAL types and parametric types
- Improved CREATE TABLE parsing to handle multi-word types like "TIMESTAMP WITH TIME ZONE"
- Fixed TypeMapper SQLite->PostgreSQL mapping consistency to match SchemaTypeMapper (INTEGER maps to int4, not int8)
- Implemented custom DECIMAL type using rust_decimal for NUMERIC, REAL, DOUBLE PRECISION, FLOAT4, and FLOAT8 types
- Added automatic query rewriting to use decimal functions for arithmetic and comparisons
- **Enhanced decimal query rewriting for complex queries**:
  - Fixed correlated subquery context inheritance (test_lateral_join_simulation now passes)
  - Improved aggregate function decimal wrapping to distinguish NUMERIC vs FLOAT types
  - Enhanced derived table decimal type propagation for WHERE clause rewriting
  - Added context merging for subqueries to recognize outer table columns
  - Fixed recursive CTE decimal rewriting by properly handling table aliases in SetExpr processing
- Enhanced support for subqueries and CTEs in decimal query rewriting

## Known Test Issues
- datatype_compat_test: Contains fundamentally broken tests that create INTEGER columns but try to extract as i16 (should be i32)
- array_types_test: Pre-existing array handling issues unrelated to type mapping changes
These tests were failing due to incorrect type expectations or pre-existing implementation limitations, not due to recent changes

## Recently Fixed Tests
- test_create_table_translator_uses_type_mapper: Updated test expectations to match current DECIMAL type support (NUMERIC and DOUBLE PRECISION now correctly map to DECIMAL instead of TEXT for proper decimal arithmetic support)
- test_smallint_metadata_fixed: Fixed by using simple_query instead of parameterized queries for system table access (parameterized queries on __pgsqlite_schema were causing UnexpectedMessage errors)

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

## Quality Standards
To avoid idiot behavior:
- Write tests that actually verify functionality, not tests that are designed to pass easily
- Only mark tasks as complete when they are actually finished and working
- Test edge cases and error conditions, not just happy paths
- Verify implementations work end-to-end, not just in isolation
- Don't claim something works without actually testing it

## Database Handler Architecture (2025-06-30)

### Background
The initial implementation used a channel-based approach with a dedicated thread for SQLite operations. This provided thread safety but introduced significant performance overhead (~20-30x vs raw SQLite).

### Performance Investigation
Multiple approaches were benchmarked:
- **Channel-based DbHandler**: ~20-27x overhead (original implementation)
- **Direct Executor with RwLock pool**: ~8.1-10.7x overhead
- **Simple Executor**: ~7.7-9.9x overhead 
- **Mutex-based Handler**: ~7.7-9.6x overhead (best performance)

### Final Architecture Decision
After extensive benchmarking, we chose a **Mutex-based implementation** as the sole database handler:

**Reasons for this choice:**
1. **Best Performance**: 2.2-3.5x faster than the channel-based approach
2. **Simplicity**: Single connection with Mutex is simpler than connection pooling
3. **Thread Safety**: Achieved through `parking_lot::Mutex` + SQLite's FULLMUTEX mode
4. **Minimal Overhead**: Nearly identical performance to more complex implementations

**Implementation details:**
- Uses `parking_lot::Mutex` for efficient synchronization
- Single `rusqlite::Connection` with `SQLITE_OPEN_FULL_MUTEX` flag
- Maintains schema cache for performance
- Supports fast path optimization for simple queries
- All database operations are async-compatible despite synchronous SQLite

**Trade-offs accepted:**
- Single connection means no parallel reads (acceptable for most use cases)
- Mutex contention under very high load (mitigated by fast path optimization)

### Benchmark Results
Run `cargo test benchmark_executor_comparison -- --ignored --nocapture` to see performance comparison:
```
Overhead vs Raw SQLite:
┌─────────┬──────────┬──────────┬──────────┬──────────┐
│ Op      │ Direct   │ Simple   │ Mutex    │ Channel  │
├─────────┼──────────┼──────────┼──────────┼──────────┤
│ INSERT  │     8.1x │     7.7x │     7.7x │    20.1x │
│ SELECT  │     8.3x │     7.8x │     7.7x │    26.6x │
│ UPDATE  │     9.4x │     8.7x │     8.7x │    20.2x │
│ DELETE  │    10.7x │     9.9x │     9.6x │    21.0x │
└─────────┴──────────┴──────────┴──────────┴──────────┘
```

## Performance Progress Update (2025-06-30)

### Work Completed
1. **Successfully replaced channel-based implementation with Mutex-based DbHandler**
   - Achieved 2.2-3.5x performance improvement as planned
   - Resolved thread safety issues using `parking_lot::Mutex` + SQLite FULLMUTEX
   - Cleaned up all experimental implementations

2. **Fixed all test failures**
   - Resolved intermittent failures by using `cache=private` for in-memory databases
   - Fixed value encoding to return text format in simple query protocol
   - Implemented proper boolean conversion (SQLite 0/1 → PostgreSQL f/t)
   - Fixed parameter type inference in extended protocol tests

### Real-World Performance Analysis
Full benchmark results (`./run_benchmark.sh -b 500 -i 5000`) show higher overhead than isolated tests:
- **Overall**: ~100x overhead vs raw SQLite (10,212.5%)
- **By operation**:
  - INSERT: ~200x slower (19,759.7% overhead) 
  - SELECT: ~98x slower (9,788.9% overhead)
  - DELETE: ~47x slower (4,749.0% overhead)
  - UPDATE: ~34x slower (3,437.8% overhead)

The discrepancy between isolated tests (7.7-9.6x) and full benchmarks (100x) is due to:
- Protocol overhead from PostgreSQL wire protocol
- Schema metadata lookups for type information
- Query rewriting for decimal support
- Boolean value conversions
- Parameter processing in extended protocol

### SELECT Query Performance Deep Dive
SELECT queries show the second-worst performance (~98x overhead) due to:

1. **Query Processing Overhead**
   - Full SQL parsing for every query execution
   - Decimal query rewriting even for non-decimal tables
   - No query plan caching

2. **Type System Overhead**
   - Schema lookups in `__pgsqlite_schema` for each query
   - Boolean conversion for every row (0/1 → f/t)
   - Text encoding of all values in simple protocol

3. **Fast Path Limitations**
   - Current fast path only handles simple queries without WHERE clauses
   - Parameterized queries always use slow path
   - No optimization for repeated queries

### SELECT Optimization Implementation Status

**Goal**: Reduce SELECT overhead from ~98x to ~10-20x

**✅ Phase 1: Query Plan Cache** (COMPLETED - 2025-06-30)
- ✅ Implemented LRU cache for parsed and analyzed queries
- ✅ Cache column types and table metadata with plans
- ✅ Skip re-parsing and re-analysis for cached queries
- ✅ Key by normalized query text
- **Result**: 1.5x speedup for repeated queries (0.105ms → 0.068ms)

**✅ Phase 2: Enhanced Fast Path** (COMPLETED - 2025-07-01)
- ✅ Extended fast path to handle simple WHERE clauses (=, >, <, >=, <=, !=, <>)
- ✅ Added parameterized query support in fast path ($1, $2, etc.)
- ✅ Direct SQLite execution for non-decimal tables
- ✅ Optimized decimal detection with dedicated cache
- ✅ Integrated with extended protocol to avoid parameter substitution overhead
- **Result**: Overall 35% improvement for cached queries, reduced overhead from ~98x to ~23x for repeated queries

**✅ Phase 3: Prepared Statement Optimization** (COMPLETED - 2025-07-01)
- ✅ Created SQLite statement pool for reusing prepared statements (up to 100 cached statements)
- ✅ Implemented statement metadata caching to avoid re-parsing column info
- ✅ Optimized parameter binding to reduce conversion overhead
- ✅ Integrated with extended protocol for parameterized queries
- ✅ Added comprehensive test coverage for statement pool functionality
- **Result**: Reduced overhead for parameterized queries, improved prepared statement reuse

**📋 Phase 4: Schema Cache Improvements** (PLANNED)
- Pre-load full schema on first table access
- Eliminate per-query metadata lookups
- Memory-efficient type information storage
- Bloom filter for decimal table detection

**📋 Phase 5: Result Processing** (PLANNED)
- Batch row processing to reduce overhead
- Optimize hot path for boolean conversion
- Consider binary protocol optimization
- Streaming result sets for large queries

### Current Performance Status (2025-07-01)
- **Uncached SELECT**: ~95x overhead (0.105ms vs 0.001ms SQLite)
- **Cached SELECT**: ~23x overhead (0.068ms vs 0.003ms SQLite) 
- **Progress toward goal**: Significant improvement for repeated queries, approaching target range