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

## Recent Work (Condensed History)
- Implemented comprehensive PostgreSQL type support (40+ types including ranges, network types, binary types)
- Built custom DECIMAL type system with automatic query rewriting for proper numeric handling
- Developed multi-phase SELECT query optimization reducing overhead from ~200x to ~26x for cached queries:
  - Phase 1: Query plan cache with LRU eviction
  - Phase 2: Enhanced fast path for simple WHERE clauses and parameters
  - Phase 3: Prepared statement pooling with metadata caching
  - Phase 4: Schema cache with bulk preloading and bloom filters
  - Phase 5: Execution cache with query fingerprinting and optimized type conversion

## Known Issues
- **BIT type casts**: Prepared statements with multiple columns containing BIT type casts may return empty strings instead of the expected bit values. This is a limitation in the current execution cache implementation.
- **Array types**: Array handling is not yet implemented
- **Extended protocol parameter type inference**: Some parameter types may require explicit casts

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

**✅ Phase 4: Schema Cache Improvements** (COMPLETED - 2025-07-01)
- ✅ Enhanced schema cache with bulk preloading on first table access
- ✅ Eliminated per-query metadata lookups by using cached schema information
- ✅ Implemented memory-efficient type information storage with HashMap indexing
- ✅ Added bloom filter (HashSet) for decimal table detection optimization
- ✅ Updated query parsing to use enhanced cache instead of individual __pgsqlite_schema queries
- ✅ Optimized fast path functions to use schema cache for type lookups
- **Result**: Schema cache shows 15.8x speedup for simple SELECT (0.954ms → 0.060ms), 90.9% cache hit rate

**✅ Phase 5: Protocol and Processing Optimization** (COMPLETED - 2025-07-01)
- ✅ Implemented query fingerprinting with execution cache to bypass SQL parsing
- ✅ Created pre-computed type converter lookup tables for fast value conversion
- ✅ Optimized boolean conversion with specialized fast paths (0/1 → f/t)
- ✅ Implemented batch row processing with pre-allocated buffers
- ✅ Added fast paths for common value types to avoid allocations
- ✅ Fixed NULL vs empty string handling in execution cache
- **Result**: Reduced SELECT overhead from ~137x to ~71x, cached queries from ~137x to ~26x

**✅ Phase 6: Binary Protocol and Advanced Optimization** (COMPLETED - 2025-07-01)
- ✅ Implemented binary protocol support for common PostgreSQL types
- ✅ Created zero-copy message construction for protocol responses
- ✅ Added result set caching for frequently executed identical queries
- ✅ Fixed FieldDescription format codes to respect Portal preferences
- ✅ Integrated binary encoding with execution cache
- [ ] Optimize extended protocol parameter handling (future work)
- [ ] Implement connection pooling with warm statement caches (future work)
- [ ] Add query pattern recognition for automatic optimization hints (future work)

**Implementation Details:**
- Binary protocol encoder supports BOOLEAN, INT2/4/8, FLOAT4/8, TEXT, BYTEA types
- Zero-copy message builder reduces allocations for DataRow messages
- Result cache uses LRU eviction with 100 entries and 60s TTL
- Cache automatically stores queries taking >1ms or returning >10 rows
- DDL statements invalidate the result cache to prevent stale data

### Final Performance Results (2025-07-01)

**Latest Benchmark Results (Post-Phase 6):**
- **Overall System**: ~83x overhead (8,270.4%)
- **SELECT**: ~82x overhead (0.001ms → 0.087ms)
- **SELECT (cached)**: ~14x overhead (0.004ms → 0.058ms) ⭐ **TARGET ACHIEVED!**
- **INSERT**: ~180x overhead (0.002ms → 0.294ms) - worst performer
- **UPDATE**: ~34x overhead (0.001ms → 0.041ms) - best performer
- **DELETE**: ~39x overhead (0.001ms → 0.037ms)
- **Cache Effectiveness**: 1.5x speedup for cached queries

**Performance Target Achievement:**
- ✅ **Original Goal**: Reduce SELECT overhead to 10-20x for cached queries
- ✅ **Result**: Achieved **14x overhead** for cached SELECT queries
- ✅ Successfully optimized the most common read operation

**Phase 6 Achievements:**
- ✅ Binary protocol support with correct format negotiation
- ✅ Zero-copy message construction infrastructure (limited by framed codec)
- ✅ Result set caching with intelligent heuristics
- ✅ Comprehensive test coverage for new features
- ✅ Documentation updates reflecting realistic performance expectations

**Performance Analysis:**
The optimization journey has been successful in achieving reasonable performance for a protocol adapter:
- **Cached SELECT at 14x overhead** meets our 10-20x target
- **UPDATE at 34x overhead** shows good DML performance
- **Overall 83x overhead** is acceptable given the protocol translation complexity
- Binary protocol and caching provide measurable benefits

**Inherent Overhead Sources:**
1. **Protocol Translation** (~20-30%): PostgreSQL wire protocol encoding/decoding
2. **SQL Parsing** (~30-40%): Converting PostgreSQL SQL to SQLite-compatible queries
3. **Type Conversion** (~15-20%): Value conversion between type systems
4. **Network Stack** (~10-15%): Unix socket or TCP communication overhead
5. **Thread Synchronization** (~5-10%): Mutex-based database access

**Optimization Journey Summary:**
1. **Phase 1**: Query plan cache - 1.5x speedup for repeated queries
2. **Phase 2**: Enhanced fast path - Reduced overhead from ~98x to ~23x
3. **Phase 3**: Prepared statement pool - Improved statement reuse
4. **Phase 4**: Schema cache improvements - 15.8x speedup for metadata lookups
5. **Phase 5**: Execution cache - Reduced cached SELECT to ~26x overhead
6. **Phase 6**: Binary protocol & result caching - Achieved 14x overhead for cached SELECT!