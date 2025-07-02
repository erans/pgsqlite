# pgsqlite TODO List

## ✅ Performance Optimization Phase 1 - COMPLETED (2025-06-30)

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

## 🚀 Performance Optimization Phase 2 - SELECT Query Optimization

### Current State
Real-world benchmarks show SELECT queries have ~98x overhead vs raw SQLite, making them the second-slowest operation after INSERT.

### Root Causes
1. Query parsing and rewriting for every execution
2. Schema metadata lookups for type information
3. Boolean value conversions for each row
4. No query plan caching
5. Limited fast path coverage

### Tasks

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

**Implementation Details**:
- Enhanced CachedQuery struct with column_types, has_decimal_columns, rewritten_query fields
- Implemented normalize_query() to handle whitespace and case normalization  
- Integrated with GLOBAL_QUERY_CACHE (1000 entries, 10 min TTL)
- Added CacheMetrics struct tracking hits/misses/evictions
- Cache clears on DDL statements to prevent stale data
- Execute path now checks cache before parsing/rewriting

#### High Priority - Enhanced Fast Path - COMPLETED (2025-07-01)
- [x] Extend fast path to handle simple WHERE clauses (=, >, <, >=, <=, !=, <>)
- [x] Add support for single-table queries with basic predicates
- [x] Implement fast path for parameterized queries ($1, $2, etc.)
- [x] Skip decimal rewriting for non-decimal tables
- [x] Add fast path detection for common patterns
- [x] Optimize boolean conversion in fast path
- [x] Integrate with extended protocol to avoid parameter substitution overhead

**Implementation Details**:
- Enhanced FastPathQuery with WHERE clause support and operation detection
- Added regex patterns for detecting simple WHERE conditions
- Implemented parameter support with rusqlite::types::Value conversion
- Created dedicated decimal detection cache for performance
- Integrated with extended protocol for parameterized query optimization
- Added comprehensive test coverage for fast path functionality

#### Medium Priority - Prepared Statement Optimization - COMPLETED (2025-07-01)
- [x] Improve SQLite prepared statement reuse
- [x] Cache statement metadata between executions
- [x] Implement statement pool with size limits (100 statements, LRU eviction)
- [x] Optimize parameter binding process
- [x] Add prepared statement metrics and statistics
- [x] Integrate with DbHandler for transparent statement reuse
- [x] Support both parameterized and non-parameterized queries

**Implementation Details**:
- Created StatementPool with global singleton pattern
- Implemented StatementMetadata caching (column names, types, parameter count)
- Added LRU eviction strategy for memory management
- Enhanced extended protocol with statement pool integration
- Optimized parameter conversion for better performance
- Added comprehensive test coverage and thread safety

#### Medium Priority - Schema Cache Improvements - COMPLETED (2025-07-01)
- [x] Implemented bulk schema preloading on first table access
- [x] Created HashMap-based efficient column type lookup
- [x] Added HashSet bloom filter for decimal table detection
- [x] Eliminated per-query __pgsqlite_schema lookups
- [x] Schema cache integrated with query parsing

**Implementation Details**:
- Enhanced SchemaCache with bulk preloading and HashMap indexing
- Added bloom filter (HashSet) for O(1) decimal table detection
- Reduced schema query overhead from N queries to 1 bulk query
- Integrated with fast path and query processing for optimal performance

#### Low Priority - Protocol and Processing Optimization - COMPLETED (2025-07-01)
- [x] Implemented query fingerprinting with execution cache
- [x] Created pre-computed type converter lookup tables
- [x] Optimized boolean conversion with specialized fast paths
- [x] Implemented batch row processing with pre-allocated buffers
- [x] Added fast paths for common value types

**Implementation Details**:
- ExecutionCache stores pre-computed metadata (columns, types, converters)
- Type converter table with indexed lookup for O(1) conversion
- Batch processing with 100-row chunks for cache efficiency
- Query fingerprinting bypasses SQL parsing for cached queries

#### High Priority - Binary Protocol and Advanced Optimization - COMPLETED (2025-07-01)
- [x] Implement binary protocol support for common PostgreSQL types
  - [x] Created BinaryEncoder module with encoders for bool, int2/4/8, float4/8, text, bytea
  - [x] Added zero-copy binary encoding infrastructure
  - [x] Updated FieldDescription to use correct format codes from Portal
  - [x] Integrated binary encoding in execute_with_cached_metadata
- [x] Create zero-copy message construction for protocol responses
  - [x] Implemented ZeroCopyMessageBuilder for efficient message construction
  - [x] Added support for DataRow, RowDescription, CommandComplete messages
  - [x] Created zero-copy encoding traits for common types
- [x] Add result set caching for frequently executed identical queries
  - [x] Implemented ResultSetCache with LRU eviction and TTL
  - [x] Cache key includes query and parameters for accurate matching
  - [x] Added heuristics to cache queries > 1ms or returning > 10 rows
  - [x] Integrated cache checks in DbHandler::query()
  - [x] Cache invalidation on DDL statements
  - [x] Added comprehensive test coverage
- [ ] Optimize extended protocol parameter handling
- [ ] Implement connection pooling with warm statement caches
- [ ] Add query pattern recognition for automatic optimization hints

**Implementation Details**:
- Binary protocol respects Portal result_formats for each column
- Zero-copy builder reduces allocations for protocol messages
- Result cache provides benefits for repeated identical queries
- Cache statistics track hits, misses, and bytes saved

### Success Metrics
- ✅ **TARGET ACHIEVED**: Reduce SELECT overhead from ~98x to ~10-20x (**16x overhead achieved for cached queries**)
- ✅ Sub-millisecond response for simple cached queries (0.085ms for cached SELECT)
- ✅ Linear performance scaling with result set size
- ✅ Minimal memory overhead from caching (LRU eviction prevents unbounded growth)

### Performance Results (2025-07-01)
**Benchmark Results**:
- **Uncached SELECT**: ~131x overhead (0.159ms vs 0.001ms SQLite)
- **Cached SELECT**: ~16x overhead (0.085ms vs 0.005ms SQLite) ⭐ **TARGET ACHIEVED**
- **Cache Speedup**: 1.9x improvement for repeated queries
- **Overall Progress**: 3-phase optimization reduced cached query overhead from ~98x to ~16x

## 🎉 Zero-Copy Protocol Architecture - FULLY COMPLETED (2025-07-01)

## ✅ Protocol Flush Fix - COMPLETED (2025-07-02)

### Background
INSERT operations showed 159x overhead despite fast path optimizations achieving 1.0x-1.5x execution overhead. Investigation revealed missing `flush()` calls in the PostgreSQL wire protocol implementation.

### Root Cause
The `tokio_util::codec::Framed` writer doesn't automatically flush messages. Without explicit `flush()` calls after `ReadyForQuery` messages, responses were delayed by ~40ms waiting for:
- Client timeout and Flush message
- Buffer to fill up
- Next incoming message

### Implementation
- [x] Added `framed.flush().await?` after ReadyForQuery in simple query protocol (main.rs:276)
- [x] Added `framed.flush().await?` after ReadyForQuery in Sync handling (lib.rs:228)

### Performance Results
**Before flush fix:**
- INSERT: ~159x overhead (40ms+ delay per operation)
- SELECT (cached): ~8.5x overhead
- Overall: ~71x overhead

**After flush fix (Latest Benchmark):**
- INSERT: ~177x overhead (0.286ms) - no more 40ms delays
- SELECT: ~180x overhead (0.187ms) - protocol overhead visible
- SELECT (cached): ~17x overhead (0.094ms) - 2.0x cache speedup
- UPDATE: ~34x overhead (0.041ms) - excellent
- DELETE: ~39x overhead (0.038ms) - excellent
- Overall: ~98x overhead

### Key Achievement
Removed artificial 40ms delay per operation by adding flush() calls after ReadyForQuery messages. Direct TCP tests show ~47µs latency for simple queries. The remaining overhead is genuine PostgreSQL wire protocol translation cost.

## 🎉 Zero-Copy Protocol Architecture - FULLY COMPLETED (2025-07-01)

### Background
Following the successful SELECT optimization, implemented a comprehensive zero-copy architecture to eliminate protocol serialization overhead and memory allocations.

### Phase 1: Memory-Mapped Value Access - COMPLETED
- [x] Implemented `MappedValue` enum for zero-copy data access (Memory/Mapped/Reference variants)
- [x] Created `MappedValueFactory` for automatic threshold-based memory mapping
- [x] Built `ValueHandler` system for smart SQLite-to-PostgreSQL value conversion
- [x] Integrated with existing query executors for seamless operation
- **Result**: Zero-copy access for large BLOB/TEXT data, reduced memory allocations

### Phase 2: Enhanced Protocol Writer System - COMPLETED
- [x] Migrated all query executors to use `ProtocolWriter` trait
- [x] Implemented `DirectWriter` for direct socket communication bypassing tokio-util framing
- [x] Created connection adapters for seamless integration with existing handlers
- [x] Added comprehensive message batching for DataRow messages
- **Result**: Eliminated framing overhead, reduced protocol serialization costs

### Phase 3: Stream Splitting and Connection Management - COMPLETED
- [x] Implemented proper async stream splitting for concurrent read/write operations
- [x] Enhanced `DirectConnection` for zero-copy operation modes
- [x] Integrated with existing connection handling infrastructure
- [x] Added comprehensive error handling and connection lifecycle management
- **Result**: Improved concurrency, reduced context switching overhead

### Phase 4: Memory-Mapped Value Integration - COMPLETED
- [x] Enhanced memory-mapped value system with configurable thresholds
- [x] Implemented `MemoryMappedExecutor` for optimized query processing
- [x] Added smart value slicing and reference management
- [x] Integrated temporary file management for large value storage
- **Result**: Efficient handling of large data without memory copying

### Phase 5: Reusable Message Buffers - COMPLETED
- [x] Implemented thread-safe `BufferPool` with automatic recycling and size management
- [x] Created `MemoryMonitor` with configurable pressure thresholds and cleanup callbacks
- [x] Built `PooledDirectWriter` using buffer pooling for reduced allocations
- [x] Added intelligent message batching with configurable flush triggers
- [x] Implemented comprehensive monitoring and statistics tracking
- **Result**: Zero-allocation message construction, intelligent memory management

### Architecture Components Implemented
- **BufferPool**: Thread-safe buffer recycling with statistics tracking
- **MemoryMonitor**: Memory pressure detection with automatic cleanup callbacks
- **PooledDirectWriter**: Enhanced DirectWriter with buffer pooling and batching
- **MappedValue**: Zero-copy value access for large data
- **ValueHandler**: Smart conversion system with memory mapping integration

### Performance Achievements
- **67% improvement** in cached SELECT queries (26x → 8.5x overhead)
- **7% improvement** in uncached SELECT queries (98x → 91x overhead)
- **12% improvement** in overall system performance (83x → 71x overhead)
- **Zero-allocation** message construction through buffer pooling
- **Intelligent memory management** with automatic pressure detection

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

## 🚧 SELECT Query Optimization - Logging Reduction (2025-07-02)

### Background
SELECT queries showed ~82x overhead with excessive error logging for missing schema metadata causing performance issues.

### Work Completed
- [x] Profiled SELECT query execution to identify logging bottlenecks
- [x] Changed error! and warn! logging to debug! level for missing metadata in:
  - src/query/executor.rs (lines 126, 131, 422, 437)
  - src/query/executor_v2.rs (lines 117, 122)
  - src/query/executor_memory_mapped.rs (lines 150, 155)
  - src/query/extended.rs (lines 698, 734, 1524)
- [x] Reduced logging overhead for user tables without schema metadata

### Impact
Missing metadata logging is now at debug level, reducing noise for legitimate cases where schema information is unavailable. This is particularly important for:
- Dynamic queries without explicit type information
- Tables created outside pgsqlite
- Queries using type inference

### Performance Results
**After logging reduction (2025-07-02):**
- SELECT: ~125ms (was ~187ms) - **33% improvement**
- SELECT (cached): ~80ms (was ~94ms) - **15% improvement**
- Overall overhead: ~84x (was ~98x) - **14% improvement**

The logging reduction provided measurable performance gains, particularly for uncached SELECT queries.

### Next Steps
- [x] Benchmark impact of logging reduction on SELECT performance - 33% improvement achieved
- [x] Implement RowDescription caching to avoid repeated field generation - 41% improvement achieved
- [ ] Remove remaining debug logging from hot paths
- [ ] Profile protocol serialization overhead
- [ ] Consider lazy schema loading for better startup performance

### RowDescription Cache Implementation (2025-07-02)
- [x] Created RowDescriptionCache with LRU eviction and TTL support
- [x] Integrated cache into all query executors (simple, v2, extended protocol)
- [x] Cache key includes query, table name, and column names for accuracy
- [x] Added environment variables for cache configuration:
  - PGSQLITE_ROW_DESC_CACHE_SIZE (default: 1000 entries)
  - PGSQLITE_ROW_DESC_CACHE_TTL_MINUTES (default: 10 minutes)

### Combined Optimization Results (2025-07-02)
**After logging reduction + RowDescription caching:**
- SELECT: ~82ms (was ~187ms) - **56% total improvement**
- SELECT (cached): ~47ms (was ~94ms) - **50% total improvement**
- Overall overhead: ~46x (was ~98x) - **53% total improvement**

The combination of logging reduction and RowDescription caching has cut SELECT query overhead in half!

### Debug Logging Investigation (2025-07-02)
Investigated removing debug! calls from hot paths but found:
- Debug macros are already compiled out in release builds with log level "error"
- No measurable performance impact from removing debug! statements
- The tracing crate's macros are zero-cost when disabled
- Keeping debug logs for development/troubleshooting has no production impact

## ✅ Performance Optimization Phase 6 - INSERT Operation Optimization - COMPLETED (2025-07-02)

### Background
INSERT operations showed the highest overhead (~200x) in benchmarks, making them the biggest performance bottleneck.

### Work Completed

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

### Performance Results
- **Single-row INSERT**: ~170x overhead (0.290ms) - Protocol translation limitation
- **UPDATE**: ~32x overhead (0.041ms) - Excellent performance  
- **DELETE**: ~35x overhead (0.037ms) - Excellent performance
- **Statement Pool**: Near-native performance (1.0x-1.5x overhead in tests)

## ✅ Batch INSERT Performance - DISCOVERED (2025-07-02)

### Background
INSERT operations showed 177x overhead for single-row operations. Investigated multi-row INSERT support to amortize protocol overhead.

### Key Discovery
Multi-row VALUES syntax is **already fully supported** - no implementation needed! The SQL parser and execution engine handle batch INSERTs natively.

### Performance Results
Benchmark with 1000 total rows:
- **Single-row INSERTs**: 65ms (15,378 rows/sec) - 6.7x overhead
- **10-row batches**: 5.7ms (176,610 rows/sec) - 11.5x speedup  
- **100-row batches**: 1.3ms (788,200 rows/sec) - 51.3x speedup
- **1000-row batch**: 0.85ms (1,174,938 rows/sec) - 76.4x speedup

**Remarkable finding**: Batch sizes ≥10 actually **outperform direct SQLite** (0.1-0.6x overhead)!

### Usage Example
```sql
INSERT INTO users (name, email, age) VALUES 
  ('Alice', 'alice@example.com', 25),
  ('Bob', 'bob@example.com', 30),
  ('Charlie', 'charlie@example.com', 35);
```

### Recommendation
For bulk INSERT operations, always use multi-row VALUES syntax. The protocol overhead is amortized across all rows in the batch, providing near-native or better performance.

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
- [x] Skip test_flush_performance in CI due to long execution time (marked with #[ignore])
- [x] Skip test_logging_reduced in CI due to server startup requirement (marked with #[ignore])
- [x] Skip test_row_description_cache in CI due to server startup requirement (marked with #[ignore])

### Documentation
- [ ] API documentation
- [ ] Migration guide from PostgreSQL
- [ ] Performance tuning guide
- [ ] Troubleshooting guide