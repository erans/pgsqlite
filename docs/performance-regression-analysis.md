# Performance Regression Analysis - Connection-Per-Session Architecture

## Current Performance (2025-07-29)
- SELECT: 376,016.7% overhead (3.799ms) vs Target: 674.9x
- SELECT (cached): 3,398.6% overhead (0.115ms) vs Target: 17.2x
- UPDATE: 4,460.1% overhead (0.052ms) vs Target: 50.9x
- DELETE: 3,670.2% overhead (0.035ms) vs Target: 35.8x
- INSERT: 10,092.4% overhead (0.159ms) vs Target: 36.6x

## Root Cause Analysis

### 1. Global Mutex Contention (PRIMARY ISSUE)
**Location**: `src/session/connection_manager.rs:98`
```rust
let mut connections = self.connections.lock();  // EVERY query acquires this mutex
```
- Every single query execution acquires a global mutex
- With 5,251 operations in benchmark, that's 5,251 mutex lock/unlock cycles
- Even cached queries must go through this bottleneck

### 2. HashMap Lookup Overhead
**Location**: `src/session/connection_manager.rs:100`
```rust
let conn = connections.get_mut(session_id)
```
- Every query requires HashMap lookup to find session's connection
- HashMap with Uuid keys has non-trivial lookup cost

### 3. Session State Management
- SessionState struct has multiple Arc<Mutex<>> fields
- Each field access potentially involves mutex acquisition
- Transaction status checks happen on every query

### 4. LazyQueryProcessor Allocations
**Location**: `src/query/lazy_processor.rs`
- Creates new processor instance for every query
- Multiple string allocations and checks
- Cow<str> conversions add overhead

## Proposed Solutions

### Short-term (Quick Wins)
1. **Cache connection reference in SessionState**
   - Store weak reference to connection in session
   - Avoid HashMap lookup for every query
   - Fall back to manager only if weak ref fails

2. **Read-Write Lock instead of Mutex**
   - Most operations only need read access
   - RwLock would allow concurrent reads
   - Only connection creation/removal needs write lock

3. **Thread-local connection cache**
   - Cache last N session->connection mappings per thread
   - Avoid mutex entirely for cache hits

### Medium-term
1. **Connection pooling within sessions**
   - Pre-create connections for active sessions
   - Amortize connection creation cost

2. **Optimize LazyQueryProcessor**
   - Pool processor instances
   - Reuse allocations between queries
   - Fast-path for simple queries

3. **Session-local query cache**
   - Cache prepared statements per session
   - Avoid re-parsing common queries

### Long-term
1. **Lock-free data structures**
   - Use crossbeam's concurrent HashMap
   - Eliminate mutex contention entirely

2. **Architecture redesign**
   - Consider thread-per-session model
   - Eliminate shared state where possible

## Immediate Action Items
1. Profile with `perf` to confirm mutex contention
2. Implement RwLock as quick fix
3. Add connection caching in SessionState
4. Benchmark after each change to measure impact