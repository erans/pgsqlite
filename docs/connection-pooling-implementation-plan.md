# Connection Pooling Implementation Plan

## Overview

Replace the current single shared connection architecture with a connection-per-PostgreSQL-session model to achieve true connection isolation and full SQLAlchemy compatibility.

## Implementation Steps

### Phase 1: Core Infrastructure

#### 1.1 Create ConnectionManager
- **Location**: `src/session/connection_manager.rs`
- **Responsibilities**:
  - Manage HashMap of session_id -> SQLite connection
  - Handle connection creation with proper PRAGMA settings
  - Clean up connections on session termination
  - Thread-safe access via Arc<Mutex<>>

#### 1.2 Update SessionState
- **Location**: `src/session/state.rs`
- **Changes**:
  - Add `session_id: Uuid` field generated on creation
  - Remove any shared connection references
  - Ensure session_id is accessible throughout query execution

#### 1.3 Modify DbHandler
- **Location**: `src/session/db_handler.rs`
- **Changes**:
  - Replace single shared connection with ConnectionManager
  - Update all execute methods to accept session_id
  - Route queries to appropriate session connection
  - Remove connection pooling code (no longer needed)

### Phase 2: Query Execution Updates

#### 2.1 Update QueryExecutor
- **Location**: `src/query/executor.rs`
- **Changes**:
  - Pass session_id to all DbHandler calls
  - Remove WAL checkpoint workarounds
  - Remove connection state refresh logic
  - Remove PRAGMA optimize calls

#### 2.2 Update ExtendedQueryHandler
- **Location**: `src/query/extended.rs`
- **Changes**:
  - Ensure session_id flows through Parse/Bind/Execute
  - Update portal management to be session-aware

#### 2.3 Update Transaction Handlers
- **Changes**:
  - Remove complex isolation refresh logic
  - Simplify COMMIT/ROLLBACK handling
  - Each session has its own transaction state

### Phase 3: Main Server Updates

#### 3.1 Update Connection Handler
- **Location**: `src/main.rs`
- **Changes**:
  - Create connection on new PostgreSQL session
  - Pass session_id through to all handlers
  - Clean up connection on disconnect
  - Remove connection initialization workarounds

#### 3.2 Session Lifecycle
- **On Connect**: Create SessionState with UUID, create SQLite connection
- **During Session**: Route all queries to session's connection
- **On Disconnect**: Remove connection from ConnectionManager

### Phase 4: Cleanup

#### 4.1 Remove Workarounds
- Remove all WAL checkpoint logic after COMMIT
- Remove connection state refresh attempts
- Remove PRAGMA optimize calls
- Remove BEGIN IMMEDIATE; ROLLBACK isolation attempts
- Remove session count tracking (no longer needed)

#### 4.2 Simplify Configuration
- Remove complex WAL mode warnings
- Update documentation to reflect new architecture
- Clean up unnecessary PRAGMA settings

## Technical Details

### Connection Creation
```rust
// Each connection configured identically
conn.execute_batch(&format!(
    "PRAGMA journal_mode = {};
     PRAGMA synchronous = {};
     PRAGMA cache_size = {};
     PRAGMA temp_store = MEMORY;
     PRAGMA mmap_size = {};",
    config.pragma_journal_mode,
    config.pragma_synchronous,
    config.pragma_cache_size,
    config.pragma_mmap_size
))?;

// Register functions for each connection
crate::functions::register_all_functions(&conn)?;
```

### Thread Safety
- ConnectionManager uses Arc<Mutex<HashMap<Uuid, Connection>>>
- Each query execution locks only its specific connection
- No global connection lock needed
- Parallel query execution for different sessions

### Memory Management
- SQLite connections use ~0.5-2MB each
- Connections cleaned up immediately on disconnect
- No connection pooling/recycling initially (can add later if needed)
- Let SQLite handle its own page cache per connection

## Benefits

1. **True Isolation**: Each session sees only its committed transactions
2. **SQLAlchemy Compatibility**: Matches PostgreSQL's connection model
3. **Simpler Code**: Remove all isolation workarounds
4. **Better Concurrency**: Sessions don't block each other
5. **Cleaner Architecture**: Follow established database patterns

## Risks and Mitigations

### Risk: Connection Creation Overhead
- **Mitigation**: SQLite connections are lightweight (<1ms creation time)
- **Future**: Can add connection pooling/recycling if needed

### Risk: Memory Usage
- **Mitigation**: Each connection uses minimal memory
- **Monitoring**: Add metrics for active connection count

### Risk: File Handle Limits
- **Mitigation**: Set reasonable max connection limits
- **Default**: 100 connections (configurable)

## Testing Plan

1. **Unit Tests**: ConnectionManager lifecycle and thread safety
2. **Integration Tests**: Full query execution with multiple sessions
3. **SQLAlchemy Tests**: All 8 tests must pass including transaction persistence
4. **Concurrent Tests**: Multiple sessions with interleaved transactions
5. **Performance Tests**: Measure connection creation overhead

## Migration Notes

- This is a breaking change in internal architecture
- No changes to PostgreSQL protocol interface
- All existing client code continues to work
- Performance characteristics may change (likely improve)

## Success Criteria

1. All SQLAlchemy ORM tests pass (8/8)
2. Transaction isolation works correctly in WAL mode
3. No significant performance regression
4. Cleaner, more maintainable codebase
5. Matches PostgreSQL's connection model