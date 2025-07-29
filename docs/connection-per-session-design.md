# Connection-Per-Session Architecture Design

## Problem Statement

SQLAlchemy expects each session to have its own isolated database connection, similar to how PostgreSQL provides each client with its own server process. Our current single shared connection architecture breaks this isolation, causing SQLAlchemy sessions to see stale data even after commits.

## Proposed Solution

Implement a connection-per-PostgreSQL-session model where each incoming PostgreSQL client connection gets its own SQLite connection.

## Architecture Design

### 1. Connection Management

```rust
pub struct SessionConnection {
    conn: Connection,
    session_id: Uuid,
    created_at: Instant,
}

pub struct ConnectionManager {
    connections: Arc<Mutex<HashMap<Uuid, SessionConnection>>>,
    db_path: String,
    config: Arc<Config>,
}

impl ConnectionManager {
    pub fn create_connection(&self, session_id: Uuid) -> Result<(), Error> {
        let conn = Connection::open(&self.db_path)?;
        
        // Configure connection for optimal performance
        conn.execute_batch(&format!(
            "PRAGMA journal_mode = {};
             PRAGMA synchronous = {};
             PRAGMA cache_size = {};
             PRAGMA temp_store = MEMORY;",
            self.config.pragma_journal_mode,
            self.config.pragma_synchronous,
            self.config.pragma_cache_size
        ))?;
        
        // Register functions
        crate::functions::register_all_functions(&conn)?;
        
        let session_conn = SessionConnection {
            conn,
            session_id,
            created_at: Instant::now(),
        };
        
        self.connections.lock().insert(session_id, session_conn);
        Ok(())
    }
    
    pub fn get_connection(&self, session_id: &Uuid) -> Option<MutexGuard<Connection>> {
        // Return a guard that provides access to the specific connection
    }
    
    pub fn remove_connection(&self, session_id: &Uuid) {
        self.connections.lock().remove(session_id);
    }
}
```

### 2. Session State Changes

```rust
pub struct SessionState {
    pub session_id: Uuid,
    pub database: String,
    pub user: String,
    pub transaction_status: Arc<RwLock<TransactionStatus>>,
    pub parameters: Arc<RwLock<HashMap<String, String>>>,
    // Remove shared connection reference
}

impl SessionState {
    pub fn new(database: String, user: String) -> Self {
        Self {
            session_id: Uuid::new_v4(),
            database,
            user,
            transaction_status: Arc::new(RwLock::new(TransactionStatus::Idle)),
            parameters: Arc::new(RwLock::new(Self::default_parameters())),
        }
    }
}
```

### 3. DbHandler Modifications

```rust
pub struct DbHandler {
    connection_manager: Arc<ConnectionManager>,
    schema_cache: Arc<SchemaCache>,
    string_validator: Arc<StringConstraintValidator>,
    statement_cache_optimizer: Arc<StatementCacheOptimizer>,
}

impl DbHandler {
    pub async fn execute(&self, query: &str, session_id: &Uuid) -> Result<DbResponse, Error> {
        let conn = self.connection_manager
            .get_connection(session_id)
            .ok_or_else(|| Error::msg("No connection for session"))?;
        
        // Execute query on session-specific connection
        // ...
    }
}
```

### 4. Main Connection Handler Updates

```rust
async fn handle_connection_generic<S>(
    stream: S,
    connection_info: &str,
    db_handler: Arc<DbHandler>,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send,
{
    // ... existing startup code ...
    
    let session = Arc::new(SessionState::new(database, user));
    let session_id = session.session_id;
    
    // Create a dedicated connection for this session
    db_handler.create_connection(session_id).await?;
    
    // ... rest of connection handling ...
    
    // Cleanup on disconnect
    defer! {
        db_handler.remove_connection(&session_id);
    }
}
```

## Benefits

1. **True Connection Isolation**: Each PostgreSQL session gets its own SQLite connection with independent transaction state
2. **SQLAlchemy Compatibility**: Matches PostgreSQL's behavior that SQLAlchemy expects
3. **No Read Snapshot Issues**: New sessions immediately see committed data from other sessions
4. **Cleaner Architecture**: Removes complex WAL checkpoint and state refresh workarounds

## Performance Considerations

1. **Connection Overhead**: Creating a new SQLite connection per session has minimal overhead
2. **Memory Usage**: Each connection uses ~0.5-2MB of memory (much less than PostgreSQL's 10MB per process)
3. **WAL Mode Benefits**: Still get concurrent readers with WAL mode
4. **Connection Pooling**: Can implement connection recycling for frequently connecting clients

## Migration Strategy

1. **Phase 1**: Implement ConnectionManager alongside existing shared connection
2. **Phase 2**: Add feature flag to enable connection-per-session mode
3. **Phase 3**: Migrate all query execution to use session-specific connections
4. **Phase 4**: Remove shared connection code and workarounds

## Testing Strategy

1. **Unit Tests**: Test ConnectionManager lifecycle and isolation
2. **SQLAlchemy Tests**: Verify all 8 tests pass including transaction persistence
3. **Performance Tests**: Benchmark connection creation overhead
4. **Concurrency Tests**: Verify proper isolation under load

## Configuration

```toml
# Enable connection-per-session mode
PGSQLITE_CONNECTION_MODE=per-session  # or "shared" for legacy mode

# Connection limits
PGSQLITE_MAX_CONNECTIONS=100
PGSQLITE_CONNECTION_TIMEOUT=30
```

## Rollback Plan

If connection-per-session causes issues, the feature flag allows easy rollback to shared connection mode while investigating problems.