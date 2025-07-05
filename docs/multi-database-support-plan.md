# Multi-Database Support Implementation Plan for pgsqlite

## Overview

This document outlines the implementation plan for adding multi-database support to pgsqlite. The feature will allow PostgreSQL clients to create, manage, and switch between multiple SQLite databases, with each database stored as a separate SQLite file. The implementation will use memory-mapped files for efficient access and maintain databases in memory as long as they have active connections.

## Goals

1. Support multiple SQLite databases within a single pgsqlite instance
2. Implement PostgreSQL-compatible database management commands (CREATE DATABASE, DROP DATABASE, etc.)
3. Use memory-mapped files for efficient database access
4. Maintain databases in memory while they have active connections
5. Provide backward compatibility with single-database mode
6. Enable the feature via command-line argument or environment variable

## Feature Flag

The multi-database support will be controlled by:
- Command-line flag: `--multi-db` or `--enable-multi-database`
- Environment variable: `PGSQLITE_MULTI_DB=true`

When disabled (default), pgsqlite will:
- Work exactly as it does today with a single database
- Return appropriate PostgreSQL error codes for unsupported database commands
- Maintain full backward compatibility

## Architecture Design

### 1. Database Manager Component

Create a new `DatabaseManager` that will:
- Track all open databases and their metadata
- Handle database lifecycle (creation, deletion, connection management)
- Implement connection counting and cleanup
- Manage memory-mapped file handles

```rust
// src/database/manager.rs
pub struct DatabaseManager {
    databases: Arc<RwLock<HashMap<String, DatabaseEntry>>>,
    data_directory: PathBuf,
    default_database: String,
    config: Arc<Config>,
}

struct DatabaseEntry {
    name: String,
    path: PathBuf,
    handler: Arc<DbHandler>,
    connections: AtomicUsize,
    created_at: SystemTime,
    last_accessed: AtomicTime,
    mmap_handle: Option<MmapHandle>,
}
```

### 2. Connection State Enhancement

Modify `SessionState` to track the current database:

```rust
// src/session/mod.rs
pub struct SessionState {
    // Existing fields...
    pub current_database: String,
    pub database_manager: Option<Arc<DatabaseManager>>, // Only set in multi-db mode
}
```

### 3. Database Commands Implementation

Implement handlers for PostgreSQL database commands:

#### CREATE DATABASE
- Parse CREATE DATABASE statement
- Create new SQLite file in data directory
- Initialize with pgsqlite metadata tables
- Add to DatabaseManager

#### DROP DATABASE
- Verify no active connections
- Close memory-mapped file
- Delete SQLite file
- Remove from DatabaseManager

#### USE / \c (connect)
- Switch current database for the session
- Create DbHandler if not already loaded
- Update session state

#### SHOW DATABASES / \l
- List all databases in data directory
- Show connection counts and sizes

### 4. Memory-Mapped File Integration

Enhance DbHandler to support memory-mapped mode:

```rust
// src/session/db_handler.rs
impl DbHandler {
    pub fn new_with_mmap(db_path: &str, config: &Config) -> Result<Self, rusqlite::Error> {
        // Enable memory-mapped I/O for better performance
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE 
            | OpenFlags::SQLITE_OPEN_CREATE 
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
        
        let conn = Connection::open_with_flags(db_path, flags)?;
        
        // Set mmap_size from config (multi_db_mmap_size field)
        // This value will be configurable via --mmap-size or PGSQLITE_MMAP_SIZE
        let mmap_size = config.multi_db_mmap_size;
        conn.pragma_update(None, "mmap_size", mmap_size)?;
        
        // Rest of initialization...
    }
}
```

### 5. Query Routing

Implement query interception for database commands:

```rust
// src/query/database_commands.rs
pub struct DatabaseCommandHandler {
    manager: Arc<DatabaseManager>,
}

impl DatabaseCommandHandler {
    pub async fn handle_query(&self, query: &str, session: &mut SessionState) 
        -> Option<Result<DbResponse, Error>> {
        if let Some(command) = parse_database_command(query) {
            match command {
                DatabaseCommand::CreateDatabase(name, options) => {
                    self.handle_create_database(name, options).await
                }
                DatabaseCommand::DropDatabase(name) => {
                    self.handle_drop_database(name).await
                }
                DatabaseCommand::Use(name) => {
                    self.handle_use_database(name, session).await
                }
                // ... other commands
            }
        } else {
            None // Not a database command, continue normal processing
        }
    }
}
```

### 6. Connection Lifecycle Management

Implement reference counting and cleanup:

```rust
impl DatabaseManager {
    pub fn acquire_connection(&self, db_name: &str) -> Result<Arc<DbHandler>, Error> {
        let mut databases = self.databases.write().unwrap();
        
        if let Some(entry) = databases.get_mut(db_name) {
            entry.connections.fetch_add(1, Ordering::Relaxed);
            entry.last_accessed.store(SystemTime::now(), Ordering::Relaxed);
            Ok(entry.handler.clone())
        } else {
            // Load database if not in memory
            self.load_database(db_name)
        }
    }
    
    pub fn release_connection(&self, db_name: &str) {
        let databases = self.databases.read().unwrap();
        
        if let Some(entry) = databases.get(db_name) {
            let count = entry.connections.fetch_sub(1, Ordering::Relaxed);
            
            if count == 1 {
                // Was the last connection, schedule cleanup
                self.schedule_cleanup(db_name);
            }
        }
    }
    
    fn schedule_cleanup(&self, db_name: &str) {
        // Keep database in memory for configurable grace period
        // Default: 5 minutes after last connection closes
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(300)).await;
            // Check if still no connections and remove if so
        });
    }
}
```

## Implementation Phases

### Phase 1: Foundation (Week 1)
1. Create DatabaseManager component
2. Add multi-database configuration flags
3. Enhance SessionState for database tracking
4. Implement basic database listing

### Phase 2: Core Commands (Week 2)
1. Implement CREATE DATABASE command
2. Implement DROP DATABASE command
3. Implement USE/\c command
4. Add error handling for single-db mode

### Phase 3: Memory Management (Week 3)
1. Integrate memory-mapped file support
2. Implement connection reference counting
3. Add automatic cleanup mechanism
4. Optimize memory usage patterns

### Phase 4: PostgreSQL Compatibility (Week 4)
1. Implement system catalog extensions (pg_database)
2. Add SHOW DATABASES support
3. Implement ALTER DATABASE (limited)
4. Add database-level permissions stub

### Phase 5: Testing & Polish (Week 5)
1. Comprehensive test suite
2. Performance benchmarking
3. Documentation updates
4. Migration guide for users

## PostgreSQL Compatibility

### Supported Commands
- `CREATE DATABASE name [WITH options]`
- `DROP DATABASE [IF EXISTS] name`
- `\c database_name` / `USE database_name`
- `\l` / `SHOW DATABASES`
- `SELECT current_database()`
- `SELECT * FROM pg_database`

### Unsupported Features
- Database templates
- Encoding/locale options (all UTF-8)
- Tablespaces
- Database-level permissions (stub implementation only)
- Cross-database queries

### Error Codes
When multi-database support is disabled:
- `CREATE DATABASE`: Error 42501 (insufficient privilege)
- `DROP DATABASE`: Error 42501 (insufficient privilege)
- `USE`: Error 0A000 (feature not supported)

## Configuration

### New Configuration Options

```toml
# pgsqlite.toml
[database]
# Enable multi-database support
multi_database = false

# Directory for database files (when multi-db enabled)
data_directory = "./data"

# Default database name
default_database = "postgres"

# Memory-mapped file size (per database)
# Can be overridden by --mmap-size or PGSQLITE_MMAP_SIZE
# Accepts values like "1GB", "2GB", "512MB", etc.
# Default: 1GB (1073741824 bytes)
mmap_size = "1GB"

# Connection idle timeout (seconds)
connection_idle_timeout = 300

# Maximum databases in memory
max_databases_in_memory = 100
```

### Configuration Priority

The memory-mapped file size can be configured in order of priority:
1. Command-line argument: `--mmap-size 2GB`
2. Environment variable: `PGSQLITE_MMAP_SIZE=2GB`
3. Configuration file: `mmap_size = "1GB"`
4. Default value: 1GB (1073741824 bytes)

### Command-Line Arguments

```bash
# Enable multi-database mode
pgsqlite --multi-db --data-dir ./databases

# With custom mmap size (default: 1GB)
pgsqlite --multi-db --mmap-size 2GB

# With environment variables
PGSQLITE_MULTI_DB=true PGSQLITE_DATA_DIR=./databases PGSQLITE_MMAP_SIZE=2GB pgsqlite
```

## Migration Path

### From Single to Multi-Database Mode

1. Start pgsqlite with `--multi-db` flag
2. Existing database becomes the default database
3. New databases can be created with CREATE DATABASE
4. Clients can switch databases with USE command

### Backward Compatibility

- Without `--multi-db` flag, behavior remains unchanged
- Existing connection strings continue to work
- Default database used if none specified

## Performance Considerations

### Memory-Mapped Benefits
- Reduced memory copying
- OS-level caching
- Faster large query results
- Shared memory between connections

### Resource Management
- Databases evicted from memory after idle timeout
- Configurable maximum databases in memory
- Automatic cleanup of unused databases
- Connection pooling per database

## Security Considerations

### File System Security
- Database files inherit directory permissions
- No cross-database queries allowed
- Each database is fully isolated

### Connection Security
- SSL/TLS applies to all databases
- Authentication is global (not per-database)

## Testing Strategy

### Unit Tests
- DatabaseManager CRUD operations
- Connection lifecycle management
- Memory-mapped file handling
- Command parsing and routing

### Integration Tests
- Multi-client database switching
- Concurrent database operations
- Cleanup and eviction behavior
- Error handling in single-db mode

### Performance Tests
- Memory usage with multiple databases
- Connection switching overhead
- Memory-mapped vs regular performance
- Concurrent access patterns

## Future Enhancements

1. **Cross-database queries** (if SQLite ATTACH support allows)
2. **Database templates** for initialization
3. **Hot backup** support per database
4. **Database-level configuration** overrides
5. **Metrics** per database (connections, queries, size)

## Risks and Mitigations

### Risk: Memory Usage
- **Mitigation**: Configurable limits and automatic eviction

### Risk: File Handle Limits
- **Mitigation**: Lazy loading and cleanup of unused databases

### Risk: Concurrent Access Issues
- **Mitigation**: Proper locking and SQLite's built-in concurrency

### Risk: Performance Regression
- **Mitigation**: Feature flag ensures zero impact when disabled

## Success Criteria

1. Zero performance impact when feature is disabled
2. Support for core PostgreSQL database commands
3. Efficient memory usage with memory-mapped files
4. Stable concurrent access to multiple databases
5. Clear documentation and migration path