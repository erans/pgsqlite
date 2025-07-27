use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags};
use uuid::Uuid;
use crate::config::Config;
use crate::PgSqliteError;
use tracing::{info, warn, debug};

/// Manages per-session SQLite connections for true isolation
pub struct ConnectionManager {
    /// Map of session_id to SQLite connection
    connections: Arc<Mutex<HashMap<Uuid, Connection>>>,
    /// Database path
    db_path: String,
    /// Configuration
    config: Arc<Config>,
    /// Maximum number of connections allowed
    max_connections: usize,
}

impl ConnectionManager {
    pub fn new(db_path: String, config: Arc<Config>) -> Self {
        Self {
            connections: Arc::new(Mutex::new(HashMap::new())),
            db_path,
            config,
            max_connections: 100, // TODO: Make configurable
        }
    }
    
    /// Create a new connection for a session
    pub fn create_connection(&self, session_id: Uuid) -> Result<(), PgSqliteError> {
        let mut connections = self.connections.lock();
        
        // Check connection limit
        if connections.len() >= self.max_connections {
            return Err(PgSqliteError::Protocol(
                format!("Maximum connection limit ({}) reached", self.max_connections)
            ));
        }
        
        // Check if connection already exists
        if connections.contains_key(&session_id) {
            warn!("Connection already exists for session {}", session_id);
            return Ok(());
        }
        
        // Create new connection
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE 
            | OpenFlags::SQLITE_OPEN_CREATE 
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
            
        let conn = if self.db_path == ":memory:" {
            // Use shared cache for memory databases to allow data sharing between connections
            Connection::open_with_flags("file::memory:?cache=shared", flags)
        } else {
            // For named shared memory databases or regular files, use the path as-is
            Connection::open_with_flags(&self.db_path, flags)
        }.map_err(|e| PgSqliteError::Sqlite(e))?;
        
        // Configure connection
        let pragma_sql = format!(
            "PRAGMA journal_mode = {};
             PRAGMA synchronous = {};
             PRAGMA cache_size = {};
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = {};",
            self.config.pragma_journal_mode,
            self.config.pragma_synchronous,
            self.config.pragma_cache_size,
            self.config.pragma_mmap_size
        );
        conn.execute_batch(&pragma_sql)
            .map_err(|e| PgSqliteError::Sqlite(e))?;
        
        // Register functions
        crate::functions::register_all_functions(&conn)
            .map_err(|e| PgSqliteError::Sqlite(e))?;
        
        // Initialize metadata
        crate::metadata::TypeMetadata::init(&conn)
            .map_err(|e| PgSqliteError::Sqlite(e))?;
        
        // Run migrations to ensure catalog tables exist
        // Note: For shared memory databases, migrations should already be applied by the first connection
        // But we still need to run them for file-based databases where each connection is separate
        let mut runner = crate::migration::MigrationRunner::new(conn);
        match runner.run_pending_migrations() {
            Ok(applied) => {
                if !applied.is_empty() {
                    debug!("Applied {} migrations to session {} connection", applied.len(), session_id);
                }
                // Get the connection back from the runner
                let conn = runner.into_connection();
                connections.insert(session_id, conn);
                info!("Created new connection for session {}", session_id);
            }
            Err(e) => {
                return Err(PgSqliteError::Sqlite(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                    Some(format!("Migration failed for session {}: {}", session_id, e))
                )));
            }
        }
        
        Ok(())
    }
    
    /// Execute a query on a session's connection
    pub fn execute_with_session<F, R>(
        &self, 
        session_id: &Uuid, 
        f: F
    ) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&Connection) -> Result<R, rusqlite::Error>
    {
        let mut connections = self.connections.lock();
        
        let conn = connections.get_mut(session_id)
            .ok_or_else(|| PgSqliteError::Protocol(
                format!("No connection found for session {}", session_id)
            ))?;
            
        f(conn).map_err(|e| PgSqliteError::Sqlite(e))
    }
    
    /// Remove a connection when session ends
    pub fn remove_connection(&self, session_id: &Uuid) {
        let mut connections = self.connections.lock();
        if connections.remove(session_id).is_some() {
            info!("Removed connection for session {}", session_id);
        }
    }
    
    /// Get the number of active connections
    pub fn active_connections(&self) -> usize {
        self.connections.lock().len()
    }
    
    /// Check if a session has a connection
    pub fn has_connection(&self, session_id: &Uuid) -> bool {
        self.connections.lock().contains_key(session_id)
    }
}