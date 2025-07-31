use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::{RwLock, Mutex};
use rusqlite::{Connection, OpenFlags};
use uuid::Uuid;
use crate::config::Config;
use crate::PgSqliteError;
use crate::session::ThreadLocalConnectionCache;
use tracing::{warn, debug, info};

/// Manages per-session SQLite connections for true isolation
pub struct ConnectionManager {
    /// Map of session_id to SQLite connection (each wrapped in its own Mutex for thread safety)
    connections: Arc<RwLock<HashMap<Uuid, Arc<Mutex<Connection>>>>>,
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
            connections: Arc::new(RwLock::new(HashMap::new())),
            db_path,
            config,
            max_connections: 100, // TODO: Make configurable
        }
    }
    
    /// Create a new connection for a session
    pub fn create_connection(&self, session_id: Uuid) -> Result<(), PgSqliteError> {
        let mut connections = self.connections.write();
        
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
        
        debug!("Creating connection for session {} with path: {}", session_id, self.db_path);
            
        let conn = Connection::open_with_flags(&self.db_path, flags)
            .map_err(PgSqliteError::Sqlite)?;
        
        // Set pragmas
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
            .map_err(PgSqliteError::Sqlite)?;
        
        // Register functions
        crate::functions::register_all_functions(&conn)
            .map_err(PgSqliteError::Sqlite)?;
        
        // Initialize metadata
        crate::metadata::TypeMetadata::init(&conn)
            .map_err(PgSqliteError::Sqlite)?;
        
        let conn_arc = Arc::new(Mutex::new(conn));
        connections.insert(session_id, conn_arc.clone());
        
        // Cache in thread-local storage for fast access
        ThreadLocalConnectionCache::insert(session_id, conn_arc);
        
        info!("Created new connection for session {} (total connections: {})", session_id, connections.len());
        
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
        // First try thread-local cache (fast path)
        if let Some(conn_arc) = ThreadLocalConnectionCache::get(session_id) {
            let conn = conn_arc.lock();
            return f(&*conn).map_err(|e| PgSqliteError::Sqlite(e));
        }
        
        // Fall back to global map (slow path)
        let connections = self.connections.read();
        
        // Get the connection Arc
        let conn_arc = connections.get(session_id)
            .ok_or_else(|| PgSqliteError::Protocol(
                format!("No connection found for session {session_id}")
            ))?;
        
        // Clone the Arc to avoid holding the read lock while executing
        let conn_arc = conn_arc.clone();
        
        // Drop the read lock early
        drop(connections);
        
        // Cache in thread-local storage for next time
        ThreadLocalConnectionCache::insert(*session_id, conn_arc.clone());
        
        // Now lock the individual connection
        let conn = conn_arc.lock();
        f(&*conn).map_err(|e| PgSqliteError::Sqlite(e))
    }
    
    /// Execute a query with a cached connection Arc (avoids HashMap lookup)
    pub fn execute_with_cached_connection<F, R>(
        &self,
        conn_arc: &Arc<Mutex<Connection>>,
        f: F
    ) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&Connection) -> Result<R, rusqlite::Error>
    {
        let conn = conn_arc.lock();
        f(&*conn).map_err(PgSqliteError::Sqlite)
    }
    
    /// Remove a connection when session ends
    pub fn remove_connection(&self, session_id: &Uuid) {
        // Remove from thread-local cache first
        ThreadLocalConnectionCache::remove(session_id);
        
        let mut connections = self.connections.write();
        if connections.remove(session_id).is_some() {
            info!("Removed connection for session {} (remaining connections: {})", session_id, connections.len());
        }
    }
    
    /// Get the number of active connections
    pub fn active_connections(&self) -> usize {
        self.connections.read().len()
    }
    
    /// Check if a session has a connection
    pub fn has_connection(&self, session_id: &Uuid) -> bool {
        self.connections.read().contains_key(session_id)
    }
    
    /// Force WAL checkpoint on all connections except the specified one
    /// This ensures all connections see committed data from other connections
    pub fn refresh_all_other_connections(&self, excluding_session: &Uuid) -> Result<(), PgSqliteError> {
        // Only do this in WAL mode
        if self.config.pragma_journal_mode != "WAL" {
            return Ok(());
        }
        
        let connections = self.connections.read();
        let mut refresh_count = 0;
        let mut error_count = 0;
        
        for (session_id, conn_arc) in connections.iter() {
            // Skip the session that just committed
            if session_id == excluding_session {
                continue;
            }
            
            // Lock the individual connection
            let conn = conn_arc.lock();
            
            // Force this connection to read the latest WAL data
            match conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |_row| Ok(())) {
                Ok(_) => {
                    refresh_count += 1;
                    debug!("Refreshed WAL for session {}", session_id);
                }
                Err(e) => {
                    error_count += 1;
                    debug!("Failed to refresh WAL for session {}: {}", session_id, e);
                }
            }
        }
        
        debug!("WAL refresh completed: {} success, {} errors, excluding session {}", 
               refresh_count, error_count, excluding_session);
        
        Ok(())
    }
    
    /// Execute a function with a mutable connection for a session
    pub fn execute_with_session_mut<F, R>(
        &self, 
        session_id: &Uuid, 
        f: F
    ) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&mut Connection) -> Result<R, rusqlite::Error>
    {
        // First try thread-local cache (fast path)
        if let Some(conn_arc) = ThreadLocalConnectionCache::get(session_id) {
            let mut conn = conn_arc.lock();
            return f(&mut *conn).map_err(|e| PgSqliteError::Sqlite(e));
        }
        
        // Fall back to global map (slow path)
        let connections = self.connections.read();
        
        // Get the connection Arc
        let conn_arc = connections.get(session_id)
            .ok_or_else(|| PgSqliteError::Protocol(format!("No connection found for session {session_id}")))?;
        
        // Clone the Arc to avoid holding the read lock
        let conn_arc = conn_arc.clone();
        
        // Drop the read lock early
        drop(connections);
        
        // Cache in thread-local storage for next time
        ThreadLocalConnectionCache::insert(*session_id, conn_arc.clone());
        
        // Now lock the individual connection for mutable access
        let mut conn = conn_arc.lock();
        f(&mut *conn).map_err(PgSqliteError::Sqlite)
    }
    
    /// Get the connection Arc for a session (for caching)
    pub fn get_connection_arc(&self, session_id: &Uuid) -> Option<Arc<Mutex<Connection>>> {
        // First try thread-local cache
        if let Some(conn_arc) = ThreadLocalConnectionCache::get(session_id) {
            return Some(conn_arc);
        }
        
        // Fall back to global map
        let conn_arc = self.connections.read().get(session_id).cloned();
        
        // Cache it if found
        if let Some(ref arc) = conn_arc {
            ThreadLocalConnectionCache::insert(*session_id, arc.clone());
        }
        
        conn_arc
    }
    
    /// Execute a function with a mutable cached connection
    pub fn execute_with_cached_connection_mut<F, R>(
        &self,
        conn_arc: &Arc<Mutex<Connection>>,
        f: F
    ) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&mut Connection) -> Result<R, rusqlite::Error>
    {
        let mut conn = conn_arc.lock();
        f(&mut *conn).map_err(PgSqliteError::Sqlite)
    }
}