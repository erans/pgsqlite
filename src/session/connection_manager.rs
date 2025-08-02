use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
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
    /// WAL checkpoint state
    wal_checkpoint_state: WalCheckpointState,
}

/// Tracks WAL checkpoint state to optimize checkpointing
struct WalCheckpointState {
    /// Number of commits since last checkpoint
    commits_since_checkpoint: AtomicUsize,
    /// Last checkpoint time
    last_checkpoint: Mutex<Instant>,
    /// WAL size at last checkpoint (in pages)
    last_wal_size: AtomicU64,
}

impl ConnectionManager {
    pub fn new(db_path: String, config: Arc<Config>) -> Self {
        Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            db_path,
            config,
            max_connections: 100, // TODO: Make configurable
            wal_checkpoint_state: WalCheckpointState {
                commits_since_checkpoint: AtomicUsize::new(0),
                last_checkpoint: Mutex::new(Instant::now()),
                last_wal_size: AtomicU64::new(0),
            },
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
        
        // For :memory: databases, run migrations on each connection since they're isolated
        let final_conn = if self.db_path.contains(":memory:") {
            info!("Running migrations for in-memory database on session connection {}", session_id);
            let mut runner = crate::migration::runner::MigrationRunner::new(conn);
            match runner.run_pending_migrations() {
                Ok(applied) => {
                    if !applied.is_empty() {
                        info!("Applied {} migrations to session connection {}", applied.len(), session_id);
                    }
                }
                Err(e) => {
                    return Err(PgSqliteError::Protocol(format!("Migration failed for session {}: {}", session_id, e)));
                }
            }
            // Get the connection back from the runner
            runner.into_connection()
        } else {
            // For file databases, no migration needed as they share the same file
            conn
        };
        
        let conn_arc = Arc::new(Mutex::new(final_conn));
        connections.insert(session_id, conn_arc.clone());
        
        // Pre-warm connection cache with thread affinity
        ThreadLocalConnectionCache::pre_warm(session_id, conn_arc);
        
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
        // Use thread affinity for fastest lookup
        if let Some(conn_arc) = ThreadLocalConnectionCache::get_with_affinity(session_id) {
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
        
        // Pre-warm the cache for next time
        ThreadLocalConnectionCache::pre_warm(*session_id, conn_arc.clone());
        
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
    
    /// Intelligently manage WAL checkpoints based on commit count and time
    /// This ensures all connections see committed data while minimizing overhead
    pub fn refresh_all_other_connections(&self, _excluding_session: &Uuid) -> Result<(), PgSqliteError> {
        // Only do this in WAL mode
        if self.config.pragma_journal_mode != "WAL" {
            return Ok(());
        }
        
        // Increment commit counter
        let commits = self.wal_checkpoint_state.commits_since_checkpoint.fetch_add(1, Ordering::Relaxed) + 1;
        
        // Check if we should perform a checkpoint
        let should_checkpoint = {
            let last_checkpoint = self.wal_checkpoint_state.last_checkpoint.lock();
            let time_since_checkpoint = Instant::now().duration_since(*last_checkpoint);
            
            // Checkpoint if:
            // 1. More than 100 commits since last checkpoint
            // 2. More than 10 seconds since last checkpoint
            // 3. WAL file is getting large (checked below)
            commits >= 100 || time_since_checkpoint >= Duration::from_secs(10)
        };
        
        if !should_checkpoint {
            // No checkpoint needed yet
            return Ok(());
        }
        
        // Get one connection to check WAL size and perform checkpoint
        let connections = self.connections.read();
        if let Some((_, conn_arc)) = connections.iter().next() {
            let conn = conn_arc.lock();
            
            // Check WAL size
            let wal_size = conn.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                // wal_checkpoint returns (busy, checkpointed, total)
                // We want the total pages
                row.get::<_, i32>(2).map(|v| v as u64)
            }).unwrap_or(0);
            
            let last_size = self.wal_checkpoint_state.last_wal_size.load(Ordering::Relaxed);
            
            // If WAL has grown significantly (>1000 pages), force a checkpoint
            if wal_size > last_size + 1000 {
                // Perform TRUNCATE checkpoint to actually shrink the WAL
                let _ = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)", []);
                debug!("Performed WAL TRUNCATE checkpoint: {} pages", wal_size);
            } else {
                debug!("Performed WAL PASSIVE checkpoint: {} pages after {} commits", wal_size, commits);
            }
            
            // Update checkpoint state
            self.wal_checkpoint_state.commits_since_checkpoint.store(0, Ordering::Relaxed);
            *self.wal_checkpoint_state.last_checkpoint.lock() = Instant::now();
            self.wal_checkpoint_state.last_wal_size.store(wal_size, Ordering::Relaxed);
        }
        
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
        // Use thread affinity for fastest lookup
        if let Some(conn_arc) = ThreadLocalConnectionCache::get_with_affinity(session_id) {
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
        
        // Pre-warm the cache for next time
        ThreadLocalConnectionCache::pre_warm(*session_id, conn_arc.clone());
        
        // Now lock the individual connection for mutable access
        let mut conn = conn_arc.lock();
        f(&mut *conn).map_err(PgSqliteError::Sqlite)
    }
    
    /// Get the connection Arc for a session (for caching)
    pub fn get_connection_arc(&self, session_id: &Uuid) -> Option<Arc<Mutex<Connection>>> {
        // Use thread affinity for fastest lookup
        if let Some(conn_arc) = ThreadLocalConnectionCache::get_with_affinity(session_id) {
            return Some(conn_arc);
        }
        
        // Fall back to global map
        let conn_arc = self.connections.read().get(session_id).cloned();
        
        // Pre-warm cache if found
        if let Some(ref arc) = conn_arc {
            ThreadLocalConnectionCache::pre_warm(*session_id, arc.clone());
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