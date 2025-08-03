use std::sync::Arc;
use uuid::Uuid;
use crate::cache::SchemaCache;
use crate::optimization::{OptimizationManager, statement_cache_optimizer::StatementCacheOptimizer};
use crate::query::{QueryTypeDetector, QueryType, process_query};
use crate::config::Config;
use crate::migration::MigrationRunner;
use crate::validator::StringConstraintValidator;
use crate::session::ConnectionManager;
use crate::PgSqliteError;
use tracing::{info, debug};

/// Database response structure
pub struct DbResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub rows_affected: usize,
}

/// Thread-safe database handler using per-session connections
/// 
/// This implementation provides true connection isolation where each
/// PostgreSQL session gets its own SQLite connection, matching PostgreSQL's
/// behavior and ensuring full SQLAlchemy compatibility.
pub struct DbHandler {
    connection_manager: Arc<ConnectionManager>,
    schema_cache: Arc<SchemaCache>,
    string_validator: Arc<StringConstraintValidator>,
    statement_cache_optimizer: Arc<StatementCacheOptimizer>,
    db_path: String,
}

impl DbHandler {
    pub fn new(db_path: &str) -> Result<Self, rusqlite::Error> {
        Self::new_with_config(db_path, &Config::load())
    }
    
    pub fn new_with_config(db_path: &str, config: &Config) -> Result<Self, rusqlite::Error> {
        // For initial setup, we need to ensure database exists and run migrations
        if !db_path.contains(":memory:") && !std::path::Path::new(db_path).exists() {
            info!("New database file detected, will run initial migrations...");
        }
        
        // Create a temporary connection for migrations
        let temp_conn = Self::create_initial_connection(db_path, config)?;
        
        // Run migrations if needed
        Self::run_migrations_if_needed(temp_conn, db_path)?;
        
        // Initialize optimization components
        let optimization_manager = Arc::new(OptimizationManager::new(true));
        let statement_cache_optimizer = Arc::new(StatementCacheOptimizer::new(200, optimization_manager));
        
        // Create connection manager
        let connection_manager = Arc::new(ConnectionManager::new(
            db_path.to_string(),
            Arc::new(config.clone())
        ));
        
        debug!("DbHandler initialized with per-session connection architecture");
        
        Ok(Self {
            connection_manager,
            schema_cache: Arc::new(SchemaCache::new(config.schema_cache_ttl)),
            string_validator: Arc::new(StringConstraintValidator::new()),
            statement_cache_optimizer,
            db_path: db_path.to_string(),
        })
    }
    
    fn create_initial_connection(db_path: &str, config: &Config) -> Result<rusqlite::Connection, rusqlite::Error> {
        use rusqlite::{Connection, OpenFlags};
        
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE 
            | OpenFlags::SQLITE_OPEN_CREATE 
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
            
        let conn = if db_path == ":memory:" {
            // For memory databases, each connection gets its own database
            Connection::open_with_flags(db_path, flags)?
        } else {
            // For file databases, use the path as-is
            Connection::open_with_flags(db_path, flags)?
        };
        
        // Set pragmas
        let pragma_sql = format!(
            "PRAGMA journal_mode = {};
             PRAGMA synchronous = {};
             PRAGMA cache_size = {};
             PRAGMA temp_store = MEMORY;
             PRAGMA mmap_size = {};",
            config.pragma_journal_mode,
            config.pragma_synchronous,
            config.pragma_cache_size,
            config.pragma_mmap_size
        );
        conn.execute_batch(&pragma_sql)?;
        
        Ok(conn)
    }
    
    fn run_migrations_if_needed(conn: rusqlite::Connection, db_path: &str) -> Result<(), rusqlite::Error> {
        // Skip all checks for in-memory databases
        if db_path.contains(":memory:") {
            info!("Running initial migrations for in-memory database...");
            
            // Register functions before migrations
            crate::functions::register_all_functions(&conn)?;
            
            let mut runner = MigrationRunner::new(conn);
            match runner.run_pending_migrations() {
                Ok(applied) => {
                    if !applied.is_empty() {
                        debug!("Applied {} migrations to new database", applied.len());
                    }
                }
                Err(e) => {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("Migration failed: {e}"))
                    ));
                }
            }
            return Ok(());
        }
        
        // For file-based databases, first check for schema drift
        // This needs to happen before migration checks to catch incomplete setups
        let schema_table_exists = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_schema'",
            [],
            |row| row.get::<_, i64>(0)
        ).unwrap_or(0) > 0;
        
        if schema_table_exists {
            // Database has pgsqlite schema - check for drift
            use crate::schema_drift::SchemaDriftDetector;
            match SchemaDriftDetector::detect_drift(&conn) {
                Ok(drift) => {
                    if !drift.is_empty() {
                        return Err(rusqlite::Error::SqliteFailure(
                            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                            Some(format!("Schema drift detected: {}", drift.format_report()))
                        ));
                    }
                }
                Err(e) => {
                    // Don't fail on drift detection errors, just log them
                    debug!("Failed to check schema drift: {e}");
                }
            }
        }
        
        // Now check if migrations are needed
        let needs_migrations = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_migrations'",
            [],
            |row| row.get::<_, i64>(0)
        ).unwrap_or(0) == 0;
        
        if needs_migrations {
            info!("Running initial migrations...");
            
            // Register functions before migrations
            crate::functions::register_all_functions(&conn)?;
            
            let mut runner = MigrationRunner::new(conn);
            match runner.run_pending_migrations() {
                Ok(applied) => {
                    if !applied.is_empty() {
                        debug!("Applied {} migrations to new database", applied.len());
                    }
                }
                Err(e) => {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("Migration failed: {e}"))
                    ));
                }
            }
        } else {
            // Check if we need to run any pending migrations
            // Register functions first
            crate::functions::register_all_functions(&conn)?;
            
            let runner = MigrationRunner::new(conn);
            match runner.check_schema_version() {
                Ok(()) => {
                    // Schema is up to date
                    debug!("Schema version check passed");
                }
                Err(e) => {
                    // Schema is outdated, run migrations
                    debug!("Schema is outdated: {}", e);
                    let mut runner = runner;
                    match runner.run_pending_migrations() {
                        Ok(applied) => {
                            if !applied.is_empty() {
                                debug!("Applied {} migrations", applied.len());
                            }
                        }
                        Err(e) => {
                            return Err(rusqlite::Error::SqliteFailure(
                                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                Some(format!("Migration failed: {e}"))
                            ));
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    /// Create a connection for a new session
    pub async fn create_session_connection(&self, session_id: Uuid) -> Result<(), PgSqliteError> {
        self.connection_manager.create_connection(session_id)
    }
    
    /// Remove a session's connection
    pub fn remove_session_connection(&self, session_id: &Uuid) {
        self.connection_manager.remove_connection(session_id);
    }
    
    
    /// Execute with bound parameters
    pub async fn execute_with_params(
        &self,
        query: &str,
        params: &[Option<Vec<u8>>],
        session_id: &Uuid
    ) -> Result<DbResponse, PgSqliteError> {
        self.connection_manager.execute_with_session(session_id, |conn| {
            // Process query with fast path optimization
            let processed_query = process_query(query, conn, &self.schema_cache)?;
            
            let mut stmt = conn.prepare(&processed_query)?;
            
            // Convert params to rusqlite values
            let values: Vec<rusqlite::types::Value> = params.iter()
                .map(|p| match p {
                    Some(data) => {
                        match String::from_utf8(data.clone()) {
                            Ok(s) => rusqlite::types::Value::Text(s),
                            Err(_) => rusqlite::types::Value::Blob(data.clone()),
                        }
                    }
                    None => rusqlite::types::Value::Null,
                })
                .collect();
            
            let query_type = QueryTypeDetector::detect_query_type(query);
            
            match query_type {
                QueryType::Select => {
                    let column_count = stmt.column_count();
                    let mut columns = Vec::with_capacity(column_count);
                    for i in 0..column_count {
                        columns.push(stmt.column_name(i)?.to_string());
                    }
                    
                    let rows: Result<Vec<_>, _> = stmt.query_map(rusqlite::params_from_iter(values.iter()), |row| {
                        let mut row_data = Vec::with_capacity(column_count);
                        for i in 0..column_count {
                            let value: Option<rusqlite::types::Value> = row.get(i)?;
                            row_data.push(match value {
                                Some(rusqlite::types::Value::Text(s)) => Some(s.into_bytes()),
                                Some(rusqlite::types::Value::Integer(i)) => Some(i.to_string().into_bytes()),
                                Some(rusqlite::types::Value::Real(f)) => Some(f.to_string().into_bytes()),
                                Some(rusqlite::types::Value::Blob(b)) => Some(b),
                                Some(rusqlite::types::Value::Null) | None => None,
                            });
                        }
                        Ok(row_data)
                    })?.collect();
                    
                    Ok(DbResponse {
                        columns,
                        rows: rows?,
                        rows_affected: 0,
                    })
                }
                _ => {
                    let rows_affected = stmt.execute(rusqlite::params_from_iter(values.iter()))?;
                    Ok(DbResponse {
                        columns: vec![],
                        rows: vec![],
                        rows_affected,
                    })
                }
            }
        })
    }
    
    /// Execute with rusqlite values (for proper binary parameter support)
    pub async fn execute_with_rusqlite_params(
        &self,
        query: &str,
        params: &[rusqlite::types::Value],
        session_id: &Uuid
    ) -> Result<DbResponse, PgSqliteError> {
        self.connection_manager.execute_with_session(session_id, |conn| {
            // For parameterized queries, don't process_query as it removes parameter placeholders
            // We need the placeholders for proper parameterized execution
            let mut stmt = conn.prepare(query)?;
            
            let query_type = QueryTypeDetector::detect_query_type(query);
            
            // Check if query has RETURNING clause - it needs to be treated as SELECT
            let has_returning = query.to_uppercase().contains(" RETURNING ");
            
            match query_type {
                QueryType::Select => {
                    let column_count = stmt.column_count();
                    let mut columns = Vec::with_capacity(column_count);
                    for i in 0..column_count {
                        columns.push(stmt.column_name(i)?.to_string());
                    }
                    
                    let rows: Result<Vec<_>, _> = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
                        let mut row_data = Vec::with_capacity(column_count);
                        for i in 0..column_count {
                            let value: Option<rusqlite::types::Value> = row.get(i)?;
                            row_data.push(match value {
                                Some(rusqlite::types::Value::Text(s)) => Some(s.into_bytes()),
                                Some(rusqlite::types::Value::Integer(i)) => Some(i.to_string().into_bytes()),
                                Some(rusqlite::types::Value::Real(f)) => Some(f.to_string().into_bytes()),
                                Some(rusqlite::types::Value::Blob(b)) => Some(b),
                                Some(rusqlite::types::Value::Null) | None => None,
                            });
                        }
                        Ok(row_data)
                    })?.collect();
                    
                    Ok(DbResponse {
                        columns,
                        rows: rows?,
                        rows_affected: 0,
                    })
                }
                _ => {
                    // For DML with RETURNING, treat as SELECT
                    if has_returning {
                        let column_count = stmt.column_count();
                        let mut columns = Vec::with_capacity(column_count);
                        for i in 0..column_count {
                            columns.push(stmt.column_name(i)?.to_string());
                        }
                        
                        let rows: Result<Vec<_>, _> = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
                            let mut row_data = Vec::with_capacity(column_count);
                            for i in 0..column_count {
                                let value: Option<rusqlite::types::Value> = row.get(i)?;
                                row_data.push(match value {
                                    Some(rusqlite::types::Value::Text(s)) => Some(s.into_bytes()),
                                    Some(rusqlite::types::Value::Integer(i)) => Some(i.to_string().into_bytes()),
                                    Some(rusqlite::types::Value::Real(f)) => Some(f.to_string().into_bytes()),
                                    Some(rusqlite::types::Value::Blob(b)) => Some(b),
                                    Some(rusqlite::types::Value::Null) | None => None,
                                });
                            }
                            Ok(row_data)
                        })?.collect();
                        
                        Ok(DbResponse {
                            columns,
                            rows: rows?,
                            rows_affected: 0,
                        })
                    } else {
                        let rows_affected = stmt.execute(rusqlite::params_from_iter(params.iter()))?;
                        Ok(DbResponse {
                            columns: vec![],
                            rows: vec![],
                            rows_affected,
                        })
                    }
                }
            }
        })
    }
    
    /// Query without session (uses temporary connection)
    pub async fn query(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        // Check if it's any form of memory database (including named shared memory)
        if self.db_path == ":memory:" || self.db_path.contains("mode=memory") {
            // For memory databases, use a temporary session connection
            let temp_session = Uuid::new_v4();
            if let Err(e) = self.create_session_connection(temp_session).await {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                    Some(format!("Failed to create temporary session: {e}"))
                ));
            }
            
            let result = self.query_with_session(query, &temp_session).await
                .map_err(|e| match e {
                    PgSqliteError::Sqlite(sqlite_err) => sqlite_err,
                    other => rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("Query error: {other}"))
                    )
                })?;
            
            self.remove_session_connection(&temp_session);
            Ok(result)
        } else {
            // For file databases, create a temporary connection
            let conn = Self::create_initial_connection(&self.db_path, &Config::load())?;
            
            // Process query with fast path optimization
            let processed_query = process_query(query, &conn, &self.schema_cache)?;
            
            let mut stmt = conn.prepare(&processed_query)?;
            let column_count = stmt.column_count();
            let mut columns = Vec::with_capacity(column_count);
            for i in 0..column_count {
                columns.push(stmt.column_name(i)?.to_string());
            }
            
            let rows: Result<Vec<_>, _> = stmt.query_map([], |row| {
                let mut row_data = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value: Option<rusqlite::types::Value> = row.get(i)?;
                    row_data.push(match value {
                        Some(rusqlite::types::Value::Text(s)) => Some(s.into_bytes()),
                        Some(rusqlite::types::Value::Integer(i)) => Some(i.to_string().into_bytes()),
                        Some(rusqlite::types::Value::Real(f)) => Some(f.to_string().into_bytes()),
                        Some(rusqlite::types::Value::Blob(b)) => Some(b),
                        Some(rusqlite::types::Value::Null) | None => None,
                    });
                }
                Ok(row_data)
            })?.collect();
            
            Ok(DbResponse {
                columns,
                rows: rows?,
                rows_affected: 0,
            })
        }
    }
    
    /// Query with session-specific connection (with optional cached connection)
    pub async fn query_with_session_cached(
        &self, 
        query: &str, 
        session_id: &Uuid,
        cached_conn: Option<&Arc<parking_lot::Mutex<rusqlite::Connection>>>
    ) -> Result<DbResponse, PgSqliteError> {
        // Check if this is a catalog query that should be intercepted
        // We need to do this before applying translations
        let lower_query = query.to_lowercase();
        
        // Handle special system function queries
        if lower_query.trim() == "select current_user()" {
            return Ok(DbResponse {
                columns: vec!["current_user".to_string()],
                rows: vec![vec![Some("postgres".to_string().into_bytes())]],
                rows_affected: 1,
            });
        }
        
        if lower_query.contains("information_schema") || lower_query.contains("pg_catalog") || 
           lower_query.contains("pg_type") || lower_query.contains("pg_class") ||
           lower_query.contains("pg_attribute") || lower_query.contains("pg_enum") {
            // For catalog queries, we need to use the catalog interceptor
            // This requires an Arc<DbHandler>, but we can't create a cyclic Arc here
            // Instead, let's handle specific queries directly for now
            if lower_query.contains("information_schema.tables") {
                return self.handle_information_schema_tables_query(query, session_id).await;
            }
            
            // Handle SQLAlchemy table existence check with a simpler query
            if lower_query.contains("pg_class.relname") && 
               lower_query.contains("pg_namespace") && 
               lower_query.contains("pg_table_is_visible") &&
               lower_query.contains("any") && 
               lower_query.contains("array") {
                return self.handle_table_existence_query(query, session_id).await;
            }
            
            // For other pg_catalog queries, let them go through LazyQueryProcessor
            // which will strip the schema prefix and allow them to query the views
        }
        
        // Use cached connection if available, otherwise fall back to lookup
        match cached_conn {
            Some(conn) => {
                self.connection_manager.execute_with_cached_connection(conn, |conn| {
                    // Process query with fast path optimization
                    let processed_query = process_query(query, conn, &self.schema_cache)?;
                    
                    let mut stmt = conn.prepare(&processed_query)?;
                    let column_count = stmt.column_count();
                    let mut columns = Vec::with_capacity(column_count);
                    for i in 0..column_count {
                        columns.push(stmt.column_name(i)?.to_string());
                    }
                    
                    let rows: Result<Vec<_>, _> = stmt.query_map([], |row| {
                        let mut row_data = Vec::with_capacity(column_count);
                        for i in 0..column_count {
                            let value: Option<rusqlite::types::Value> = row.get(i)?;
                            row_data.push(match value {
                                Some(rusqlite::types::Value::Text(s)) => Some(s.into_bytes()),
                                Some(rusqlite::types::Value::Integer(i)) => Some(i.to_string().into_bytes()),
                                Some(rusqlite::types::Value::Real(f)) => Some(f.to_string().into_bytes()),
                                Some(rusqlite::types::Value::Blob(b)) => Some(b),
                                Some(rusqlite::types::Value::Null) | None => None,
                            });
                        }
                        Ok(row_data)
                    })?.collect();
                    
                    Ok(DbResponse {
                        columns,
                        rows: rows?,
                        rows_affected: 0,
                    })
                })
            }
            None => {
                // Fall back to regular lookup
                self.query_with_session(query, session_id).await
            }
        }
    }
    
    /// Query with session-specific connection
    pub async fn query_with_session(&self, query: &str, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        // Check if this is a catalog query that should be intercepted
        // We need to do this before applying translations
        let lower_query = query.to_lowercase();
        
        // Handle special system function queries
        if lower_query.trim() == "select current_user()" {
            return Ok(DbResponse {
                columns: vec!["current_user".to_string()],
                rows: vec![vec![Some("postgres".to_string().into_bytes())]],
                rows_affected: 1,
            });
        }
        
        if lower_query.contains("information_schema") || lower_query.contains("pg_catalog") || 
           lower_query.contains("pg_type") || lower_query.contains("pg_class") ||
           lower_query.contains("pg_attribute") || lower_query.contains("pg_enum") {
            // For catalog queries, we need to use the catalog interceptor
            // This requires an Arc<DbHandler>, but we can't create a cyclic Arc here
            // Instead, let's handle specific queries directly for now
            if lower_query.contains("information_schema.tables") {
                return self.handle_information_schema_tables_query(query, session_id).await;
            }
            
            // Handle SQLAlchemy table existence check with a simpler query
            if lower_query.contains("pg_class.relname") && 
               lower_query.contains("pg_namespace") && 
               lower_query.contains("pg_table_is_visible") &&
               lower_query.contains("any") && 
               lower_query.contains("array") {
                return self.handle_table_existence_query(query, session_id).await;
            }
            
            // For other pg_catalog queries, let them go through LazyQueryProcessor
            // which will strip the schema prefix and allow them to query the views
        }
        
        self.connection_manager.execute_with_session(session_id, |conn| {
            // Process query with fast path optimization
            let processed_query = process_query(query, conn, &self.schema_cache)?;
            
            let mut stmt = conn.prepare(&processed_query)?;
            let column_count = stmt.column_count();
            let mut columns = Vec::with_capacity(column_count);
            for i in 0..column_count {
                columns.push(stmt.column_name(i)?.to_string());
            }
            
            let rows: Result<Vec<_>, _> = stmt.query_map([], |row| {
                let mut row_data = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value: Option<rusqlite::types::Value> = row.get(i)?;
                    row_data.push(match value {
                        Some(rusqlite::types::Value::Text(s)) => Some(s.into_bytes()),
                        Some(rusqlite::types::Value::Integer(i)) => Some(i.to_string().into_bytes()),
                        Some(rusqlite::types::Value::Real(f)) => Some(f.to_string().into_bytes()),
                        Some(rusqlite::types::Value::Blob(b)) => Some(b),
                        Some(rusqlite::types::Value::Null) | None => None,
                    });
                }
                Ok(row_data)
            })?.collect();
            
            Ok(DbResponse {
                columns,
                rows: rows?,
                rows_affected: 0,
            })
        })
    }
    
    /// Execute without session (compatibility - creates temporary connection)
    pub async fn execute(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        // For compatibility with tests, use shared connection if available
        // Check if it's any form of memory database (including named shared memory)
        debug!("DbHandler::execute - db_path: {}, query: {}", self.db_path, query);
        if self.db_path == ":memory:" || self.db_path.contains("mode=memory") {
            // For memory databases, we need to use a session connection
            // Create a temporary session for backward compatibility
            let temp_session = Uuid::new_v4();
            if let Err(e) = self.create_session_connection(temp_session).await {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                    Some(format!("Failed to create temporary session: {e}"))
                ));
            }
            
            let result = self.execute_with_session(query, &temp_session).await
                .map_err(|e| match e {
                    PgSqliteError::Sqlite(sqlite_err) => sqlite_err,
                    other => rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("Execution error: {other}"))
                    )
                })?;
            
            self.remove_session_connection(&temp_session);
            Ok(result)
        } else {
            let conn = Self::create_initial_connection(&self.db_path, &Config::load())?;
            
            // Process query with fast path optimization
            let processed_query = process_query(query, &conn, &self.schema_cache)?;
            
            let rows_affected = conn.execute(&processed_query, [])?;
            Ok(DbResponse {
                columns: vec![],
                rows: vec![],
                rows_affected,
            })
        }
    }
    
    /// Execute with session-specific connection (with optional cached connection)
    pub async fn execute_with_session_cached(
        &self, 
        query: &str, 
        session_id: &Uuid,
        cached_conn: Option<&Arc<parking_lot::Mutex<rusqlite::Connection>>>
    ) -> Result<DbResponse, PgSqliteError> {
        match cached_conn {
            Some(conn) => {
                self.connection_manager.execute_with_cached_connection(conn, |conn| {
                    // Process query with fast path optimization
                    let processed_query = process_query(query, conn, &self.schema_cache)?;
                    
                    let rows_affected = conn.execute(&processed_query, [])?;
                    Ok(DbResponse {
                        columns: vec![],
                        rows: vec![],
                        rows_affected,
                    })
                })
            }
            None => {
                // Fall back to regular lookup
                self.execute_with_session(query, session_id).await
            }
        }
    }
    
    /// Execute with session-specific connection
    pub async fn execute_with_session(&self, query: &str, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        self.connection_manager.execute_with_session(session_id, |conn| {
            // Process query with fast path optimization
            let processed_query = process_query(query, conn, &self.schema_cache)?;
            
            let rows_affected = conn.execute(&processed_query, [])?;
            Ok(DbResponse {
                columns: vec![],
                rows: vec![],
                rows_affected,
            })
        })
    }
    
    /// Transaction control methods
    pub async fn begin_with_session(&self, session_id: &Uuid) -> Result<(), PgSqliteError> {
        self.connection_manager.execute_with_session(session_id, |conn| {
            conn.execute("BEGIN", [])?;
            Ok(())
        })
    }
    
    pub async fn commit(&self, session_id: &Uuid) -> Result<(), PgSqliteError> {
        // Execute the commit on the current session
        self.connection_manager.execute_with_session(session_id, |conn| {
            conn.execute("COMMIT", [])?;
            Ok(())
        })?;
        
        // Force all other connections to refresh their WAL view (WAL mode only)
        // This ensures committed data is visible to all other sessions
        self.connection_manager.refresh_all_other_connections(session_id)?;
        
        Ok(())
    }
    
    pub async fn commit_with_session(&self, session_id: &Uuid) -> Result<(), PgSqliteError> {
        self.commit(session_id).await
    }
    
    pub async fn rollback(&self, session_id: &Uuid) -> Result<(), PgSqliteError> {
        self.connection_manager.execute_with_session(session_id, |conn| {
            match conn.execute("ROLLBACK", []) {
                Ok(_) => Ok(()),
                Err(rusqlite::Error::SqliteFailure(_, Some(msg))) 
                    if msg.contains("cannot rollback - no transaction is active") => {
                    // This is fine - no transaction was active
                    debug!("ROLLBACK called with no active transaction - ignoring");
                    Ok(())
                }
                Err(e) => Err(e),
            }?;
            Ok(())
        })
    }
    
    pub async fn rollback_with_session(&self, session_id: &Uuid) -> Result<(), PgSqliteError> {
        self.rollback(session_id).await
    }
    
    
    
    /// Get a mutable connection for operations that require &mut Connection
    pub fn get_mut_connection(&self) -> Result<std::sync::MutexGuard<rusqlite::Connection>, rusqlite::Error> {
        // Create a temporary connection for operations that need it
        // This is not ideal but maintains compatibility
        Err(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
            Some("get_mut_connection not available in per-session mode".to_string())
        ))
    }
    
    /// Get table schema
    pub async fn get_table_schema(&self, table_name: &str) -> Result<crate::cache::schema::TableSchema, rusqlite::Error> {
        let conn = Self::create_initial_connection(&self.db_path, &Config::load())?;
        self.schema_cache.get_or_load(&conn, table_name)
    }
    
    /// Get schema type for a column
    pub async fn get_schema_type(&self, table_name: &str, column_name: &str) -> Result<Option<String>, rusqlite::Error> {
        // For file databases, create a new connection (old behavior)
        // For memory databases, this won't work as each connection is isolated
        if !self.db_path.contains(":memory:") {
            let conn = Self::create_initial_connection(&self.db_path, &Config::load())?;
            
            let mut stmt = conn.prepare(
                "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
            )?;
            
            use rusqlite::OptionalExtension;
            let result = stmt.query_row([table_name, column_name], |row| {
                row.get::<_, String>(0)
            }).optional()?;
            
            return Ok(result);
        }
        
        // For memory databases, we need to use the session connection
        // This method doesn't have access to session_id, so return None
        // The caller should use get_schema_type_with_session instead
        Ok(None)
    }
    
    pub async fn get_schema_type_with_session(&self, session_id: &uuid::Uuid, table_name: &str, column_name: &str) -> Result<Option<String>, PgSqliteError> {
        self.connection_manager.execute_with_session(session_id, |conn| {
            let mut stmt = conn.prepare(
                "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
            )?;
            
            use rusqlite::OptionalExtension;
            let result = stmt.query_row([table_name, column_name], |row| {
                row.get::<_, String>(0)
            }).optional()?;
            
            Ok(result)
        })
    }
    
    /// Try fast path execution with parameters
    pub async fn try_execute_fast_path_with_params(
        &self,
        _query: &str,
        _params: &[rusqlite::types::Value],
        _session_id: &Uuid,
    ) -> Result<Option<DbResponse>, PgSqliteError> {
        // For now, always return None to fall back to regular execution
        Ok(None)
    }
    
    /// Query with statement pool and parameters
    pub async fn query_with_statement_pool_params(
        &self,
        query: &str,
        params: &[Option<Vec<u8>>],
        session_id: &Uuid,
    ) -> Result<DbResponse, PgSqliteError> {
        // Forward to execute_with_params
        self.execute_with_params(query, params, session_id).await
    }
    
    /// Execute with statement pool and parameters
    pub async fn execute_with_statement_pool_params(
        &self,
        query: &str,
        params: &[Option<Vec<u8>>],
        session_id: &Uuid,
    ) -> Result<DbResponse, PgSqliteError> {
        // Forward to execute_with_params
        self.execute_with_params(query, params, session_id).await
    }
    
    // Execute a closure with access to the session's connection
    pub async fn with_session_connection<F, R>(
        &self,
        session_id: &Uuid,
        f: F
    ) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<R, rusqlite::Error>
    {
        self.connection_manager.execute_with_session(session_id, f)
    }
    
    pub async fn with_session_connection_mut<F, R>(
        &self,
        session_id: &Uuid,
        f: F
    ) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R, rusqlite::Error>
    {
        self.connection_manager.execute_with_session_mut(session_id, f)
    }
    
    /// Execute with a cached connection (fast path - no HashMap lookup)
    pub async fn with_cached_connection<F, R>(
        &self,
        cached_conn: &Arc<parking_lot::Mutex<rusqlite::Connection>>,
        f: F
    ) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<R, rusqlite::Error>
    {
        self.connection_manager.execute_with_cached_connection(cached_conn, f)
    }
    
    /// Execute with a mutable cached connection (fast path - no HashMap lookup)
    pub async fn with_cached_connection_mut<F, R>(
        &self,
        cached_conn: &Arc<parking_lot::Mutex<rusqlite::Connection>>,
        f: F
    ) -> Result<R, PgSqliteError>
    where
        F: FnOnce(&mut rusqlite::Connection) -> Result<R, rusqlite::Error>
    {
        self.connection_manager.execute_with_cached_connection_mut(cached_conn, f)
    }
    
    /// Get the connection manager for caching purposes
    pub fn connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.connection_manager
    }
    
    // Compatibility methods for existing code
    pub fn get_schema_cache(&self) -> &Arc<SchemaCache> {
        &self.schema_cache
    }
    
    pub fn get_string_validator(&self) -> &Arc<StringConstraintValidator> {
        &self.string_validator
    }
    
    pub fn get_statement_cache_optimizer(&self) -> &Arc<StatementCacheOptimizer> {
        &self.statement_cache_optimizer
    }
    
    /// Handle information_schema.tables query
    async fn handle_information_schema_tables_query(&self, query: &str, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling information_schema.tables query: {}", query);
        
        // Check if this is a simple table_name only query
        if query.contains("SELECT table_name") && !query.contains("table_catalog") {
            // Simple query - just return table names
            let tables_query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%' ORDER BY name";
            
            return self.connection_manager.execute_with_session(session_id, |conn| {
                let mut stmt = conn.prepare(tables_query)?;
                let rows: Result<Vec<_>, _> = stmt.query_map([], |row| {
                    let table_name: String = row.get(0)?;
                    Ok(vec![Some(table_name.into_bytes())])
                })?.collect();
                
                Ok(DbResponse {
                    columns: vec!["table_name".to_string()],
                    rows: rows?,
                    rows_affected: 0,
                })
            });
        }
        
        // Full information_schema.tables query
        let tables_query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%' ORDER BY name";
        
        self.connection_manager.execute_with_session(session_id, |conn| {
            let mut stmt = conn.prepare(tables_query)?;
            let rows: Result<Vec<_>, _> = stmt.query_map([], |row| {
                let table_name: String = row.get(0)?;
                // Return full information_schema.tables row
                Ok(vec![
                    Some("main".to_string().into_bytes()),      // table_catalog
                    Some("public".to_string().into_bytes()),    // table_schema  
                    Some(table_name.into_bytes()),              // table_name
                    Some("BASE TABLE".to_string().into_bytes()), // table_type
                    None,                                       // self_referencing_column_name
                    None,                                       // reference_generation
                    None,                                       // user_defined_type_catalog
                    None,                                       // user_defined_type_schema
                    None,                                       // user_defined_type_name
                    None,                                       // is_insertable_into
                    None,                                       // is_typed
                    None,                                       // commit_action
                ])
            })?.collect();
            
            Ok(DbResponse {
                columns: vec![
                    "table_catalog".to_string(),
                    "table_schema".to_string(),
                    "table_name".to_string(),
                    "table_type".to_string(),
                    "self_referencing_column_name".to_string(),
                    "reference_generation".to_string(),
                    "user_defined_type_catalog".to_string(),
                    "user_defined_type_schema".to_string(),
                    "user_defined_type_name".to_string(),
                    "is_insertable_into".to_string(),
                    "is_typed".to_string(),
                    "commit_action".to_string(),
                ],
                rows: rows?,
                rows_affected: 0,
            })
        })
    }
    
    /// Handle SQLAlchemy table existence check query
    /// This optimizes the complex JOIN query by doing a simple table lookup
    async fn handle_table_existence_query(&self, query: &str, session_id: &Uuid) -> Result<DbResponse, PgSqliteError> {
        // Extract table name from the query
        // Look for patterns like "relname = 'table_name'" or "relname = $1"
        let table_name = if let Some(captures) = regex::Regex::new(r"relname\s*=\s*'([^']+)'").unwrap().captures(query) {
            captures[1].to_string()
        } else {
            // For parameterized queries, we need to look at the actual parameters
            // For now, return empty result to indicate table doesn't exist
            // This will cause SQLAlchemy to proceed with CREATE TABLE
            return Ok(DbResponse {
                columns: vec!["relname".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        };
        
        debug!("Checking table existence for: {}", table_name);
        
        // Simple table existence check
        let existence_query = "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') AND name = ? AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'";
        
        self.connection_manager.execute_with_session(session_id, |conn| {
            let mut stmt = conn.prepare(existence_query)?;
            let rows: Result<Vec<_>, _> = stmt.query_map([&table_name], |row| {
                let name: String = row.get(0)?;
                Ok(vec![Some(name.into_bytes())])
            })?.collect();
            
            Ok(DbResponse {
                columns: vec!["relname".to_string()],
                rows: rows?,
                rows_affected: 0,
            })
        })
    }
}

/// Helper function to extract table name from INSERT query
pub fn extract_insert_table_name(query: &str) -> Option<String> {
    // Simple regex-free parsing for performance - use case-insensitive search
    let into_pos = query.as_bytes().windows(6)
        .position(|window| window.eq_ignore_ascii_case(b" INTO "))?;
    let after_into = &query[into_pos + 6..].trim();
    // Find the table name (ends at space or opening parenthesis)
    let end = after_into.find(' ').or_else(|| after_into.find('(')).unwrap_or(after_into.len());
    let table_name = after_into[..end].trim();
    if !table_name.is_empty() {
        return Some(table_name.to_string());
    }
    None
}

/// Rewrite query to handle DECIMAL types if needed
pub fn rewrite_query_for_decimal(query: &str, conn: &rusqlite::Connection) -> Result<String, rusqlite::Error> {
    use sqlparser::parser::Parser;
    use sqlparser::dialect::PostgreSqlDialect;
    
    // Parse the SQL statement (keep JSON path placeholders for now)
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, query)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    
    if statements.is_empty() {
        return Ok(query.to_string());
    }
    
    // Rewrite the first statement for decimal handling
    let mut rewriter = crate::rewriter::DecimalQueryRewriter::new(conn);
    if let Err(e) = rewriter.rewrite_statement(&mut statements[0]) {
        // If rewriting fails, log and return original query
        tracing::warn!("Failed to rewrite query for decimal: {}", e);
        return Ok(query.to_string());
    }
    
    let rewritten = statements[0].to_string();
    tracing::debug!("Decimal rewriter output: {}", rewritten);
    Ok(rewritten)
}