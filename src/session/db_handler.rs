use std::sync::Arc;
use uuid::Uuid;
use rusqlite::OptionalExtension;
use crate::cache::SchemaCache;
use crate::optimization::{OptimizationManager, statement_cache_optimizer::StatementCacheOptimizer};
use crate::query::{QueryTypeDetector, QueryType, process_query, executor::extract_table_name_from_create};
use crate::config::Config;
use crate::migration::MigrationRunner;
use crate::validator::StringConstraintValidator;
use crate::session::ConnectionManager;
use crate::ddl::CommentDdlHandler;
use crate::PgSqliteError;
use tracing::debug;

/// Database response structure
#[derive(Debug)]
pub struct DbResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub rows_affected: usize,
}

/// Type alias for database result rows to simplify complex type signatures
pub type DbRows = Vec<Vec<Option<Vec<u8>>>>;

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
            debug!("New database file detected, will run initial migrations...");
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
        
        // DbHandler initialized
        
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
            debug!("Running initial migrations for in-memory database...");
            
            // Register functions before migrations
            crate::functions::register_all_functions(&conn)?;
            
            let mut runner = MigrationRunner::new(conn);
            match runner.run_pending_migrations() {
                Ok(applied) => {
                    if !applied.is_empty() {
                        // Migrations applied
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
                Err(_e) => {
                    // Don't fail on drift detection errors, just log them
                    // Schema drift check failed
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
            debug!("Running initial migrations...");
            
            // Register functions before migrations
            crate::functions::register_all_functions(&conn)?;
            
            let mut runner = MigrationRunner::new(conn);
            match runner.run_pending_migrations() {
                Ok(applied) => {
                    if !applied.is_empty() {
                        // Migrations applied
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
        debug!("execute_with_params called with query: {}", query);
        debug!("execute_with_params params count: {}", params.len());
        let result = self.connection_manager.execute_with_session(session_id, |conn| {
            // Process query with fast path optimization
            let processed_query = process_query(query, conn, &self.schema_cache)?;
            debug!("Processed query: {}", processed_query);
            
            let mut stmt = conn.prepare(&processed_query)?;
            
            // Convert params to rusqlite values
            // For now, be more aggressive about converting to text since most PostgreSQL
            // parameters in text mode should be text-compatible
            let values: Vec<rusqlite::types::Value> = params.iter()
                .map(|p| match p {
                    Some(data) => {
                        match String::from_utf8(data.clone()) {
                            Ok(s) => {
                                // Parameter converted to text
                                rusqlite::types::Value::Text(s)
                            },
                            Err(_e) => {
                                // For psycopg3 in text mode, all parameters should be UTF-8 text
                                // If UTF-8 conversion fails, try to recover by using lossy conversion
                                // UTF-8 conversion failed, trying lossy
                                let lossy_string = String::from_utf8_lossy(data);
                                if !lossy_string.is_empty() {
                                    // Lossy conversion successful
                                    rusqlite::types::Value::Text(lossy_string.into_owned())
                                } else {
                                    // Storing as blob
                                    rusqlite::types::Value::Blob(data.clone())
                                }
                            },
                        }
                    }
                    None => {
                        // Null parameter
                        rusqlite::types::Value::Null
                    },
                })
                .collect();
            
            let query_type = QueryTypeDetector::detect_query_type(query);
            
            let result = match query_type {
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
                    
                    let result_rows = rows?;
                    debug!("Query returned {} rows", result_rows.len());
                    DbResponse {
                        columns,
                        rows: result_rows,
                        rows_affected: 0,
                    }
                }
                _ => {
                    let rows_affected = stmt.execute(rusqlite::params_from_iter(values.iter()))?;
                    DbResponse {
                        columns: vec![],
                        rows: vec![],
                        rows_affected,
                    }
                }
            };
            
            // After a successful DML operation, check if we need to trigger WAL refresh
            // This is needed for autocommit mode where no explicit COMMIT is sent
            if query_type != QueryType::Select && result.rows_affected > 0 {
                // Check if we're in autocommit mode
                if conn.is_autocommit() {
                    debug!("DML operation completed in autocommit mode, need to trigger WAL refresh for session {}", session_id);
                    // Note: We can't trigger refresh from within the connection closure
                    // We'll need to return a flag to the caller
                }
            }
            
            Ok(result)
        })?;
        
        // After the closure completes, check if we need WAL refresh
        let query_type = QueryTypeDetector::detect_query_type(query);
        if query_type != QueryType::Select && result.rows_affected > 0 {
            // Check if we're in autocommit mode
            let is_autocommit = self.connection_manager.execute_with_session(session_id, |conn| {
                Ok(conn.is_autocommit())
            })?;
            
            if is_autocommit {
                debug!("DML operation completed in autocommit mode, triggering WAL refresh for session {}", session_id);
                self.connection_manager.refresh_all_other_connections(session_id)?;
            }
        }
        
        Ok(result)
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
            
            // Register functions on the temporary connection
            crate::functions::register_all_functions(&conn)?;
            
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

        // We'll rewrite the query just before execution if needed
        
        // Handle special system function queries
        if lower_query.trim() == "select current_user()" {
            return Ok(DbResponse {
                columns: vec!["current_user".to_string()],
                rows: vec![vec![Some("postgres".to_string().into_bytes())]],
                rows_affected: 1,
            });
        }

        // Handle pg_tablespace queries
        if lower_query.contains("pg_tablespace") || lower_query.contains("pg_catalog.pg_tablespace") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return Ok(CatalogInterceptor::handle_pg_tablespace_query(select));
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["oid".to_string(), "spcname".to_string(), "spcowner".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.routines queries first
        if lower_query.contains("information_schema.routines") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_routines_query(select, self).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["routine_name".to_string(), "routine_type".to_string(), "data_type".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.triggers queries
        if lower_query.contains("information_schema.triggers") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_triggers_query_with_session(select, self, session_id).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["trigger_catalog".to_string(), "trigger_schema".to_string(), "trigger_name".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.referential_constraints queries
        if lower_query.contains("information_schema.referential_constraints") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_referential_constraints_query_with_session(select, self, session_id).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["constraint_name".to_string(), "constraint_catalog".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.check_constraints queries
        if lower_query.contains("information_schema.check_constraints") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_check_constraints_query_with_session(select, self, session_id).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["constraint_catalog".to_string(), "constraint_schema".to_string(), "constraint_name".to_string(), "check_clause".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.views queries
        if lower_query.contains("information_schema.views") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_views_query_with_session(select, self, session_id).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["table_name".to_string(), "view_definition".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        if (lower_query.contains("pg_catalog") ||
           lower_query.contains("pg_type") || lower_query.contains("pg_class") ||
           lower_query.contains("pg_attribute") || lower_query.contains("pg_enum") ||
           lower_query.contains("pg_stats") || lower_query.contains("pg_roles") ||
           lower_query.contains("pg_user")) &&
           !lower_query.contains("information_schema") {
            // For catalog queries, we need to use the catalog interceptor
            // This requires an Arc<DbHandler>, but we can't create a cyclic Arc here
            // Instead, let's handle specific queries directly for now
            if lower_query.contains("information_schema.tables") {
                return self.handle_information_schema_tables_query(query, session_id).await;
            }

            if lower_query.contains("information_schema.columns") {
                // Use the catalog interceptor for columns queries with session-based execution
                use crate::catalog::query_interceptor::CatalogInterceptor;
                let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
                if let Ok(mut statements) = parsed_query
                    && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                        && let Some(select) = query_ast.body.as_select() {
                            return CatalogInterceptor::handle_information_schema_columns_query_with_session(select, self, session_id).await;
                        }
                // Fallback to empty response if parsing fails
                return Ok(DbResponse {
                    columns: vec!["table_name".to_string(), "column_name".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
            }

            if lower_query.contains("information_schema.key_column_usage") {
                use crate::catalog::query_interceptor::CatalogInterceptor;
                let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
                if let Ok(mut statements) = parsed_query
                    && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                        && let Some(select) = query_ast.body.as_select() {
                            return CatalogInterceptor::handle_information_schema_key_column_usage_query(select, self, session_id).await;
                        }
                // Fallback to empty response if parsing fails
                return Ok(DbResponse {
                    columns: vec!["constraint_name".to_string(), "table_name".to_string(), "column_name".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
            }

            if lower_query.contains("information_schema.table_constraints") {
                use crate::catalog::query_interceptor::CatalogInterceptor;
                let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
                if let Ok(mut statements) = parsed_query
                    && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                        && let Some(select) = query_ast.body.as_select() {
                            return CatalogInterceptor::handle_information_schema_table_constraints_query(select, self, session_id).await;
                        }
                // Fallback to empty response if parsing fails
                return Ok(DbResponse {
                    columns: vec!["constraint_name".to_string(), "table_name".to_string(), "constraint_type".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
            }

            if lower_query.contains("information_schema.routines") {
                use crate::catalog::query_interceptor::CatalogInterceptor;
                let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
                if let Ok(mut statements) = parsed_query
                    && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                        && let Some(select) = query_ast.body.as_select() {
                            return CatalogInterceptor::handle_information_schema_routines_query(select, self).await;
                        }
                // Fallback to empty response if parsing fails
                return Ok(DbResponse {
                    columns: vec!["routine_name".to_string(), "routine_type".to_string(), "data_type".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
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
        eprintln!("🔍 query_with_session called with query: {}", query);
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

        // Handle pg_tablespace queries
        if lower_query.contains("pg_tablespace") || lower_query.contains("pg_catalog.pg_tablespace") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return Ok(CatalogInterceptor::handle_pg_tablespace_query(select));
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["oid".to_string(), "spcname".to_string(), "spcowner".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.routines queries first
        if lower_query.contains("information_schema.routines") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_routines_query(select, self).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["routine_name".to_string(), "routine_type".to_string(), "data_type".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.triggers queries
        if lower_query.contains("information_schema.triggers") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_triggers_query_with_session(select, self, session_id).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["trigger_catalog".to_string(), "trigger_schema".to_string(), "trigger_name".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.referential_constraints queries
        if lower_query.contains("information_schema.referential_constraints") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_referential_constraints_query_with_session(select, self, session_id).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["constraint_name".to_string(), "constraint_catalog".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.check_constraints queries
        if lower_query.contains("information_schema.check_constraints") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_check_constraints_query_with_session(select, self, session_id).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["constraint_catalog".to_string(), "constraint_schema".to_string(), "constraint_name".to_string(), "check_clause".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle information_schema.views queries
        if lower_query.contains("information_schema.views") {
            use crate::catalog::query_interceptor::CatalogInterceptor;
            let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
            if let Ok(mut statements) = parsed_query
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                    && let Some(select) = query_ast.body.as_select() {
                        return CatalogInterceptor::handle_information_schema_views_query_with_session(select, self, session_id).await;
                    }
            // Fallback to empty response if parsing fails
            return Ok(DbResponse {
                columns: vec!["table_name".to_string(), "view_definition".to_string()],
                rows: vec![],
                rows_affected: 0,
            });
        }

        // Handle pg_stats queries directly (simplified approach)
        if lower_query.contains("pg_stats") {
            use crate::catalog::pg_stats::PgStatsHandler;
            use sqlparser::parser::Parser;
            use sqlparser::dialect::PostgreSqlDialect;

            if let Ok(mut statements) = Parser::parse_sql(&PostgreSqlDialect {}, query)
                && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                && let Some(select) = query_ast.body.as_select() {
                match PgStatsHandler::handle_query(select, self).await {
                    Ok(response) => return Ok(response),
                    Err(_) => {
                        // Fallback to empty response
                        return Ok(DbResponse {
                            columns: vec!["schemaname".to_string(), "tablename".to_string(), "attname".to_string()],
                            rows: vec![],
                            rows_affected: 0,
                        });
                    }
                }
            }
        }

        if (lower_query.contains("pg_catalog") ||
           lower_query.contains("pg_type") || lower_query.contains("pg_class") ||
           lower_query.contains("pg_attribute") || lower_query.contains("pg_enum") ||
           lower_query.contains("pg_stats") || lower_query.contains("pg_roles") ||
           lower_query.contains("pg_user")) &&
           !lower_query.contains("information_schema") {
            // For catalog queries, we need to use the catalog interceptor
            // This requires an Arc<DbHandler>, but we can't create a cyclic Arc here
            // Instead, let's handle specific queries directly for now
            if lower_query.contains("information_schema.tables") {
                return self.handle_information_schema_tables_query(query, session_id).await;
            }

            if lower_query.contains("information_schema.columns") {
                // Use the catalog interceptor for columns queries with session-based execution
                use crate::catalog::query_interceptor::CatalogInterceptor;
                let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
                if let Ok(mut statements) = parsed_query
                    && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                        && let Some(select) = query_ast.body.as_select() {
                            return CatalogInterceptor::handle_information_schema_columns_query_with_session(select, self, session_id).await;
                        }
                // Fallback to empty response if parsing fails
                return Ok(DbResponse {
                    columns: vec!["table_name".to_string(), "column_name".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
            }

            if lower_query.contains("information_schema.key_column_usage") {
                use crate::catalog::query_interceptor::CatalogInterceptor;
                let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
                if let Ok(mut statements) = parsed_query
                    && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                        && let Some(select) = query_ast.body.as_select() {
                            return CatalogInterceptor::handle_information_schema_key_column_usage_query(select, self, session_id).await;
                        }
                // Fallback to empty response if parsing fails
                return Ok(DbResponse {
                    columns: vec!["constraint_name".to_string(), "table_name".to_string(), "column_name".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
            }

            if lower_query.contains("information_schema.table_constraints") {
                use crate::catalog::query_interceptor::CatalogInterceptor;
                let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
                if let Ok(mut statements) = parsed_query
                    && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                        && let Some(select) = query_ast.body.as_select() {
                            return CatalogInterceptor::handle_information_schema_table_constraints_query(select, self, session_id).await;
                        }
                // Fallback to empty response if parsing fails
                return Ok(DbResponse {
                    columns: vec!["constraint_name".to_string(), "table_name".to_string(), "constraint_type".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
            }

            if lower_query.contains("information_schema.routines") {
                use crate::catalog::query_interceptor::CatalogInterceptor;
                let parsed_query = sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::PostgreSqlDialect {}, query);
                if let Ok(mut statements) = parsed_query
                    && let Some(sqlparser::ast::Statement::Query(query_ast)) = statements.pop()
                        && let Some(select) = query_ast.body.as_select() {
                            return CatalogInterceptor::handle_information_schema_routines_query(select, self).await;
                        }
                // Fallback to empty response if parsing fails
                return Ok(DbResponse {
                    columns: vec!["routine_name".to_string(), "routine_type".to_string(), "data_type".to_string()],
                    rows: vec![],
                    rows_affected: 0,
                });
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

        // Rewrite information_schema queries to use real SQLite views
        let rewritten_query = if lower_query.contains("information_schema") {
            self.rewrite_information_schema_query(query)
        } else {
            query.to_string()
        };

        self.connection_manager.execute_with_session(session_id, move |conn| {
            // Process query with fast path optimization
            let processed_query = process_query(&rewritten_query, conn, &self.schema_cache)?;
            
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
            let mut conn = Self::create_initial_connection(&self.db_path, &Config::load())?;

            // Register functions on the temporary connection
            crate::functions::register_all_functions(&conn)?;

            // Handle COMMENT DDL statements
            if CommentDdlHandler::is_comment_ddl(query) {
                return match CommentDdlHandler::handle_comment_ddl(&mut conn, query) {
                    Ok(()) => Ok(DbResponse {
                        columns: vec![],
                        rows: vec![],
                        rows_affected: 0,
                    }),
                    Err(PgSqliteError::Sqlite(e)) => Err(e),
                    Err(PgSqliteError::Protocol(msg)) => Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(msg)
                    )),
                    Err(_) => Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some("Comment operation failed".to_string())
                    )),
                };
            }

            // Check if this is a CREATE TABLE statement that needs special handling
            let (processed_query, type_mappings) =
                if query.trim_start().to_uppercase().starts_with("CREATE TABLE") {
                    eprintln!("🔨 Processing CREATE TABLE statement in query_with_session...");
                    // Use CREATE TABLE translator with full metadata capture
                    use crate::translator::CreateTableTranslator;
                    match CreateTableTranslator::translate_with_connection_full(query, Some(&conn)) {
                        Ok(result) => {
                            debug!("CREATE TABLE translated with {} type mappings", result.type_mappings.len());
                            (result.sql, result.type_mappings)
                        }
                        Err(e) => {
                            return Err(rusqlite::Error::SqliteFailure(
                                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                Some(format!("CREATE TABLE translation failed: {}", e))
                            ));
                        }
                    }
                } else {
                    // Process query with fast path optimization for non-CREATE TABLE statements
                    let processed = process_query(query, &conn, &self.schema_cache)?;
                    (processed, std::collections::HashMap::new())
                };

            let rows_affected = conn.execute(&processed_query, [])?;

            // Handle CREATE TABLE metadata storage and constraints
            if query.trim_start().to_uppercase().starts_with("CREATE TABLE")
                && let Some(table_name) = extract_table_name_from_create(query) {

                // Store type mappings in schema metadata table
                debug!("CREATE TABLE metadata storage: {} type mappings found", type_mappings.len());
                if !type_mappings.is_empty() {
                    // Initialize the metadata table if it doesn't exist
                    let init_query = "CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
                        table_name TEXT NOT NULL,
                        column_name TEXT NOT NULL,
                        pg_type TEXT NOT NULL,
                        sqlite_type TEXT NOT NULL,
                        PRIMARY KEY (table_name, column_name)
                    )";

                    if let Err(e) = conn.execute(init_query, []) {
                        debug!("Failed to create __pgsqlite_schema table: {}", e);
                    }

                    // Initialize numeric constraints table if it doesn't exist
                    let init_numeric_query = "CREATE TABLE IF NOT EXISTS __pgsqlite_numeric_constraints (
                        table_name TEXT NOT NULL,
                        column_name TEXT NOT NULL,
                        precision INTEGER NOT NULL,
                        scale INTEGER NOT NULL,
                        PRIMARY KEY (table_name, column_name)
                    )";

                    if let Err(e) = conn.execute(init_numeric_query, []) {
                        debug!("Failed to create __pgsqlite_numeric_constraints table: {}", e);
                    }

                    // Store each type mapping
                    for (full_column, type_mapping) in &type_mappings {
                        // Split table.column format
                        let parts: Vec<&str> = full_column.split('.').collect();
                        if parts.len() == 2 && parts[0] == table_name {
                            let insert_query = format!(
                                "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES ('{}', '{}', '{}', '{}')",
                                table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type
                            );

                            if let Err(e) = conn.execute(&insert_query, []) {
                                debug!("Failed to store metadata for {}.{}: {}", table_name, parts[1], e);
                            } else {
                                debug!("Stored metadata: {}.{} -> {} ({})", table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type);
                            }

                            // Store numeric constraints if present
                            debug!("Checking for numeric constraints: pg_type={}, modifier={:?}", type_mapping.pg_type, type_mapping.type_modifier);
                            if let Some(modifier) = type_mapping.type_modifier {
                                // Extract base type without parameters
                                let base_type = if let Some(paren_pos) = type_mapping.pg_type.find('(') {
                                    type_mapping.pg_type[..paren_pos].trim()
                                } else {
                                    &type_mapping.pg_type
                                };
                                let pg_type_lower = base_type.to_lowercase();

                                if pg_type_lower == "numeric" || pg_type_lower == "decimal" {
                                    // Decode precision and scale from modifier
                                    let tmp_typmod = modifier - 4; // Remove VARHDRSZ
                                    let precision = (tmp_typmod >> 16) & 0xFFFF;
                                    let scale = tmp_typmod & 0xFFFF;

                                    let constraint_query = format!(
                                        "INSERT OR REPLACE INTO __pgsqlite_numeric_constraints (table_name, column_name, precision, scale) VALUES ('{}', '{}', {}, {})",
                                        table_name, parts[1], precision, scale
                                    );

                                    if let Err(e) = conn.execute(&constraint_query, []) {
                                        debug!("Failed to store numeric constraint for {}.{}: {}", table_name, parts[1], e);
                                    } else {
                                        debug!("Stored numeric constraint: {}.{} precision={} scale={}", table_name, parts[1], precision, scale);
                                    }
                                }
                            }
                        }
                    }
                }

                // Populate constraints for CREATE TABLE statements
                if let Err(e) = crate::catalog::constraint_populator::populate_constraints_for_table(&conn, &table_name) {
                    // Log the error but don't fail the CREATE TABLE operation
                    debug!("Failed to populate constraints for table {}: {}", table_name, e);
                } else {
                    debug!("Successfully populated constraint catalog tables for table: {}", table_name);
                }
            }

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
        eprintln!("🗂️ execute_with_session_cached called, cached_conn: {}", cached_conn.is_some());
        match cached_conn {
            Some(conn) => {
                self.connection_manager.execute_with_cached_connection(conn, |conn| {
                    // Process query with fast path optimization
                    let processed_query = process_query(query, conn, &self.schema_cache)?;

                    let rows_affected = conn.execute(&processed_query, [])?;

                    // Handle CREATE TABLE metadata storage
                    if query.trim_start().to_uppercase().starts_with("CREATE TABLE") {
                        if let Some(table_name) = extract_table_name_from_create(query) {
                            // Get type mappings from CREATE TABLE translator
                            use crate::translator::CreateTableTranslator;
                            if let Ok(result) = CreateTableTranslator::translate_with_connection_full(query, Some(conn)) {
                                if !result.type_mappings.is_empty() {
                                    // Store type mappings and numeric constraints
                                    if let Err(e) = self.store_create_table_metadata(conn, &table_name, &result.type_mappings) {
                                        debug!("Failed to store CREATE TABLE metadata: {}", e);
                                    }
                                }

                                // Populate constraints for CREATE TABLE statements
                                if let Err(e) = crate::catalog::constraint_populator::populate_constraints_for_table(conn, &table_name) {
                                    // Log the error but don't fail the CREATE TABLE operation
                                    debug!("Failed to populate constraints for table {}: {}", table_name, e);
                                } else {
                                    debug!("Successfully populated constraint catalog tables for table: {}", table_name);
                                }
                            }
                        }
                    }

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
        // Handle COMMENT DDL statements (need mutable connection)
        if CommentDdlHandler::is_comment_ddl(query) {
            return self.connection_manager.execute_with_session_mut(session_id, |conn| {
                match CommentDdlHandler::handle_comment_ddl(conn, query) {
                    Ok(()) => Ok(DbResponse {
                        columns: vec![],
                        rows: vec![],
                        rows_affected: 0,
                    }),
                    Err(PgSqliteError::Sqlite(e)) => Err(e),
                    Err(PgSqliteError::Protocol(msg)) => Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(msg)
                    )),
                    Err(_) => Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some("Comment operation failed".to_string())
                    )),
                }
            });
        }

        self.connection_manager.execute_with_session(session_id, |conn| {
            // Check if this is a CREATE TABLE statement that needs special handling
            let (processed_query, type_mappings, _array_columns, _enum_columns) =
                if query.trim_start().to_uppercase().starts_with("CREATE TABLE") {
                    debug!("Processing CREATE TABLE statement with translation...");
                    // Use CREATE TABLE translator with full metadata capture
                    use crate::translator::CreateTableTranslator;
                    match CreateTableTranslator::translate_with_connection_full(query, Some(conn)) {
                        Ok(result) => {
                            debug!("CREATE TABLE translated with {} type mappings and {} array columns",
                                result.type_mappings.len(), result.array_columns.len());
                            (result.sql, result.type_mappings, result.array_columns, result.enum_columns)
                        }
                        Err(e) => {
                            return Err(rusqlite::Error::SqliteFailure(
                                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                                Some(format!("CREATE TABLE translation failed: {}", e))
                            ));
                        }
                    }
                } else {
                    // Process query with fast path optimization for non-CREATE TABLE statements
                    let processed = process_query(query, conn, &self.schema_cache)?;
                    (processed, std::collections::HashMap::new(), Vec::new(), Vec::new())
                };

            let rows_affected = conn.execute(&processed_query, [])?;

            // Handle CREATE TABLE metadata storage and constraints
            if query.trim_start().to_uppercase().starts_with("CREATE TABLE")
                && let Some(table_name) = extract_table_name_from_create(query) {

                // Store type mappings in schema metadata table
                debug!("CREATE TABLE metadata storage: {} type mappings found", type_mappings.len());
                if !type_mappings.is_empty() {
                    // Initialize the metadata table if it doesn't exist
                    let init_query = "CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
                        table_name TEXT NOT NULL,
                        column_name TEXT NOT NULL,
                        pg_type TEXT NOT NULL,
                        sqlite_type TEXT NOT NULL,
                        PRIMARY KEY (table_name, column_name)
                    )";

                    if let Err(e) = conn.execute(init_query, []) {
                        debug!("Failed to create __pgsqlite_schema table: {}", e);
                    }

                    // Initialize numeric constraints table if it doesn't exist
                    let init_numeric_query = "CREATE TABLE IF NOT EXISTS __pgsqlite_numeric_constraints (
                        table_name TEXT NOT NULL,
                        column_name TEXT NOT NULL,
                        precision INTEGER NOT NULL,
                        scale INTEGER NOT NULL,
                        PRIMARY KEY (table_name, column_name)
                    )";

                    if let Err(e) = conn.execute(init_numeric_query, []) {
                        debug!("Failed to create __pgsqlite_numeric_constraints table: {}", e);
                    }

                    // Store each type mapping
                    for (full_column, type_mapping) in &type_mappings {
                        // Split table.column format
                        let parts: Vec<&str> = full_column.split('.').collect();
                        if parts.len() == 2 && parts[0] == table_name {
                            let insert_query = format!(
                                "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES ('{}', '{}', '{}', '{}')",
                                table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type
                            );

                            if let Err(e) = conn.execute(&insert_query, []) {
                                debug!("Failed to store metadata for {}.{}: {}", table_name, parts[1], e);
                            } else {
                                debug!("Stored metadata: {}.{} -> {} ({})", table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type);
                            }

                            // Store numeric constraints if present
                            debug!("Checking for numeric constraints: pg_type={}, modifier={:?}", type_mapping.pg_type, type_mapping.type_modifier);
                            if let Some(modifier) = type_mapping.type_modifier {
                                // Extract base type without parameters
                                let base_type = if let Some(paren_pos) = type_mapping.pg_type.find('(') {
                                    type_mapping.pg_type[..paren_pos].trim()
                                } else {
                                    &type_mapping.pg_type
                                };
                                let pg_type_lower = base_type.to_lowercase();

                                if pg_type_lower == "numeric" || pg_type_lower == "decimal" {
                                    // Decode precision and scale from modifier
                                    let tmp_typmod = modifier - 4; // Remove VARHDRSZ
                                    let precision = (tmp_typmod >> 16) & 0xFFFF;
                                    let scale = tmp_typmod & 0xFFFF;

                                    let constraint_query = format!(
                                        "INSERT OR REPLACE INTO __pgsqlite_numeric_constraints (table_name, column_name, precision, scale) VALUES ('{}', '{}', {}, {})",
                                        table_name, parts[1], precision, scale
                                    );

                                    if let Err(e) = conn.execute(&constraint_query, []) {
                                        debug!("Failed to store numeric constraint for {}.{}: {}", table_name, parts[1], e);
                                    } else {
                                        debug!("Stored numeric constraint: {}.{} precision={} scale={}", table_name, parts[1], precision, scale);
                                    }
                                }
                            }
                        }
                    }
                }

                // Populate constraints for CREATE TABLE statements
                if let Err(e) = crate::catalog::constraint_populator::populate_constraints_for_table(conn, &table_name) {
                    // Log the error but don't fail the CREATE TABLE operation
                    debug!("Failed to populate constraints for table {}: {}", table_name, e);
                } else {
                    debug!("Successfully populated constraint catalog tables for table: {}", table_name);
                }
            }

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
    pub fn get_mut_connection(&self) -> Result<std::sync::MutexGuard<'_, rusqlite::Connection>, rusqlite::Error> {
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
    
    /// Get schema type for a column using a dedicated connection
    pub async fn get_schema_type(&self, table_name: &str, column_name: &str) -> Result<Option<String>, rusqlite::Error> {
        // Create a dedicated connection to read schema data
        // This ensures we can read committed schema metadata regardless of session isolation
        let conn = Self::create_initial_connection(&self.db_path, &Config::load())?;
        
        debug!("get_schema_type: Looking for table='{}', column='{}'", table_name, column_name);
        
        // First, check what entries exist in the schema table
        let mut all_stmt = conn.prepare("SELECT table_name, column_name, pg_type FROM __pgsqlite_schema LIMIT 10")?;
        let mut rows = all_stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?;
        
        debug!("get_schema_type: Schema table contains:");
        let mut found_entries = 0;
        while let Ok(Some(row)) = rows.next().transpose() {
            found_entries += 1;
            debug!("  table='{}', column='{}', pg_type='{}'", row.0, row.1, row.2);
        }
        debug!("get_schema_type: Found {} total schema entries", found_entries);
        
        let mut stmt = conn.prepare(
            "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
        )?;
        
        use rusqlite::OptionalExtension;
        let result = stmt.query_row([table_name, column_name], |row| {
            row.get::<_, String>(0)
        }).optional()?;
        
        debug!("get_schema_type: Query result for '{}','{}'= {:?}", table_name, column_name, result);
        
        Ok(result)
    }
    
    /// Get schema type for a column using the session connection to see uncommitted data
    pub async fn get_schema_type_with_session(&self, session_id: &Uuid, table_name: &str, column_name: &str) -> Result<Option<String>, PgSqliteError> {
        debug!("get_schema_type_with_session: Looking for table='{}', column='{}' in session {}", table_name, column_name, session_id);
        
        let result = self.with_session_connection(session_id, |conn| {
            let mut stmt = conn.prepare(
                "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
            )?;
            
            use rusqlite::OptionalExtension;
            let result = stmt.query_row([table_name, column_name], |row| {
                row.get::<_, String>(0)
            }).optional()?;
            
            Ok(result)
        }).await;
        
        match result {
            Ok(schema_result) => {
                debug!("get_schema_type_with_session: Query result for '{}','{}' in session {}= {:?}", 
                       table_name, column_name, session_id, schema_result);
                Ok(schema_result)
            }
            Err(e) => {
                debug!("get_schema_type_with_session: Error querying schema for '{}','{}': {}", table_name, column_name, e);
                Err(e)
            }
        }
    }
    
    /// Try fast path execution with parameters
    pub async fn try_execute_fast_path_with_params(
        &self,
        query: &str,
        params: &[rusqlite::types::Value],
        session_id: &Uuid,
    ) -> Result<Option<DbResponse>, PgSqliteError> {
        
        // Detect query type before the closure
        let query_type = QueryTypeDetector::detect_query_type(query);
        
        // Use the connection manager to get the session connection
        let result = self.connection_manager.execute_with_session(session_id, |conn| {
            // Execute the query directly with rusqlite parameters
            let mut stmt = conn.prepare(query)?;
            
            let response: Result<DbResponse, rusqlite::Error> = match query_type {
                QueryType::Select => {
                    let column_count = stmt.column_count();
                    let mut column_names = Vec::with_capacity(column_count);
                    for i in 0..column_count {
                        column_names.push(stmt.column_name(i).unwrap_or("").to_string());
                    }
                    
                    // Build datetime column info for conversion
                    let mut datetime_columns = std::collections::HashMap::new();
                    
                    // Try to extract table name from query for schema lookup
                    let table_name = regex::Regex::new(r"(?i)FROM\s+(\w+)").unwrap().captures(query).map(|captures| captures[1].to_string());
                    
                    
                    // Look up column types for datetime conversion
                    if let Some(ref table) = table_name {
                        for (i, column_name) in column_names.iter().enumerate() {
                            // Handle aliased columns by extracting the base column name
                            let base_column_name = if column_name.contains("_") {
                                // For aliased columns like "users_created_at", try to extract "created_at"
                                if let Some(underscore_pos) = column_name.rfind('_') {
                                    &column_name[underscore_pos + 1..]
                                } else {
                                    column_name
                                }
                            } else {
                                column_name
                            };
                            
                            // Look up schema type
                            let mut schema_stmt = conn.prepare(
                                "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
                            )?;
                            
                            if let Ok(Some(pg_type)) = schema_stmt.query_row([table, base_column_name], |row| {
                                row.get::<_, String>(0)
                            }).optional() {
                                if pg_type == "TIMESTAMP" || pg_type == "TIMESTAMP WITHOUT TIME ZONE" {
                                    datetime_columns.insert(i, "timestamp");
                                } else if pg_type == "DATE" {
                                    datetime_columns.insert(i, "date");
                                } else if pg_type == "TIME" || pg_type == "TIME WITHOUT TIME ZONE" {
                                    datetime_columns.insert(i, "time");
                                }
                            }
                        }
                    }
                    
                    let mut rows = Vec::new();
                    let mut prepared_stmt = stmt.query(rusqlite::params_from_iter(params.iter()))?;
                    
                    while let Some(row) = prepared_stmt.next()? {
                        let mut row_data = Vec::with_capacity(column_count);
                        for i in 0..column_count {
                            let value: Option<Vec<u8>> = match row.get_ref(i)? {
                                rusqlite::types::ValueRef::Null => None,
                                rusqlite::types::ValueRef::Integer(int_value) => {
                                    // Check if this column needs datetime conversion
                                    if let Some(datetime_type) = datetime_columns.get(&i) {
                                        match *datetime_type {
                                            "timestamp" => {
                                                let formatted = crate::types::datetime_utils::format_microseconds_to_timestamp(int_value);
                                                Some(formatted.into_bytes())
                                            }
                                            "date" => {
                                                let formatted = crate::types::datetime_utils::format_days_to_date(int_value);
                                                Some(formatted.into_bytes())
                                            }
                                            "time" => {
                                                let formatted = crate::types::datetime_utils::format_microseconds_to_time(int_value);
                                                Some(formatted.into_bytes())
                                            }
                                            _ => Some(int_value.to_string().into_bytes()),
                                        }
                                    } else {
                                        Some(int_value.to_string().into_bytes())
                                    }
                                }
                                rusqlite::types::ValueRef::Real(f) => Some(f.to_string().into_bytes()),
                                rusqlite::types::ValueRef::Text(s) => Some(s.to_vec()),
                                rusqlite::types::ValueRef::Blob(b) => Some(b.to_vec()),
                            };
                            row_data.push(value);
                        }
                        rows.push(row_data);
                    }
                    
                    Ok(DbResponse {
                        columns: column_names,
                        rows,
                        rows_affected: 0,
                    })
                }
                QueryType::Insert | QueryType::Update | QueryType::Delete => {
                    if query.contains("RETURNING") {
                        // Handle RETURNING clause
                        let column_count = stmt.column_count();
                        let mut column_names = Vec::with_capacity(column_count);
                        for i in 0..column_count {
                            column_names.push(stmt.column_name(i).unwrap_or("").to_string());
                        }
                        
                        // Build datetime column info for conversion
                        let mut datetime_columns = std::collections::HashMap::new();
                        
                        // Try to extract table name from query for schema lookup (INSERT/UPDATE/DELETE)
                        let table_name = regex::Regex::new(r"(?i)(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(\w+)").unwrap().captures(query).map(|captures| captures[1].to_string());
                        
                        // Look up column types for datetime conversion
                        if let Some(ref table) = table_name {
                            for (i, column_name) in column_names.iter().enumerate() {
                                // Handle table-prefixed columns like "users.created_at" -> "created_at"
                                let base_column_name = if column_name.contains('.') {
                                    column_name.split('.').next_back().unwrap_or(column_name)
                                } else {
                                    column_name
                                };
                                
                                // Look up schema type
                                let mut schema_stmt = conn.prepare(
                                    "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
                                )?;
                                
                                if let Ok(Some(pg_type)) = schema_stmt.query_row([table, base_column_name], |row| {
                                    row.get::<_, String>(0)
                                }).optional() {
                                    if pg_type == "TIMESTAMP" || pg_type == "TIMESTAMP WITHOUT TIME ZONE" {
                                        datetime_columns.insert(i, "timestamp");
                                    } else if pg_type == "DATE" {
                                        datetime_columns.insert(i, "date");
                                    } else if pg_type == "TIME" || pg_type == "TIME WITHOUT TIME ZONE" {
                                        datetime_columns.insert(i, "time");
                                    }
                                }
                            }
                        }
                        
                        let mut rows = Vec::new();
                        let mut prepared_stmt = stmt.query(rusqlite::params_from_iter(params.iter()))?;
                        let mut changes = 0;
                        
                        while let Some(row) = prepared_stmt.next()? {
                            let mut row_data = Vec::with_capacity(column_count);
                            for i in 0..column_count {
                                let value: Option<Vec<u8>> = match row.get_ref(i)? {
                                    rusqlite::types::ValueRef::Null => None,
                                    rusqlite::types::ValueRef::Integer(int_value) => {
                                        // Check if this column needs datetime conversion
                                        if let Some(datetime_type) = datetime_columns.get(&i) {
                                            match *datetime_type {
                                                "timestamp" => {
                                                    let formatted = crate::types::datetime_utils::format_microseconds_to_timestamp(int_value);
                                                    Some(formatted.into_bytes())
                                                }
                                                "date" => {
                                                    let formatted = crate::types::datetime_utils::format_days_to_date(int_value);
                                                    Some(formatted.into_bytes())
                                                }
                                                "time" => {
                                                    let formatted = crate::types::datetime_utils::format_microseconds_to_time(int_value);
                                                    Some(formatted.into_bytes())
                                                }
                                                _ => Some(int_value.to_string().into_bytes()),
                                            }
                                        } else {
                                            Some(int_value.to_string().into_bytes())
                                        }
                                    }
                                    rusqlite::types::ValueRef::Real(f) => Some(f.to_string().into_bytes()),
                                    rusqlite::types::ValueRef::Text(s) => Some(s.to_vec()),
                                    rusqlite::types::ValueRef::Blob(b) => Some(b.to_vec()),
                                };
                                row_data.push(value);
                            }
                            rows.push(row_data);
                            changes += 1;
                        }
                        
                        Ok(DbResponse {
                            columns: column_names,
                            rows,
                            rows_affected: changes,
                        })
                    } else {
                        // Regular DML without RETURNING
                        let changes = stmt.execute(rusqlite::params_from_iter(params.iter()))?;
                        
                        Ok(DbResponse {
                            columns: vec![],
                            rows: vec![],
                            rows_affected: changes,
                        })
                    }
                }
                _ => {
                    // Unsupported query type, fall back
                    return Ok(None);
                }
            };
            
            Ok(Some(response?))
        })?;
        
        // After a successful DML operation, check if we need to trigger WAL refresh
        // This is needed for autocommit mode where no explicit COMMIT is sent
        if let Some(ref response) = result
            && query_type != QueryType::Select && response.rows_affected > 0 {
                // Check if we're in autocommit mode
                let is_autocommit = self.connection_manager.execute_with_session(session_id, |conn| {
                    let autocommit = conn.is_autocommit();
                    Ok(autocommit)
                })?;
                
                if is_autocommit {
                    debug!("DML operation completed in autocommit mode, triggering WAL refresh for session {}", session_id);
                    self.connection_manager.refresh_all_other_connections(session_id)?;
                }
            }
        
        Ok(result)
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

    /// Store CREATE TABLE metadata including type mappings and numeric constraints
    fn store_create_table_metadata(
        &self,
        conn: &rusqlite::Connection,
        table_name: &str,
        type_mappings: &std::collections::HashMap<String, crate::metadata::TypeMapping>
    ) -> Result<(), rusqlite::Error> {
        debug!("Storing CREATE TABLE metadata: {} type mappings found", type_mappings.len());

        // Initialize the metadata table if it doesn't exist
        let init_query = "CREATE TABLE IF NOT EXISTS __pgsqlite_schema (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            pg_type TEXT NOT NULL,
            sqlite_type TEXT NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )";

        conn.execute(init_query, [])?;

        // Initialize numeric constraints table if it doesn't exist
        let init_numeric_query = "CREATE TABLE IF NOT EXISTS __pgsqlite_numeric_constraints (
            table_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            precision INTEGER NOT NULL,
            scale INTEGER NOT NULL,
            PRIMARY KEY (table_name, column_name)
        )";

        conn.execute(init_numeric_query, [])?;

        // Store each type mapping
        for (full_column, type_mapping) in type_mappings {
            // Split table.column format
            let parts: Vec<&str> = full_column.split('.').collect();
            if parts.len() == 2 && parts[0] == table_name {
                let insert_query = format!(
                    "INSERT OR REPLACE INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES ('{}', '{}', '{}', '{}')",
                    table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type
                );

                if let Err(e) = conn.execute(&insert_query, []) {
                    debug!("Failed to store metadata for {}.{}: {}", table_name, parts[1], e);
                } else {
                    debug!("Stored metadata: {}.{} -> {} ({})", table_name, parts[1], type_mapping.pg_type, type_mapping.sqlite_type);
                }

                // Store numeric constraints if present
                debug!("Checking for numeric constraints: pg_type={}, modifier={:?}", type_mapping.pg_type, type_mapping.type_modifier);
                if let Some(modifier) = type_mapping.type_modifier {
                    // Extract base type without parameters
                    let base_type = if let Some(paren_pos) = type_mapping.pg_type.find('(') {
                        type_mapping.pg_type[..paren_pos].trim()
                    } else {
                        &type_mapping.pg_type
                    };
                    let pg_type_lower = base_type.to_lowercase();

                    if pg_type_lower == "numeric" || pg_type_lower == "decimal" {
                        // Decode precision and scale from modifier
                        let tmp_typmod = modifier - 4; // Remove VARHDRSZ
                        let precision = (tmp_typmod >> 16) & 0xFFFF;
                        let scale = tmp_typmod & 0xFFFF;

                        let constraint_query = format!(
                            "INSERT OR REPLACE INTO __pgsqlite_numeric_constraints (table_name, column_name, precision, scale) VALUES ('{}', '{}', {}, {})",
                            table_name, parts[1], precision, scale
                        );

                        if let Err(e) = conn.execute(&constraint_query, []) {
                            debug!("Failed to store numeric constraint for {}.{}: {}", table_name, parts[1], e);
                        } else {
                            debug!("Stored numeric constraint: {}.{} precision={} scale={}", table_name, parts[1], precision, scale);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Rewrite information_schema queries to use real SQLite views
    fn rewrite_information_schema_query(&self, query: &str) -> String {
        query
            .replace("information_schema.tables", "information_schema_tables")
            .replace("information_schema.columns", "information_schema_columns")
            .replace("information_schema.key_column_usage", "information_schema_key_column_usage")
            .replace("information_schema.table_constraints", "information_schema_table_constraints")
            .replace("information_schema.referential_constraints", "information_schema_referential_constraints")
            .replace("information_schema.schemata", "information_schema_schemata")
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