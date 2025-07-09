use std::sync::Arc;
use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::ast::Statement;
use crate::cache::{SchemaCache, CachedQuery, StatementPool, global_execution_cache, global_type_converter_table, ExecutionMetadata, ExecutionCache};
use crate::cache::{global_result_cache, ResultCacheKey, ResultSetCache};
use crate::cache::schema::TableSchema;
use crate::rewriter::DecimalQueryRewriter;
use crate::types::PgType;
use crate::query::{QueryTypeDetector, QueryType};
use crate::config::Config;
use crate::migration::MigrationRunner;
use crate::validator::StringConstraintValidator;
use tracing::{info, debug};

/// Database response structure
pub struct DbResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub rows_affected: usize,
}

/// Thread-safe database handler using a Mutex-protected SQLite connection
/// 
/// This implementation was chosen after extensive benchmarking showed it provides
/// the best performance characteristics:
/// - ~7.7-9.6x overhead vs raw SQLite (compared to ~20-27x for channel-based)
/// - 2.2-3.5x better performance than channel-based approach
/// - Simpler than connection pooling with nearly identical performance
/// - Thread-safe through parking_lot::Mutex and SQLite's FULLMUTEX mode
pub struct DbHandler {
    conn: Arc<Mutex<Connection>>,
    schema_cache: Arc<SchemaCache>,
    string_validator: Arc<StringConstraintValidator>,
}

impl DbHandler {
    pub fn new(db_path: &str) -> Result<Self, rusqlite::Error> {
        Self::new_with_config(db_path, &Config::load())
    }
    
    #[doc(hidden)]
    pub fn new_for_test(db_path: &str) -> Result<Self, rusqlite::Error> {
        // For tests, create an in-memory database and run migrations automatically
        let config = Config::load();
        
        // Use FULLMUTEX for SQLite's internal thread safety
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE 
            | OpenFlags::SQLITE_OPEN_CREATE 
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
        
        let conn = if db_path == ":memory:" {
            Connection::open_with_flags("file::memory:?cache=private", flags)?
        } else {
            Connection::open_with_flags(db_path, flags)?
        };
        
        // Set pragmas for performance
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
        
        // For tests, run migrations automatically
        let mut runner = MigrationRunner::new(conn);
        match runner.run_pending_migrations() {
            Ok(_) => {
                // Migrations applied successfully
            }
            Err(e) => {
                return Err(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                    Some(format!("Test migration failed: {}", e))
                ));
            }
        }
        
        // Get connection back from runner
        let conn = runner.into_connection();
        
        // Initialize functions and metadata
        crate::functions::register_all_functions(&conn)?;
        crate::metadata::TypeMetadata::init(&conn)?;
        
        info!("Test DbHandler initialized with mutex-based implementation");
        
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            schema_cache: Arc::new(SchemaCache::new(config.schema_cache_ttl)),
            string_validator: Arc::new(StringConstraintValidator::new()),
        })
    }
    
    pub fn new_with_config(db_path: &str, config: &Config) -> Result<Self, rusqlite::Error> {
        // Use FULLMUTEX for SQLite's internal thread safety
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE 
            | OpenFlags::SQLITE_OPEN_CREATE 
            | OpenFlags::SQLITE_OPEN_FULL_MUTEX
            | OpenFlags::SQLITE_OPEN_URI;
        
        let conn = if db_path == ":memory:" {
            // Use a unique in-memory database for each instance to avoid test interference
            // The cache=private ensures each connection gets its own database
            Connection::open_with_flags("file::memory:?cache=private", flags)?
        } else if db_path.starts_with("file:") && db_path.contains(":memory:") {
            // Allow explicit file::memory: URIs with custom parameters
            Connection::open_with_flags(db_path, flags)?
        } else {
            Connection::open_with_flags(db_path, flags)?
        };
        
        // Set pragmas for performance using config values
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
        
        // Check if we're using an in-memory database
        let is_memory_db = db_path == ":memory:" || db_path.contains(":memory:");
        
        // For in-memory databases, always run migrations automatically
        // since they always start fresh and have no existing data
        if is_memory_db {
            // Register functions before running migrations
            crate::functions::register_all_functions(&conn)?;
            
            // For in-memory databases, run migrations automatically
            let mut runner = MigrationRunner::new(conn);
            match runner.run_pending_migrations() {
                Ok(applied) => {
                    if !applied.is_empty() {
                        info!("Applied {} migrations to in-memory database", applied.len());
                    }
                }
                Err(e) => {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("In-memory database migration failed: {}", e))
                    ));
                }
            }
            let conn = runner.into_connection();
            
            // Initialize functions and metadata
            crate::functions::register_all_functions(&conn)?;
            crate::metadata::TypeMetadata::init(&conn)?;
            
            info!("In-memory database initialized with migrations");
            
            return Ok(Self {
                conn: Arc::new(Mutex::new(conn)),
                schema_cache: Arc::new(SchemaCache::new(config.schema_cache_ttl)),
                string_validator: Arc::new(StringConstraintValidator::new()),
            });
        }
        
        // Check if this is a new database file by looking for any tables
        let table_count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table'",
            [],
            |row| row.get(0)
        )?;
        
        let is_new_database = table_count == 0;
        
        let conn = if is_new_database {
            // New database file - run migrations automatically
            info!("New database file detected, running initial migrations...");
            
            // Register functions before running migrations
            crate::functions::register_all_functions(&conn)?;
            
            let mut runner = MigrationRunner::new(conn);
            match runner.run_pending_migrations() {
                Ok(applied) => {
                    if !applied.is_empty() {
                        info!("Applied {} migrations to new database", applied.len());
                    }
                }
                Err(e) => {
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(format!("Initial migration failed for new database: {}", e))
                    ));
                }
            }
            // Get connection back from runner
            runner.into_connection()
        } else {
            // Existing database - check schema version
            // IMPORTANT: Check schema version on database load
            // This ensures the database schema is up-to-date before any operations
            let runner = MigrationRunner::new(conn);
            match runner.check_schema_version() {
                Ok(()) => {
                    // Schema is up to date
                }
                Err(e) => {
                    // Convert anyhow::Error to rusqlite::Error
                    return Err(rusqlite::Error::SqliteFailure(
                        rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                        Some(e.to_string())
                    ));
                }
            }
            
            // Get connection back from runner
            let conn = runner.into_connection();
            
            // Check for schema drift
            match crate::schema_drift::SchemaDriftDetector::detect_drift(&conn) {
                Ok(drift) => {
                    if !drift.is_empty() {
                        let report = drift.format_report();
                        return Err(rusqlite::Error::SqliteFailure(
                            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                            Some(format!("Schema drift detected:\n{}\n\nTo fix this, ensure your SQLite schema matches the pgsqlite metadata.", report))
                        ));
                    }
                }
                Err(e) => {
                    // Log warning but don't fail - drift detection is not critical
                    debug!("Schema drift detection failed: {}", e);
                }
            }
            
            conn
        };
        
        // Initialize functions and metadata
        crate::functions::register_all_functions(&conn)?;
        crate::metadata::TypeMetadata::init(&conn)?;
        
        info!("DbHandler initialized with mutex-based implementation");
        
        // Populate string constraints from schema if migration v6 has been applied
        let string_validator = Arc::new(StringConstraintValidator::new());
        let _ = StringConstraintValidator::populate_constraints_from_schema(&conn);
        
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            schema_cache: Arc::new(SchemaCache::new(config.schema_cache_ttl)),
            string_validator,
        })
    }
    
    pub async fn execute(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        // Ultra-fast path for truly simple queries
        if crate::query::simple_query_detector::is_ultra_simple_query(query) {
            let conn = self.conn.lock();
            
            // Try direct fast path execution
            if let Ok(Some(rows_affected)) = crate::query::execute_fast_path_enhanced(&conn, query, &self.schema_cache) {
                return Ok(DbResponse {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected,
                });
            }
            
            // Fall back to direct execution
            let rows_affected = conn.execute(query, [])?;
            return Ok(DbResponse {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected,
            });
        }
        
        // Check if DDL to clear cache
        if is_ddl_statement(query) {
            self.schema_cache.clear();
            // Clear global query cache
            crate::session::GLOBAL_QUERY_CACHE.clear();
            // Clear decimal table cache
            crate::query::clear_decimal_cache();
            // Clear statement pool
            StatementPool::global().clear();
            // Clear result cache
            global_result_cache().clear();
            // Clear ENUM cache
            crate::cache::global_enum_cache().clear();
            // Clear translation cache
            crate::cache::global_translation_cache().clear();
        }
        
        let conn = self.conn.lock();
        
        // Create lazy processor
        let mut processor = crate::query::LazyQueryProcessor::new(query);
        
        // Check if we need any processing
        if !processor.needs_processing(&self.schema_cache) {
            // Fast path - no processing needed
            let query_to_execute = processor.get_unprocessed();
            
            // Try enhanced fast path first
            if let Ok(Some(rows_affected)) = crate::query::execute_fast_path_enhanced(&conn, query_to_execute, &self.schema_cache) {
                return Ok(DbResponse {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected,
                });
            }
        }
        
        // Process the query if needed
        let query_to_execute = processor.process(&conn, &self.schema_cache)?;
        
        // For INSERT queries, try statement pool for better performance
        if matches!(QueryTypeDetector::detect_query_type(query_to_execute), QueryType::Insert) {
            // First check if we can use fast path with statement pool
            if let Some(table_name) = extract_insert_table_name(query_to_execute) {
                if !self.schema_cache.has_decimal_columns(&table_name) {
                    // No decimal columns, use statement pool for optimal performance
                    match StatementPool::global().execute_cached(&conn, query_to_execute, []) {
                        Ok(rows_affected) => {
                            return Ok(DbResponse {
                                columns: Vec::new(),
                                rows: Vec::new(),
                                rows_affected,
                            });
                        }
                        Err(e) => {
                            // Log error but continue to fallback
                            debug!("Statement pool execution failed: {}", e);
                        }
                    }
                }
            }
            
            // Check INSERT query cache to avoid re-parsing
            if let Some(cached) = crate::session::GLOBAL_QUERY_CACHE.get(query_to_execute) {
                // Use cached rewritten query if available
                let final_query = cached.rewritten_query.as_ref().unwrap_or(&cached.normalized_query);
                match conn.execute(final_query, []) {
                    Ok(rows_affected) => return Ok(DbResponse {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        rows_affected,
                    }),
                    Err(e) => {
                        // Convert SQLite CHECK constraint errors to PostgreSQL-compatible enum errors
                        return Err(convert_enum_error(e, query_to_execute));
                    }
                }
            }
        }
        
        // Fall back to normal execution - but skip decimal rewriting since processor already did it
        match conn.execute(query_to_execute, []) {
            Ok(rows_affected) => Ok(DbResponse {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected,
            }),
            Err(e) => {
                // Convert SQLite CHECK constraint errors to PostgreSQL-compatible enum errors
                Err(convert_enum_error(e, query_to_execute))
            }
        }
    }
    
    pub async fn query(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        // Ensure schema cache is populated (especially after CREATE TABLE)
        self.schema_cache.ensure_schema_loaded(&self.conn.lock(), query);
        
        // Ultra-fast path for truly simple queries
        if crate::query::simple_query_detector::is_ultra_simple_query(query) {
            let conn = self.conn.lock();
            
            // Try direct fast path execution
            if let Ok(Some(response)) = crate::query::query_fast_path_enhanced(&conn, query, &self.schema_cache) {
                return Ok(response);
            }
            
            // Fall back to regular execution without any processing
            return execute_query_optimized(&conn, query, &self.schema_cache);
        }
        
        // Create lazy processor for the query
        let mut processor = crate::query::LazyQueryProcessor::new(query);
        
        // Check result cache first with original query
        let cache_key = ResultCacheKey::new(processor.cache_key(), &[]);
        let cached_result = if crate::profiling::is_profiling_enabled() {
            crate::time_cache_lookup!({
                let result = global_result_cache().get(&cache_key);
                if result.is_some() {
                    crate::profiling::METRICS.cache_hit_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                result
            })
        } else {
            global_result_cache().get(&cache_key)
        };
        
        if let Some(cached_result) = cached_result {
            debug!("Result cache hit for query: {}", query);
            return Ok(DbResponse {
                columns: cached_result.columns,
                rows: cached_result.rows,
                rows_affected: cached_result.rows_affected as usize,
            });
        }
        
        let conn = self.conn.lock();
        
        // Check if we need any processing at all
        if !processor.needs_processing(&self.schema_cache) {
            // Fast path - no processing needed, use original query
            let query_to_execute = processor.get_unprocessed();
            
            if crate::profiling::is_profiling_enabled() {
                crate::profiling::METRICS.fast_path_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            
            // Try enhanced fast path first
            if let Ok(Some(response)) = crate::query::query_fast_path_enhanced(&conn, query_to_execute, &self.schema_cache) {
                if crate::profiling::is_profiling_enabled() {
                    crate::profiling::METRICS.fast_path_success.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                let execution_time_us = 0; // Fast path doesn't track time
                
                // Cache the result
                if ResultSetCache::should_cache(query_to_execute, execution_time_us, response.rows.len()) {
                    global_result_cache().insert(
                        cache_key,
                        response.columns.clone(),
                        response.rows.clone(),
                        response.rows_affected as u64,
                        execution_time_us,
                    );
                }
                
                return Ok(response);
            }
        }
        
        let start = std::time::Instant::now();
        
        // Process the query (lazy - only does work if needed)
        let query_to_execute = processor.process(&conn, &self.schema_cache)?;
        
        // Check cache again with processed query if it changed
        let final_cache_key = if query_to_execute != query {
            let new_key = ResultCacheKey::new(query_to_execute, &[]);
            if let Some(cached_result) = global_result_cache().get(&new_key) {
                debug!("Result cache hit for processed query: {}", query_to_execute);
                return Ok(DbResponse {
                    columns: cached_result.columns,
                    rows: cached_result.rows,
                    rows_affected: cached_result.rows_affected as usize,
                });
            }
            new_key
        } else {
            cache_key
        };
        
        // Try enhanced fast path with processed query
        if let Ok(Some(response)) = crate::query::query_fast_path_enhanced(&conn, query_to_execute, &self.schema_cache) {
            let execution_time_us = start.elapsed().as_micros() as u64;
            
            // Cache the result
            if ResultSetCache::should_cache(query_to_execute, execution_time_us, response.rows.len()) {
                global_result_cache().insert(
                    final_cache_key,
                    response.columns.clone(),
                    response.rows.clone(),
                    response.rows_affected as u64,
                    execution_time_us,
                );
            }
            
            return Ok(response);
        }
        
        // Fall back to normal query execution
        let response = execute_query_optimized(&conn, query_to_execute, &self.schema_cache)?;
        let execution_time_us = start.elapsed().as_micros() as u64;
        
        // Cache the result
        if ResultSetCache::should_cache(query_to_execute, execution_time_us, response.rows.len()) {
            global_result_cache().insert(
                final_cache_key,
                response.columns.clone(),
                response.rows.clone(),
                response.rows_affected as u64,
                execution_time_us,
            );
        }
        
        Ok(response)
    }
    
    pub async fn get_schema_type(&self, table_name: &str, column_name: &str) -> Result<Option<String>, rusqlite::Error> {
        let conn = self.conn.lock();
        
        let mut stmt = conn.prepare(
            "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
        )?;
        
        let result = stmt.query_row([table_name, column_name], |row| {
            row.get::<_, String>(0)
        }).optional()?;
        
        Ok(result)
    }
    
    pub async fn get_table_schema(&self, table_name: &str) -> Result<TableSchema, rusqlite::Error> {
        let conn = self.conn.lock();
        
        // Use enhanced schema cache with automatic preloading
        self.schema_cache.get_or_load(&conn, table_name)
    }
    
    /// Begin transaction
    pub async fn begin(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock();
        conn.execute("BEGIN", [])?;
        Ok(())
    }
    
    /// Commit transaction  
    pub async fn commit(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock();
        conn.execute("COMMIT", [])?;
        Ok(())
    }
    
    /// Rollback transaction
    pub async fn rollback(&self) -> Result<(), rusqlite::Error> {
        let conn = self.conn.lock();
        conn.execute("ROLLBACK", [])?;
        Ok(())
    }
    
    /// Get a mutable connection for operations that require &mut Connection
    pub fn get_mut_connection(&self) -> Result<parking_lot::MutexGuard<Connection>, rusqlite::Error> {
        Ok(self.conn.lock())
    }
    
    /// Try executing a query with parameters using the fast path
    pub async fn try_execute_fast_path_with_params(
        &self, 
        query: &str, 
        params: &[rusqlite::types::Value]
    ) -> Result<Option<DbResponse>, rusqlite::Error> {
        let conn = self.conn.lock();
        
        // Try fast path for DML operations
        if let Ok(Some(rows_affected)) = crate::query::execute_fast_path_enhanced_with_params(&conn, query, params, &self.schema_cache) {
            return Ok(Some(DbResponse {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected,
            }));
        }
        
        // Try fast path for SELECT operations
        if let Ok(Some(response)) = crate::query::query_fast_path_enhanced_with_params(&conn, query, params, &self.schema_cache) {
            return Ok(Some(response));
        }
        
        Ok(None)
    }
    
    /// Execute a query using the statement pool for optimization
    pub async fn execute_with_statement_pool(
        &self,
        query: &str
    ) -> Result<DbResponse, rusqlite::Error> {
        let conn = self.conn.lock();
        
        // Use statement pool for execution
        let rows_affected = StatementPool::global().execute_cached(&conn, query, [])?;
        
        Ok(DbResponse {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected,
        })
    }
    
    /// Query using the statement pool for optimization  
    pub async fn query_with_statement_pool(
        &self,
        query: &str
    ) -> Result<DbResponse, rusqlite::Error> {
        let conn = self.conn.lock();
        
        // Use statement pool for querying
        let (columns, rows) = StatementPool::global().query_cached(&conn, query, [])?;
        let rows_affected = rows.len();
        
        Ok(DbResponse {
            columns,
            rows,
            rows_affected,
        })
    }
    
    /// Execute a parameterized query using the statement pool
    pub async fn execute_with_statement_pool_params(
        &self,
        query: &str,
        params: &[rusqlite::types::Value]
    ) -> Result<DbResponse, rusqlite::Error> {
        let conn = self.conn.lock();
        
        // Use statement pool for execution with parameters
        let rows_affected = StatementPool::global().execute_cached(
            &conn, 
            query, 
            rusqlite::params_from_iter(params.iter())
        )?;
        
        Ok(DbResponse {
            columns: Vec::new(),
            rows: Vec::new(),
            rows_affected,
        })
    }
    
    /// Query with parameters using the statement pool
    pub async fn query_with_statement_pool_params(
        &self,
        query: &str,
        params: &[rusqlite::types::Value]
    ) -> Result<DbResponse, rusqlite::Error> {
        let conn = self.conn.lock();
        
        // Use statement pool for querying with parameters
        let (columns, rows) = StatementPool::global().query_cached(
            &conn, 
            query, 
            rusqlite::params_from_iter(params.iter())
        )?;
        let rows_affected = rows.len();
        
        Ok(DbResponse {
            columns,
            rows,
            rows_affected,
        })
    }
    
    /// Get reference to the string validator
    pub fn string_validator(&self) -> &StringConstraintValidator {
        &self.string_validator
    }
    
    /// Shutdown (no-op for mutex handler)
    pub async fn shutdown(&self) {
        // Nothing to do
    }
}

impl Clone for DbHandler {
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            schema_cache: self.schema_cache.clone(),
            string_validator: self.string_validator.clone(),
        }
    }
}

// Helper functions (previously in db_executor.rs)

pub fn is_ddl_statement(query: &str) -> bool {
    QueryTypeDetector::is_ddl(query)
}

pub fn execute_dml_sync(
    conn: &Connection,
    query: &str,
    schema_cache: &SchemaCache,
) -> Result<DbResponse, rusqlite::Error> {
    // Fast path: For simple INSERT statements without decimals, execute directly
    if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Insert) {
        if let Some(table_name) = extract_insert_table_name(query) {
            // Check if table has decimal columns using bloom filter
            if !schema_cache.has_decimal_columns(&table_name) {
                // No decimal columns, execute directly without parsing
                let rows_affected = conn.execute(query, [])?;
                return Ok(DbResponse {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected,
                });
            }
        }
    }
    
    // Fall back to decimal rewriting for other cases
    let rewritten_query = rewrite_query_for_decimal(query, conn)?;
    let rows_affected = conn.execute(&rewritten_query, [])?;
    
    Ok(DbResponse {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected,
    })
}

/// Ultra-fast execution path using execution cache
pub fn execute_query_optimized(
    conn: &Connection,
    query: &str,
    schema_cache: &SchemaCache,
) -> Result<DbResponse, rusqlite::Error> {
    // Generate cache key
    let cache_key = ExecutionCache::generate_key(query, &[]);
    
    // Try execution cache first
    if let Some(metadata) = global_execution_cache().get(&cache_key) {
        return execute_with_cached_metadata(conn, &metadata);
    }
    
    // Cache miss - analyze and build metadata
    let metadata = build_execution_metadata(conn, query, schema_cache)?;
    
    // Cache for future use
    global_execution_cache().insert(cache_key, metadata.clone());
    
    // Execute with new metadata
    execute_with_cached_metadata(conn, &metadata)
}

/// Execute query using pre-computed execution metadata with batch processing
fn execute_with_cached_metadata(
    conn: &Connection,
    metadata: &ExecutionMetadata,
) -> Result<DbResponse, rusqlite::Error> {
    // Use prepared statement for execution
    let mut stmt = if crate::profiling::is_profiling_enabled() {
        crate::time_sqlite_prepare!({
            conn.prepare(&metadata.prepared_sql)?
        })
    } else {
        conn.prepare(&metadata.prepared_sql)?
    };
    
    // Batch processing optimization: Process rows in chunks for better cache locality
    const BATCH_SIZE: usize = 100;
    let mut rows = Vec::new();
    
    // Pre-allocate conversion buffers for better performance
    let type_converter_table = global_type_converter_table();
    let num_columns = metadata.type_converters.len();
    
    let query_result = stmt.query_map([], |row| {
        // Pre-allocate row data vector
        let mut row_data = Vec::with_capacity(num_columns);
        
        // Optimized row processing with minimal allocations
        for (col_idx, &converter_idx) in metadata.type_converters.iter().enumerate() {
            let value = row.get_ref(col_idx)?;
            
            // Fast path for common value types to avoid allocation
            // Handle NULL values explicitly
            if matches!(value, rusqlite::types::ValueRef::Null) {
                row_data.push(None);
                continue;
            }
            
            // Check if binary encoding is requested for this column
            let converted = if col_idx < metadata.result_formats.len() && metadata.result_formats[col_idx] == 1 {
                // Binary encoding requested
                let type_oid = if col_idx < metadata.type_oids.len() {
                    metadata.type_oids[col_idx]
                } else {
                    25 // Default to TEXT
                };
                
                let owned_value: rusqlite::types::Value = match value {
                    rusqlite::types::ValueRef::Integer(i) => rusqlite::types::Value::Integer(i),
                    rusqlite::types::ValueRef::Real(r) => rusqlite::types::Value::Real(r),
                    rusqlite::types::ValueRef::Text(t) => rusqlite::types::Value::Text(String::from_utf8_lossy(t).to_string()),
                    rusqlite::types::ValueRef::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
                    _ => unreachable!("NULL already handled above"),
                };
                
                type_converter_table.convert_binary(&owned_value, type_oid)?
            } else {
                // Text encoding (existing logic)
                match (value, converter_idx) {
                    // Fast boolean conversion (most common case)
                    (rusqlite::types::ValueRef::Integer(i), 2) => {
                        if i == 0 { b"f".to_vec() } else { b"t".to_vec() }
                    },
                    // Fast text conversion for strings
                    (rusqlite::types::ValueRef::Text(t), 0) => t.to_vec(),
                    // Fast integer conversion
                    (rusqlite::types::ValueRef::Integer(i), 1) => i.to_string().into_bytes(),
                    // Fast date conversion (INTEGER days -> YYYY-MM-DD)
                    (rusqlite::types::ValueRef::Integer(days), 6) => {
                        use crate::types::datetime_utils::format_days_to_date_buf;
                        let mut buf = vec![0u8; 32];
                        let len = format_days_to_date_buf(days as i32, &mut buf);
                        buf.truncate(len);
                        buf
                    },
                    // Fast time conversion (INTEGER microseconds -> HH:MM:SS.ffffff)
                    (rusqlite::types::ValueRef::Integer(micros), 7) => {
                        use crate::types::datetime_utils::format_microseconds_to_time_buf;
                        let mut buf = vec![0u8; 32];
                        let len = format_microseconds_to_time_buf(micros, &mut buf);
                        buf.truncate(len);
                        buf
                    },
                    // Fast timestamp conversion (INTEGER microseconds -> YYYY-MM-DD HH:MM:SS.ffffff)
                    (rusqlite::types::ValueRef::Integer(micros), 8) => {
                        use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                        let mut buf = vec![0u8; 64];
                        let len = format_microseconds_to_timestamp_buf(micros, &mut buf);
                        buf.truncate(len);
                        buf
                    },
                    // Fallback to generic converter for complex cases
                    _ => {
                        let owned_value: rusqlite::types::Value = match value {
                            rusqlite::types::ValueRef::Integer(i) => rusqlite::types::Value::Integer(i),
                            rusqlite::types::ValueRef::Real(r) => rusqlite::types::Value::Real(r),
                            rusqlite::types::ValueRef::Text(t) => rusqlite::types::Value::Text(String::from_utf8_lossy(t).to_string()),
                            rusqlite::types::ValueRef::Blob(b) => rusqlite::types::Value::Blob(b.to_vec()),
                            _ => unreachable!("NULL already handled above"),
                        };
                        type_converter_table.convert(converter_idx, &owned_value)?
                    }
                }
            };
            
            // For non-NULL values, empty vectors are valid (e.g., empty strings)
            row_data.push(Some(converted));
        }
        
        Ok(row_data)
    })?;

    // Collect rows with batch processing for better memory efficiency
    if crate::profiling::is_profiling_enabled() {
        crate::time_result_format!({
            for row in query_result {
                rows.push(row?);
                
                // Process in batches for better cache performance (though we collect all here)
                if rows.len() % BATCH_SIZE == 0 && !rows.is_empty() {
                    // Reserve capacity for next batch
                    rows.reserve(BATCH_SIZE);
                }
            }
        });
    } else {
        for row in query_result {
            rows.push(row?);
            
            // Process in batches for better cache performance (though we collect all here)
            if rows.len() % BATCH_SIZE == 0 && !rows.is_empty() {
                // Reserve capacity for next batch
                rows.reserve(BATCH_SIZE);
            }
        }
    }

    Ok(DbResponse {
        columns: metadata.columns.clone(),
        rows,
        rows_affected: 0,
    })
}

/// Build execution metadata for a query
fn build_execution_metadata(
    conn: &Connection, 
    query: &str, 
    schema_cache: &SchemaCache
) -> Result<ExecutionMetadata, rusqlite::Error> {
    // Check if query can use fast path
    let fast_path_eligible = is_fast_path_query(query);
    
    // Prepare the query to get column information
    let prepared_sql = if needs_decimal_rewriting(query, schema_cache) {
        rewrite_query_for_decimal(query, conn)?
    } else {
        query.to_string()
    };
    
    let stmt = conn.prepare(&prepared_sql)?;
    let column_count = stmt.column_count();
    
    // Extract column information
    let mut columns = Vec::new();
    let mut boolean_columns = Vec::new();
    let mut type_converters = Vec::new();
    
    for i in 0..column_count {
        let col_name = stmt.column_name(i).unwrap_or("").to_string();
        columns.push(col_name.clone());
        
        // Determine if this is a boolean column
        let is_boolean = is_boolean_column(&col_name, query, schema_cache);
        boolean_columns.push(is_boolean);
        
        // Select appropriate type converter based on column type
        let converter_idx = if is_boolean {
            2 // Boolean converter
        } else {
            // Try to infer type from column name and schema
            let col_type = infer_column_type(&col_name, query, schema_cache);
            match col_type.to_lowercase().as_str() {
                "integer" | "int4" | "int8" | "int2" | "bigint" | "smallint" => 1, // Integer converter
                "real" | "float4" | "float8" | "double" | "numeric" => 3, // Float converter  
                "bytea" | "blob" => 4, // Blob converter
                "date" => 6, // Date converter (INTEGER days -> YYYY-MM-DD)
                "time" | "timetz" | "time without time zone" | "time with time zone" => 7, // Time converter
                "timestamp" | "timestamptz" | "timestamp without time zone" | "timestamp with time zone" => 8, // Timestamp converter
                _ => 0, // Text converter (default)
            }
        };
        type_converters.push(converter_idx);
    }
    
    let column_count = columns.len();
    Ok(ExecutionMetadata {
        columns,
        boolean_columns,
        type_converters,
        type_oids: vec![PgType::Text.to_oid(); column_count], // Default to TEXT, will be populated later
        result_formats: vec![], // Will be populated from Portal when executing
        fast_path_eligible,
        prepared_sql,
        param_count: 0, // TODO: Count parameters
    })
}

/// Check if a query needs decimal rewriting
fn needs_decimal_rewriting(query: &str, schema_cache: &SchemaCache) -> bool {
    // Quick check for decimal operations
    if !query.contains('+') && !query.contains('-') && !query.contains('*') && !query.contains('/') {
        return false;
    }
    
    // Extract table names and check if any have decimal columns
    if let Ok(table_names) = extract_table_names_simple(query) {
        for table_name in table_names {
            if schema_cache.has_decimal_columns(&table_name) {
                return true;
            }
        }
    }
    
    false
}

/// Simple table name extraction without full parsing
fn extract_table_names_simple(query: &str) -> Result<Vec<String>, ()> {
    let mut table_names = Vec::new();
    
    // Look for "FROM table_name" patterns - use case-insensitive search
    let from_pos = query.as_bytes().windows(6)
        .position(|window| window.eq_ignore_ascii_case(b" FROM "))
        .ok_or(())?;
    if from_pos > 0 {
        let after_from = &query[from_pos + 6..];
        if let Some(next_space) = after_from.find(' ') {
            let table_name = after_from[..next_space].trim().to_string();
            table_names.push(table_name);
        } else {
            let table_name = after_from.trim().to_string();
            table_names.push(table_name);
        }
    }
    
    if table_names.is_empty() {
        Err(())
    } else {
        Ok(table_names)
    }
}

/// Check if this is a fast path eligible query
fn is_fast_path_query(query: &str) -> bool {
    use crate::query::{QueryTypeDetector, QueryType};
    
    let trimmed = query.trim();
    let query_bytes = trimmed.as_bytes();
    
    // Only SELECT queries can use fast path
    matches!(QueryTypeDetector::detect_query_type(trimmed), QueryType::Select) &&
    !query_bytes.windows(4).any(|w| w.eq_ignore_ascii_case(b"JOIN")) &&
    !query_bytes.windows(5).any(|w| w.eq_ignore_ascii_case(b"UNION")) &&
    !query_bytes.windows(8).any(|w| w.eq_ignore_ascii_case(b"SUBQUERY")) &&
    !query_bytes.windows(8).any(|w| w.eq_ignore_ascii_case(b"GROUP BY")) &&
    !query_bytes.windows(8).any(|w| w.eq_ignore_ascii_case(b"ORDER BY")) &&
    !query_bytes.windows(6).any(|w| w.eq_ignore_ascii_case(b"HAVING"))
}

/// Check if a column is a boolean type
fn is_boolean_column(col_name: &str, query: &str, schema_cache: &SchemaCache) -> bool {
    // Try to extract table name from query
    if let Ok(table_names) = extract_table_names_simple(query) {
        for table_name in table_names {
            if let Some(schema) = schema_cache.get(&table_name) {
                if let Some(col_info) = schema.column_map.get(&col_name.to_lowercase()) {
                    return col_info.pg_type.to_lowercase() == "boolean" || col_info.pg_oid == PgType::Bool.to_oid();
                }
            }
        }
    }
    false
}

/// Infer the column type for optimized conversion
fn infer_column_type(col_name: &str, query: &str, schema_cache: &SchemaCache) -> String {
    // Check if this is a datetime function expression
    let lower_col_name = col_name.to_lowercase();
    
    // Check for NOW() and CURRENT_TIMESTAMP variations
    if lower_col_name == "now()" || lower_col_name == "now" ||
       lower_col_name == "current_timestamp()" || lower_col_name == "current_timestamp" {
        return "timestamptz".to_string();
    }
    
    // Also check if the query contains these functions and the column might be aliased
    let lower_query = query.to_lowercase();
    if lower_query.contains("now()") || lower_query.contains("current_timestamp") {
        // If the query contains these functions, and this looks like it could be the result column
        // (not a table column), assume it's a timestamp
        if !lower_col_name.contains('.') {
            // Check if this column exists in any known table
            let mut is_table_column = false;
            if let Ok(table_names) = extract_table_names_simple(query) {
                for table_name in table_names {
                    if let Some(schema) = schema_cache.get(&table_name) {
                        if schema.column_map.contains_key(&lower_col_name) {
                            is_table_column = true;
                            break;
                        }
                    }
                }
            }
            if !is_table_column {
                return "timestamptz".to_string();
            }
        }
    }
    
    // Also check for current_time() and current_date()
    if lower_col_name == "current_time()" || lower_col_name == "current_time" {
        return "timetz".to_string();
    }
    if lower_col_name == "current_date()" || lower_col_name == "current_date" {
        return "date".to_string();
    }
    
    // Try to extract table name from query and get column type
    if let Ok(table_names) = extract_table_names_simple(query) {
        for table_name in table_names {
            if let Some(schema) = schema_cache.get(&table_name) {
                if let Some(col_info) = schema.column_map.get(&col_name.to_lowercase()) {
                    return col_info.pg_type.clone();
                }
            } else {
            }
        }
    }
    
    // Fallback to text type
    "text".to_string()
}

pub fn execute_query_sync(
    conn: &Connection,
    query: &str,
    schema_cache: &SchemaCache,
) -> Result<DbResponse, rusqlite::Error> {
    // Translate PostgreSQL cast syntax if present
    let translated_query = if query.contains("::") || query.to_uppercase().contains("CAST") {
        use crate::translator::CastTranslator;
        CastTranslator::translate_query(query, Some(conn))
    } else {
        query.to_string()
    };
    
    let query_to_execute = translated_query.as_str();
    
    // Try ultra-fast execution path first
    match execute_query_optimized(conn, query_to_execute, schema_cache) {
        Ok(result) => return Ok(result),
        Err(_) => {
            // Fall back to original path if optimized path fails
            debug!("Optimized execution failed, falling back to original path for: {}", query_to_execute);
        }
    }
    
    // Check global query cache first
    if let Some(cached) = crate::session::GLOBAL_QUERY_CACHE.get(query_to_execute) {
        // Use cached rewritten query if available
        let final_query = cached.rewritten_query.as_ref().unwrap_or(&cached.normalized_query);
        debug!("Query cache HIT for: {}", query_to_execute);
        
        // Log metrics periodically (every 100 queries)
        let metrics = crate::session::GLOBAL_QUERY_CACHE.get_metrics();
        if metrics.total_queries % 100 == 0 {
            info!(
                "Query cache metrics - Total: {}, Hits: {}, Hit Rate: {:.1}%, Evictions: {}",
                metrics.total_queries,
                metrics.cache_hits,
                (metrics.cache_hits as f64 / metrics.total_queries as f64) * 100.0,
                metrics.evictions
            );
        }
        
        // For cached queries, try to use statement pool for better performance
        return execute_cached_query_with_statement_pool(conn, query_to_execute, &cached, final_query);
    }
    
    debug!("Query cache MISS for: {}", query_to_execute);
    
    // Parse and rewrite query for DECIMAL types if needed
    let (rewritten_query, parsed_info) = parse_and_rewrite_query(query_to_execute, conn, schema_cache)?;
    
    // Cache the parsed query for future use
    let cached_query = CachedQuery {
        statement: parsed_info.statement,
        param_types: Vec::new(), // Will be filled for extended protocol
        is_decimal_query: parsed_info.is_decimal_query,
        table_names: parsed_info.table_names,
        column_types: parsed_info.column_types,
        has_decimal_columns: parsed_info.has_decimal_columns,
        rewritten_query: if parsed_info.is_decimal_query && rewritten_query != query_to_execute {
            Some(rewritten_query.clone())
        } else {
            None
        },
        normalized_query: crate::cache::QueryCache::normalize_query(query_to_execute),
    };
    
    // Insert into global cache using the translated query as key
    crate::session::GLOBAL_QUERY_CACHE.insert(query_to_execute.to_string(), cached_query.clone());
    debug!(
        "Cached query - Tables: {:?}, Decimal: {}, Column types: {}",
        cached_query.table_names,
        cached_query.has_decimal_columns,
        cached_query.column_types.len()
    );
    
    // Execute using cached information and statement pool
    execute_cached_query_with_statement_pool(conn, query_to_execute, &cached_query, &rewritten_query)
}


/// Extract table name from INSERT statement
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
pub fn rewrite_query_for_decimal(query: &str, conn: &Connection) -> Result<String, rusqlite::Error> {
    // Parse the SQL statement
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, query)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    
    if statements.is_empty() {
        return Ok(query.to_string());
    }
    
    // Rewrite the first statement for decimal handling
    let mut rewriter = DecimalQueryRewriter::new(conn);
    if let Err(e) = rewriter.rewrite_statement(&mut statements[0]) {
        // If rewriting fails, log and return original query
        tracing::warn!("Failed to rewrite query for decimal: {}", e);
        return Ok(query.to_string());
    }
    
    let rewritten = statements[0].to_string();
    tracing::debug!("Decimal rewriter output: {}", rewritten);
    Ok(rewritten)
}

struct ParsedQueryInfo {
    statement: Statement,
    table_names: Vec<String>,
    column_types: Vec<(String, PgType)>,
    has_decimal_columns: bool,
    is_decimal_query: bool,
}

/// Parse and rewrite query, returning rewritten query and parsed info
fn parse_and_rewrite_query(query: &str, conn: &Connection, schema_cache: &SchemaCache) -> Result<(String, ParsedQueryInfo), rusqlite::Error> {
    // Parse the SQL statement
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, query)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    
    if statements.is_empty() {
        return Err(rusqlite::Error::InvalidQuery);
    }
    
    let statement = statements.into_iter().next().unwrap();
    
    // Extract table names
    let table_names = extract_table_names_from_statement(&statement);
    
    // Get column types for the tables using enhanced schema cache
    let mut column_types = Vec::new();
    let mut has_decimal_columns = false;
    
    for table_name in &table_names {
        // Use the enhanced cache for fast schema lookup
        if let Ok(table_schema) = schema_cache.get_or_load(conn, table_name) {
            for col_info in &table_schema.columns {
                if let Some(pg_type) = pg_type_from_string(&col_info.pg_type) {
                    if pg_type == PgType::Numeric {
                        has_decimal_columns = true;
                    }
                    column_types.push((col_info.name.clone(), pg_type));
                }
            }
        } else {
            // Fast path: check decimal table bloom filter
            if schema_cache.has_decimal_columns(table_name) {
                has_decimal_columns = true;
            }
        }
    }
    
    // Rewrite for decimal if needed
    let mut statement_clone = statement.clone();
    let mut rewriter = DecimalQueryRewriter::new(conn);
    let is_decimal_query = has_decimal_columns;
    
    let rewritten_query = if is_decimal_query {
        if let Err(e) = rewriter.rewrite_statement(&mut statement_clone) {
            tracing::warn!("Failed to rewrite query for decimal: {}", e);
            query.to_string()
        } else {
            statement_clone.to_string()
        }
    } else {
        query.to_string()
    };
    
    Ok((rewritten_query, ParsedQueryInfo {
        statement,
        table_names,
        column_types,
        has_decimal_columns,
        is_decimal_query,
    }))
}

/// Execute a query using cached information and statement pool
fn execute_cached_query_with_statement_pool(
    conn: &Connection,
    _original_query: &str,
    cached: &CachedQuery,
    final_query: &str,
) -> Result<DbResponse, rusqlite::Error> {
    // For simple queries, try to use statement pool
    if !final_query.contains('$') && matches!(crate::query::QueryTypeDetector::detect_query_type(final_query), crate::query::QueryType::Select) {
        if let Ok((columns, rows)) = StatementPool::global().query_cached(conn, final_query, []) {
            // Update touch for the statement pool entry
            StatementPool::global().touch(final_query);
            
            return Ok(DbResponse {
                columns,
                rows,
                rows_affected: 0,
            });
        }
    }
    
    // Fall back to original execution method
    execute_cached_query(conn, _original_query, cached, final_query)
}

/// Execute a query using cached information
fn execute_cached_query(
    conn: &Connection,
    _original_query: &str,
    cached: &CachedQuery,
    final_query: &str,
) -> Result<DbResponse, rusqlite::Error> {
    let mut stmt = conn.prepare(final_query)?;
    let column_count = stmt.column_count();
    
    let columns: Vec<String> = (0..column_count)
        .map(|i| stmt.column_name(i).unwrap_or("").to_string())
        .collect();
    
    // Use cached column types for type detection
    let mut is_boolean_col = vec![false; column_count];
    let mut is_date_col = vec![false; column_count];
    let mut is_time_col = vec![false; column_count];
    let mut is_timestamp_col = vec![false; column_count];
    
    for (i, col_name) in columns.iter().enumerate() {
        for (cached_col, pg_type) in &cached.column_types {
            if cached_col == col_name {
                match pg_type {
                    PgType::Bool => is_boolean_col[i] = true,
                    PgType::Date => is_date_col[i] = true,
                    PgType::Time | PgType::Timetz => is_time_col[i] = true,
                    PgType::Timestamp | PgType::Timestamptz => is_timestamp_col[i] = true,
                    _ => {}
                }
                break;
            }
        }
    }
    
    let rows = stmt.query_map([], |row| {
        let mut row_data = Vec::new();
        for i in 0..column_count {
            let value = row.get_ref(i)?;
            match value {
                rusqlite::types::ValueRef::Null => row_data.push(None),
                rusqlite::types::ValueRef::Integer(int_val) => {
                    if is_boolean_col[i] {
                        // Convert SQLite's 0/1 to PostgreSQL's f/t format
                        let bool_str = if int_val == 0 { "f" } else { "t" };
                        row_data.push(Some(bool_str.as_bytes().to_vec()));
                    } else if is_date_col[i] {
                        // Convert INTEGER days to YYYY-MM-DD
                        use crate::types::datetime_utils::format_days_to_date_buf;
                        let mut buf = vec![0u8; 32];
                        let len = format_days_to_date_buf(int_val as i32, &mut buf);
                        buf.truncate(len);
                        row_data.push(Some(buf));
                    } else if is_time_col[i] {
                        // Convert INTEGER microseconds to HH:MM:SS.ffffff
                        use crate::types::datetime_utils::format_microseconds_to_time_buf;
                        let mut buf = vec![0u8; 32];
                        let len = format_microseconds_to_time_buf(int_val, &mut buf);
                        buf.truncate(len);
                        row_data.push(Some(buf));
                    } else if is_timestamp_col[i] {
                        // Convert INTEGER microseconds to YYYY-MM-DD HH:MM:SS.ffffff
                        use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                        let mut buf = vec![0u8; 64];
                        let len = format_microseconds_to_timestamp_buf(int_val, &mut buf);
                        buf.truncate(len);
                        row_data.push(Some(buf));
                    } else {
                        // For simple query protocol, always return text format
                        row_data.push(Some(int_val.to_string().into_bytes()));
                    }
                },
                rusqlite::types::ValueRef::Real(f) => {
                    row_data.push(Some(f.to_string().into_bytes()));
                },
                rusqlite::types::ValueRef::Text(s) => {
                    row_data.push(Some(s.to_vec()));
                },
                rusqlite::types::ValueRef::Blob(b) => {
                    row_data.push(Some(b.to_vec()));
                },
            }
        }
        Ok(row_data)
    })?;
    
    let mut result_rows = Vec::new();
    for row in rows {
        result_rows.push(row?);
    }
    
    Ok(DbResponse {
        columns,
        rows: result_rows,
        rows_affected: 0,
    })
}

/// Extract table names from a statement
fn extract_table_names_from_statement(statement: &Statement) -> Vec<String> {
    match statement {
        Statement::Query(query) => {
            let mut tables = Vec::new();
            // This is a simplified version - in production you'd want full AST traversal
            if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
                for table_with_joins in &select.from {
                    if let sqlparser::ast::TableFactor::Table { name, .. } = &table_with_joins.relation {
                        tables.push(name.to_string());
                    }
                }
            }
            tables
        }
        _ => Vec::new(),
    }
}

/// Convert PostgreSQL type string to PgType enum
fn pg_type_from_string(type_str: &str) -> Option<PgType> {
    match type_str.to_lowercase().as_str() {
        "boolean" | "bool" => Some(PgType::Bool),
        "smallint" | "int2" => Some(PgType::Int2),
        "integer" | "int" | "int4" => Some(PgType::Int4),
        "bigint" | "int8" => Some(PgType::Int8),
        "real" | "float4" => Some(PgType::Float4),
        "double precision" | "float8" => Some(PgType::Float8),
        "text" => Some(PgType::Text),
        "varchar" => Some(PgType::Varchar),
        "char" => Some(PgType::Char),
        "uuid" => Some(PgType::Uuid),
        "json" => Some(PgType::Json),
        "jsonb" => Some(PgType::Jsonb),
        "date" => Some(PgType::Date),
        "time" => Some(PgType::Time),
        "timestamp" => Some(PgType::Timestamp),
        "timestamptz" | "timestamp with time zone" => Some(PgType::Timestamptz),
        "numeric" | "decimal" => Some(PgType::Numeric),
        "bytea" => Some(PgType::Bytea),
        "money" => Some(PgType::Money),
        _ => None,
    }
}

/// Convert SQLite CHECK constraint errors to PostgreSQL-compatible enum errors
fn convert_enum_error(error: rusqlite::Error, query: &str) -> rusqlite::Error {
    if let rusqlite::Error::SqliteFailure(sqlite_error, Some(msg)) = &error {
        if sqlite_error.code == rusqlite::ErrorCode::ConstraintViolation && msg.contains("CHECK constraint failed") {
            // Try to extract the enum type and value from the error message
            // SQLite error format: "CHECK constraint failed: column_name IN ('val1', 'val2', ...)"
            if let Some(start) = msg.find("CHECK constraint failed: ") {
                let constraint_part = &msg[start + 25..];
                if let Some(space_pos) = constraint_part.find(' ') {
                    let column_name = &constraint_part[..space_pos];
                    
                    // Try to extract the value from the INSERT query
                    if let Some(value) = extract_enum_value_from_query(query, column_name) {
                        // Try to find the enum type name from the constraint
                        if let Some(enum_type) = extract_enum_type_from_constraint(constraint_part) {
                            return rusqlite::Error::SqliteFailure(
                                *sqlite_error,
                                Some(format!("invalid input value for enum {}: \"{}\"", enum_type, value))
                            );
                        }
                    }
                }
            }
        }
    }
    error
}

/// Extract the enum value being inserted from the query
fn extract_enum_value_from_query(query: &str, column_name: &str) -> Option<String> {
    // Parse INSERT statement to find column position and corresponding value
    let insert_re = regex::Regex::new(r"(?i)INSERT\s+INTO\s+\w+\s*\(([^)]+)\)\s*VALUES\s*\(([^)]+)\)").ok()?;
    let captures = insert_re.captures(query)?;
    
    let columns_str = captures.get(1)?.as_str();
    let values_str = captures.get(2)?.as_str();
    
    // Find column index
    let columns: Vec<&str> = columns_str.split(',').map(|c| c.trim()).collect();
    let column_index = columns.iter().position(|&c| c == column_name)?;
    
    // Extract corresponding value
    let values: Vec<&str> = values_str.split(',').map(|v| v.trim()).collect();
    let value = values.get(column_index)?;
    
    // Remove quotes from value
    let trimmed = value.trim_matches('\'').trim_matches('"');
    Some(trimmed.to_string())
}

/// Extract enum type name from CHECK constraint
fn extract_enum_type_from_constraint(constraint: &str) -> Option<String> {
    // Try to infer the enum type from the column name
    // In our implementation, column names often match or relate to enum type names
    if let Some(column_start) = constraint.find(' ') {
        let column_part = &constraint[..column_start];
        // Remove common suffixes like _status, _type, _mood, etc.
        let type_name = column_part
            .trim_end_matches("_status")
            .trim_end_matches("_type")
            .trim_end_matches("_state")
            .trim_end_matches("_mood")
            .trim_end_matches("_level");
        
        // If it ends with the column name pattern, just use it
        if column_part.contains('_') {
            return Some(column_part.split('_').last()?.to_string());
        }
        
        return Some(type_name.to_string());
    }
    None
}