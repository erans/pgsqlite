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
}

impl DbHandler {
    pub fn new(db_path: &str) -> Result<Self, rusqlite::Error> {
        Self::new_with_config(db_path, &Config::load())
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
        
        // Initialize functions and metadata
        crate::functions::register_all_functions(&conn)?;
        crate::metadata::TypeMetadata::init(&conn)?;
        
        info!("DbHandler initialized with mutex-based implementation");
        
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            schema_cache: Arc::new(SchemaCache::new(config.schema_cache_ttl)),
        })
    }
    
    pub async fn execute(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
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
        }
        
        let conn = self.conn.lock();
        
        // Try enhanced fast path first
        if let Ok(Some(rows_affected)) = crate::query::execute_fast_path_enhanced(&*conn, query, &self.schema_cache) {
            return Ok(DbResponse {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected,
            });
        }
        
        // For INSERT queries, try statement pool for better performance
        if matches!(QueryTypeDetector::detect_query_type(query), QueryType::Insert) {
            // First check if we can use fast path with statement pool
            if let Some(table_name) = extract_insert_table_name(query) {
                if !self.schema_cache.has_decimal_columns(&table_name) {
                    // No decimal columns, use statement pool for optimal performance
                    match StatementPool::global().execute_cached(&*conn, query, []) {
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
            if let Some(cached) = crate::session::GLOBAL_QUERY_CACHE.get(query) {
                // Use cached rewritten query if available
                let final_query = cached.rewritten_query.as_ref().unwrap_or(&cached.normalized_query);
                let rows_affected = conn.execute(final_query, [])?;
                return Ok(DbResponse {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rows_affected,
                });
            }
        }
        
        // Fall back to normal execution
        execute_dml_sync(&*conn, query, &self.schema_cache)
    }
    
    pub async fn query(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        // Check result cache first for non-parameterized queries
        let cache_key = ResultCacheKey::new(query, &[]);
        if let Some(cached_result) = global_result_cache().get(&cache_key) {
            debug!("Result cache hit for query: {}", query);
            return Ok(DbResponse {
                columns: cached_result.columns,
                rows: cached_result.rows,
                rows_affected: cached_result.rows_affected as usize,
            });
        }
        
        let start = std::time::Instant::now();
        let conn = self.conn.lock();
        
        // Try enhanced fast path first for queries
        if let Ok(Some(response)) = crate::query::query_fast_path_enhanced(&*conn, query, &self.schema_cache) {
            let execution_time_us = start.elapsed().as_micros() as u64;
            
            // Cache the result if appropriate
            if ResultSetCache::should_cache(query, execution_time_us, response.rows.len()) {
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
        
        // Fall back to normal query execution
        let response = execute_query_sync(&*conn, query, &self.schema_cache)?;
        let execution_time_us = start.elapsed().as_micros() as u64;
        
        // Cache the result if appropriate
        if ResultSetCache::should_cache(query, execution_time_us, response.rows.len()) {
            global_result_cache().insert(
                cache_key,
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
        self.schema_cache.get_or_load(&*conn, table_name)
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
    
    /// Try executing a query with parameters using the fast path
    pub async fn try_execute_fast_path_with_params(
        &self, 
        query: &str, 
        params: &[rusqlite::types::Value]
    ) -> Result<Option<DbResponse>, rusqlite::Error> {
        let conn = self.conn.lock();
        
        // Try fast path for DML operations
        if let Ok(Some(rows_affected)) = crate::query::execute_fast_path_enhanced_with_params(&*conn, query, params, &self.schema_cache) {
            return Ok(Some(DbResponse {
                columns: Vec::new(),
                rows: Vec::new(),
                rows_affected,
            }));
        }
        
        // Try fast path for SELECT operations
        if let Ok(Some(response)) = crate::query::query_fast_path_enhanced_with_params(&*conn, query, params, &self.schema_cache) {
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
        let rows_affected = StatementPool::global().execute_cached(&*conn, query, [])?;
        
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
        let (columns, rows) = StatementPool::global().query_cached(&*conn, query, [])?;
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
            &*conn, 
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
            &*conn, 
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
    let mut stmt = conn.prepare(&metadata.prepared_sql)?;
    
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
    for row in query_result {
        rows.push(row?);
        
        // Process in batches for better cache performance (though we collect all here)
        if rows.len() % BATCH_SIZE == 0 && rows.len() > 0 {
            // Reserve capacity for next batch
            rows.reserve(BATCH_SIZE);
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
            match col_type.as_str() {
                "integer" | "int4" | "int8" | "int2" | "bigint" | "smallint" => 1, // Integer converter
                "real" | "float4" | "float8" | "double" | "numeric" => 3, // Float converter  
                "bytea" | "blob" => 4, // Blob converter
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
    // Try to extract table name from query and get column type
    if let Ok(table_names) = extract_table_names_simple(query) {
        for table_name in table_names {
            if let Some(schema) = schema_cache.get(&table_name) {
                if let Some(col_info) = schema.column_map.get(&col_name.to_lowercase()) {
                    return col_info.pg_type.clone();
                }
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
    // Try ultra-fast execution path first - but skip for queries with casts for now
    if !query.contains("::") {
        match execute_query_optimized(conn, query, schema_cache) {
            Ok(result) => return Ok(result),
            Err(_) => {
                // Fall back to original path if optimized path fails
                debug!("Optimized execution failed, falling back to original path for: {}", query);
            }
        }
    }
    
    // Check global query cache first
    if let Some(cached) = crate::session::GLOBAL_QUERY_CACHE.get(query) {
        // Use cached rewritten query if available
        let final_query = cached.rewritten_query.as_ref().unwrap_or(&cached.normalized_query);
        debug!("Query cache HIT for: {}", query);
        
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
        return execute_cached_query_with_statement_pool(conn, query, &cached, final_query);
    }
    
    debug!("Query cache MISS for: {}", query);
    
    // Parse and rewrite query for DECIMAL types if needed
    let (rewritten_query, parsed_info) = parse_and_rewrite_query(query, conn, schema_cache)?;
    
    // Cache the parsed query for future use
    let cached_query = CachedQuery {
        statement: parsed_info.statement,
        param_types: Vec::new(), // Will be filled for extended protocol
        is_decimal_query: parsed_info.is_decimal_query,
        table_names: parsed_info.table_names,
        column_types: parsed_info.column_types,
        has_decimal_columns: parsed_info.has_decimal_columns,
        rewritten_query: if parsed_info.is_decimal_query && rewritten_query != query {
            Some(rewritten_query.clone())
        } else {
            None
        },
        normalized_query: crate::cache::QueryCache::normalize_query(query),
    };
    
    // Insert into global cache
    crate::session::GLOBAL_QUERY_CACHE.insert(query.to_string(), cached_query.clone());
    debug!(
        "Cached query - Tables: {:?}, Decimal: {}, Column types: {}",
        cached_query.table_names,
        cached_query.has_decimal_columns,
        cached_query.column_types.len()
    );
    
    // Execute using cached information and statement pool
    execute_cached_query_with_statement_pool(conn, query, &cached_query, &rewritten_query)
}


/// Extract table name from INSERT statement
fn extract_insert_table_name(query: &str) -> Option<String> {
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
fn rewrite_query_for_decimal(query: &str, conn: &Connection) -> Result<String, rusqlite::Error> {
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
    
    Ok(statements[0].to_string())
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
    
    eprintln!("DEBUG: execute_cached_query executing: '{}'", final_query);
    let mut stmt = conn.prepare(final_query)?;
    let column_count = stmt.column_count();
    
    let columns: Vec<String> = (0..column_count)
        .map(|i| stmt.column_name(i).unwrap_or("").to_string())
        .collect();
    
    // Use cached column types for boolean detection
    let mut is_boolean_col = vec![false; column_count];
    for (i, col_name) in columns.iter().enumerate() {
        for (cached_col, pg_type) in &cached.column_types {
            if cached_col == col_name && *pg_type == PgType::Bool {
                is_boolean_col[i] = true;
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
                    // Debug for integer values
                    if int_val == 1952805748 {
                        eprintln!("WARNING: Found 1952805748 as INTEGER in SQLite result!");
                        eprintln!("This is 0x{:x} which is 'test' as bytes!", int_val);
                    }
                    
                    if is_boolean_col[i] {
                        // Convert SQLite's 0/1 to PostgreSQL's f/t format
                        let bool_str = if int_val == 0 { "f" } else { "t" };
                        row_data.push(Some(bool_str.as_bytes().to_vec()));
                    } else {
                        // For simple query protocol, always return text format
                        row_data.push(Some(int_val.to_string().into_bytes()));
                    }
                },
                rusqlite::types::ValueRef::Real(f) => {
                    row_data.push(Some(f.to_string().into_bytes()));
                },
                rusqlite::types::ValueRef::Text(s) => {
                    // Debug for text values
                    if s == b"1952805748" {
                        eprintln!("WARNING: Found '1952805748' as TEXT in SQLite result!");
                        eprintln!("This should have been 'test'!");
                    }
                    row_data.push(Some(s.to_vec()));
                },
                rusqlite::types::ValueRef::Blob(b) => {
                    // Debug for blob values
                    if b.len() == 4 && b == b"test" {
                        eprintln!("WARNING: Found 'test' as BLOB in SQLite result!");
                        eprintln!("This will be misinterpreted. Hex: {}", hex::encode(b));
                    }
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