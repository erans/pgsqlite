use std::sync::Arc;
use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::ast::Statement;
use crate::cache::{SchemaCache, CachedQuery, StatementPool};
use crate::cache::schema::TableSchema;
use crate::rewriter::DecimalQueryRewriter;
use crate::types::PgType;
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
        
        // Set pragmas for performance
        conn.execute_batch("
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA cache_size = -64000;
            PRAGMA temp_store = MEMORY;
            PRAGMA mmap_size = 268435456;
        ")?;
        
        // Initialize functions and metadata
        crate::functions::register_all_functions(&conn)?;
        crate::metadata::TypeMetadata::init(&conn)?;
        
        info!("DbHandler initialized with mutex-based implementation");
        
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            schema_cache: Arc::new(SchemaCache::new(300)),
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
        
        // Fall back to normal execution
        execute_dml_sync(&*conn, query, &self.schema_cache)
    }
    
    pub async fn query(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        let conn = self.conn.lock();
        
        // Try enhanced fast path first for queries
        if let Ok(Some(response)) = crate::query::query_fast_path_enhanced(&*conn, query, &self.schema_cache) {
            return Ok(response);
        }
        
        // Fall back to normal query execution
        execute_query_sync(&*conn, query, &self.schema_cache)
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
        // Check cache first
        if let Some(schema) = self.schema_cache.get(table_name) {
            return Ok(schema);
        }
        
        let conn = self.conn.lock();
        
        // Get schema information from both metadata and SQLite schema
        let mut column_data = Vec::new();
        
        // First get all columns from SQLite schema
        let pragma_query = format!("PRAGMA table_info({})", table_name);
        let mut stmt = conn.prepare(&pragma_query)?;
        let rows = stmt.query_map([], |row| {
            let name: String = row.get(1)?;
            let sqlite_type: String = row.get(2)?;
            Ok((name, sqlite_type))
        })?;
        
        for row in rows {
            let (col_name, sqlite_type) = row?;
            
            // Try to get PostgreSQL type from metadata
            let (pg_type, pg_oid) = match conn.query_row(
                "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2",
                [table_name, &col_name],
                |row| row.get::<_, String>(0),
            ) {
                Ok(pg_type_str) => {
                    let oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type_str);
                    (pg_type_str, oid)
                },
                Err(_) => {
                    // If no metadata, map from SQLite type
                    let type_mapper = crate::types::TypeMapper::new();
                    let pg_type = type_mapper.sqlite_to_pg(&sqlite_type);
                    let oid = pg_type.to_oid();
                    let pg_type_str = match pg_type {
                        crate::types::PgType::Text => "text",
                        crate::types::PgType::Int8 => "int8",
                        crate::types::PgType::Int4 => "int4",
                        crate::types::PgType::Int2 => "int2",
                        crate::types::PgType::Float8 => "float8",
                        crate::types::PgType::Float4 => "float4",
                        crate::types::PgType::Bool => "boolean",
                        crate::types::PgType::Bytea => "bytea",
                        crate::types::PgType::Date => "date",
                        crate::types::PgType::Timestamp => "timestamp",
                        crate::types::PgType::Timestamptz => "timestamptz",
                        crate::types::PgType::Uuid => "uuid",
                        crate::types::PgType::Json => "json",
                        crate::types::PgType::Jsonb => "jsonb",
                        crate::types::PgType::Numeric => "numeric",
                        crate::types::PgType::Varchar => "varchar",
                        crate::types::PgType::Char => "char",
                        crate::types::PgType::Time => "time",
                        crate::types::PgType::Money => "money",
                        _ => "text",
                    }.to_string();
                    (pg_type_str, oid as i32)
                }
            };
            
            column_data.push((col_name, pg_type, sqlite_type, pg_oid));
        }
        
        let schema = crate::cache::schema::SchemaCache::build_table_schema(column_data);
        
        // Cache it
        self.schema_cache.insert(table_name.to_string(), schema.clone());
        
        Ok(schema)
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
    let query_upper = query.trim().to_uppercase();
    query_upper.starts_with("CREATE") ||
    query_upper.starts_with("DROP") ||
    query_upper.starts_with("ALTER") ||
    query_upper.starts_with("TRUNCATE")
}

pub fn execute_dml_sync(
    conn: &Connection,
    query: &str,
    _schema_cache: &SchemaCache,
) -> Result<DbResponse, rusqlite::Error> {
    // Rewrite query for DECIMAL types if needed
    let rewritten_query = rewrite_query_for_decimal(query, conn)?;
    
    let rows_affected = conn.execute(&rewritten_query, [])?;
    
    Ok(DbResponse {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected,
    })
}

pub fn execute_query_sync(
    conn: &Connection,
    query: &str,
    _schema_cache: &SchemaCache,
) -> Result<DbResponse, rusqlite::Error> {
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
    let (rewritten_query, parsed_info) = parse_and_rewrite_query(query, conn)?;
    
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
fn parse_and_rewrite_query(query: &str, conn: &Connection) -> Result<(String, ParsedQueryInfo), rusqlite::Error> {
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
    
    // Get column types for the tables
    let mut column_types = Vec::new();
    let mut has_decimal_columns = false;
    
    for table_name in &table_names {
        // Query schema for column types
        if let Ok(mut stmt) = conn.prepare(
            "SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name = ?1"
        ) {
            if let Ok(rows) = stmt.query_map([table_name], |row| {
                let col_name: String = row.get(0)?;
                let pg_type_str: String = row.get(1)?;
                Ok((col_name, pg_type_str))
            }) {
                for row in rows.flatten() {
                    if let Some(pg_type) = pg_type_from_string(&row.1) {
                        if pg_type == PgType::Numeric {
                            has_decimal_columns = true;
                        }
                        column_types.push((row.0, pg_type));
                    }
                }
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
    if !final_query.contains('$') && final_query.trim().to_uppercase().starts_with("SELECT") {
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