use std::sync::Arc;
use parking_lot::Mutex;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;
use crate::cache::SchemaCache;
use crate::cache::schema::TableSchema;
use crate::rewriter::DecimalQueryRewriter;
use tracing::info;

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
        }
        
        let conn = self.conn.lock();
        
        // Try fast path first
        if let Ok(Some(rows_affected)) = crate::query::execute_fast_path(&*conn, query, &self.schema_cache) {
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
        
        // Try fast path first for queries
        if let Ok(Some(response)) = crate::query::query_fast_path(&*conn, query, &self.schema_cache) {
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
    // Rewrite query for DECIMAL types if needed
    let rewritten_query = rewrite_query_for_decimal(query, conn)?;
    
    let mut stmt = conn.prepare(&rewritten_query)?;
    let column_count = stmt.column_count();
    
    let columns: Vec<String> = (0..column_count)
        .map(|i| stmt.column_name(i).unwrap_or("").to_string())
        .collect();
    
    // Extract table name from query to look up schema
    let table_name = extract_table_name_from_select(query);
    
    // Pre-fetch schema types for all columns if we have a table name
    let mut schema_types = std::collections::HashMap::new();
    if let Some(ref table) = table_name {
        for col_name in &columns {
            // Try to get type from __pgsqlite_schema
            if let Ok(mut meta_stmt) = conn.prepare(
                "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
            ) {
                if let Ok(pg_type) = meta_stmt.query_row([table, col_name], |row| {
                    row.get::<_, String>(0)
                }) {
                    schema_types.insert(col_name.clone(), pg_type);
                }
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
                    // Check if this column is a boolean type
                    let col_name = &columns[i];
                    let is_boolean = schema_types.get(col_name)
                        .map(|pg_type| {
                            let type_lower = pg_type.to_lowercase();
                            type_lower == "boolean" || type_lower == "bool"
                        })
                        .unwrap_or(false);
                    
                    if is_boolean {
                        // Convert SQLite's 0/1 to PostgreSQL's f/t format
                        let bool_str = if int_val == 0 { "f" } else { "t" };
                        row_data.push(Some(bool_str.as_bytes().to_vec()));
                    } else {
                        // For simple query protocol, always return text format
                        row_data.push(Some(int_val.to_string().into_bytes()));
                    }
                },
                rusqlite::types::ValueRef::Real(f) => {
                    // For simple query protocol, always return text format
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

/// Extract table name from SELECT statement
fn extract_table_name_from_select(query: &str) -> Option<String> {
    let query_lower = query.to_lowercase();
    
    // Look for FROM clause
    if let Some(from_pos) = query_lower.find(" from ") {
        let after_from = &query[from_pos + 6..].trim();
        
        // Find the end of table name (space, where, order by, etc.)
        let table_end = after_from.find(|c: char| {
            c.is_whitespace() || c == ',' || c == ';' || c == '('
        }).unwrap_or(after_from.len());
        
        let table_name = after_from[..table_end].trim();
        
        // Remove quotes if present
        let table_name = table_name.trim_matches('"').trim_matches('\'');
        
        if !table_name.is_empty() {
            Some(table_name.to_string())
        } else {
            None
        }
    } else {
        None
    }
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