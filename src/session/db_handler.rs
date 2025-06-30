use rusqlite::Connection;
use tokio::sync::{mpsc, oneshot};
use std::thread;
use tracing::{info, error};
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::ast::{Statement, Query, TableFactor};
use crate::cache::SchemaCache;

pub enum DbCommand {
    Execute {
        query: String,
        response: oneshot::Sender<Result<DbResponse, rusqlite::Error>>,
    },
    Query {
        query: String,
        response: oneshot::Sender<Result<DbResponse, rusqlite::Error>>,
    },
    GetSchemaType {
        table_name: String,
        column_name: String,
        response: oneshot::Sender<Result<Option<String>, rusqlite::Error>>,
    },
    GetTableSchema {
        table_name: String,
        response: oneshot::Sender<Result<crate::cache::schema::TableSchema, rusqlite::Error>>,
    },
    InvalidateSchemaCache {
        table_name: Option<String>,
    },
    Shutdown,
}

pub struct DbResponse {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    pub rows_affected: usize,
}

pub struct DbHandler {
    sender: mpsc::Sender<DbCommand>,
}

impl DbHandler {
    pub fn new(db_path: &str) -> Result<Self, rusqlite::Error> {
        let (sender, mut receiver) = mpsc::channel(100);
        let db_path = db_path.to_string();
        
        // Spawn a dedicated thread for SQLite operations
        thread::spawn(move || {
            let conn = if db_path == ":memory:" {
                Connection::open_in_memory()
            } else {
                Connection::open(&db_path)
            };
            
            let conn = match conn {
                Ok(c) => c,
                Err(e) => {
                    error!("Failed to open database: {}", e);
                    return;
                }
            };
            
            // Set pragmas
            if let Err(e) = conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 PRAGMA cache_size=-64000;
                 PRAGMA temp_store=MEMORY;"
            ) {
                error!("Failed to set pragmas: {}", e);
            }
            
            // Register custom functions
            if let Err(e) = crate::functions::register_all_functions(&conn) {
                error!("Failed to register custom functions: {}", e);
            }
            
            // Initialize metadata table
            if let Err(e) = crate::metadata::TypeMetadata::init(&conn) {
                error!("Failed to initialize metadata table: {}", e);
            }
            
            // Initialize schema cache with 5 minute TTL
            let schema_cache = SchemaCache::new(300);
            
            // Main command loop
            while let Some(cmd) = receiver.blocking_recv() {
                match cmd {
                    DbCommand::Execute { query, response } => {
                        // Check if this is a DDL statement that might invalidate cache
                        if is_ddl_statement(&query) {
                            schema_cache.clear();
                        }
                        let result = execute_dml(&conn, &query);
                        let _ = response.send(result);
                    }
                    DbCommand::Query { query, response } => {
                        let result = execute_query(&conn, &query);
                        let _ = response.send(result);
                    }
                    DbCommand::GetSchemaType { table_name, column_name, response } => {
                        let result = crate::metadata::TypeMetadata::get_pg_type(&conn, &table_name, &column_name);
                        let _ = response.send(result);
                    }
                    DbCommand::GetTableSchema { table_name, response } => {
                        let result = get_table_schema_cached(&conn, &schema_cache, &table_name);
                        let _ = response.send(result);
                    }
                    DbCommand::InvalidateSchemaCache { table_name } => {
                        if let Some(table) = table_name {
                            schema_cache.invalidate(&table);
                        } else {
                            schema_cache.clear();
                        }
                    }
                    DbCommand::Shutdown => break,
                }
            }
            
            info!("Database handler thread shutting down");
        });
        
        Ok(DbHandler { sender })
    }
    
    pub async fn execute(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        let (tx, rx) = oneshot::channel();
        
        self.sender.send(DbCommand::Execute {
            query: query.to_string(),
            response: tx,
        }).await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Database handler unavailable".to_string())
        ))?;
        
        rx.await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Failed to receive response".to_string())
        ))?
    }
    
    pub async fn query(&self, query: &str) -> Result<DbResponse, rusqlite::Error> {
        let (tx, rx) = oneshot::channel();
        
        self.sender.send(DbCommand::Query {
            query: query.to_string(),
            response: tx,
        }).await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Database handler unavailable".to_string())
        ))?;
        
        rx.await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Failed to receive response".to_string())
        ))?
    }
    
    pub async fn get_schema_type(&self, table_name: &str, column_name: &str) -> Result<Option<String>, rusqlite::Error> {
        let (tx, rx) = oneshot::channel();
        
        self.sender.send(DbCommand::GetSchemaType {
            table_name: table_name.to_string(),
            column_name: column_name.to_string(),
            response: tx,
        }).await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Database handler unavailable".to_string())
        ))?;
        
        rx.await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Failed to receive response".to_string())
        ))?
    }
    
    pub async fn get_table_schema(&self, table_name: &str) -> Result<crate::cache::schema::TableSchema, rusqlite::Error> {
        let (tx, rx) = oneshot::channel();
        
        self.sender.send(DbCommand::GetTableSchema {
            table_name: table_name.to_string(),
            response: tx,
        }).await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Database handler unavailable".to_string())
        ))?;
        
        rx.await.map_err(|_| rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
            Some("Failed to receive response".to_string())
        ))?
    }

    pub async fn shutdown(&self) {
        let _ = self.sender.send(DbCommand::Shutdown).await;
    }
}

fn execute_query(conn: &Connection, query: &str) -> Result<DbResponse, rusqlite::Error> {
    // Rewrite query to use decimal functions if needed
    let rewritten_query = rewrite_query_if_needed(conn, query)?;
    let mut stmt = conn.prepare(&rewritten_query)?;
    let column_count = stmt.column_count();
    
    // Get column names
    let mut columns = Vec::new();
    for i in 0..column_count {
        columns.push(stmt.column_name(i)?.to_string());
    }
    
    // Try to extract table name from query for type lookup
    let table_name = extract_table_name_from_query(&rewritten_query);
    
    // Get column types from metadata if we have a table name
    let mut column_types = Vec::new();
    if let Some(ref table) = table_name {
        for col_name in &columns {
            let pg_type = get_column_type_sync(conn, table, col_name)?;
            column_types.push(pg_type);
        }
    } else {
        // Fill with None if we can't determine table
        column_types.resize(columns.len(), None);
    }
    
    // Get rows
    let mut rows = Vec::new();
    let result_rows = stmt.query_map([], |row| {
        let mut values = Vec::new();
        for i in 0..column_count {
            use rusqlite::types::ValueRef;
            match row.get_ref(i)? {
                ValueRef::Null => values.push(None),
                ValueRef::Integer(int_val) => {
                    // Check if this column is a boolean type
                    if let Some(Some(pg_type)) = column_types.get(i) {
                        if pg_type.to_uppercase() == "BOOLEAN" {
                            // Convert 0/1 to PostgreSQL boolean format 'f'/'t'
                            let bool_str = if int_val == 0 { "f" } else { "t" };
                            values.push(Some(bool_str.as_bytes().to_vec()));
                        } else {
                            // Regular integer
                            values.push(Some(int_val.to_string().into_bytes()));
                        }
                    } else {
                        // No type info, treat as regular integer
                        values.push(Some(int_val.to_string().into_bytes()));
                    }
                },
                ValueRef::Real(f) => values.push(Some(f.to_string().into_bytes())),
                ValueRef::Text(s) => values.push(Some(s.to_vec())),
                ValueRef::Blob(b) => values.push(Some(b.to_vec())),
            }
        }
        Ok(values)
    })?;
    
    for row in result_rows {
        rows.push(row?);
    }
    
    let rows_affected = rows.len();
    Ok(DbResponse {
        columns,
        rows,
        rows_affected,
    })
}

fn execute_dml(conn: &Connection, query: &str) -> Result<DbResponse, rusqlite::Error> {
    // Rewrite query to use decimal functions if needed
    let rewritten_query = rewrite_query_if_needed(conn, query)?;
    let rows_affected = conn.execute(&rewritten_query, [])?;
    
    Ok(DbResponse {
        columns: Vec::new(),
        rows: Vec::new(),
        rows_affected,
    })
}

/// Rewrite query to use decimal functions if it contains NUMERIC operations
fn rewrite_query_if_needed(conn: &Connection, query: &str) -> Result<String, rusqlite::Error> {
    // Parse the query
    let dialect = PostgreSqlDialect {};
    let mut statements = match Parser::parse_sql(&dialect, query) {
        Ok(stmts) => stmts,
        Err(e) => {
            // If we can't parse it, just return the original query
            error!("Failed to parse query for decimal rewriting: {}", e);
            return Ok(query.to_string());
        }
    };
    
    // If we have a statement, try to rewrite it
    if let Some(stmt) = statements.first_mut() {
        let mut rewriter = crate::rewriter::DecimalQueryRewriter::new(conn);
        if let Err(e) = rewriter.rewrite_statement(stmt) {
            error!("Failed to rewrite query for decimal operations: {}", e);
            return Ok(query.to_string());
        }
        
        // Return the rewritten query
        Ok(stmt.to_string())
    } else {
        Ok(query.to_string())
    }
}

/// Extract table name from a SQL query (simplified version)
fn extract_table_name_from_query(query: &str) -> Option<String> {
    let dialect = PostgreSqlDialect {};
    match Parser::parse_sql(&dialect, query) {
        Ok(statements) => {
            if let Some(Statement::Query(query)) = statements.first() {
                extract_table_from_query_box(query)
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

/// Extract table name from a Query
fn extract_table_from_query_box(query: &Box<Query>) -> Option<String> {
    if let Some(_with) = &query.with {
        // Handle CTEs - for now, skip them
    }
    
    match &*query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            // Look for the first table in FROM clause
            for table in &select.from {
                if let TableFactor::Table { name, .. } = &table.relation {
                    if let Some(ident) = name.0.first() {
                        return Some(ident.to_string());
                    }
                }
            }
        }
        _ => {}
    }
    None
}

/// Get column type from metadata table synchronously
fn get_column_type_sync(conn: &Connection, table_name: &str, column_name: &str) -> Result<Option<String>, rusqlite::Error> {
    let mut stmt = conn.prepare(
        "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
    )?;
    
    match stmt.query_row([table_name, column_name], |row| {
        row.get::<_, String>(0)
    }) {
        Ok(pg_type) => Ok(Some(pg_type)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(e) => Err(e),
    }
}

/// Get table schema with caching
fn get_table_schema_cached(
    conn: &Connection,
    cache: &SchemaCache,
    table_name: &str,
) -> Result<crate::cache::schema::TableSchema, rusqlite::Error> {
    // Check cache first
    if let Some(schema) = cache.get(table_name) {
        info!("Schema cache hit for table: {}", table_name);
        return Ok(schema);
    }
    
    info!("Schema cache miss for table: {}, querying database", table_name);
    
    // Get schema information from both metadata and SQLite schema
    let mut columns = Vec::new();
    
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
        let pg_type_result = conn.query_row(
            "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2",
            [table_name, &col_name],
            |row| row.get::<_, String>(0),
        );
        
        let (pg_type, pg_oid) = match pg_type_result {
            Ok(pg_type) => {
                let oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(&pg_type);
                (pg_type, oid)
            }
            Err(_) => {
                // Fall back to mapping from SQLite type
                let pg_oid = crate::types::SchemaTypeMapper::sqlite_type_to_pg_oid(&sqlite_type);
                let pg_type = crate::types::SchemaTypeMapper::pg_oid_to_type_name(pg_oid);
                (pg_type.to_string(), pg_oid)
            }
        };
        
        columns.push((col_name, pg_type, sqlite_type, pg_oid));
    }
    
    let schema = SchemaCache::build_table_schema(columns);
    cache.insert(table_name.to_string(), schema.clone());
    
    Ok(schema)
}

/// Check if a query is a DDL statement that might affect schema
fn is_ddl_statement(query: &str) -> bool {
    let query_upper = query.trim().to_uppercase();
    query_upper.starts_with("CREATE") ||
    query_upper.starts_with("ALTER") ||
    query_upper.starts_with("DROP") ||
    query_upper.starts_with("TRUNCATE")
}