use rusqlite::{Connection, types::ValueRef};
use regex::Regex;
use once_cell::sync::Lazy;
use crate::cache::SchemaCache;
use crate::session::db_handler::DbResponse;
use std::collections::HashMap;
use std::sync::Mutex;

// Pre-compiled regexes for fast path detection
static INSERT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*INSERT\s+INTO\s+(\w+)\s*\(").unwrap()
});

static BATCH_INSERT_REGEX: Lazy<Regex> = Lazy::new(|| {
    // Matches multi-row INSERT: INSERT INTO table (cols) VALUES (row1), (row2), ...
    Regex::new(r"(?i)^\s*INSERT\s+INTO\s+(\w+)\s*\([^)]+\)\s*VALUES\s*\([^)]+\)(?:\s*,\s*\([^)]+\))+\s*;?\s*$").unwrap()
});

static SELECT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SELECT\s+.+\s+FROM\s+(\w+)").unwrap()
});

static SELECT_WHERE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SELECT\s+.+\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$").unwrap()
});

static SELECT_WHERE_PARAM_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SELECT\s+.+\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*\$(\d+)\s*$").unwrap()
});

static UPDATE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*UPDATE\s+(\w+)\s+SET").unwrap()
});

static UPDATE_WHERE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*UPDATE\s+(\w+)\s+SET\s+.+\s+WHERE\s+(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$").unwrap()
});

static UPDATE_WHERE_PARAM_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*UPDATE\s+(\w+)\s+SET\s+.+\s+WHERE\s+(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*\$(\d+)\s*$").unwrap()
});

static DELETE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*DELETE\s+FROM\s+(\w+)").unwrap()
});

static DELETE_WHERE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*(.+)$").unwrap()
});

static DELETE_WHERE_PARAM_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*DELETE\s+FROM\s+(\w+)\s+WHERE\s+(\w+)\s*(>=|<=|!=|<>|=|>|<)\s*\$(\d+)\s*$").unwrap()
});

// Cache for decimal table detection to avoid repeated schema lookups
static DECIMAL_TABLE_CACHE: Lazy<Mutex<HashMap<String, bool>>> = Lazy::new(|| {
    Mutex::new(HashMap::new())
});

/// Represents a fast path query with its components
#[derive(Debug, Clone)]
pub struct FastPathQuery {
    pub table_name: String,
    pub operation: FastPathOperation,
    pub where_clause: Option<WhereClause>,
}

#[derive(Debug, Clone)]
pub enum FastPathOperation {
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub column: String,
    pub operator: String,
    pub value: String,
    pub is_parameter: bool,
    pub parameter_index: Option<usize>,
}

/// Enhanced fast path detection that supports simple WHERE clauses
pub fn can_use_fast_path_enhanced(query: &str) -> Option<FastPathQuery> {
    // Remove any trailing semicolon and trim
    let query = query.trim().trim_end_matches(';');
    
    // Check for complex patterns that disqualify fast path
    if query.contains("JOIN") || 
       query.contains("UNION") ||
       query.contains("(SELECT") ||
       query.contains("CASE") ||
       query.contains("LIMIT") ||
       query.contains("ORDER BY") ||
       query.contains("GROUP BY") ||
       query.contains("HAVING") {
        return None;
    }
    
    // Try SELECT with WHERE (parameter version first)
    if let Some(caps) = SELECT_WHERE_PARAM_REGEX.captures(query) {
        let param_index = caps.get(4).unwrap().as_str().parse::<usize>().unwrap_or(1);
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Select,
            where_clause: Some(WhereClause {
                column: caps.get(2).unwrap().as_str().to_string(),
                operator: caps.get(3).unwrap().as_str().to_string(),
                value: format!("${param_index}"),
                is_parameter: true,
                parameter_index: Some(param_index),
            }),
        });
    }
    
    // Try SELECT with WHERE (literal value)
    if let Some(caps) = SELECT_WHERE_REGEX.captures(query) {
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Select,
            where_clause: Some(WhereClause {
                column: caps.get(2).unwrap().as_str().to_string(),
                operator: caps.get(3).unwrap().as_str().to_string(),
                value: caps.get(4).unwrap().as_str().trim().to_string(),
                is_parameter: false,
                parameter_index: None,
            }),
        });
    }
    
    // Try UPDATE with WHERE (parameter version first)
    if let Some(caps) = UPDATE_WHERE_PARAM_REGEX.captures(query) {
        let param_index = caps.get(4).unwrap().as_str().parse::<usize>().unwrap_or(1);
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Update,
            where_clause: Some(WhereClause {
                column: caps.get(2).unwrap().as_str().to_string(),
                operator: caps.get(3).unwrap().as_str().to_string(),
                value: format!("${param_index}"),
                is_parameter: true,
                parameter_index: Some(param_index),
            }),
        });
    }
    
    // Try UPDATE with WHERE (literal value)
    if let Some(caps) = UPDATE_WHERE_REGEX.captures(query) {
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Update,
            where_clause: Some(WhereClause {
                column: caps.get(2).unwrap().as_str().to_string(),
                operator: caps.get(3).unwrap().as_str().to_string(),
                value: caps.get(4).unwrap().as_str().trim().to_string(),
                is_parameter: false,
                parameter_index: None,
            }),
        });
    }
    
    // Try DELETE with WHERE (parameter version first)
    if let Some(caps) = DELETE_WHERE_PARAM_REGEX.captures(query) {
        let param_index = caps.get(4).unwrap().as_str().parse::<usize>().unwrap_or(1);
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Delete,
            where_clause: Some(WhereClause {
                column: caps.get(2).unwrap().as_str().to_string(),
                operator: caps.get(3).unwrap().as_str().to_string(),
                value: format!("${param_index}"),
                is_parameter: true,
                parameter_index: Some(param_index),
            }),
        });
    }
    
    // Try DELETE with WHERE (literal value)
    if let Some(caps) = DELETE_WHERE_REGEX.captures(query) {
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Delete,
            where_clause: Some(WhereClause {
                column: caps.get(2).unwrap().as_str().to_string(),
                operator: caps.get(3).unwrap().as_str().to_string(),
                value: caps.get(4).unwrap().as_str().trim().to_string(),
                is_parameter: false,
                parameter_index: None,
            }),
        });
    }
    
    // Fall back to simple patterns without WHERE
    if let Some(caps) = INSERT_REGEX.captures(query) {
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Insert,
            where_clause: None,
        });
    }
    
    if let Some(caps) = SELECT_REGEX.captures(query) {
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Select,
            where_clause: None,
        });
    }
    
    if let Some(caps) = UPDATE_REGEX.captures(query) && !query.contains("WHERE") {
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Update,
            where_clause: None,
        });
    }
    
    if let Some(caps) = DELETE_REGEX.captures(query) && !query.contains("WHERE") {
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Delete,
            where_clause: None,
        });
    }
    
    // Try INSERT - most INSERTs can use fast path
    if let Some(caps) = INSERT_REGEX.captures(query) {
        return Some(FastPathQuery {
            table_name: caps.get(1).unwrap().as_str().to_string(),
            operation: FastPathOperation::Insert,
            where_clause: None,
        });
    }
    
    None
}

/// Check if a batch INSERT can use fast path (no datetime/decimal types)
pub fn can_batch_insert_use_fast_path(query: &str) -> Option<String> {
    // First check if it matches batch INSERT pattern
    if let Some(caps) = BATCH_INSERT_REGEX.captures(query) {
        // Check for patterns that would require translation
        if query.contains("::") || // PostgreSQL casts
           query.contains("CURRENT_") || // DateTime functions
           query.contains("NOW()") ||
           (query.contains("'") && query.contains('-')) || // Date patterns
           (query.contains("'") && query.contains(':')) {  // Time patterns
            return None;
        }
        
        return caps.get(1).map(|m| m.as_str().to_string());
    }
    None
}

/// Check if a query is simple enough for fast path execution (legacy function)
pub fn can_use_fast_path(query: &str) -> Option<String> {
    // Check for batch INSERT first
    if let Some(table) = can_batch_insert_use_fast_path(query) {
        return Some(table);
    }
    
    // Check for simple patterns and extract table name
    if let Some(caps) = INSERT_REGEX.captures(query) {
        return caps.get(1).map(|m| m.as_str().to_string());
    }
    
    if let Some(caps) = SELECT_REGEX.captures(query) {
        // Avoid complex SELECT with JOINs, subqueries, etc
        if !query.contains("JOIN") && !query.contains("(") {
            return caps.get(1).map(|m| m.as_str().to_string());
        }
    }
    
    if let Some(caps) = UPDATE_REGEX.captures(query) {
        // Avoid complex UPDATE with subqueries
        if !query.contains("(") {
            return caps.get(1).map(|m| m.as_str().to_string());
        }
    }
    
    if let Some(caps) = DELETE_REGEX.captures(query) {
        // Avoid complex DELETE with subqueries
        if !query.contains("(") {
            return caps.get(1).map(|m| m.as_str().to_string());
        }
    }
    
    None
}

/// Clear the decimal table cache (should be called on DDL operations)
pub fn clear_decimal_cache() {
    if let Ok(mut cache) = DECIMAL_TABLE_CACHE.lock() {
        cache.clear();
    }
}

/// Check if a table has any DECIMAL columns that would require query rewriting
pub fn table_has_decimal_columns(
    _conn: &Connection,
    table_name: &str,
    schema_cache: &SchemaCache,
) -> Result<bool, rusqlite::Error> {
    // Check dedicated decimal cache first
    if let Ok(cache) = DECIMAL_TABLE_CACHE.lock() {
        if let Some(&has_decimal) = cache.get(table_name) {
            return Ok(has_decimal);
        }
    }
    
    // Fast decimal detection using bloom filter
    let has_decimal = schema_cache.has_decimal_columns(table_name);
    
    // Cache the result
    if let Ok(mut cache) = DECIMAL_TABLE_CACHE.lock() {
        cache.insert(table_name.to_string(), has_decimal);
    }
    
    Ok(has_decimal)
}

/// Fast path DML execution that bypasses parsing and rewriting
pub fn execute_fast_path(
    conn: &Connection,
    query: &str,
    schema_cache: &SchemaCache,
) -> Result<Option<usize>, rusqlite::Error> {
    // Check if query qualifies for fast path
    if let Some(table_name) = can_use_fast_path(query) {
        // Skip SELECT queries here, they need special handling
        if matches!(crate::query::QueryTypeDetector::detect_query_type(query), crate::query::QueryType::Select) {
            return Ok(None);
        }
        
        // Check if table has decimal columns
        match table_has_decimal_columns(conn, &table_name, schema_cache) {
            Ok(false) => {
                // No decimal columns, execute directly
                let rows_affected = conn.execute(query, [])?;
                return Ok(Some(rows_affected));
            }
            _ => {
                // Has decimal columns or error checking, fall back to normal path
                return Ok(None);
            }
        }
    }
    
    Ok(None)
}

/// Fast path SELECT execution that bypasses parsing and rewriting
pub fn query_fast_path(
    conn: &Connection,
    query: &str,
    schema_cache: &SchemaCache,
) -> Result<Option<DbResponse>, rusqlite::Error> {
    // Check if query qualifies for fast path
    if let Some(table_name) = can_use_fast_path(query) {
        // Only handle SELECT queries
        if !matches!(crate::query::QueryTypeDetector::detect_query_type(query), crate::query::QueryType::Select) {
            return Ok(None);
        }
        
        // Check if table has decimal columns
        match table_has_decimal_columns(conn, &table_name, schema_cache) {
            Ok(false) => {
                // No decimal columns, execute directly
                let mut stmt = conn.prepare(query)?;
                let column_count = stmt.column_count();
                
                // Get column names
                let mut columns = Vec::new();
                for i in 0..column_count {
                    columns.push(stmt.column_name(i)?.to_string());
                }
                
                // Check for boolean columns in the schema using cache
                let mut column_types = Vec::new();
                if let Ok(table_schema) = schema_cache.get_or_load(conn, &table_name) {
                    for col_name in &columns {
                        if let Some(col_info) = table_schema.column_map.get(&col_name.to_lowercase()) {
                            column_types.push(Some(col_info.pg_type.clone()));
                        } else {
                            column_types.push(None);
                        }
                    }
                } else {
                    // Fallback to None for all columns
                    column_types.resize(columns.len(), None);
                }
                
                // Get rows - with boolean type conversions
                let mut rows = Vec::new();
                let result_rows = stmt.query_map([], |row| {
                    let mut values = Vec::new();
                    for i in 0..column_count {
                        match row.get_ref(i)? {
                            ValueRef::Null => values.push(None),
                            ValueRef::Integer(int_val) => {
                                // Check column type for proper formatting
                                let pg_type = column_types.get(i)
                                    .and_then(|opt| opt.as_ref())
                                    .map(|s| s.to_lowercase())
                                    .unwrap_or_default();
                                
                                if pg_type == "boolean" || pg_type == "bool" {
                                    // Convert SQLite's 0/1 to PostgreSQL's f/t format
                                    let bool_str = if int_val == 0 { "f" } else { "t" };
                                    values.push(Some(bool_str.as_bytes().to_vec()));
                                } else if pg_type == "date" {
                                    // Convert INTEGER days to YYYY-MM-DD
                                    use crate::types::datetime_utils::format_days_to_date_buf;
                                    let mut buf = vec![0u8; 32];
                                    let len = format_days_to_date_buf(int_val as i32, &mut buf);
                                    buf.truncate(len);
                                    values.push(Some(buf));
                                } else if pg_type == "time" || pg_type == "timetz" || pg_type == "time without time zone" || pg_type == "time with time zone" {
                                    // Convert INTEGER microseconds to HH:MM:SS.ffffff
                                    use crate::types::datetime_utils::format_microseconds_to_time_buf;
                                    let mut buf = vec![0u8; 32];
                                    let len = format_microseconds_to_time_buf(int_val, &mut buf);
                                    buf.truncate(len);
                                    values.push(Some(buf));
                                } else if pg_type == "timestamp" || pg_type == "timestamptz" || pg_type == "timestamp without time zone" || pg_type == "timestamp with time zone" {
                                    // Convert INTEGER microseconds to YYYY-MM-DD HH:MM:SS.ffffff
                                    use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                                    let mut buf = vec![0u8; 64];
                                    let len = format_microseconds_to_timestamp_buf(int_val, &mut buf);
                                    buf.truncate(len);
                                    values.push(Some(buf));
                                } else {
                                    values.push(Some(int_val.to_string().into_bytes()));
                                }
                            },
                            ValueRef::Real(f) => {
                                // Check if this is a numeric/decimal column that needs formatting
                                let pg_type = column_types.get(i)
                                    .and_then(|opt| opt.as_ref())
                                    .map(|s| s.to_lowercase())
                                    .unwrap_or_default();
                                
                                if pg_type == "numeric" || pg_type == "decimal" {
                                    let formatted = crate::types::numeric_utils::format_numeric_with_scale(
                                        f, 
                                        &table_name, 
                                        &columns[i], 
                                        conn
                                    );
                                    values.push(Some(formatted.into_bytes()));
                                } else {
                                    values.push(Some(f.to_string().into_bytes()));
                                }
                            },
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
                return Ok(Some(DbResponse {
                    columns,
                    rows,
                    rows_affected,
                }));
            }
            _ => {
                // Has decimal columns or error checking, fall back to normal path
                return Ok(None);
            }
        }
    }
    
    Ok(None)
}

/// Enhanced fast path execution that supports WHERE clauses and parameters
pub fn execute_fast_path_enhanced_with_params(
    conn: &Connection,
    query: &str,
    params: &[rusqlite::types::Value],
    schema_cache: &SchemaCache,
) -> Result<Option<usize>, rusqlite::Error> {
    // Try enhanced fast path detection
    if let Some(fast_query) = can_use_fast_path_enhanced(query) {
        // Skip SELECT queries here, they need special handling
        if matches!(fast_query.operation, FastPathOperation::Select) {
            return Ok(None);
        }
        
        // Check if table has decimal columns
        match table_has_decimal_columns(conn, &fast_query.table_name, schema_cache) {
            Ok(false) => {
                // No decimal columns, execute directly with parameters
                let rows_affected = conn.execute(query, rusqlite::params_from_iter(params.iter()))?;
                return Ok(Some(rows_affected));
            }
            _ => {
                // Has decimal columns or error checking, fall back to normal path
                return Ok(None);
            }
        }
    }
    
    Ok(None)
}

/// Enhanced fast path execution that supports WHERE clauses
pub fn execute_fast_path_enhanced(
    conn: &Connection,
    query: &str,
    schema_cache: &SchemaCache,
) -> Result<Option<usize>, rusqlite::Error> {
    // Try enhanced fast path detection
    if let Some(fast_query) = can_use_fast_path_enhanced(query) {
        // Skip SELECT queries here, they need special handling
        if matches!(fast_query.operation, FastPathOperation::Select) {
            return Ok(None);
        }
        
        // Check if table has decimal columns
        match table_has_decimal_columns(conn, &fast_query.table_name, schema_cache) {
            Ok(false) => {
                // No decimal columns, execute directly
                let rows_affected = conn.execute(query, [])?;
                return Ok(Some(rows_affected));
            }
            _ => {
                // Has decimal columns or error checking, fall back to normal path
                return Ok(None);
            }
        }
    }
    
    // Fall back to legacy fast path
    execute_fast_path(conn, query, schema_cache)
}

/// Enhanced fast path SELECT execution with parameters
pub fn query_fast_path_enhanced_with_params(
    conn: &Connection,
    query: &str,
    params: &[rusqlite::types::Value],
    schema_cache: &SchemaCache,
) -> Result<Option<DbResponse>, rusqlite::Error> {
    // Try enhanced fast path detection
    if let Some(fast_query) = can_use_fast_path_enhanced(query) {
        // Only handle SELECT queries
        if !matches!(fast_query.operation, FastPathOperation::Select) {
            return Ok(None);
        }
        
        // Check if table has decimal columns
        match table_has_decimal_columns(conn, &fast_query.table_name, schema_cache) {
            Ok(false) => {
                return execute_fast_select_with_params(conn, query, &fast_query.table_name, params, schema_cache);
            }
            _ => {
                // Has decimal columns or error checking, fall back to normal path
                return Ok(None);
            }
        }
    }
    
    Ok(None)
}

/// Enhanced fast path SELECT execution that supports WHERE clauses
pub fn query_fast_path_enhanced(
    conn: &Connection,
    query: &str,
    schema_cache: &SchemaCache,
) -> Result<Option<DbResponse>, rusqlite::Error> {
    // Try enhanced fast path detection
    if let Some(fast_query) = can_use_fast_path_enhanced(query) {
        // Only handle SELECT queries
        if !matches!(fast_query.operation, FastPathOperation::Select) {
            return Ok(None);
        }
        
        // Check if table has decimal columns
        match table_has_decimal_columns(conn, &fast_query.table_name, schema_cache) {
            Ok(false) => {
                return execute_fast_select(conn, query, &fast_query.table_name, schema_cache);
            }
            _ => {
                // Has decimal columns or error checking, fall back to normal path
                return Ok(None);
            }
        }
    }
    
    // Fall back to legacy fast path
    query_fast_path(conn, query, schema_cache)
}

/// Execute a fast SELECT query with parameters
fn execute_fast_select_with_params(
    conn: &Connection,
    query: &str,
    table_name: &str,
    params: &[rusqlite::types::Value],
    schema_cache: &SchemaCache,
) -> Result<Option<DbResponse>, rusqlite::Error> {
    let mut stmt = conn.prepare(query)?;
    let column_count = stmt.column_count();
    
    // Get column names
    let mut columns = Vec::new();
    for i in 0..column_count {
        columns.push(stmt.column_name(i)?.to_string());
    }
    
    // Check for boolean columns in the schema using cache
    let mut column_types = Vec::new();
    if let Ok(table_schema) = schema_cache.get_or_load(conn, table_name) {
        for col_name in &columns {
            if let Some(col_info) = table_schema.column_map.get(&col_name.to_lowercase()) {
                column_types.push(Some(col_info.pg_type.clone()));
            } else {
                column_types.push(None);
            }
        }
    } else {
        // Fallback to None for all columns
        column_types.resize(columns.len(), None);
    }
    
    // Get rows - with boolean type conversions, using parameters
    let mut rows = Vec::new();
    let result_rows = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
        let mut values = Vec::new();
        for i in 0..column_count {
            match row.get_ref(i)? {
                ValueRef::Null => values.push(None),
                ValueRef::Integer(int_val) => {
                    // Get the column type
                    let pg_type = column_types.get(i)
                        .and_then(|opt| opt.as_ref())
                        .map(|t| t.to_lowercase());
                    
                    match pg_type.as_deref() {
                        Some("boolean") | Some("bool") => {
                            // Convert SQLite's 0/1 to PostgreSQL's f/t format
                            let bool_str = if int_val == 0 { "f" } else { "t" };
                            values.push(Some(bool_str.as_bytes().to_vec()));
                        },
                        Some("date") => {
                            // Convert INTEGER days to YYYY-MM-DD
                            use crate::types::datetime_utils::format_days_to_date_buf;
                            let mut buf = vec![0u8; 32];
                            let len = format_days_to_date_buf(int_val as i32, &mut buf);
                            buf.truncate(len);
                            values.push(Some(buf));
                        },
                        Some("time") | Some("timetz") => {
                            // Convert INTEGER microseconds to HH:MM:SS.ffffff
                            use crate::types::datetime_utils::format_microseconds_to_time_buf;
                            let mut buf = vec![0u8; 32];
                            let len = format_microseconds_to_time_buf(int_val, &mut buf);
                            buf.truncate(len);
                            values.push(Some(buf));
                        },
                        Some("timestamp") | Some("timestamptz") => {
                            // Convert INTEGER microseconds to YYYY-MM-DD HH:MM:SS.ffffff
                            use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                            let mut buf = vec![0u8; 64];
                            let len = format_microseconds_to_timestamp_buf(int_val, &mut buf);
                            buf.truncate(len);
                            values.push(Some(buf));
                        },
                        _ => {
                            // Default integer to string conversion
                            values.push(Some(int_val.to_string().into_bytes()));
                        }
                    }
                },
                ValueRef::Real(f) => {
                    // Check if this is a numeric/decimal column that needs formatting
                    let pg_type = column_types.get(i)
                        .and_then(|opt| opt.as_ref())
                        .map(|s| s.to_lowercase())
                        .unwrap_or_default();
                    
                    if pg_type == "numeric" || pg_type == "decimal" {
                        let formatted = crate::types::numeric_utils::format_numeric_with_scale(
                            f, 
                            table_name, 
                            &columns[i], 
                            conn
                        );
                        values.push(Some(formatted.into_bytes()));
                    } else {
                        values.push(Some(f.to_string().into_bytes()));
                    }
                },
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
    Ok(Some(DbResponse {
        columns,
        rows,
        rows_affected,
    }))
}

/// Execute a fast SELECT query without decimal rewriting
fn execute_fast_select(
    conn: &Connection,
    query: &str,
    table_name: &str,
    schema_cache: &SchemaCache,
) -> Result<Option<DbResponse>, rusqlite::Error> {
    let mut stmt = conn.prepare(query)?;
    let column_count = stmt.column_count();
    
    // Get column names
    let mut columns = Vec::new();
    for i in 0..column_count {
        columns.push(stmt.column_name(i)?.to_string());
    }
    
    // Check for boolean columns in the schema using cache
    let mut column_types = Vec::new();
    if let Ok(table_schema) = schema_cache.get_or_load(conn, table_name) {
        for col_name in &columns {
            if let Some(col_info) = table_schema.column_map.get(&col_name.to_lowercase()) {
                column_types.push(Some(col_info.pg_type.clone()));
            } else {
                column_types.push(None);
            }
        }
    } else {
        // Fallback to None for all columns
        column_types.resize(columns.len(), None);
    }
    
    // Get rows - with boolean type conversions
    let mut rows = Vec::new();
    let result_rows = stmt.query_map([], |row| {
        let mut values = Vec::new();
        for i in 0..column_count {
            match row.get_ref(i)? {
                ValueRef::Null => values.push(None),
                ValueRef::Integer(int_val) => {
                    // Get the column type
                    let pg_type = column_types.get(i)
                        .and_then(|opt| opt.as_ref())
                        .map(|t| t.to_lowercase());
                    
                    match pg_type.as_deref() {
                        Some("boolean") | Some("bool") => {
                            // Convert SQLite's 0/1 to PostgreSQL's f/t format
                            let bool_str = if int_val == 0 { "f" } else { "t" };
                            values.push(Some(bool_str.as_bytes().to_vec()));
                        },
                        Some("date") => {
                            // Convert INTEGER days to YYYY-MM-DD
                            use crate::types::datetime_utils::format_days_to_date_buf;
                            let mut buf = vec![0u8; 32];
                            let len = format_days_to_date_buf(int_val as i32, &mut buf);
                            buf.truncate(len);
                            values.push(Some(buf));
                        },
                        Some("time") | Some("timetz") => {
                            // Convert INTEGER microseconds to HH:MM:SS.ffffff
                            use crate::types::datetime_utils::format_microseconds_to_time_buf;
                            let mut buf = vec![0u8; 32];
                            let len = format_microseconds_to_time_buf(int_val, &mut buf);
                            buf.truncate(len);
                            values.push(Some(buf));
                        },
                        Some("timestamp") | Some("timestamptz") => {
                            // Convert INTEGER microseconds to YYYY-MM-DD HH:MM:SS.ffffff
                            use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                            let mut buf = vec![0u8; 64];
                            let len = format_microseconds_to_timestamp_buf(int_val, &mut buf);
                            buf.truncate(len);
                            values.push(Some(buf));
                        },
                        _ => {
                            // Default integer to string conversion
                            values.push(Some(int_val.to_string().into_bytes()));
                        }
                    }
                },
                ValueRef::Real(f) => {
                    // Check if this is a numeric/decimal column that needs formatting
                    let pg_type = column_types.get(i)
                        .and_then(|opt| opt.as_ref())
                        .map(|s| s.to_lowercase())
                        .unwrap_or_default();
                    
                    if pg_type == "numeric" || pg_type == "decimal" {
                        let formatted = crate::types::numeric_utils::format_numeric_with_scale(
                            f, 
                            table_name, 
                            &columns[i], 
                            conn
                        );
                        values.push(Some(formatted.into_bytes()));
                    } else {
                        values.push(Some(f.to_string().into_bytes()));
                    }
                },
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
    Ok(Some(DbResponse {
        columns,
        rows,
        rows_affected,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_can_use_fast_path() {
        // Simple queries that should use fast path
        assert!(can_use_fast_path("INSERT INTO users (name) VALUES (?)").is_some());
        assert!(can_use_fast_path("SELECT * FROM users").is_some());
        assert!(can_use_fast_path("UPDATE users SET name = ?").is_some());
        assert!(can_use_fast_path("DELETE FROM users WHERE id = ?").is_some());
        
        // Complex queries that should not use fast path
        assert!(can_use_fast_path("SELECT * FROM users JOIN posts").is_none());
        assert!(can_use_fast_path("SELECT * FROM (SELECT * FROM users)").is_none());
        assert!(can_use_fast_path("UPDATE users SET name = (SELECT name FROM other)").is_none());
    }
    
    #[test]
    fn test_batch_insert_fast_path() {
        // Simple batch INSERTs that should use fast path
        assert_eq!(
            can_batch_insert_use_fast_path("INSERT INTO users (id, name) VALUES (1, 'test'), (2, 'test2')"),
            Some("users".to_string())
        );
        assert_eq!(
            can_batch_insert_use_fast_path("INSERT INTO products (id, price) VALUES (1, 99.99), (2, 149.99), (3, 199.99)"),
            Some("products".to_string())
        );
        
        // Batch INSERTs with datetime that should NOT use fast path
        assert!(can_batch_insert_use_fast_path("INSERT INTO orders (id, date) VALUES (1, '2024-01-01'), (2, '2024-01-02')").is_none());
        assert!(can_batch_insert_use_fast_path("INSERT INTO logs (id, time) VALUES (1, '14:30:00'), (2, '15:45:00')").is_none());
        
        // Non-batch INSERT should not match
        assert!(can_batch_insert_use_fast_path("INSERT INTO users (id, name) VALUES (1, 'test')").is_none());
    }
    
    #[test]
    fn test_extract_table_name() {
        assert_eq!(can_use_fast_path("INSERT INTO users (name) VALUES (?)"), Some("users".to_string()));
        assert_eq!(can_use_fast_path("SELECT * FROM products"), Some("products".to_string()));
        assert_eq!(can_use_fast_path("UPDATE items SET price = 10"), Some("items".to_string()));
        assert_eq!(can_use_fast_path("DELETE FROM orders WHERE id = 1"), Some("orders".to_string()));
    }
    
    #[test]
    fn test_enhanced_fast_path_detection() {
        // Simple WHERE clauses that should work
        let query = can_use_fast_path_enhanced("SELECT * FROM users WHERE id = 42");
        assert!(query.is_some());
        let q = query.unwrap();
        assert_eq!(q.table_name, "users");
        assert!(matches!(q.operation, FastPathOperation::Select));
        assert!(q.where_clause.is_some());
        let where_clause = q.where_clause.unwrap();
        assert_eq!(where_clause.column, "id");
        assert_eq!(where_clause.operator, "=");
        assert_eq!(where_clause.value, "42");
        assert!(!where_clause.is_parameter);
        assert_eq!(where_clause.parameter_index, None);
        
        // UPDATE with WHERE
        let query = can_use_fast_path_enhanced("UPDATE products SET price = 100 WHERE id = 5");
        assert!(query.is_some());
        let q = query.unwrap();
        assert_eq!(q.table_name, "products");
        assert!(matches!(q.operation, FastPathOperation::Update));
        
        // DELETE with WHERE
        let query = can_use_fast_path_enhanced("DELETE FROM orders WHERE user_id > 100");
        assert!(query.is_some());
        let q = query.unwrap();
        assert_eq!(q.table_name, "orders");
        assert!(matches!(q.operation, FastPathOperation::Delete));
        
        // Complex queries that should NOT work
        assert!(can_use_fast_path_enhanced("SELECT * FROM users JOIN orders").is_none());
        assert!(can_use_fast_path_enhanced("SELECT * FROM users WHERE id IN (SELECT id FROM active)").is_none());
        assert!(can_use_fast_path_enhanced("SELECT * FROM users ORDER BY name").is_none());
        assert!(can_use_fast_path_enhanced("SELECT * FROM users LIMIT 10").is_none());
        assert!(can_use_fast_path_enhanced("SELECT COUNT(*) FROM users GROUP BY status").is_none());
    }
    
    #[test]
    fn test_where_clause_operators() {
        let operators = ["=", ">", "<", ">=", "<=", "!=", "<>"];
        
        for op in operators {
            let query_str = format!("SELECT * FROM test WHERE col {op} 42");
            let query = can_use_fast_path_enhanced(&query_str);
            assert!(query.is_some(), "Should support operator: {op}");
            let q = query.unwrap();
            assert_eq!(q.where_clause.unwrap().operator, op);
        }
    }
    
    #[test]
    fn test_parameterized_queries() {
        // SELECT with parameter
        let query = can_use_fast_path_enhanced("SELECT * FROM users WHERE id = $1");
        assert!(query.is_some());
        let q = query.unwrap();
        assert_eq!(q.table_name, "users");
        assert!(matches!(q.operation, FastPathOperation::Select));
        let where_clause = q.where_clause.unwrap();
        assert_eq!(where_clause.column, "id");
        assert_eq!(where_clause.operator, "=");
        assert_eq!(where_clause.value, "$1");
        assert!(where_clause.is_parameter);
        assert_eq!(where_clause.parameter_index, Some(1));
        
        // UPDATE with parameter
        let query = can_use_fast_path_enhanced("UPDATE products SET price = 100 WHERE id = $2");
        assert!(query.is_some());
        let q = query.unwrap();
        assert_eq!(q.table_name, "products");
        assert!(matches!(q.operation, FastPathOperation::Update));
        let where_clause = q.where_clause.unwrap();
        assert_eq!(where_clause.column, "id");
        assert_eq!(where_clause.operator, "=");
        assert_eq!(where_clause.value, "$2");
        assert!(where_clause.is_parameter);
        assert_eq!(where_clause.parameter_index, Some(2));
        
        // DELETE with parameter
        let query = can_use_fast_path_enhanced("DELETE FROM orders WHERE user_id > $1");
        assert!(query.is_some());
        let q = query.unwrap();
        assert_eq!(q.table_name, "orders");
        assert!(matches!(q.operation, FastPathOperation::Delete));
        let where_clause = q.where_clause.unwrap();
        assert_eq!(where_clause.column, "user_id");
        assert_eq!(where_clause.operator, ">");
        assert_eq!(where_clause.value, "$1");
        assert!(where_clause.is_parameter);
        assert_eq!(where_clause.parameter_index, Some(1));
        
        // Test different parameter operators
        let operators = ["=", ">", "<", ">=", "<=", "!=", "<>"];
        for op in operators {
            let query_str = format!("SELECT * FROM test WHERE col {op} $1");
            let query = can_use_fast_path_enhanced(&query_str);
            assert!(query.is_some(), "Should support parameterized operator: {op}");
            let q = query.unwrap();
            let where_clause = q.where_clause.unwrap();
            assert_eq!(where_clause.operator, op);
            assert!(where_clause.is_parameter);
            assert_eq!(where_clause.parameter_index, Some(1));
        }
    }
}