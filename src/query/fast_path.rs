use rusqlite::{Connection, types::ValueRef};
use regex::Regex;
use once_cell::sync::Lazy;
use crate::cache::SchemaCache;
use crate::session::db_handler::DbResponse;

// Pre-compiled regexes for fast path detection
static INSERT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*INSERT\s+INTO\s+(\w+)\s*\(").unwrap()
});

static SELECT_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SELECT\s+.+\s+FROM\s+(\w+)").unwrap()
});

static UPDATE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*UPDATE\s+(\w+)\s+SET").unwrap()
});

static DELETE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*DELETE\s+FROM\s+(\w+)").unwrap()
});

/// Check if a query is simple enough for fast path execution
pub fn can_use_fast_path(query: &str) -> Option<String> {
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

/// Check if a table has any DECIMAL columns that would require query rewriting
pub fn table_has_decimal_columns(
    conn: &Connection,
    table_name: &str,
    schema_cache: &SchemaCache,
) -> Result<bool, rusqlite::Error> {
    // Try cache first
    if let Some(schema) = schema_cache.get(table_name) {
        for col in &schema.columns {
            if col.pg_type.to_uppercase() == "NUMERIC" || 
               col.pg_type.to_uppercase() == "DECIMAL" {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    
    // Fall back to checking metadata
    let mut stmt = conn.prepare(
        "SELECT COUNT(*) FROM __pgsqlite_schema 
         WHERE table_name = ?1 AND pg_type IN ('NUMERIC', 'DECIMAL')"
    )?;
    
    let count: i32 = stmt.query_row([table_name], |row| row.get(0))?;
    Ok(count > 0)
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
        if query.trim().to_uppercase().starts_with("SELECT") {
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
        if !query.trim().to_uppercase().starts_with("SELECT") {
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
                
                // Check for boolean columns in the schema
                let mut column_types = Vec::new();
                for col_name in &columns {
                    // Try to get type from __pgsqlite_schema
                    if let Ok(mut meta_stmt) = conn.prepare(
                        "SELECT pg_type FROM __pgsqlite_schema WHERE table_name = ?1 AND column_name = ?2"
                    ) {
                        if let Ok(pg_type) = meta_stmt.query_row([&table_name, col_name], |row| {
                            row.get::<_, String>(0)
                        }) {
                            column_types.push(Some(pg_type));
                        } else {
                            column_types.push(None);
                        }
                    } else {
                        column_types.push(None);
                    }
                }
                
                // Get rows - with boolean type conversions
                let mut rows = Vec::new();
                let result_rows = stmt.query_map([], |row| {
                    let mut values = Vec::new();
                    for i in 0..column_count {
                        match row.get_ref(i)? {
                            ValueRef::Null => values.push(None),
                            ValueRef::Integer(int_val) => {
                                // Check if this column is a boolean type
                                let is_boolean = column_types.get(i)
                                    .and_then(|opt| opt.as_ref())
                                    .map(|pg_type| {
                                        let type_lower = pg_type.to_lowercase();
                                        type_lower == "boolean" || type_lower == "bool"
                                    })
                                    .unwrap_or(false);
                                
                                if is_boolean {
                                    // Convert SQLite's 0/1 to PostgreSQL's f/t format
                                    let bool_str = if int_val == 0 { "f" } else { "t" };
                                    values.push(Some(bool_str.as_bytes().to_vec()));
                                } else {
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
    fn test_extract_table_name() {
        assert_eq!(can_use_fast_path("INSERT INTO users (name) VALUES (?)"), Some("users".to_string()));
        assert_eq!(can_use_fast_path("SELECT * FROM products"), Some("products".to_string()));
        assert_eq!(can_use_fast_path("UPDATE items SET price = 10"), Some("items".to_string()));
        assert_eq!(can_use_fast_path("DELETE FROM orders WHERE id = 1"), Some("orders".to_string()));
    }
}