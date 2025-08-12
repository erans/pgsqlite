use rusqlite::{Connection, types::ValueRef};
use crate::types::PgType;

/// Get PostgreSQL type OID from SQLite column information
pub fn get_pg_type_oid_from_sqlite(
    conn: &Connection, 
    table_name: Option<&str>,
    column_name: &str,
    value: Option<&ValueRef>
) -> i32 {
    // First, try to get type from table schema if we have table name
    if let Some(table) = table_name
        && let Ok(type_oid) = get_type_from_schema(conn, table, column_name) {
            return type_oid;
        }
    
    // Fall back to value-based inference
    match value {
        Some(ValueRef::Null) => PgType::Text.to_oid(), // text for NULL
        Some(ValueRef::Integer(i)) => {
            // Default to int4 for most integers, only use int8 for very large values
            if *i >= i32::MIN as i64 && *i <= i32::MAX as i64 {
                PgType::Int4.to_oid() // int4
            } else {
                PgType::Int8.to_oid() // int8
            }
        },
        Some(ValueRef::Real(_)) => PgType::Float8.to_oid(), // float8
        Some(ValueRef::Text(_)) => PgType::Text.to_oid(),  // text
        Some(ValueRef::Blob(_)) => PgType::Bytea.to_oid(),   // bytea
        None => PgType::Text.to_oid(), // Default to text
    }
}

/// Get type from SQLite schema
fn get_type_from_schema(conn: &Connection, table_name: &str, column_name: &str) -> Result<i32, rusqlite::Error> {
    // First check if we have stored type metadata in __pgsqlite_schema
    if let Some(type_oid) = crate::types::SchemaTypeMapper::get_type_from_schema(conn, table_name, column_name) {
        return Ok(type_oid);
    }
    
    // Fall back to PRAGMA table_info
    let query = format!("PRAGMA table_info({table_name})");
    let mut stmt = conn.prepare(&query)?;
    
    let mut rows = stmt.query_map([], |row| {
        let col_name: String = row.get(1)?;
        let col_type: String = row.get(2)?;
        Ok((col_name, col_type))
    })?;
    
    while let Some(Ok((col_name, col_type))) = rows.next() {
        if col_name == column_name {
            return Ok(sqlite_type_to_pg_oid(&col_type));
        }
    }
    
    // Column not found
    Err(rusqlite::Error::QueryReturnedNoRows)
}

/// Convert SQLite type declaration to PostgreSQL OID
pub fn sqlite_type_to_pg_oid(sqlite_type: &str) -> i32 {
    let type_upper = sqlite_type.to_uppercase();
    
    // Check for specific types first
    if type_upper.contains("BLOB") {
        return PgType::Bytea.to_oid(); // bytea
    }
    
    if type_upper.contains("REAL") || type_upper.contains("FLOAT") || type_upper.contains("DOUBLE") {
        return PgType::Float8.to_oid(); // float8
    }
    
    if type_upper.contains("INT") {
        // Check for size hints
        if type_upper.contains("INT2") || type_upper.contains("SMALLINT") {
            return PgType::Int2.to_oid(); // int2
        } else if type_upper.contains("INT8") || type_upper.contains("BIGINT") {
            return PgType::Int8.to_oid(); // int8
        } else {
            return PgType::Int4.to_oid(); // int4 (default for INTEGER)
        }
    }
    
    if type_upper.contains("BOOL") {
        return PgType::Bool.to_oid(); // bool
    }
    
    if type_upper.contains("DATE") && !type_upper.contains("TIME") {
        return PgType::Date.to_oid(); // date
    }
    
    if type_upper.contains("TIME") && !type_upper.contains("STAMP") {
        return PgType::Time.to_oid(); // time
    }
    
    if type_upper.contains("TIMESTAMP") {
        return PgType::Timestamp.to_oid(); // timestamp
    }
    
    if type_upper.contains("NUMERIC") || type_upper.contains("DECIMAL") || type_upper == "DECIMAL" {
        return PgType::Numeric.to_oid(); // numeric
    }
    
    if type_upper.contains("UUID") {
        return PgType::Uuid.to_oid(); // uuid
    }
    
    if type_upper.contains("JSON") {
        return PgType::Json.to_oid(); // json
    }
    
    // Default to text
    PgType::Text.to_oid() // text
}

/// Infer PostgreSQL type from a text value
pub fn infer_pg_type_from_text(value: &str) -> i32 {
    // Try boolean - but be careful not to confuse with regular integers
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") 
        || value == "t" || value == "f" {
        return PgType::Bool.to_oid(); // bool
    }
    
    // Try integer types
    if let Ok(i) = value.parse::<i64>() {
        // Default to int4 for most integers, only use int8 for very large values
        // This matches PostgreSQL behavior better where int4 is the default integer type
        if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
            return PgType::Int4.to_oid(); // int4
        } else {
            return PgType::Int8.to_oid(); // int8
        }
    }
    
    // Try float
    if value.parse::<f64>().is_ok() {
        return PgType::Float8.to_oid(); // float8
    }
    
    // Check for UUID pattern
    if value.len() == 36 && value.chars().filter(|&c| c == '-').count() == 4 {
        let parts: Vec<&str> = value.split('-').collect();
        if parts.len() == 5 
            && parts[0].len() == 8 
            && parts[1].len() == 4 
            && parts[2].len() == 4 
            && parts[3].len() == 4 
            && parts[4].len() == 12 
            && parts.iter().all(|p| p.chars().all(|c| c.is_ascii_hexdigit())) {
            return PgType::Uuid.to_oid(); // uuid
        }
    }
    
    // Check for JSON
    if (value.starts_with('{') && value.ends_with('}')) 
        || (value.starts_with('[') && value.ends_with(']')) {
        return PgType::Json.to_oid(); // json
    }
    
    // For date/time patterns, return text type since SQLite stores them as text
    // and we can't be sure about the format without more context
    
    // Default to text
    PgType::Text.to_oid() // text
}