use rusqlite::{Connection, Result};
use rusqlite::functions::FunctionFlags;
use crate::types::{UuidHandler, generate_uuid_v4};

/// Register UUID-related functions in SQLite
pub fn register_uuid_functions(conn: &Connection) -> Result<()> {
    // gen_random_uuid() - PostgreSQL compatible UUID v4 generator
    // Note: SQLite may cache function results in certain contexts, but each call generates a new UUID
    conn.create_scalar_function(
        "gen_random_uuid",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            Ok(generate_uuid_v4())
        },
    )?;
    
    // uuid_generate_v4() - Alternative name for compatibility
    conn.create_scalar_function(
        "uuid_generate_v4",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            Ok(generate_uuid_v4())
        },
    )?;
    
    // is_valid_uuid(text) - Check if a string is a valid UUID
    conn.create_scalar_function(
        "is_valid_uuid",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let value: String = ctx.get(0)?;
            Ok(UuidHandler::validate_uuid(&value))
        },
    )?;
    
    // uuid_normalize(text) - Normalize UUID to lowercase
    conn.create_scalar_function(
        "uuid_normalize",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let value: Result<String> = ctx.get(0);
            match value {
                Ok(v) if UuidHandler::validate_uuid(&v) => Ok(Some(UuidHandler::normalize_uuid(&v))),
                _ => Ok(None),
            }
        },
    )?;
    
    // Create a collation for UUID comparison (case-insensitive)
    conn.create_collation("uuid", |a, b| {
        a.to_lowercase().cmp(&b.to_lowercase())
    })?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    #[test]
    fn test_uuid_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_uuid_functions(&conn).unwrap();
        
        // Test gen_random_uuid
        let uuid: String = conn.query_row("SELECT gen_random_uuid()", [], |row| row.get(0)).unwrap();
        assert!(UuidHandler::validate_uuid(&uuid));
        
        // Test uuid_generate_v4
        let uuid2: String = conn.query_row("SELECT uuid_generate_v4()", [], |row| row.get(0)).unwrap();
        assert!(UuidHandler::validate_uuid(&uuid2));
        assert_ne!(uuid, uuid2); // Should generate different UUIDs
        
        // Test is_valid_uuid
        let valid: bool = conn.query_row("SELECT is_valid_uuid(?)", ["550e8400-e29b-41d4-a716-446655440000"], |row| row.get(0)).unwrap();
        assert!(valid);
        
        let invalid: bool = conn.query_row("SELECT is_valid_uuid(?)", ["not-a-uuid"], |row| row.get(0)).unwrap();
        assert!(!invalid);
        
        // Test uuid_normalize
        let normalized: String = conn.query_row("SELECT uuid_normalize(?)", ["550E8400-E29B-41D4-A716-446655440000"], |row| row.get(0)).unwrap();
        assert_eq!(normalized, "550e8400-e29b-41d4-a716-446655440000");
        
        // Test UUID collation
        conn.execute("CREATE TABLE test_uuid (id TEXT COLLATE uuid)", []).unwrap();
        conn.execute("INSERT INTO test_uuid VALUES (?), (?)", 
            ["550E8400-E29B-41D4-A716-446655440000", "550e8400-e29b-41d4-a716-446655440000"]).unwrap();
        
        let count: i32 = conn.query_row(
            "SELECT COUNT(DISTINCT id) FROM test_uuid", 
            [], 
            |row| row.get(0)
        ).unwrap();
        assert_eq!(count, 1); // Should be treated as same UUID due to collation
    }
}