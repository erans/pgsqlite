use rusqlite::{Connection, Result, functions::FunctionFlags};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use tracing::debug;

/// Register hash functions for OID generation
pub fn register_hash_functions(conn: &Connection) -> Result<()> {
    debug!("Registering hash functions");
    
    // hash(text) - generates a stable hash from text
    conn.create_scalar_function(
        "hash",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let text: String = ctx.get(0)?;
            let mut hasher = DefaultHasher::new();
            text.hash(&mut hasher);
            Ok(hasher.finish() as i64)
        },
    )?;
    
    // oid_hash(text) - generates PostgreSQL-compatible OID from text
    conn.create_scalar_function(
        "oid_hash",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let text: String = ctx.get(0)?;
            let mut hasher = DefaultHasher::new();
            text.hash(&mut hasher);
            // Keep it positive and in a reasonable range for OIDs
            Ok(((hasher.finish() & 0x7FFFFFFF) % 1000000 + 16384) as i32)
        },
    )?;
    
    debug!("Hash functions registered successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hash_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_hash_functions(&conn).unwrap();
        
        // Test hash function
        let hash1: i64 = conn
            .query_row("SELECT hash('test_table')", [], |row| row.get(0))
            .unwrap();
        let hash2: i64 = conn
            .query_row("SELECT hash('test_table')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(hash1, hash2); // Should be deterministic
        
        // Different inputs should produce different hashes
        let hash3: i64 = conn
            .query_row("SELECT hash('other_table')", [], |row| row.get(0))
            .unwrap();
        assert_ne!(hash1, hash3);
    }
    
    #[test]
    fn test_oid_hash_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_hash_functions(&conn).unwrap();
        
        // Test oid_hash function
        let oid1: i32 = conn
            .query_row("SELECT oid_hash('test_table')", [], |row| row.get(0))
            .unwrap();
        let oid2: i32 = conn
            .query_row("SELECT oid_hash('test_table')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(oid1, oid2); // Should be deterministic
        assert!(oid1 >= 16384); // Should be in the expected range
        assert!(oid1 < 1016384); // Should be less than max range
    }
}