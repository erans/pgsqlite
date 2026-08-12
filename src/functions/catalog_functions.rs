use rusqlite::{Connection, Result, functions::FunctionFlags};
use tracing::debug;

/// Register PostgreSQL catalog-related functions
pub fn register_catalog_functions(conn: &Connection) -> Result<()> {
    debug!("Registering catalog functions");
    
    // pg_table_is_visible(oid) - checks if table is in search path
    // For SQLite, all tables are visible
    conn.create_scalar_function(
        "pg_table_is_visible",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Accept either integer or text OID
            // Try to get as i64 first, if that fails try as string and parse
            let _oid = match ctx.get::<i64>(0) {
                Ok(oid) => oid,
                Err(_) => {
                    // Try as string
                    let oid_str: String = ctx.get(0)?;
                    oid_str.parse::<i64>().unwrap_or(0)
                }
            };
            // In SQLite, all tables are visible
            // Return 1 for true (SQLite boolean convention)
            Ok(1i32)
        },
    )?;
    
    // Note: SQLite doesn't support schema-qualified function names,
    // so we handle pg_catalog.pg_table_is_visible through query rewriting
    
    // regclass type cast function
    conn.create_scalar_function(
        "regclass",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let table_name: String = ctx.get(0)?;
            
            // Look up table OID from pg_class view
            // For now, just generate a consistent OID
            let oid = generate_table_oid(&table_name);
            Ok(oid)
        },
    )?;
    
    // __pgsqlite_relnamespace(name) - namespace OID for a relation.
    // Lets catalog views ask "is this relation ours?" by exact name rather than
    // by prefix. Returns NULL for NULL input so a view row degrades rather than
    // failing the query.
    conn.create_scalar_function(
        "__pgsqlite_relnamespace",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let name: Option<String> = ctx.get(0)?;
            Ok(name.map(|n| crate::catalog::internal_relations::relnamespace(&n)))
        },
    )?;

    debug!("Catalog functions registered successfully");
    Ok(())
}

// Generate a stable OID from table name
fn generate_table_oid(name: &str) -> i32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    ((hasher.finish() & 0x7FFFFFFF) % 1000000 + 16384) as i32
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_pg_table_is_visible() {
        let conn = Connection::open_in_memory().unwrap();
        register_catalog_functions(&conn).unwrap();
        
        // Test with any OID - should always return true
        let result: bool = conn
            .query_row("SELECT pg_table_is_visible(12345)", [], |row| row.get(0))
            .unwrap();
        assert!(result);
    }
    
    #[test]
    fn test_regclass_cast() {
        let conn = Connection::open_in_memory().unwrap();
        register_catalog_functions(&conn).unwrap();
        
        // Test regclass cast
        let oid: i32 = conn
            .query_row("SELECT regclass('test_table')", [], |row| row.get(0))
            .unwrap();
        assert!(oid > 0);
        
        // Same table name should produce same OID
        let oid2: i32 = conn
            .query_row("SELECT regclass('test_table')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(oid, oid2);
    }
}