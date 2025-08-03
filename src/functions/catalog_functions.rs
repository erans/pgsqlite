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
    
    // to_regtype(typename) - converts type name to OID, returns NULL for non-existent types
    conn.create_scalar_function(
        "to_regtype",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let type_name: String = ctx.get(0)?;
            
            // Map common PostgreSQL types to their OIDs
            let oid = match type_name.to_lowercase().as_str() {
                "bool" | "boolean" => Some(16),
                "bytea" => Some(17),
                "int8" | "bigint" => Some(20),
                "int2" | "smallint" => Some(21),
                "int4" | "int" | "integer" => Some(23),
                "text" => Some(25),
                "json" => Some(114),
                "float4" | "real" => Some(700),
                "float8" | "double precision" => Some(701),
                "char" => Some(1042),
                "varchar" | "character varying" => Some(1043),
                "date" => Some(1082),
                "time" => Some(1083),
                "timestamp" => Some(1114),
                "timestamptz" | "timestamp with time zone" => Some(1184),
                "interval" => Some(1186),
                "timetz" | "time with time zone" => Some(1266),
                "bit" => Some(1560),
                "varbit" | "bit varying" => Some(1562),
                "numeric" | "decimal" => Some(1700),
                "uuid" => Some(2950),
                "tsvector" => Some(3614),
                "tsquery" => Some(3615),
                "regconfig" => Some(3734),
                "jsonb" => Some(3802),
                // Array types
                "_bool" | "bool[]" | "boolean[]" => Some(1000),
                "_bytea" | "bytea[]" => Some(1001),
                "_int2" | "int2[]" | "smallint[]" => Some(1005),
                "_int4" | "int4[]" | "int[]" | "integer[]" => Some(1007),
                "_text" | "text[]" => Some(1009),
                "_char" | "char[]" => Some(1014),
                "_varchar" | "varchar[]" => Some(1015),
                "_int8" | "int8[]" | "bigint[]" => Some(1016),
                "_float4" | "float4[]" | "real[]" => Some(1021),
                "_float8" | "float8[]" | "double precision[]" => Some(1022),
                "_timestamp" | "timestamp[]" => Some(1115),
                "_date" | "date[]" => Some(1182),
                "_time" | "time[]" => Some(1183),
                "_timestamptz" | "timestamptz[]" => Some(1185),
                "_interval" | "interval[]" => Some(1187),
                "_numeric" | "numeric[]" | "decimal[]" => Some(1231),
                "_timetz" | "timetz[]" => Some(1270),
                "_bit" | "bit[]" => Some(1561),
                "_varbit" | "varbit[]" => Some(1563),
                "_uuid" | "uuid[]" => Some(2951),
                "_tsvector" | "tsvector[]" => Some(3643),
                "_tsquery" | "tsquery[]" => Some(3645),
                "_regconfig" | "regconfig[]" => Some(3735),
                "_jsonb" | "jsonb[]" => Some(3807),
                "_json" | "json[]" => Some(199),
                // Extensions and unknown types return NULL
                "hstore" | "citext" | "ltree" | "cube" | "postgis" => None,
                _ => {
                    // Check if it's a schema-qualified name
                    if let Some((_schema, typename)) = type_name.split_once('.') {
                        // Try again with just the type name
                        match typename.to_lowercase().as_str() {
                            "hstore" | "citext" | "ltree" | "cube" => None,
                            _ => None,
                        }
                    } else {
                        None
                    }
                }
            };
            
            match oid {
                Some(oid) => Ok(rusqlite::types::Value::Integer(oid as i64)),
                None => Ok(rusqlite::types::Value::Null),
            }
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