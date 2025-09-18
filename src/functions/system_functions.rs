use rusqlite::{Connection, Result, functions::FunctionFlags};
use tracing::debug;

/// Register PostgreSQL system information functions
pub fn register_system_functions(conn: &Connection) -> Result<()> {
    debug!("Registering system functions");
    
    // version() - Returns PostgreSQL version string
    conn.create_scalar_function(
        "version",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            // Return a PostgreSQL-compatible version string
            // This format is what SQLAlchemy expects to parse
            Ok(format!("PostgreSQL 15.0 (pgsqlite {}) on x86_64-pc-linux-gnu, compiled by rustc, 64-bit", 
                env!("CARGO_PKG_VERSION")))
        },
    )?;
    
    // current_database() - Returns the current database name
    conn.create_scalar_function(
        "current_database",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            // In SQLite, we'll return "main" as the database name
            Ok("main".to_string())
        },
    )?;
    
    // current_schema() - Returns the current schema name
    conn.create_scalar_function(
        "current_schema",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            // SQLite doesn't have schemas, return "public" for PostgreSQL compatibility
            Ok("public".to_string())
        },
    )?;
    
    // current_schemas(include_implicit) - Returns array of schemas in search path
    conn.create_scalar_function(
        "current_schemas",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let include_implicit: bool = ctx.get(0)?;
            if include_implicit {
                // Include system schemas
                Ok(r#"["pg_catalog","public"]"#.to_string())
            } else {
                // Just user schemas
                Ok(r#"["public"]"#.to_string())
            }
        },
    )?;
    
    // current_user() - Returns the current user name
    conn.create_scalar_function(
        "current_user",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            // Return a default PostgreSQL-like username
            Ok("postgres".to_string())
        },
    )?;
    
    // session_user() - Returns the session user name
    conn.create_scalar_function(
        "session_user",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            // Return the same as current_user
            Ok("postgres".to_string())
        },
    )?;
    
    // pg_backend_pid() - Returns the backend process ID
    conn.create_scalar_function(
        "pg_backend_pid",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            // Return the current process ID
            Ok(std::process::id() as i32)
        },
    )?;
    
    // pg_is_in_recovery() - Returns whether server is in recovery mode
    conn.create_scalar_function(
        "pg_is_in_recovery",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            // SQLite is never in recovery mode
            Ok(0i32) // false in SQLite boolean representation
        },
    )?;
    
    // pg_database_size(name) - Returns database size in bytes
    conn.create_scalar_function(
        "pg_database_size",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let _db_name: String = ctx.get(0)?;
            // For SQLite, we can't easily get the database size without file access
            // Return a reasonable default size
            Ok(8192i64) // 8KB minimum SQLite database size
        },
    )?;
    
    // pg_postmaster_start_time() - Returns server start time
    conn.create_scalar_function(
        "pg_postmaster_start_time",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            // Return current timestamp as a reasonable approximation
            use chrono::{DateTime, Utc};
            let now: DateTime<Utc> = Utc::now();
            Ok(now.format("%Y-%m-%d %H:%M:%S.%f%:z").to_string())
        },
    )?;
    
    // pg_conf_load_time() - Returns configuration load time
    conn.create_scalar_function(
        "pg_conf_load_time",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            // Return current timestamp
            use chrono::{DateTime, Utc};
            let now: DateTime<Utc> = Utc::now();
            Ok(now.format("%Y-%m-%d %H:%M:%S.%f%:z").to_string())
        },
    )?;
    
    // inet_client_addr() - Returns client's IP address
    conn.create_scalar_function(
        "inet_client_addr",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            // Return localhost as default
            Ok("127.0.0.1".to_string())
        },
    )?;
    
    // inet_client_port() - Returns client's port number
    conn.create_scalar_function(
        "inet_client_port",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            // Return a typical PostgreSQL client port
            Ok(5432i32)
        },
    )?;
    
    // inet_server_addr() - Returns server's IP address
    conn.create_scalar_function(
        "inet_server_addr",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            // Return localhost as default
            Ok("127.0.0.1".to_string())
        },
    )?;
    
    // inet_server_port() - Returns server's port number
    conn.create_scalar_function(
        "inet_server_port",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            // Return the standard PostgreSQL port
            Ok(5432i32)
        },
    )?;
    
    // pg_has_role(user, role, privilege) - Check if user has role privilege
    conn.create_scalar_function(
        "pg_has_role",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _user: String = ctx.get(0)?;
            let _role: String = ctx.get(1)?;
            let _privilege: String = ctx.get(2)?;
            // In SQLite, always return true for compatibility
            Ok(1i32) // true in SQLite boolean representation
        },
    )?;
    
    // has_database_privilege(user, database, privilege) - Check database privilege
    conn.create_scalar_function(
        "has_database_privilege",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _user: String = ctx.get(0)?;
            let _database: String = ctx.get(1)?;
            let _privilege: String = ctx.get(2)?;
            // In SQLite, always return true for compatibility
            Ok(1i32) // true
        },
    )?;
    
    // has_schema_privilege(user, schema, privilege) - Check schema privilege
    conn.create_scalar_function(
        "has_schema_privilege",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _user: String = ctx.get(0)?;
            let _schema: String = ctx.get(1)?;
            let _privilege: String = ctx.get(2)?;
            // In SQLite, always return true for compatibility
            Ok(1i32) // true
        },
    )?;
    
    // has_table_privilege(user, table, privilege) - Check table privilege
    conn.create_scalar_function(
        "has_table_privilege",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _user: String = ctx.get(0)?;
            let _table: String = ctx.get(1)?;
            let _privilege: String = ctx.get(2)?;
            // In SQLite, always return true for compatibility
            Ok(1i32) // true
        },
    )?;
    
    // pg_get_userbyid(user_oid) - Returns username for user OID
    conn.create_scalar_function(
        "pg_get_userbyid",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _user_oid: i64 = ctx.get(0)?;
            // SQLite doesn't have users, return a default user
            // This matches what psql expects for the \d command
            Ok("postgres".to_string())
        },
    )?;
    
    // obj_description(object_oid, catalog_name) - Returns comment for database object
    conn.create_scalar_function(
        "obj_description",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let _object_oid: i64 = ctx.get(0)?;
            let _catalog_name: String = ctx.get(1)?;
            
            // For SQLite functions, we can't easily access the connection
            // So we return NULL for now - this will be handled by query interceptor
            // or comment function processor
            Ok(Option::<String>::None)
        },
    )?;
    
    // obj_description(object_oid) - Deprecated one-parameter form
    conn.create_scalar_function(
        "obj_description",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let _object_oid: i64 = ctx.get(0)?;
            // Use the two-parameter version with default catalog
            // For now, return NULL - will be handled by query interceptor for real queries
            Ok(Option::<String>::None)
        },
    )?;
    
    // col_description(table_oid, column_number) - Returns comment for table column
    conn.create_scalar_function(
        "col_description",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let _table_oid: i64 = ctx.get(0)?;
            let _column_number: i32 = ctx.get(1)?;
            
            // Query __pgsqlite_comments table for column comment
            // For now, return NULL - will be handled by query interceptor
            Ok(Option::<String>::None)
        },
    )?;

    // pg_size_pretty(size_in_bytes) - Format size in bytes as human-readable string
    conn.create_scalar_function(
        "pg_size_pretty",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Try to get the value as either i64 or string that can be parsed
            let size_bytes = match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Integer => ctx.get::<i64>(0)?,
                rusqlite::types::Type::Text => {
                    let text: String = ctx.get(0)?;
                    match text.parse::<i64>() {
                        Ok(value) => value,
                        Err(_) => return Ok(Option::<String>::None), // Return NULL for invalid strings
                    }
                }
                rusqlite::types::Type::Null => {
                    return Ok(Option::<String>::None);
                }
                _ => {
                    return Err(rusqlite::Error::UserFunctionError("Invalid size type".into()));
                }
            };

            Ok(Some(format_size_pretty(size_bytes)))
        },
    )?;

    // pg_size_pretty() - No argument version returns NULL
    conn.create_scalar_function(
        "pg_size_pretty",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            Ok(Option::<String>::None)
        },
    )?;

    debug!("System functions registered successfully");
    Ok(())
}

/// Format size in bytes as human-readable string using PostgreSQL's algorithm
/// Uses binary prefixes: 1 kB = 1024 bytes, 1 MB = 1024² bytes, etc.
/// Based on PostgreSQL source code in src/backend/utils/adt/dbsize.c
fn format_size_pretty(mut size: i64) -> String {
    let abs_size = size.unsigned_abs();

    // PostgreSQL unit definitions
    const BYTES_LIMIT: u64 = 10 * 1024;  // 10240 bytes
    const UNIT_LIMIT: u64 = 20 * 1024 - 1;  // 20479 (for kB, MB, GB, TB, PB)

    // Check if we should display as bytes
    if abs_size < BYTES_LIMIT {
        return format!("{} bytes", size);
    }

    // Convert to kB and check limit
    size = (size + 512) / 1024; // Half-rounded division
    let abs_size_kb = size.unsigned_abs();
    if abs_size_kb < UNIT_LIMIT {
        return format!("{} kB", size);
    }

    // Convert to MB and check limit
    size = (size + 512) / 1024; // Half-rounded division
    let abs_size_mb = size.unsigned_abs();
    if abs_size_mb < UNIT_LIMIT {
        return format!("{} MB", size);
    }

    // Convert to GB and check limit
    size = (size + 512) / 1024; // Half-rounded division
    let abs_size_gb = size.unsigned_abs();
    if abs_size_gb < UNIT_LIMIT {
        return format!("{} GB", size);
    }

    // Convert to TB and check limit
    size = (size + 512) / 1024; // Half-rounded division
    let abs_size_tb = size.unsigned_abs();
    if abs_size_tb < UNIT_LIMIT {
        return format!("{} TB", size);
    }

    // Convert to PB (final unit)
    size = (size + 512) / 1024; // Half-rounded division
    format!("{} PB", size)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_version_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        let version: String = conn.query_row("SELECT version()", [], |row| row.get(0)).unwrap();
        assert!(version.starts_with("PostgreSQL"));
        assert!(version.contains("pgsqlite"));
    }
    
    #[test]
    fn test_current_database() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        let db_name: String = conn.query_row("SELECT current_database()", [], |row| row.get(0)).unwrap();
        assert_eq!(db_name, "main");
    }
    
    #[test]
    fn test_current_schema() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        let schema: String = conn.query_row("SELECT current_schema()", [], |row| row.get(0)).unwrap();
        assert_eq!(schema, "public");
    }
    
    #[test]
    fn test_current_user() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        let user: String = conn.query_row("SELECT current_user()", [], |row| row.get(0)).unwrap();
        assert_eq!(user, "postgres");
    }
    
    #[test]
    fn test_pg_backend_pid() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        let pid: i32 = conn.query_row("SELECT pg_backend_pid()", [], |row| row.get(0)).unwrap();
        assert!(pid > 0);
    }
    
    #[test]
    fn test_pg_is_in_recovery() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        let in_recovery: i32 = conn.query_row("SELECT pg_is_in_recovery()", [], |row| row.get(0)).unwrap();
        assert_eq!(in_recovery, 0); // false
    }
    
    #[test]
    fn test_privilege_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        // Test pg_has_role
        let has_role: i32 = conn.query_row(
            "SELECT pg_has_role('postgres', 'pg_read_all_data', 'USAGE')", 
            [], 
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_role, 1); // true
        
        // Test has_database_privilege
        let has_db_priv: i32 = conn.query_row(
            "SELECT has_database_privilege('postgres', 'main', 'CREATE')", 
            [], 
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_db_priv, 1); // true
        
        // Test has_schema_privilege
        let has_schema_priv: i32 = conn.query_row(
            "SELECT has_schema_privilege('postgres', 'public', 'CREATE')", 
            [], 
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_schema_priv, 1); // true
        
        // Test has_table_privilege
        let has_table_priv: i32 = conn.query_row(
            "SELECT has_table_privilege('postgres', 'pg_class', 'SELECT')", 
            [], 
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_table_priv, 1); // true
    }
    
    #[test]
    fn test_obj_description() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        // Test two-parameter form (returns NULL since no comments table)
        let desc: Option<String> = conn.query_row(
            "SELECT obj_description(123456, 'pg_class')", 
            [], 
            |row| row.get(0)
        ).unwrap();
        assert_eq!(desc, None); // Should return NULL
        
        // Test one-parameter form (deprecated)
        let desc: Option<String> = conn.query_row(
            "SELECT obj_description(123456)", 
            [], 
            |row| row.get(0)
        ).unwrap();
        assert_eq!(desc, None); // Should return NULL
    }
    
    #[test]
    fn test_col_description() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();
        
        // Test col_description function (returns NULL since no comments table)
        let desc: Option<String> = conn.query_row(
            "SELECT col_description(123456, 1)", 
            [], 
            |row| row.get(0)
        ).unwrap();
        assert_eq!(desc, None); // Should return NULL
    }
}