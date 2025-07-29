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
            Ok("PostgreSQL 15.0 (pgsqlite 0.0.11) on x86_64-pc-linux-gnu, compiled by rustc, 64-bit".to_string())
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
    
    debug!("System functions registered successfully");
    Ok(())
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
}