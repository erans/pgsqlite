use rusqlite::{Connection, Result, functions::FunctionFlags};
use tracing::debug;

/// Register PostgreSQL system information functions
/// PATCH v13: map a PostgreSQL type OID (+ typmod) to its SQL type name.
/// Mirrors the OID table previously only available to the AST-rewrite path, but
/// now runs per-row inside SQLite so column-reference arguments work.
fn pg_type_name(oid: i64, typmod: Option<i64>) -> String {
    let t = typmod.unwrap_or(-1);
    match oid {
        16 => "boolean".to_string(),
        17 => "bytea".to_string(),
        18 => "\"char\"".to_string(),
        19 => "name".to_string(),
        20 => "bigint".to_string(),
        21 => "smallint".to_string(),
        23 => "integer".to_string(),
        25 => "text".to_string(),
        26 => "oid".to_string(),
        27 => "tid".to_string(),
        28 => "xid".to_string(),
        29 => "cid".to_string(),
        700 => "real".to_string(),
        701 => "double precision".to_string(),
        790 => "money".to_string(),
        1042 => {
            if t > 4 { format!("character({})", t - 4) } else { "character".to_string() }
        }
        1043 => {
            if t > 4 { format!("character varying({})", t - 4) } else { "character varying".to_string() }
        }
        1082 => "date".to_string(),
        1083 => "time without time zone".to_string(),
        1114 => "timestamp without time zone".to_string(),
        1184 => "timestamp with time zone".to_string(),
        1186 => "interval".to_string(),
        1266 => "time with time zone".to_string(),
        1700 => {
            if t > 4 {
                let precision = (t - 4) >> 16;
                let scale = (t - 4) & 0xFFFF;
                if scale > 0 { format!("numeric({precision},{scale})") } else { format!("numeric({precision})") }
            } else { "numeric".to_string() }
        }
        114 => "json".to_string(),
        3802 => "jsonb".to_string(),
        2950 => "uuid".to_string(),
        600 => "point".to_string(),
        601 => "lseg".to_string(),
        602 => "path".to_string(),
        603 => "box".to_string(),
        604 => "polygon".to_string(),
        628 => "line".to_string(),
        718 => "circle".to_string(),
        829 => "macaddr".to_string(),
        869 => "inet".to_string(),
        650 => "cidr".to_string(),
        1560 => "bit".to_string(),
        1562 => "bit varying".to_string(),
        _ => format!("unknown({oid})"),
    }
}

/// PATCH v13: map a type name to its OID (inverse of pg_type_name).
fn regtype_to_oid(name: &str) -> Option<String> {
    let oid = match name.to_lowercase().as_str() {
        "bool" | "boolean" => 16,
        "bytea" => 17,
        "int8" | "bigint" => 20,
        "int2" | "smallint" => 21,
        "int4" | "integer" | "int" => 23,
        "text" => 25,
        "json" => 114,
        "float4" | "real" => 700,
        "float8" | "double precision" => 701,
        "char" => 1042,
        "varchar" | "character varying" => 1043,
        "date" => 1082,
        "time" => 1083,
        "timestamp" | "timestamp without time zone" => 1114,
        "timestamptz" | "timestamp with time zone" => 1184,
        "interval" => 1186,
        "timetz" | "time with time zone" => 1266,
        "bit" => 1560,
        "varbit" | "bit varying" => 1562,
        "numeric" | "decimal" => 1700,
        "uuid" => 2950,
        "jsonb" => 3802,
        _ => return None,
    };
    Some(oid.to_string())
}

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
            Ok(format!("PostgreSQL 16.0 (pgsqlite {}) on x86_64-pc-linux-gnu, compiled by rustc, 64-bit",
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
    
    // pg_has_role(role, privilege) - Check if current user has role privilege (2-parameter version)
    conn.create_scalar_function(
        "pg_has_role",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let role: String = ctx.get(0)?;
            let privilege: String = ctx.get(1)?;

            // Validate privilege type
            let privilege_upper = privilege.to_uppercase();
            if !matches!(privilege_upper.as_str(), "MEMBER" | "USAGE" | "SET") {
                return Err(rusqlite::Error::UserFunctionError(
                    format!("unrecognized privilege type: \"{}\"", privilege).into()
                ));
            }

            // In SQLite, simulate reasonable PostgreSQL role behavior
            // Most common roles that ORMs check for
            match role.as_str() {
                "pg_read_all_data" | "pg_read_all_settings" | "pg_monitor" => Ok(1i32), // true
                "pg_read_server_files" | "pg_write_server_files" | "pg_execute_server_program" => Ok(0i32), // false - security-sensitive
                _ => Ok(1i32), // Default: assume user has access for compatibility
            }
        },
    )?;

    // pg_has_role(user, role, privilege) - Check if specified user has role privilege (3-parameter version)
    conn.create_scalar_function(
        "pg_has_role",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let user: String = ctx.get(0)?;
            let role: String = ctx.get(1)?;
            let privilege: String = ctx.get(2)?;

            // Validate privilege type
            let privilege_upper = privilege.to_uppercase();
            if !matches!(privilege_upper.as_str(), "MEMBER" | "USAGE" | "SET") {
                return Err(rusqlite::Error::UserFunctionError(
                    format!("unrecognized privilege type: \"{}\"", privilege).into()
                ));
            }

            // In SQLite, simulate reasonable PostgreSQL role behavior
            // Check for common system users that might have restricted access
            if user == "public" {
                // Public role has limited access to sensitive roles
                match role.as_str() {
                    "pg_read_server_files" | "pg_write_server_files" | "pg_execute_server_program" => Ok(0i32), // false
                    _ => Ok(1i32), // true for most roles
                }
            } else {
                // Regular users: check role type
                match role.as_str() {
                    "pg_read_all_data" | "pg_read_all_settings" | "pg_monitor" => Ok(1i32), // true
                    "pg_read_server_files" | "pg_write_server_files" | "pg_execute_server_program" => Ok(0i32), // false - security-sensitive
                    _ => Ok(1i32), // Default: assume user has access for compatibility
                }
            }
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
    
    // has_table_privilege(table, privilege) - Check if current user has table privilege (2-parameter version)
    conn.create_scalar_function(
        "has_table_privilege",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let table: String = ctx.get(0)?;
            let privilege: String = ctx.get(1)?;

            // Validate privilege type
            let privilege_upper = privilege.to_uppercase();
            if !matches!(privilege_upper.as_str(),
                "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE" |
                "REFERENCES" | "TRIGGER" | "MAINTAIN" | "ALL" | "ALL PRIVILEGES") {
                return Err(rusqlite::Error::UserFunctionError(
                    format!("unrecognized privilege type: \"{}\"", privilege).into()
                ));
            }

            // In SQLite, simulate table access control
            // System catalog tables should be readable but not writable
            if table.starts_with("pg_") || table.starts_with("information_schema.") || table.starts_with("information_schema_") {
                match privilege_upper.as_str() {
                    "SELECT" => Ok(1i32), // true - can read system catalogs
                    "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE" => Ok(0i32), // false - cannot modify system catalogs
                    "REFERENCES" | "TRIGGER" => Ok(0i32), // false - cannot create references/triggers on system tables
                    "MAINTAIN" => Ok(0i32), // false - cannot maintain system tables
                    "ALL" | "ALL PRIVILEGES" => Ok(0i32), // false - no full privileges on system tables
                    _ => Ok(0i32), // false by default for system tables
                }
            } else {
                // Regular user tables - grant all privileges for compatibility
                Ok(1i32) // true
            }
        },
    )?;

    // has_table_privilege(user, table, privilege) - Check if specified user has table privilege (3-parameter version)
    conn.create_scalar_function(
        "has_table_privilege",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let user: String = ctx.get(0)?;
            let table: String = ctx.get(1)?;
            let privilege: String = ctx.get(2)?;

            // Validate privilege type
            let privilege_upper = privilege.to_uppercase();
            if !matches!(privilege_upper.as_str(),
                "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE" |
                "REFERENCES" | "TRIGGER" | "MAINTAIN" | "ALL" | "ALL PRIVILEGES") {
                return Err(rusqlite::Error::UserFunctionError(
                    format!("unrecognized privilege type: \"{}\"", privilege).into()
                ));
            }

            // In SQLite, simulate table access control based on user and table
            if user == "public" {
                // Public role has limited access
                if table.starts_with("pg_") || table.starts_with("information_schema.") || table.starts_with("information_schema_") {
                    match privilege_upper.as_str() {
                        "SELECT" => Ok(1i32), // true - public can read most system catalogs
                        _ => Ok(0i32), // false - public cannot modify system catalogs
                    }
                } else {
                    // Public access to user tables depends on privilege
                    match privilege_upper.as_str() {
                        "SELECT" => Ok(1i32), // true - public can typically read user tables
                        "INSERT" | "UPDATE" | "DELETE" => Ok(0i32), // false - public typically cannot modify
                        _ => Ok(0i32), // false by default for public
                    }
                }
            } else {
                // Regular users
                if table.starts_with("pg_") || table.starts_with("information_schema.") || table.starts_with("information_schema_") {
                    match privilege_upper.as_str() {
                        "SELECT" => Ok(1i32), // true - users can read system catalogs
                        "INSERT" | "UPDATE" | "DELETE" | "TRUNCATE" => Ok(0i32), // false - cannot modify system catalogs
                        "REFERENCES" | "TRIGGER" => Ok(0i32), // false - cannot create references/triggers on system tables
                        "MAINTAIN" => Ok(0i32), // false - cannot maintain system tables
                        "ALL" | "ALL PRIVILEGES" => Ok(0i32), // false - no full privileges on system tables
                        _ => Ok(0i32), // false by default for system tables
                    }
                } else {
                    // Regular user tables - grant all privileges for compatibility
                    Ok(1i32) // true
                }
            }
        },
    )?;
    
    // pg_get_userbyid(user_oid) - Returns username for user OID
    conn.create_scalar_function(
        "pg_get_userbyid",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            // PATCH v12: arguments are deliberately not bound. Catalog views hand us
            // TEXT oids, and ctx.get::<i64>() would abort the whole query with
            // "Invalid function parameter type Text at index 0". The values were
            // already unused -- this function has no way to reach the connection.
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
        |_ctx| {
            // PATCH v12: arguments are deliberately not bound. Catalog views hand us
            // TEXT oids, and ctx.get::<i64>() would abort the whole query with
            // "Invalid function parameter type Text at index 0". The values were
            // already unused -- this function has no way to reach the connection.
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
        |_ctx| {
            // PATCH v12: arguments are deliberately not bound. Catalog views hand us
            // TEXT oids, and ctx.get::<i64>() would abort the whole query with
            // "Invalid function parameter type Text at index 0". The values were
            // already unused -- this function has no way to reach the connection.
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
        |_ctx| {
            // PATCH v12: arguments are deliberately not bound. Catalog views hand us
            // TEXT oids, and ctx.get::<i64>() would abort the whole query with
            // "Invalid function parameter type Text at index 0". The values were
            // already unused -- this function has no way to reach the connection.
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

    // === PATCH v13: register GUI column-query system functions as SQLite UDFs ===
    // These run per-row inside SQLite, so they work with column-reference arguments
    // (e.g. format_type(a.atttypid, a.atttypmod)) which the AST-rewrite path cannot evaluate.
    conn.create_scalar_function(
        "format_type",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let typid: Option<i64> = match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Integer => Some(ctx.get::<i64>(0)?),
                rusqlite::types::Type::Text => {
                    let s: String = ctx.get(0)?;
                    s.parse::<i64>().ok()
                }
                _ => None,
            };
            let typmod: Option<i64> = match ctx.get_raw(1).data_type() {
                rusqlite::types::Type::Integer => Some(ctx.get::<i64>(1)?),
                _ => None,
            };
            Ok(typid.map(|oid| pg_type_name(oid, typmod)))
        },
    )?;

    conn.create_scalar_function(
        "pg_get_expr",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Null => Ok(Option::<String>::None),
                rusqlite::types::Type::Text => {
                    let s: String = ctx.get(0)?;
                    if s.is_empty() { Ok(Option::<String>::None) } else { Ok(Some(s)) }
                }
                _ => Ok(Option::<String>::None),
            }
        },
    )?;

    conn.create_scalar_function(
        "pg_get_indexdef",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Some("".to_string())),
    )?;

    conn.create_scalar_function(
        "pg_get_constraintdef",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Some("".to_string())),
    )?;

    // === PATCH v13b: additional PostgreSQL arities (SQLite matches UDFs by name+arity) ===
    conn.create_scalar_function(
        "pg_get_expr",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Text => {
                    let s: String = ctx.get(0)?;
                    if s.is_empty() { Ok(Option::<String>::None) } else { Ok(Some(s)) }
                }
                _ => Ok(Option::<String>::None),
            }
        },
    )?;

    conn.create_scalar_function(
        "pg_get_indexdef",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Some("".to_string())),
    )?;

    conn.create_scalar_function(
        "pg_get_constraintdef",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Some("".to_string())),
    )?;

    conn.create_scalar_function(
        "to_regtype",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Text => {
                    let name: String = ctx.get(0)?;
                    Ok(regtype_to_oid(&name))
                }
                _ => Ok(Option::<String>::None),
            }
        },
    )?;


    // ==================== PATCH v16: catalog function gap-fill ====================
    // 任何未注册的 pg_* 函数都会让 SQLite 抛 "no such function"，进而把客户端
    // 事务标记为 aborted，之后所有元数据查询级联失败（GUI 表树/列树整体崩）。
    // 这里按 PostgreSQL 语义补齐 GUI 高频函数；无法真实实现者返回中性值而非
    // 报错。统一使用 n_arg = -1（变参）以覆盖 PG 的全部重载形式。

    // ---- reg* 转换族 ----
    // to_regclass('schema.tbl') -> pg_class.oid（TEXT，公式与 pg_class 视图一致）
    conn.create_scalar_function(
        "to_regclass",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Option::<String>::None);
            }
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Text => {
                    let raw: String = ctx.get(0)?;
                    Ok(Some(relname_to_oid_string(&raw)))
                }
                _ => Ok(Option::<String>::None),
            }
        },
    )?;

    // to_regproc / to_regprocedure -> 稳定 OID（同一名字总得到同一值）
    conn.create_scalar_function(
        "to_regproc",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Option::<String>::None);
            }
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Text => {
                    let raw: String = ctx.get(0)?;
                    Ok(Some(relname_to_oid_string(&raw)))
                }
                _ => Ok(Option::<String>::None),
            }
        },
    )?;
    conn.create_scalar_function(
        "to_regprocedure",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Option::<String>::None);
            }
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Text => {
                    let raw: String = ctx.get(0)?;
                    Ok(Some(relname_to_oid_string(&raw)))
                }
                _ => Ok(Option::<String>::None),
            }
        },
    )?;

    // to_regnamespace：内置 schema 用 PG 的真实 OID，其余走公式
    conn.create_scalar_function(
        "to_regnamespace",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Option::<String>::None);
            }
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Text => {
                    let raw: String = ctx.get(0)?;
                    let name = strip_relation_name(&raw).to_lowercase();
                    let oid = match name.as_str() {
                        "pg_catalog" => "11".to_string(),
                        "public" => "2200".to_string(),
                        "information_schema" => "13000".to_string(),
                        "pg_toast" => "99".to_string(),
                        _ => relname_to_oid_string(&raw),
                    };
                    Ok(Some(oid))
                }
                _ => Ok(Option::<String>::None),
            }
        },
    )?;

    // to_regrole：单用户模型，统一映射到 bootstrap superuser OID 10
    conn.create_scalar_function(
        "to_regrole",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Option::<String>::None);
            }
            Ok(Some("10".to_string()))
        },
    )?;

    // ---- 描述 / 定义族：返回空串或 NULL，绝不报错 ----
    // shobj_description(oid, catalog) -> 共享对象注释，本地无 -> NULL
    conn.create_scalar_function(
        "shobj_description",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Option::<String>::None),
    )?;

    for fname in [
        "pg_get_viewdef",                     // (oid) / (oid, pretty) / (oid, wrap)
        "pg_get_functiondef",                 // (oid)
        "pg_get_function_arguments",          // (oid)
        "pg_get_function_result",             // (oid)
        "pg_get_function_identity_arguments", // (oid)
        "pg_get_ruledef",                     // (oid) / (oid, pretty)
        "pg_get_triggerdef",                  // (oid) / (oid, pretty)
    ] {
        conn.create_scalar_function(
            fname,
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_ctx| Ok(Some(String::new())),
        )?;
    }

    // 这几个在 PG 里对"没有"的对象就返回 NULL，保持一致
    for fname in [
        "pg_get_serial_sequence", // SQLite 无独立 sequence
        "pg_get_partkeydef",      // 无分区表
        "pg_relation_filepath",   // 无物理 relfilenode
        "pg_sequence_last_value",
        "pg_get_statisticsobjdef",
    ] {
        conn.create_scalar_function(
            fname,
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_ctx| Ok(Option::<String>::None),
        )?;
    }

    // ---- 权限族：单用户模型，一律放行 ----
    for fname in [
        "has_column_privilege",
        "has_function_privilege",
        "has_any_column_privilege",
        "has_sequence_privilege",
        "has_tablespace_privilege",
        "has_language_privilege",
        "has_foreign_data_wrapper_privilege",
        "has_server_privilege",
        "has_type_privilege",
        // 下面两个已有 3 参数版本，这里补变参以修好 2 参数调用的 arity 报错
        "has_schema_privilege",
        "has_database_privilege",
        "has_table_privilege",
        "pg_has_role",
    ] {
        conn.create_scalar_function(
            fname,
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_ctx| Ok(1i32),
        )?;
    }

    // ---- 可见性族：单 schema 模型，一律可见 ----
    for fname in [
        "pg_type_is_visible",
        "pg_function_is_visible",
        "pg_opclass_is_visible",
        "pg_operator_is_visible",
        "pg_opfamily_is_visible",
        "pg_collation_is_visible",
        "pg_conversion_is_visible",
        "pg_ts_config_is_visible",
        "pg_ts_dict_is_visible",
        "pg_ts_parser_is_visible",
        "pg_ts_template_is_visible",
        "pg_statistics_obj_is_visible",
    ] {
        conn.create_scalar_function(
            fname,
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_ctx| Ok(1i32),
        )?;
    }

    // ---- 尺寸族：SQLite 无 per-relation 统计，返回 0 而不是报错 ----
    for fname in [
        "pg_relation_size",
        "pg_total_relation_size",
        "pg_table_size",
        "pg_indexes_size",
        "pg_column_size",
    ] {
        conn.create_scalar_function(
            fname,
            -1,
            FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
            |_ctx| Ok(0i64),
        )?;
    }

    // ---- 编码族 ----
    conn.create_scalar_function(
        "pg_encoding_to_char",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Some("UTF8".to_string())),
    )?;
    conn.create_scalar_function(
        "pg_client_encoding",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(Some("UTF8".to_string())),
    )?;
    conn.create_scalar_function(
        "pg_char_to_encoding",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(6i64),
    )?;

    // ---- GUC：current_setting(name) / current_setting(name, missing_ok) ----
    // 未知参数返回 NULL 而不是报错（PG 在 missing_ok=false 时会报错，但对 GUI
    // 而言 abort 事务的代价远大于返回 NULL）。
    conn.create_scalar_function(
        "current_setting",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Option::<String>::None);
            }
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Text => {
                    let name: String = ctx.get(0)?;
                    Ok(guc_value(&name))
                }
                _ => Ok(Option::<String>::None),
            }
        },
    )?;

    // ---- 事务 / 临时 schema ----
    conn.create_scalar_function(
        "txid_current",
        -1,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| Ok(1i64),
    )?;
    conn.create_scalar_function(
        "pg_current_xact_id",
        -1,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| Ok(1i64),
    )?;
    conn.create_scalar_function(
        "pg_my_temp_schema",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(0i64),
    )?;
    conn.create_scalar_function(
        "pg_is_other_temp_schema",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| Ok(0i32),
    )?;

    // ---- 标识符引用（DBeaver 生成 DDL 时用） ----
    conn.create_scalar_function(
        "quote_ident",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Option::<String>::None);
            }
            let raw: String = match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Text => ctx.get(0)?,
                _ => return Ok(Option::<String>::None),
            };
            let needs_quote = raw.is_empty()
                || raw.chars().next().is_some_and(|c| c.is_ascii_digit())
                || !raw.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if needs_quote {
                Ok(Some(format!("\"{}\"", raw.replace('"', "\"\""))))
            } else {
                Ok(Some(raw))
            }
        },
    )?;
    conn.create_scalar_function(
        "quote_literal",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Option::<String>::None);
            }
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Null => Ok(Option::<String>::None),
                _ => {
                    let raw: String = ctx.get(0)?;
                    Ok(Some(format!("'{}'", raw.replace('\'', "''"))))
                }
            }
        },
    )?;
    conn.create_scalar_function(
        "quote_nullable",
        -1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            if ctx.len() == 0 {
                return Ok(Some("NULL".to_string()));
            }
            match ctx.get_raw(0).data_type() {
                rusqlite::types::Type::Null => Ok(Some("NULL".to_string())),
                _ => {
                    let raw: String = ctx.get(0)?;
                    Ok(Some(format!("'{}'", raw.replace('\'', "''"))))
                }
            }
        },
    )?;

    // ================== end PATCH v16 ==================

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
    const UNIT_LIMIT: u64 = 10 * 1024;  // 10240 (for kB, MB, GB, TB, PB)

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

        // Test pg_has_role - 2 parameter version (current user)
        let has_role_2p: i32 = conn.query_row(
            "SELECT pg_has_role('pg_read_all_data', 'USAGE')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_role_2p, 1); // true

        // Test pg_has_role - 3 parameter version
        let has_role_3p: i32 = conn.query_row(
            "SELECT pg_has_role('postgres', 'pg_read_all_data', 'USAGE')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_role_3p, 1); // true

        // Test pg_has_role - security-sensitive role should return false
        let has_sensitive_role: i32 = conn.query_row(
            "SELECT pg_has_role('pg_read_server_files', 'USAGE')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_sensitive_role, 0); // false

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

        // Test has_table_privilege - 2 parameter version (current user)
        let has_table_priv_2p: i32 = conn.query_row(
            "SELECT has_table_privilege('pg_class', 'SELECT')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_table_priv_2p, 1); // true - can read system catalogs

        // Test has_table_privilege - 3 parameter version
        let has_table_priv_3p: i32 = conn.query_row(
            "SELECT has_table_privilege('postgres', 'pg_class', 'SELECT')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_table_priv_3p, 1); // true

        // Test has_table_privilege - should deny INSERT on system tables
        let has_table_insert: i32 = conn.query_row(
            "SELECT has_table_privilege('pg_class', 'INSERT')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_table_insert, 0); // false - cannot insert into system catalogs

        // Test has_table_privilege - should allow operations on user tables
        let has_user_table_priv: i32 = conn.query_row(
            "SELECT has_table_privilege('user_table', 'INSERT')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_user_table_priv, 1); // true - can modify user tables

        // Test has_table_privilege - public user with limited access
        let has_public_insert: i32 = conn.query_row(
            "SELECT has_table_privilege('public', 'user_table', 'INSERT')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(has_public_insert, 0); // false - public cannot insert into user tables
    }

    #[test]
    fn test_privilege_functions_error_handling() {
        let conn = Connection::open_in_memory().unwrap();
        register_system_functions(&conn).unwrap();

        // Test pg_has_role with invalid privilege
        let result = conn.query_row(
            "SELECT pg_has_role('pg_read_all_data', 'INVALID')",
            [],
            |row| row.get::<_, i32>(0)
        );
        assert!(result.is_err()); // Should fail with invalid privilege

        // Test has_table_privilege with invalid privilege
        let result = conn.query_row(
            "SELECT has_table_privilege('test_table', 'INVALID')",
            [],
            |row| row.get::<_, i32>(0)
        );
        assert!(result.is_err()); // Should fail with invalid privilege
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


// ==================== PATCH v16 helpers ====================

/// PATCH v16: 从关系引用中剥离 schema 限定与双引号。
/// `public."My Table"` -> `My Table`，`pg_catalog.pg_class` -> `pg_class`
fn strip_relation_name(raw: &str) -> String {
    let s = raw.trim();
    let mut in_quote = false;
    let mut last_dot: Option<usize> = None;
    for (i, ch) in s.char_indices() {
        match ch {
            '"' => in_quote = !in_quote,
            '.' if !in_quote => last_dot = Some(i),
            _ => {}
        }
    }
    let tail = match last_dot {
        Some(i) => &s[i + 1..],
        None => s,
    };
    tail.trim().trim_matches('"').to_string()
}

/// PATCH v16: 与 `pg_class` 视图（migration/registry.rs）及 pg_class.rs 的
/// generate_oid_from_name 逐字节等价的 OID 公式。必须保持一致，否则客户端
/// 拿 to_regclass() 的结果去查 pg_attribute 会得到零行。
fn relname_to_oid_string(raw: &str) -> String {
    let name = strip_relation_name(raw);
    let chars: Vec<char> = name.chars().collect();
    let at = |i: usize| chars.get(i).copied().unwrap_or(' ') as u32;
    let len = chars.len() as u32;
    let oid =
        ((at(0) * 1_000_000) + (at(1) * 10_000) + (at(2) * 100) + (len * 7)) % 1_000_000 + 16384;
    oid.to_string()
}

/// PATCH v16: current_setting() 支持的 GUC。未知名字返回 None（-> SQL NULL），
/// 刻意不报错：让 GUI 少一条信息，好过让整个事务 abort。
fn guc_value(name: &str) -> Option<String> {
    let v = match strip_relation_name(name).to_lowercase().as_str() {
        "server_version" => "16.0",
        "server_version_num" => "160000",
        "server_encoding" | "client_encoding" => "UTF8",
        "lc_collate" | "lc_ctype" | "lc_messages" | "lc_monetary" | "lc_numeric" | "lc_time" => "C",
        "timezone" => "UTC",
        "datestyle" => "ISO, MDY",
        "intervalstyle" => "postgres",
        "standard_conforming_strings" | "integer_datetimes" | "is_superuser" => "on",
        "search_path" => "\"$user\", public",
        "application_name" => "",
        "bytea_output" => "hex",
        "default_transaction_isolation" | "transaction_isolation" => "read committed",
        "default_transaction_read_only" | "transaction_read_only" | "in_hot_standby" => "off",
        "max_index_keys" => "32",
        "max_identifier_length" => "63",
        "block_size" => "8192",
        "session_authorization" => "postgres",
        "role" => "none",
        _ => return None,
    };
    Some(v.to_string())
}
