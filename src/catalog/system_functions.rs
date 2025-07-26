use crate::session::db_handler::DbHandler;
use crate::types::PgType;
use sqlparser::ast::Expr;
use std::sync::Arc;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Handles PostgreSQL system function calls within catalog queries
pub struct SystemFunctions;

impl SystemFunctions {
    /// Process a function call expression and return the result as a literal value
    pub async fn process_function_call(
        function_name: &str,
        args: &[Expr],
        db: Arc<DbHandler>,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        match function_name.to_lowercase().as_str() {
            "pg_get_constraintdef" => Self::pg_get_constraintdef(args, db).await,
            "pg_table_is_visible" => Self::pg_table_is_visible(args, db).await,
            "format_type" => Self::format_type(args, db).await,
            "pg_get_expr" => Self::pg_get_expr(args, db).await,
            "pg_get_userbyid" => Self::pg_get_userbyid(args).await,
            "pg_get_indexdef" => Self::pg_get_indexdef(args, db).await,
            _ => Ok(None), // Unknown function, let it pass through
        }
    }

    /// pg_get_constraintdef(constraint_oid) - Returns the definition of a constraint
    /// For full implementation, we would need a pg_constraint table that maps OIDs to constraints
    async fn pg_get_constraintdef(
        args: &[Expr],
        db: Arc<DbHandler>,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        if args.is_empty() {
            return Ok(Some("".to_string()));
        }

        // Extract the constraint OID from the first argument
        let constraint_oid = match &args[0] {
            Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => n.parse::<i64>().ok(),
            _ => None,
        };

        if let Some(oid) = constraint_oid {
            // Since we don't have a pg_constraint table yet, we'll do a simple implementation
            // that tries to match common constraint OIDs that psql might be looking for
            
            // For demonstration, let's handle some special cases:
            // 1. Primary key constraints typically have predictable OIDs
            // 2. Foreign key constraints 
            // 3. Check constraints
            
            // Query all tables to find constraints
            let tables_query = "SELECT name, sql FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'";
            let tables_response = db.query(tables_query).await?;
            
            if !tables_response.rows.is_empty() {
                let rows = &tables_response.rows;
                for row in rows {
                    if row.len() >= 2 {
                        let table_name = row[0].as_ref().and_then(|v| std::str::from_utf8(v).ok()).unwrap_or("");
                        let create_sql = row[1].as_ref().and_then(|v| std::str::from_utf8(v).ok()).unwrap_or("");
                        
                        // Try to extract constraints from the CREATE TABLE statement
                        // This is a simplified approach - a full implementation would parse the SQL
                        if let Some(constraint_def) = extract_constraint_by_oid(create_sql, oid, table_name) {
                            return Ok(Some(constraint_def));
                        }
                    }
                }
            }
            
            // Also check indexes (for UNIQUE constraints)
            let index_query = "SELECT name, sql FROM sqlite_master WHERE type = 'index' AND sql IS NOT NULL";
            let index_response = db.query(index_query).await?;
            
            if !index_response.rows.is_empty() {
                let rows = &index_response.rows;
                for row in rows {
                    if row.len() >= 2 {
                        let _index_name = row[0].as_ref().and_then(|v| std::str::from_utf8(v).ok()).unwrap_or("");
                        let create_sql = row[1].as_ref().and_then(|v| std::str::from_utf8(v).ok()).unwrap_or("");
                        
                        // Check if this might be a UNIQUE constraint with matching OID
                        if create_sql.to_uppercase().contains("UNIQUE") {
                            // For now, return a generic UNIQUE constraint definition
                            // A full implementation would parse and match OIDs properly
                            if oid % 1000 == 500 { // Arbitrary check for demonstration
                                return Ok(Some("UNIQUE (column_name)".to_string()));
                            }
                        }
                    }
                }
            }
            
            // Return empty string for unknown constraint OID
            Ok(Some("".to_string()))
        } else {
            Ok(Some("".to_string()))
        }
    }

    /// pg_table_is_visible(table_oid) - Returns true if table is in search path
    async fn pg_table_is_visible(
        args: &[Expr],
        _db: Arc<DbHandler>,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        // In SQLite, all tables are visible, so always return true
        if !args.is_empty() {
            Ok(Some("1".to_string()))  // Return "1" for true (SQLite boolean)
        } else {
            Ok(Some("0".to_string()))  // Return "0" for false
        }
    }

    /// format_type(type_oid, typemod) - Returns formatted type name
    async fn format_type(
        args: &[Expr],
        _db: Arc<DbHandler>,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        if args.is_empty() {
            return Ok(Some("".to_string()));
        }

        // Extract type OID from first argument
        let type_oid = match &args[0] {
            Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => n.parse::<i32>().ok(),
            _ => None,
        };

        // Extract typemod from second argument if present
        let typemod = if args.len() > 1 {
            match &args[1] {
                Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => n.parse::<i32>().ok(),
                _ => None,
            }
        } else {
            None
        };

        if let Some(oid) = type_oid {
            // Convert OID to PgType and format
            let type_name = match oid {
                t if t == PgType::Bool.to_oid() => "boolean".to_string(),
                t if t == PgType::Bytea.to_oid() => "bytea".to_string(),
                t if t == PgType::Char.to_oid() => "\"char\"".to_string(),
                19 => "name".to_string(), // PostgreSQL name type OID
                t if t == PgType::Int8.to_oid() => "bigint".to_string(),
                t if t == PgType::Int2.to_oid() => "smallint".to_string(),
                t if t == PgType::Int4.to_oid() => "integer".to_string(),
                t if t == PgType::Text.to_oid() => "text".to_string(),
                26 => "oid".to_string(), // PostgreSQL OID type
                27 => "tid".to_string(), // PostgreSQL TID type
                28 => "xid".to_string(), // PostgreSQL XID type
                29 => "cid".to_string(), // PostgreSQL CID type
                t if t == PgType::Float4.to_oid() => "real".to_string(),
                t if t == PgType::Float8.to_oid() => "double precision".to_string(),
                t if t == PgType::Money.to_oid() => "money".to_string(),
                t if t == PgType::Varchar.to_oid() => {
                    // Handle varchar with length modifier
                    if let Some(mod_val) = typemod {
                        if mod_val > 4 {
                            format!("character varying({})", mod_val - 4)
                        } else {
                            "character varying".to_string()
                        }
                    } else {
                        "character varying".to_string()
                    }
                },
                1042 => {
                    // Handle char with length modifier (bpchar)
                    if let Some(mod_val) = typemod {
                        if mod_val > 4 {
                            format!("character({})", mod_val - 4)
                        } else {
                            "character".to_string()
                        }
                    } else {
                        "character".to_string()
                    }
                },
                t if t == PgType::Numeric.to_oid() => {
                    // Handle numeric with precision and scale
                    if let Some(mod_val) = typemod {
                        if mod_val > 4 {
                            let precision = (mod_val - 4) >> 16;
                            let scale = (mod_val - 4) & 0xFFFF;
                            if scale > 0 {
                                format!("numeric({precision},{scale})")
                            } else {
                                format!("numeric({precision})")
                            }
                        } else {
                            "numeric".to_string()
                        }
                    } else {
                        "numeric".to_string()
                    }
                },
                t if t == PgType::Date.to_oid() => "date".to_string(),
                t if t == PgType::Time.to_oid() => "time without time zone".to_string(),
                t if t == PgType::Timestamp.to_oid() => "timestamp without time zone".to_string(),
                t if t == PgType::Timestamptz.to_oid() => "timestamp with time zone".to_string(),
                1186 => "interval".to_string(), // PostgreSQL interval type
                1266 => "time with time zone".to_string(), // PostgreSQL timetz type
                t if t == PgType::Bit.to_oid() => "bit".to_string(),
                t if t == PgType::Varbit.to_oid() => "bit varying".to_string(),
                603 => "box".to_string(), // PostgreSQL box type
                718 => "circle".to_string(), // PostgreSQL circle type
                628 => "line".to_string(), // PostgreSQL line type
                601 => "lseg".to_string(), // PostgreSQL lseg type
                602 => "path".to_string(), // PostgreSQL path type
                600 => "point".to_string(), // PostgreSQL point type
                604 => "polygon".to_string(), // PostgreSQL polygon type
                t if t == PgType::Inet.to_oid() => "inet".to_string(),
                t if t == PgType::Cidr.to_oid() => "cidr".to_string(),
                t if t == PgType::Macaddr.to_oid() => "macaddr".to_string(),
                t if t == PgType::Uuid.to_oid() => "uuid".to_string(),
                t if t == PgType::Json.to_oid() => "json".to_string(),
                t if t == PgType::Jsonb.to_oid() => "jsonb".to_string(),
                _ => format!("unknown({oid})"),
            };

            Ok(Some(type_name))
        } else {
            Ok(Some("".to_string()))
        }
    }

    /// pg_get_expr(node_tree, relation_oid) - Returns the expression from a node tree
    async fn pg_get_expr(
        args: &[Expr],
        _db: Arc<DbHandler>,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        // SQLite doesn't have expression trees like PostgreSQL
        // Return empty string for now
        if args.len() >= 2 {
            Ok(Some("".to_string()))
        } else {
            Ok(Some("".to_string()))
        }
    }

    /// pg_get_userbyid(user_oid) - Returns username for an OID
    async fn pg_get_userbyid(
        args: &[Expr],
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        // SQLite doesn't have users, return a default user
        if !args.is_empty() {
            Ok(Some("sqlite".to_string()))
        } else {
            Ok(Some("".to_string()))
        }
    }

    /// pg_get_indexdef(index_oid) - Returns CREATE INDEX statement
    async fn pg_get_indexdef(
        args: &[Expr],
        _db: Arc<DbHandler>,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>> {
        if args.is_empty() {
            return Ok(Some("".to_string()));
        }

        // Extract the index OID from the first argument
        let index_oid = match &args[0] {
            Expr::Value(sqlparser::ast::ValueWithSpan { value: sqlparser::ast::Value::Number(n, _), .. }) => n.parse::<i64>().ok(),
            _ => None,
        };

        if let Some(_oid) = index_oid {
            // Query SQLite metadata to find index information
            // For now, return empty string since we don't have OID -> index mapping
            Ok(Some("".to_string()))
        } else {
            Ok(Some("".to_string()))
        }
    }
}

/// Helper function to extract constraint definition by OID from CREATE TABLE SQL
/// This is a simplified implementation - a full version would properly parse the SQL
fn extract_constraint_by_oid(create_sql: &str, target_oid: i64, table_name: &str) -> Option<String> {
    let sql_upper = create_sql.to_uppercase();
    
    // Generate OIDs for common constraint patterns
    // We use a hash of table_name + constraint_type + some identifier
    
    // Check for PRIMARY KEY
    if sql_upper.contains("PRIMARY KEY") {
        let mut hasher = DefaultHasher::new();
        format!("{table_name}_pkey").hash(&mut hasher);
        let pkey_oid = hasher.finish() as i64 & 0x7FFFFFFF; // Keep it positive
        
        if pkey_oid == target_oid {
            // Extract PRIMARY KEY definition
            if let Some(start) = sql_upper.find("PRIMARY KEY") {
                // Simple extraction - look for column names in parentheses
                let remaining = &create_sql[start..];
                if let Some(paren_start) = remaining.find('(') {
                    if let Some(paren_end) = remaining.find(')') {
                        let columns = &remaining[paren_start..=paren_end];
                        return Some(format!("PRIMARY KEY {columns}"));
                    }
                }
            }
        }
    }
    
    // Check for FOREIGN KEY constraints
    if sql_upper.contains("FOREIGN KEY") {
        let mut fkey_count = 0;
        let mut search_start = 0;
        
        while let Some(fkey_pos) = sql_upper[search_start..].find("FOREIGN KEY") {
            let actual_pos = search_start + fkey_pos;
            fkey_count += 1;
            
            let mut hasher = DefaultHasher::new();
            format!("{table_name}_fkey_{fkey_count}").hash(&mut hasher);
            let fkey_oid = hasher.finish() as i64 & 0x7FFFFFFF;
            
            if fkey_oid == target_oid {
                // Extract FOREIGN KEY definition
                let remaining = &create_sql[actual_pos..];
                // Look for pattern: FOREIGN KEY (columns) REFERENCES table (columns)
                if let Some(ref_pos) = remaining.to_uppercase().find("REFERENCES") {
                    // Find the end of the constraint (next comma or closing paren)
                    let mut depth = 0;
                    let mut end_pos = ref_pos;
                    
                    for (i, ch) in remaining[ref_pos..].chars().enumerate() {
                        match ch {
                            '(' => depth += 1,
                            ')' => {
                                depth -= 1;
                                if depth <= 0 {
                                    end_pos = ref_pos + i + 1;
                                    break;
                                }
                            },
                            ',' if depth == 0 => {
                                end_pos = ref_pos + i;
                                break;
                            },
                            _ => {}
                        }
                    }
                    
                    return Some(remaining[..end_pos].trim().to_string());
                }
            }
            
            search_start = actual_pos + 11; // Move past "FOREIGN KEY"
        }
    }
    
    // Check for CHECK constraints
    if sql_upper.contains("CHECK") {
        let mut check_count = 0;
        let mut search_start = 0;
        
        while let Some(check_pos) = sql_upper[search_start..].find("CHECK") {
            let actual_pos = search_start + check_pos;
            check_count += 1;
            
            let mut hasher = DefaultHasher::new();
            format!("{table_name}_check_{check_count}").hash(&mut hasher);
            let check_oid = hasher.finish() as i64 & 0x7FFFFFFF;
            
            if check_oid == target_oid {
                // Extract CHECK definition
                let remaining = &create_sql[actual_pos..];
                if let Some(paren_start) = remaining.find('(') {
                    // Find matching closing parenthesis
                    let mut depth = 0;
                    let mut end_pos = paren_start;
                    
                    for (i, ch) in remaining[paren_start..].chars().enumerate() {
                        match ch {
                            '(' => depth += 1,
                            ')' => {
                                depth -= 1;
                                if depth == 0 {
                                    end_pos = paren_start + i + 1;
                                    break;
                                }
                            },
                            _ => {}
                        }
                    }
                    
                    return Some(remaining[..end_pos].to_string());
                }
            }
            
            search_start = actual_pos + 5; // Move past "CHECK"
        }
    }
    
    None
}