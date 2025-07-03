use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use crate::types::PgType;
use sqlparser::ast::{Select, Expr, Value as SqlValue};
use tracing::debug;

pub struct PgAttributeHandler;

impl PgAttributeHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_attribute query");
        
        let columns = vec![
            "attrelid".to_string(),
            "attname".to_string(),
            "atttypid".to_string(),
            "attstattarget".to_string(),
            "attlen".to_string(),
            "attnum".to_string(),
            "attndims".to_string(),
            "attcacheoff".to_string(),
            "atttypmod".to_string(),
            "attbyval".to_string(),
            "attstorage".to_string(),
            "attalign".to_string(),
            "attnotnull".to_string(),
            "atthasdef".to_string(),
            "atthasmissing".to_string(),
            "attidentity".to_string(),
            "attgenerated".to_string(),
            "attisdropped".to_string(),
            "attislocal".to_string(),
            "attinhcount".to_string(),
            "attcollation".to_string(),
            "attacl".to_string(),
            "attoptions".to_string(),
            "attfdwoptions".to_string(),
            "attmissingval".to_string(),
        ];
        
        // Check if there's a WHERE clause filtering by attrelid
        let filter_table = extract_table_filter(select);
        
        let mut rows = Vec::new();
        
        if let Some(table_name) = filter_table {
            // Query specific table
            add_table_attributes(&table_name, db, &mut rows).await?;
        } else {
            // Query all tables
            let tables_response = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'").await?;
            
            for table_row in &tables_response.rows {
                if let Some(Some(table_name_bytes)) = table_row.get(0) {
                    let table_name = String::from_utf8_lossy(table_name_bytes);
                    add_table_attributes(&table_name, db, &mut rows).await?;
                }
            }
        }
        
        let rows_affected = rows.len();
        
        Ok(DbResponse {
            columns,
            rows,
            rows_affected,
        })
    }
}

async fn add_table_attributes(
    table_name: &str,
    db: &DbHandler,
    rows: &mut Vec<Vec<Option<Vec<u8>>>>,
) -> Result<(), PgSqliteError> {
    let table_oid = generate_oid_from_name(table_name);
    
    // Get column information from PRAGMA
    let col_info_query = format!("PRAGMA table_info({})", table_name);
    let col_info = db.query(&col_info_query).await?;
    
    // Also check if we have type info in __pgsqlite_schema
    let schema_query = format!(
        "SELECT column_name, pg_type FROM __pgsqlite_schema WHERE table_name = '{}'",
        table_name
    );
    let schema_info = db.query(&schema_query).await.ok();
    
    // Build a map of column name to pg_type
    let mut type_map = std::collections::HashMap::new();
    if let Some(schema) = schema_info {
        for row in &schema.rows {
            if let (Some(Some(col_bytes)), Some(Some(type_bytes))) = (row.get(0), row.get(1)) {
                let col_name = String::from_utf8_lossy(col_bytes);
                let pg_type = String::from_utf8_lossy(type_bytes);
                type_map.insert(col_name.to_string(), pg_type.to_string());
            }
        }
    }
    
    for (idx, col_row) in col_info.rows.iter().enumerate() {
        // PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        if let Some(Some(col_name_bytes)) = col_row.get(1) {
            let col_name = String::from_utf8_lossy(col_name_bytes);
            let sqlite_type = col_row.get(2)
                .and_then(|v| v.as_ref())
                .map(|v| String::from_utf8_lossy(v).to_string())
                .unwrap_or_else(|| "TEXT".to_string());
            
            let notnull = col_row.get(3)
                .and_then(|v| v.as_ref())
                .map(|v| String::from_utf8_lossy(v) == "1")
                .unwrap_or(false);
                
            let has_default = col_row.get(4).and_then(|v| v.as_ref()).is_some();
            
            // Determine PostgreSQL type
            let (pg_type_oid, attlen, atttypmod) = if let Some(pg_type_str) = type_map.get(col_name.as_ref()) {
                parse_pg_type(pg_type_str)
            } else {
                map_sqlite_to_pg_type(&sqlite_type)
            };
            
            let row = vec![
                Some(table_oid.to_string().into_bytes()),              // attrelid
                Some(col_name.to_string().into_bytes()),               // attname
                Some(pg_type_oid.to_string().into_bytes()),            // atttypid
                Some("-1".to_string().into_bytes()),                   // attstattarget
                Some(attlen.to_string().into_bytes()),                 // attlen
                Some(((idx + 1) as i16).to_string().into_bytes()),    // attnum (1-based)
                Some("0".to_string().into_bytes()),                    // attndims
                Some("-1".to_string().into_bytes()),                   // attcacheoff
                Some(atttypmod.to_string().into_bytes()),              // atttypmod
                Some(b"f".to_vec()),                                // attbyval
                Some(b"p".to_vec()),                                // attstorage (plain)
                Some(b"i".to_vec()),                                // attalign
                Some(if notnull { b"t".to_vec() } else { b"f".to_vec() }),   // attnotnull
                Some(if has_default { b"t".to_vec() } else { b"f".to_vec() }), // atthasdef
                Some(b"f".to_vec()),                                // atthasmissing
                Some(b"".to_vec()),                                 // attidentity
                Some(b"".to_vec()),                                 // attgenerated
                Some(b"f".to_vec()),                                // attisdropped
                Some(b"t".to_vec()),                                // attislocal
                Some("0".to_string().into_bytes()),                    // attinhcount
                Some("0".to_string().into_bytes()),                    // attcollation
                None,                                                  // attacl
                None,                                                  // attoptions
                None,                                                  // attfdwoptions
                None,                                                  // attmissingval
            ];
            
            rows.push(row);
        }
    }
    
    Ok(())
}

fn extract_table_filter(select: &Select) -> Option<String> {
    // Look for WHERE attrelid = 'schema.table'::regclass or similar
    if let Some(selection) = &select.selection {
        if let Expr::BinaryOp { left, op, right } = selection {
            if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                let is_attrelid = match left.as_ref() {
                    Expr::Identifier(ident) => ident.value.to_lowercase() == "attrelid",
                    Expr::CompoundIdentifier(parts) => {
                        parts.last().map(|p| p.value.to_lowercase() == "attrelid").unwrap_or(false)
                    }
                    _ => false,
                };
                
                if is_attrelid {
                    // Extract table name from right side
                    match right.as_ref() {
                        Expr::Cast { expr, .. } => {
                            if let Expr::Value(sqlparser::ast::ValueWithSpan { 
                                value: SqlValue::SingleQuotedString(s), .. 
                            }) = expr.as_ref() {
                                // Remove schema prefix if present
                                let table_name = s.split('.').last().unwrap_or(s);
                                return Some(table_name.to_string());
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    None
}

fn parse_pg_type(pg_type_str: &str) -> (i32, i16, i32) {
    // Parse PostgreSQL type string and return (oid, attlen, atttypmod)
    let type_upper = pg_type_str.to_uppercase();
    
    // Extract base type and modifiers
    let (base_type, type_mod) = if let Some(paren_pos) = type_upper.find('(') {
        let base = &type_upper[..paren_pos].trim();
        let mod_str = &type_upper[paren_pos+1..type_upper.len()-1];
        let mods: Vec<&str> = mod_str.split(',').map(|s| s.trim()).collect();
        (base.to_string(), Some(mods))
    } else {
        (type_upper.trim().to_string(), None)
    };
    
    // Map base type to OID and attlen
    let (oid, attlen) = match base_type.as_str() {
        "BOOL" | "BOOLEAN" => (PgType::Bool.to_oid(), 1),
        "INT2" | "SMALLINT" => (PgType::Int2.to_oid(), 2),
        "INT4" | "INT" | "INTEGER" => (PgType::Int4.to_oid(), 4),
        "INT8" | "BIGINT" => (PgType::Int8.to_oid(), 8),
        "FLOAT4" | "REAL" => (PgType::Float4.to_oid(), 4),
        "FLOAT8" | "DOUBLE PRECISION" => (PgType::Float8.to_oid(), 8),
        "TEXT" => (PgType::Text.to_oid(), -1),
        "VARCHAR" => (PgType::Varchar.to_oid(), -1),
        "CHAR" => (PgType::Char.to_oid(), -1),
        "BYTEA" => (PgType::Bytea.to_oid(), -1),
        "DATE" => (PgType::Date.to_oid(), 4),
        "TIME" => (PgType::Time.to_oid(), 8),
        "TIMESTAMP" => (PgType::Timestamp.to_oid(), 8),
        "TIMESTAMPTZ" => (PgType::Timestamptz.to_oid(), 8),
        "UUID" => (PgType::Uuid.to_oid(), 16),
        "JSON" => (PgType::Json.to_oid(), -1),
        "JSONB" => (PgType::Jsonb.to_oid(), -1),
        "NUMERIC" | "DECIMAL" => (PgType::Numeric.to_oid(), -1),
        _ => (PgType::Text.to_oid(), -1), // Default to text
    };
    
    // Calculate atttypmod
    let atttypmod = match base_type.as_str() {
        "VARCHAR" | "CHAR" => {
            if let Some(mods) = type_mod {
                if let Ok(len) = mods[0].parse::<i32>() {
                    len + 4 // PostgreSQL adds 4 to the length
                } else {
                    -1
                }
            } else {
                -1
            }
        }
        "NUMERIC" | "DECIMAL" => {
            if let Some(mods) = type_mod {
                if mods.len() >= 2 {
                    if let (Ok(precision), Ok(scale)) = (mods[0].parse::<i32>(), mods[1].parse::<i32>()) {
                        ((precision << 16) | scale) + 4
                    } else {
                        -1
                    }
                } else if let Ok(precision) = mods[0].parse::<i32>() {
                    (precision << 16) + 4
                } else {
                    -1
                }
            } else {
                -1
            }
        }
        _ => -1,
    };
    
    (oid as i32, attlen, atttypmod)
}

fn map_sqlite_to_pg_type(sqlite_type: &str) -> (i32, i16, i32) {
    let type_upper = sqlite_type.to_uppercase();
    
    // SQLite is very flexible with types, try to match common patterns
    let (oid, attlen) = if type_upper.contains("INT") {
        if type_upper.contains("BIGINT") || type_upper.contains("INT8") {
            (PgType::Int8.to_oid(), 8)
        } else if type_upper.contains("SMALLINT") || type_upper.contains("INT2") {
            (PgType::Int2.to_oid(), 2)
        } else {
            (PgType::Int4.to_oid(), 4)
        }
    } else if type_upper.contains("REAL") || type_upper.contains("FLOAT") || type_upper.contains("DOUBLE") {
        if type_upper.contains("DOUBLE") {
            (PgType::Float8.to_oid(), 8)
        } else {
            (PgType::Float4.to_oid(), 4)
        }
    } else if type_upper.contains("BOOL") {
        (PgType::Bool.to_oid(), 1)
    } else if type_upper.contains("BLOB") || type_upper.contains("BYTEA") {
        (PgType::Bytea.to_oid(), -1)
    } else if type_upper.contains("DATE") && !type_upper.contains("TIME") {
        (PgType::Date.to_oid(), 4)
    } else if type_upper.contains("TIME") && !type_upper.contains("STAMP") {
        (PgType::Time.to_oid(), 8)
    } else if type_upper.contains("TIMESTAMP") {
        (PgType::Timestamp.to_oid(), 8)
    } else if type_upper.contains("NUMERIC") || type_upper.contains("DECIMAL") {
        (PgType::Numeric.to_oid(), -1)
    } else if type_upper.contains("UUID") {
        (PgType::Uuid.to_oid(), 16)
    } else if type_upper.contains("JSON") {
        (PgType::Json.to_oid(), -1)
    } else if type_upper.contains("VARCHAR") {
        (PgType::Varchar.to_oid(), -1)
    } else if type_upper.contains("CHAR") {
        (PgType::Char.to_oid(), -1)
    } else {
        // Default to TEXT for SQLite's flexible typing
        (PgType::Text.to_oid(), -1)
    };
    
    (oid as i32, attlen, -1) // atttypmod = -1 for no modifier
}

fn generate_oid_from_name(name: &str) -> u32 {
    // Generate a stable OID from name using a simple hash
    // Start at 16384 to avoid conflicts with system OIDs
    let mut hash = 0u32;
    for byte in name.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
    }
    16384 + (hash % 1000000)
}