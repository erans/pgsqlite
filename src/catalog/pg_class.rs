use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgClassHandler;

impl PgClassHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_class query");
        
        // Get list of tables from SQLite
        let tables_response = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'").await?;
        
        // Define all available columns - PostgreSQL has 33 columns in pg_class
        let all_columns = vec![
            "oid".to_string(),
            "relname".to_string(),
            "relnamespace".to_string(),
            "reltype".to_string(),
            "reloftype".to_string(),
            "relowner".to_string(),
            "relam".to_string(),
            "relfilenode".to_string(),
            "reltablespace".to_string(),
            "relpages".to_string(),
            "reltuples".to_string(),
            "relallvisible".to_string(),
            "reltoastrelid".to_string(),
            "relhasindex".to_string(),
            "relisshared".to_string(),
            "relpersistence".to_string(),
            "relkind".to_string(),
            "relnatts".to_string(),
            "relchecks".to_string(),
            "relhasrules".to_string(),
            "relhastriggers".to_string(),
            "relhassubclass".to_string(),
            "relrowsecurity".to_string(),
            "relforcerowsecurity".to_string(),
            "relispopulated".to_string(),
            "relreplident".to_string(),
            "relispartition".to_string(),
            "relrewrite".to_string(),
            "relfrozenxid".to_string(),
            "relminmxid".to_string(),
            "relacl".to_string(),
            "reloptions".to_string(),
            "relpartbound".to_string(),
        ];
        
        // Determine which columns to return based on projection
        let (columns, column_indices) = Self::get_projected_columns(&select, &all_columns);
        
        // Create column mapping for WHERE evaluation (uses all columns)
        let column_mapping: HashMap<String, usize> = all_columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect();
        
        let mut rows = Vec::new();
        
        // Process each table
        for table_row in &tables_response.rows {
            if let Some(Some(table_name_bytes)) = table_row.get(0) {
                let table_name = String::from_utf8_lossy(table_name_bytes);
                
                // Get column count for this table
                let col_count_query = format!("PRAGMA table_info({})", table_name);
                let col_info = db.query(&col_count_query).await?;
                let relnatts = col_info.rows.len() as i16;
                
                // Generate a stable OID from table name
                let oid = generate_oid_from_name(&table_name);
                
                // Check if table has indexes
                let index_query = format!("PRAGMA index_list({})", table_name);
                let index_info = db.query(&index_query).await?;
                let relhasindex = !index_info.rows.is_empty();
                
                // Build row data for WHERE evaluation
                let mut row_data = HashMap::new();
                row_data.insert("oid".to_string(), oid.to_string());
                row_data.insert("relname".to_string(), table_name.to_string());
                row_data.insert("relnamespace".to_string(), "2200".to_string());
                row_data.insert("reltype".to_string(), (oid + 1).to_string());
                row_data.insert("reloftype".to_string(), "0".to_string());
                row_data.insert("relowner".to_string(), "10".to_string());
                row_data.insert("relam".to_string(), "0".to_string());
                row_data.insert("relfilenode".to_string(), oid.to_string());
                row_data.insert("reltablespace".to_string(), "0".to_string());
                row_data.insert("relpages".to_string(), "0".to_string());
                row_data.insert("reltuples".to_string(), "-1".to_string());
                row_data.insert("relallvisible".to_string(), "0".to_string());
                row_data.insert("reltoastrelid".to_string(), "0".to_string());
                row_data.insert("relhasindex".to_string(), if relhasindex { "t" } else { "f" }.to_string());
                row_data.insert("relisshared".to_string(), "f".to_string());
                row_data.insert("relpersistence".to_string(), "p".to_string());
                row_data.insert("relkind".to_string(), "r".to_string());
                row_data.insert("relnatts".to_string(), relnatts.to_string());
                row_data.insert("relchecks".to_string(), "0".to_string());
                row_data.insert("relhasrules".to_string(), "f".to_string());
                row_data.insert("relhastriggers".to_string(), "f".to_string());
                row_data.insert("relhassubclass".to_string(), "f".to_string());
                row_data.insert("relrowsecurity".to_string(), "f".to_string());
                row_data.insert("relforcerowsecurity".to_string(), "f".to_string());
                row_data.insert("relispopulated".to_string(), "t".to_string());
                row_data.insert("relreplident".to_string(), "d".to_string());
                row_data.insert("relispartition".to_string(), "f".to_string());
                row_data.insert("relrewrite".to_string(), "0".to_string());
                row_data.insert("relfrozenxid".to_string(), "0".to_string());
                row_data.insert("relminmxid".to_string(), "0".to_string());
                row_data.insert("relacl".to_string(), "".to_string());
                row_data.insert("reloptions".to_string(), "".to_string());
                row_data.insert("relpartbound".to_string(), "".to_string());
                
                // Evaluate WHERE clause if present
                let include_row = if let Some(selection) = &select.selection {
                    let result = WhereEvaluator::evaluate(selection, &row_data, &column_mapping);
                    debug!("WHERE evaluation for table '{}': {} (selection: {:?})", table_name, result, selection);
                    result
                } else {
                    true
                };
                
                if include_row {
                    // Build full row with all columns (33 total)
                    let full_row = vec![
                        Some(oid.to_string().into_bytes()),                    // oid
                        Some(table_name.to_string().into_bytes()),            // relname
                        Some("2200".to_string().into_bytes()),                 // relnamespace (public schema)
                        Some((oid + 1).to_string().into_bytes()),             // reltype
                        Some("0".to_string().into_bytes()),                    // reloftype
                        Some("10".to_string().into_bytes()),                   // relowner (postgres user)
                        Some("0".to_string().into_bytes()),                    // relam (0 for tables)
                        Some(oid.to_string().into_bytes()),                    // relfilenode
                        Some("0".to_string().into_bytes()),                    // reltablespace
                        Some("0".to_string().into_bytes()),                    // relpages
                        Some("-1".to_string().into_bytes()),                   // reltuples
                        Some("0".to_string().into_bytes()),                    // relallvisible
                        Some("0".to_string().into_bytes()),                    // reltoastrelid
                        Some(if relhasindex { b"t".to_vec() } else { b"f".to_vec() }), // relhasindex
                        Some(b"f".to_vec()),                                // relisshared
                        Some(b"p".to_vec()),                                // relpersistence (permanent)
                        Some(b"r".to_vec()),                                // relkind (regular table)
                        Some(relnatts.to_string().into_bytes()),              // relnatts
                        Some("0".to_string().into_bytes()),                    // relchecks
                        Some(b"f".to_vec()),                                // relhasrules
                        Some(b"f".to_vec()),                                // relhastriggers
                        Some(b"f".to_vec()),                                // relhassubclass
                        Some(b"f".to_vec()),                                // relrowsecurity
                        Some(b"f".to_vec()),                                // relforcerowsecurity
                        Some(b"t".to_vec()),                                // relispopulated
                        Some(b"d".to_vec()),                                // relreplident (default)
                        Some(b"f".to_vec()),                                // relispartition
                        Some("0".to_string().into_bytes()),                    // relrewrite
                        Some("0".to_string().into_bytes()),                    // relfrozenxid
                        Some("0".to_string().into_bytes()),                    // relminmxid
                        None,                                                   // relacl (NULL)
                        None,                                                   // reloptions (NULL)
                        None,                                                   // relpartbound (NULL)
                    ];
                    
                    // Project only the requested columns
                    let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                        .map(|&idx| full_row[idx].clone())
                        .collect();
                    
                    rows.push(projected_row);
                }
            }
        }
        
        // Also add indexes to pg_class
        let indexes_response = db.query("SELECT name, tbl_name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").await?;
        
        for index_row in &indexes_response.rows {
            if let (Some(Some(index_name_bytes)), Some(Some(table_name_bytes))) = 
                (index_row.get(0), index_row.get(1)) {
                let index_name = String::from_utf8_lossy(index_name_bytes);
                let table_name = String::from_utf8_lossy(table_name_bytes);
                
                let index_oid = generate_oid_from_name(&index_name);
                let _table_oid = generate_oid_from_name(&table_name);
                
                // Build row data for WHERE evaluation
                let mut row_data = HashMap::new();
                row_data.insert("oid".to_string(), index_oid.to_string());
                row_data.insert("relname".to_string(), index_name.to_string());
                row_data.insert("relnamespace".to_string(), "2200".to_string());
                row_data.insert("reltype".to_string(), "0".to_string());
                row_data.insert("reloftype".to_string(), "0".to_string());
                row_data.insert("relowner".to_string(), "10".to_string());
                row_data.insert("relam".to_string(), "403".to_string());
                row_data.insert("relfilenode".to_string(), index_oid.to_string());
                row_data.insert("reltablespace".to_string(), "0".to_string());
                row_data.insert("relpages".to_string(), "0".to_string());
                row_data.insert("reltuples".to_string(), "0".to_string());
                row_data.insert("relallvisible".to_string(), "0".to_string());
                row_data.insert("reltoastrelid".to_string(), "0".to_string());
                row_data.insert("relhasindex".to_string(), "f".to_string());
                row_data.insert("relisshared".to_string(), "f".to_string());
                row_data.insert("relpersistence".to_string(), "p".to_string());
                row_data.insert("relkind".to_string(), "i".to_string());
                row_data.insert("relnatts".to_string(), "0".to_string());
                row_data.insert("relchecks".to_string(), "0".to_string());
                row_data.insert("relhasrules".to_string(), "f".to_string());
                row_data.insert("relhastriggers".to_string(), "f".to_string());
                row_data.insert("relhassubclass".to_string(), "f".to_string());
                row_data.insert("relrowsecurity".to_string(), "f".to_string());
                row_data.insert("relforcerowsecurity".to_string(), "f".to_string());
                row_data.insert("relispopulated".to_string(), "t".to_string());
                row_data.insert("relreplident".to_string(), "n".to_string());
                row_data.insert("relispartition".to_string(), "f".to_string());
                row_data.insert("relrewrite".to_string(), "0".to_string());
                row_data.insert("relfrozenxid".to_string(), "0".to_string());
                row_data.insert("relminmxid".to_string(), "0".to_string());
                row_data.insert("relacl".to_string(), "".to_string());
                row_data.insert("reloptions".to_string(), "".to_string());
                row_data.insert("relpartbound".to_string(), "".to_string());
                
                // Evaluate WHERE clause if present
                let include_row = if let Some(selection) = &select.selection {
                    let result = WhereEvaluator::evaluate(selection, &row_data, &column_mapping);
                    debug!("WHERE evaluation for table '{}': {} (selection: {:?})", table_name, result, selection);
                    result
                } else {
                    true
                };
                
                if include_row {
                    // Build full row with all columns (33 total)
                    let full_row = vec![
                        Some(index_oid.to_string().into_bytes()),              // oid
                        Some(index_name.to_string().into_bytes()),            // relname
                        Some("2200".to_string().into_bytes()),                 // relnamespace (public schema)
                        Some("0".to_string().into_bytes()),                    // reltype (0 for indexes)
                        Some("0".to_string().into_bytes()),                    // reloftype
                        Some("10".to_string().into_bytes()),                   // relowner (postgres user)
                        Some("403".to_string().into_bytes()),                  // relam (btree)
                        Some(index_oid.to_string().into_bytes()),              // relfilenode
                        Some("0".to_string().into_bytes()),                    // reltablespace
                        Some("0".to_string().into_bytes()),                    // relpages
                        Some("0".to_string().into_bytes()),                    // reltuples
                        Some("0".to_string().into_bytes()),                    // relallvisible
                        Some("0".to_string().into_bytes()),                    // reltoastrelid
                        Some(b"f".to_vec()),                                // relhasindex
                        Some(b"f".to_vec()),                                // relisshared
                        Some(b"p".to_vec()),                                // relpersistence (permanent)
                        Some(b"i".to_vec()),                                // relkind (index)
                        Some("0".to_string().into_bytes()),                    // relnatts
                        Some("0".to_string().into_bytes()),                    // relchecks
                        Some(b"f".to_vec()),                                // relhasrules
                        Some(b"f".to_vec()),                                // relhastriggers
                        Some(b"f".to_vec()),                                // relhassubclass
                        Some(b"f".to_vec()),                                // relrowsecurity
                        Some(b"f".to_vec()),                                // relforcerowsecurity
                        Some(b"t".to_vec()),                                // relispopulated
                        Some(b"n".to_vec()),                                // relreplident (nothing)
                        Some(b"f".to_vec()),                                // relispartition
                        Some("0".to_string().into_bytes()),                    // relrewrite
                        Some("0".to_string().into_bytes()),                    // relfrozenxid
                        Some("0".to_string().into_bytes()),                    // relminmxid
                        None,                                                   // relacl (NULL)
                        None,                                                   // reloptions (NULL)
                        None,                                                   // relpartbound (NULL)
                    ];
                    
                    // Project only the requested columns
                    let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                        .map(|&idx| full_row[idx].clone())
                        .collect();
                    
                    rows.push(projected_row);
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
    
    /// Determine which columns to return based on the SELECT projection
    fn get_projected_columns(select: &Select, all_columns: &[String]) -> (Vec<String>, Vec<usize>) {
        let mut columns = Vec::new();
        let mut column_indices = Vec::new();
        
        
        // Check if it's SELECT *
        let is_select_star = select.projection.len() == 1 && matches!(&select.projection[0], SelectItem::Wildcard(_));
        
        if is_select_star {
            // Return all columns
            columns = all_columns.to_vec();
            column_indices = (0..all_columns.len()).collect();
        } else {
            // Process each projection item
            for item in &select.projection {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        if let Some(col_name) = Self::extract_column_name(expr) {
                            // Find the index of this column
                            if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                                columns.push(col_name);
                                column_indices.push(idx);
                            }
                        }
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        if let Some(col_name) = Self::extract_column_name(expr) {
                            // Find the index of this column
                            if let Some(idx) = all_columns.iter().position(|c| c == &col_name) {
                                columns.push(alias.value.clone());
                                column_indices.push(idx);
                            }
                        }
                    }
                    SelectItem::QualifiedWildcard(_, _) => {
                        // For table.*, return all columns
                        columns = all_columns.to_vec();
                        column_indices = (0..all_columns.len()).collect();
                        break;
                    }
                    SelectItem::Wildcard(_) => {
                        // SELECT * - return all columns
                        columns = all_columns.to_vec();
                        column_indices = (0..all_columns.len()).collect();
                        break;
                    }
                }
            }
        }
        
        (columns, column_indices)
    }
    
    /// Extract column name from an expression
    fn extract_column_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
            Expr::CompoundIdentifier(parts) => {
                // For table.column, return just the column name
                parts.last().map(|ident| ident.value.to_lowercase())
            }
            Expr::Cast { expr, .. } => {
                // Handle CAST expressions like CAST(oid AS TEXT)
                Self::extract_column_name(expr)
            }
            _ => None,
        }
    }
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