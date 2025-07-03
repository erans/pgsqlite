use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::Select;
use tracing::debug;

pub struct PgClassHandler;

impl PgClassHandler {
    pub async fn handle_query(
        _select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_class query");
        
        // Get list of tables from SQLite
        let tables_response = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'").await?;
        
        // For now, return all columns - column projection will be implemented later
        let columns = vec![
            "oid".to_string(),
            "relname".to_string(),
            "relnamespace".to_string(),
            "reltype".to_string(),
            "relowner".to_string(),
            "relam".to_string(),
            "relfilenode".to_string(),
            "reltablespace".to_string(),
            "relpages".to_string(),
            "reltuples".to_string(),
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
        ];
        
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
                
                let row = vec![
                    Some(oid.to_string().into_bytes()),                    // oid
                    Some(table_name.to_string().into_bytes()),            // relname
                    Some("2200".to_string().into_bytes()),                 // relnamespace (public schema)
                    Some((oid + 1).to_string().into_bytes()),             // reltype
                    Some("10".to_string().into_bytes()),                   // relowner (postgres user)
                    Some("0".to_string().into_bytes()),                    // relam (0 for tables)
                    Some(oid.to_string().into_bytes()),                    // relfilenode
                    Some("0".to_string().into_bytes()),                    // reltablespace
                    Some("0".to_string().into_bytes()),                    // relpages
                    Some("-1".to_string().into_bytes()),                   // reltuples
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
                ];
                
                rows.push(row);
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
                
                let row = vec![
                    Some(index_oid.to_string().into_bytes()),              // oid
                    Some(index_name.to_string().into_bytes()),            // relname
                    Some("2200".to_string().into_bytes()),                 // relnamespace (public schema)
                    Some("0".to_string().into_bytes()),                    // reltype (0 for indexes)
                    Some("10".to_string().into_bytes()),                   // relowner (postgres user)
                    Some("403".to_string().into_bytes()),                  // relam (btree)
                    Some(index_oid.to_string().into_bytes()),              // relfilenode
                    Some("0".to_string().into_bytes()),                    // reltablespace
                    Some("0".to_string().into_bytes()),                    // relpages
                    Some("0".to_string().into_bytes()),                    // reltuples
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
                ];
                
                rows.push(row);
            }
        }
        
        let rows_affected = rows.len();
        
        // TODO: Apply WHERE clause filtering from select
        // For now, return all rows
        
        Ok(DbResponse {
            columns,
            rows,
            rows_affected,
        })
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