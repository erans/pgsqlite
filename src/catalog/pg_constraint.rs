use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgConstraintHandler;

impl PgConstraintHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_constraint query");

        // Define all available columns - PostgreSQL has 26 columns in pg_constraint
        let all_columns = vec![
            "oid".to_string(),
            "conname".to_string(),
            "connamespace".to_string(),
            "contype".to_string(),
            "condeferrable".to_string(),
            "condeferred".to_string(),
            "convalidated".to_string(),
            "conrelid".to_string(),
            "contypid".to_string(),
            "conindid".to_string(),
            "conparentid".to_string(),
            "confrelid".to_string(),
            "confupdtype".to_string(),
            "confdeltype".to_string(),
            "confmatchtype".to_string(),
            "conislocal".to_string(),
            "coninhcount".to_string(),
            "connoinherit".to_string(),
            "conkey".to_string(),
            "confkey".to_string(),
            "conpfeqop".to_string(),
            "conppeqop".to_string(),
            "conffeqop".to_string(),
            "confdelsetcols".to_string(),
            "conexclop".to_string(),
            "conbin".to_string(),
        ];

        // Determine which columns to return based on SELECT clause
        let requested_columns = Self::extract_requested_columns(select, &all_columns);

        // Create column indices for projection
        let column_indices: Vec<usize> = requested_columns.iter()
            .filter_map(|col| all_columns.iter().position(|c| c == col))
            .collect();

        // Get constraints from SQLite
        let constraints = Self::get_sqlite_constraints(db).await?;

        // Filter based on WHERE clause if present
        let filtered_constraints = if let Some(ref where_clause) = select.selection {
            let column_mapping = HashMap::new(); // Empty mapping for now
            constraints.into_iter()
                .filter(|constraint| {
                    WhereEvaluator::evaluate(where_clause, &Self::constraint_to_map(constraint), &column_mapping)
                })
                .collect()
        } else {
            constraints
        };

        // Build rows
        let mut rows = Vec::new();
        for constraint in filtered_constraints {
            let full_row = Self::constraint_to_row(&constraint);

            // Project only the requested columns
            let projected_row: Vec<Option<Vec<u8>>> = column_indices.iter()
                .map(|&idx| full_row[idx].clone())
                .collect();

            rows.push(projected_row);
        }

        let rows_affected = rows.len();
        Ok(DbResponse {
            columns: requested_columns,
            rows,
            rows_affected,
        })
    }

    fn extract_requested_columns(select: &Select, all_columns: &[String]) -> Vec<String> {
        // Check for wildcard
        if select.projection.len() == 1
            && let SelectItem::Wildcard(_) = &select.projection[0] {
                return all_columns.to_vec();
            }

        // Extract specific columns
        let mut columns = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_string();
                    if all_columns.contains(&col_name) {
                        columns.push(col_name);
                    }
                }
                SelectItem::ExprWithAlias { expr: Expr::Identifier(ident), alias } => {
                    let col_name = ident.value.to_string();
                    if all_columns.contains(&col_name) {
                        columns.push(alias.value.to_string());
                    }
                }
                SelectItem::ExprWithAlias { .. } => {
                    // Handle other expression types if needed
                }
                _ => {}
            }
        }

        if columns.is_empty() {
            all_columns.to_vec()
        } else {
            columns
        }
    }

    async fn get_sqlite_constraints(db: &DbHandler) -> Result<Vec<ConstraintInfo>, PgSqliteError> {
        let mut constraints = Vec::new();
        let mut constraint_id = 1000; // Start with arbitrary OID

        // Get all tables
        let tables_response = db.query(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'"
        ).await?;


        for table_row in &tables_response.rows {
            if let Some(Some(table_name_bytes)) = table_row.first() {
                let table_name = String::from_utf8_lossy(table_name_bytes).to_string();

                // Get table info to find primary keys and constraints
                let table_info_response = db.query(&format!("PRAGMA table_info({})", table_name)).await?;

                // Extract primary key constraints
                let mut pk_columns = Vec::new();
                for info_row in &table_info_response.rows {
                    if info_row.len() >= 6
                        && let (Some(Some(cid_bytes)), Some(Some(name_bytes)), Some(Some(pk_bytes))) =
                            (info_row.first(), info_row.get(1), info_row.get(5)) {
                            let pk_flag = String::from_utf8_lossy(pk_bytes);
                            if pk_flag == "1" {
                                let column_name = String::from_utf8_lossy(name_bytes).to_string();
                                let cid = String::from_utf8_lossy(cid_bytes).parse::<i32>().unwrap_or(0);
                                pk_columns.push((cid + 1, column_name)); // PostgreSQL uses 1-based indexing
                            }
                        }
                }

                // Create primary key constraint if any columns found
                if !pk_columns.is_empty() {
                    pk_columns.sort_by_key(|&(cid, _)| cid);
                    let constraint = ConstraintInfo {
                        oid: constraint_id,
                        conname: format!("{}_pkey", table_name),
                        connamespace: 2200, // public schema OID
                        contype: 'p',
                        condeferrable: false,
                        condeferred: false,
                        convalidated: true,
                        conrelid: Self::generate_table_oid(&table_name),
                        contypid: 0,
                        conindid: constraint_id + 1000, // Arbitrary index OID
                        conparentid: 0,
                        confrelid: 0,
                        confupdtype: ' ',
                        confdeltype: ' ',
                        confmatchtype: ' ',
                        conislocal: true,
                        coninhcount: 0,
                        connoinherit: true,
                        conkey: pk_columns.iter().map(|&(cid, _)| cid).collect(),
                        confkey: vec![],
                        table_name: table_name.clone(),
                    };
                    constraints.push(constraint);
                    constraint_id += 1;
                }

                // Get foreign key constraints
                let fk_response = db.query(&format!("PRAGMA foreign_key_list({})", table_name)).await?;

                for fk_row in &fk_response.rows {
                    if fk_row.len() >= 8
                        && let (Some(Some(id_bytes)), Some(Some(table_bytes)), Some(Some(from_bytes)), Some(Some(to_bytes))) =
                            (fk_row.first(), fk_row.get(2), fk_row.get(3), fk_row.get(4)) {

                            let fk_id = String::from_utf8_lossy(id_bytes);
                            let ref_table = String::from_utf8_lossy(table_bytes).to_string();
                            let from_column = String::from_utf8_lossy(from_bytes).to_string();
                            let to_column = String::from_utf8_lossy(to_bytes).to_string();

                            // Find column position
                            let from_col_pos = Self::find_column_position(db, &table_name, &from_column).await.unwrap_or(1);
                            let to_col_pos = Self::find_column_position(db, &ref_table, &to_column).await.unwrap_or(1);

                            let constraint = ConstraintInfo {
                                oid: constraint_id,
                                conname: format!("{}_{}_{}_fkey", table_name, from_column, fk_id),
                                connamespace: 2200, // public schema OID
                                contype: 'f',
                                condeferrable: false,
                                condeferred: false,
                                convalidated: true,
                                conrelid: Self::generate_table_oid(&table_name),
                                contypid: 0,
                                conindid: 0,
                                conparentid: 0,
                                confrelid: Self::generate_table_oid(&ref_table),
                                confupdtype: 'a', // NO ACTION (default)
                                confdeltype: 'a', // NO ACTION (default)
                                confmatchtype: 's', // SIMPLE (default)
                                conislocal: true,
                                coninhcount: 0,
                                connoinherit: true,
                                conkey: vec![from_col_pos],
                                confkey: vec![to_col_pos],
                                table_name: table_name.clone(),
                            };
                            constraints.push(constraint);
                            constraint_id += 1;
                        }
                }
            }
        }

        Ok(constraints)
    }

    async fn find_column_position(db: &DbHandler, table_name: &str, column_name: &str) -> Result<i32, PgSqliteError> {
        let table_info = db.query(&format!("PRAGMA table_info({})", table_name)).await?;

        for (idx, row) in table_info.rows.iter().enumerate() {
            if row.len() >= 2
                && let Some(Some(name_bytes)) = row.get(1) {
                    let name = String::from_utf8_lossy(name_bytes);
                    if name == column_name {
                        return Ok((idx + 1) as i32); // PostgreSQL uses 1-based indexing
                    }
                }
        }

        Ok(1) // Default fallback
    }

    fn generate_table_oid(table_name: &str) -> u32 {
        // Generate deterministic OID from table name (same as pg_class handler)
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        table_name.hash(&mut hasher);
        let hash = hasher.finish();
        16384 + ((hash % 65536) as u32) // Keep OIDs in reasonable range
    }

    fn constraint_to_row(constraint: &ConstraintInfo) -> Vec<Option<Vec<u8>>> {
        vec![
            Some(constraint.oid.to_string().into_bytes()),                    // oid
            Some(constraint.conname.clone().into_bytes()),                   // conname
            Some(constraint.connamespace.to_string().into_bytes()),          // connamespace
            Some(constraint.contype.to_string().into_bytes()),               // contype
            Some((if constraint.condeferrable { "t" } else { "f" }).to_string().into_bytes()), // condeferrable
            Some((if constraint.condeferred { "t" } else { "f" }).to_string().into_bytes()),   // condeferred
            Some((if constraint.convalidated { "t" } else { "f" }).to_string().into_bytes()),  // convalidated
            Some(constraint.conrelid.to_string().into_bytes()),              // conrelid
            Some(constraint.contypid.to_string().into_bytes()),              // contypid
            Some(constraint.conindid.to_string().into_bytes()),              // conindid
            Some(constraint.conparentid.to_string().into_bytes()),           // conparentid
            Some(constraint.confrelid.to_string().into_bytes()),             // confrelid
            Some(constraint.confupdtype.to_string().into_bytes()),           // confupdtype
            Some(constraint.confdeltype.to_string().into_bytes()),           // confdeltype
            Some(constraint.confmatchtype.to_string().into_bytes()),         // confmatchtype
            Some((if constraint.conislocal { "t" } else { "f" }).to_string().into_bytes()),     // conislocal
            Some(constraint.coninhcount.to_string().into_bytes()),           // coninhcount
            Some((if constraint.connoinherit { "t" } else { "f" }).to_string().into_bytes()), // connoinherit
            Some(Self::array_to_pg_format(&constraint.conkey).into_bytes()), // conkey
            Some(Self::array_to_pg_format(&constraint.confkey).into_bytes()), // confkey
            None,                                                            // conpfeqop
            None,                                                            // conppeqop
            None,                                                            // conffeqop
            None,                                                            // confdelsetcols
            None,                                                            // conexclop
            None,                                                            // conbin
        ]
    }

    fn constraint_to_map(constraint: &ConstraintInfo) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("oid".to_string(), constraint.oid.to_string());
        map.insert("conname".to_string(), constraint.conname.clone());
        map.insert("connamespace".to_string(), constraint.connamespace.to_string());
        map.insert("contype".to_string(), constraint.contype.to_string());
        map.insert("conrelid".to_string(), constraint.conrelid.to_string());
        map.insert("confrelid".to_string(), constraint.confrelid.to_string());
        map
    }

    fn array_to_pg_format(arr: &[i32]) -> String {
        if arr.is_empty() {
            "{}".to_string()
        } else {
            format!("{{{}}}", arr.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","))
        }
    }
}

#[derive(Clone)]
struct ConstraintInfo {
    oid: u32,
    conname: String,
    connamespace: u32,
    contype: char,
    condeferrable: bool,
    condeferred: bool,
    convalidated: bool,
    conrelid: u32,
    contypid: u32,
    conindid: u32,
    conparentid: u32,
    confrelid: u32,
    confupdtype: char,
    confdeltype: char,
    confmatchtype: char,
    conislocal: bool,
    coninhcount: i32,
    connoinherit: bool,
    conkey: Vec<i32>,
    confkey: Vec<i32>,
    #[allow(dead_code)]
    table_name: String,
}