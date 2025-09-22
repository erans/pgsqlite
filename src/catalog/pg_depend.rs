use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgDependHandler;

impl PgDependHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_depend query");

        // Define all available columns - PostgreSQL has 7 columns in pg_depend
        let all_columns = vec![
            "classid".to_string(),
            "objid".to_string(),
            "objsubid".to_string(),
            "refclassid".to_string(),
            "refobjid".to_string(),
            "refobjsubid".to_string(),
            "deptype".to_string(),
        ];

        // Determine which columns to return based on SELECT clause
        let requested_columns = Self::extract_requested_columns(select, &all_columns);

        // Create column indices for projection
        let column_indices: Vec<usize> = requested_columns.iter()
            .filter_map(|col| all_columns.iter().position(|c| c == col))
            .collect();

        // Get dependencies from pg_depend table
        let dependencies = Self::get_dependencies_from_table(db).await?;

        // Filter based on WHERE clause if present
        let filtered_dependencies = if let Some(ref where_clause) = select.selection {
            let column_mapping = HashMap::new(); // Empty mapping for now
            dependencies.into_iter()
                .filter(|dependency| {
                    WhereEvaluator::evaluate(where_clause, &Self::dependency_to_map(dependency), &column_mapping)
                })
                .collect()
        } else {
            dependencies
        };

        // Build rows
        let mut rows = Vec::new();
        for dependency in filtered_dependencies {
            let full_row = Self::dependency_to_row(&dependency);

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

    async fn get_dependencies_from_table(db: &DbHandler) -> Result<Vec<DependencyInfo>, PgSqliteError> {
        let response = db.query("SELECT classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype FROM pg_depend").await?;

        let mut dependencies = Vec::new();
        for row in &response.rows {
            if row.len() >= 7
                && let (Some(Some(classid_bytes)), Some(Some(objid_bytes)), Some(Some(objsubid_bytes)),
                        Some(Some(refclassid_bytes)), Some(Some(refobjid_bytes)), Some(Some(refobjsubid_bytes)),
                        Some(Some(deptype_bytes))) =
                    (row.first(), row.get(1), row.get(2), row.get(3), row.get(4), row.get(5), row.get(6)) {

                    let classid = String::from_utf8_lossy(classid_bytes).parse::<u32>().unwrap_or(0);
                    let objid = String::from_utf8_lossy(objid_bytes).parse::<u32>().unwrap_or(0);
                    let objsubid = String::from_utf8_lossy(objsubid_bytes).parse::<i32>().unwrap_or(0);
                    let refclassid = String::from_utf8_lossy(refclassid_bytes).parse::<u32>().unwrap_or(0);
                    let refobjid = String::from_utf8_lossy(refobjid_bytes).parse::<u32>().unwrap_or(0);
                    let refobjsubid = String::from_utf8_lossy(refobjsubid_bytes).parse::<i32>().unwrap_or(0);
                    let deptype = String::from_utf8_lossy(deptype_bytes).chars().next().unwrap_or('a');

                    dependencies.push(DependencyInfo {
                        classid,
                        objid,
                        objsubid,
                        refclassid,
                        refobjid,
                        refobjsubid,
                        deptype,
                    });
                }
        }

        Ok(dependencies)
    }


    fn dependency_to_row(dependency: &DependencyInfo) -> Vec<Option<Vec<u8>>> {
        vec![
            Some(dependency.classid.to_string().into_bytes()),        // classid
            Some(dependency.objid.to_string().into_bytes()),          // objid
            Some(dependency.objsubid.to_string().into_bytes()),       // objsubid
            Some(dependency.refclassid.to_string().into_bytes()),     // refclassid
            Some(dependency.refobjid.to_string().into_bytes()),       // refobjid
            Some(dependency.refobjsubid.to_string().into_bytes()),    // refobjsubid
            Some(dependency.deptype.to_string().into_bytes()),        // deptype
        ]
    }

    fn dependency_to_map(dependency: &DependencyInfo) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("classid".to_string(), dependency.classid.to_string());
        map.insert("objid".to_string(), dependency.objid.to_string());
        map.insert("objsubid".to_string(), dependency.objsubid.to_string());
        map.insert("refclassid".to_string(), dependency.refclassid.to_string());
        map.insert("refobjid".to_string(), dependency.refobjid.to_string());
        map.insert("refobjsubid".to_string(), dependency.refobjsubid.to_string());
        map.insert("deptype".to_string(), dependency.deptype.to_string());
        map
    }
}

#[derive(Clone)]
struct DependencyInfo {
    classid: u32,        // System catalog OID where dependent object is listed
    objid: u32,          // OID of the dependent object
    objsubid: i32,       // Column number for table dependencies, 0 otherwise
    refclassid: u32,     // System catalog OID where referenced object is listed
    refobjid: u32,       // OID of the referenced object
    refobjsubid: i32,    // Column number for referenced object
    deptype: char,       // Dependency type ('a' = automatic)
}