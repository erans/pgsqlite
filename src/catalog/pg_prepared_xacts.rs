use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

/// Handler for pg_prepared_xacts view - shows prepared transactions
pub struct PgPreparedXactsHandler;

impl PgPreparedXactsHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_prepared_xacts query");

        // pg_prepared_xacts columns
        let all_columns = vec![
            "transaction".to_string(),
            "gid".to_string(),
            "prepared".to_string(),
            "owner".to_string(),
            "database".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Get prepared transactions (empty - SQLite doesn't support 2PC)
        let xacts = Self::get_prepared_xacts();

        // Apply WHERE clause filtering if present
        let filtered_xacts = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&xacts, where_clause, &selected_columns)?
        } else {
            xacts
        };

        // Build response
        let mut rows = Vec::new();
        for xact in filtered_xacts {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = xact.get(column).cloned().unwrap_or_else(|| b"".to_vec());
                row.push(Some(value));
            }
            rows.push(row);
        }

        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns,
            rows,
            rows_affected: rows_count,
        })
    }

    fn get_selected_columns(projection: &[SelectItem], all_columns: &[String]) -> Vec<String> {
        let mut selected = Vec::new();

        for item in projection {
            match item {
                SelectItem::Wildcard(_) => {
                    selected.extend_from_slice(all_columns);
                    break;
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    if all_columns.contains(&col_name) {
                        selected.push(col_name);
                    }
                }
                SelectItem::ExprWithAlias { expr: Expr::Identifier(ident), alias } => {
                    let col_name = ident.value.to_lowercase();
                    if all_columns.contains(&col_name) {
                        selected.push(alias.value.clone());
                    }
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    selected.extend_from_slice(all_columns);
                    break;
                }
                _ => {}
            }
        }

        selected
    }

    fn get_prepared_xacts() -> Vec<HashMap<String, Vec<u8>>> {
        // SQLite doesn't support two-phase commit
        // Return empty result set
        vec![]
    }

    fn apply_where_filter(
        xacts: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for xact in xacts {
            let mut string_data = HashMap::new();
            for (key, value) in xact {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(xact.clone());
            }
        }

        Ok(filtered)
    }
}
