use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

/// Handler for pg_locks view - provides information about locks held
pub struct PgLocksHandler;

impl PgLocksHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_locks query");

        // pg_lock columns
        let all_columns = vec![
            "locktype".to_string(),
            "database".to_string(),
            "relation".to_string(),
            "page".to_string(),
            "tuple".to_string(),
            "virtualxid".to_string(),
            "transactionid".to_string(),
            "classid".to_string(),
            "objid".to_string(),
            "objsubid".to_string(),
            "virtualtransaction".to_string(),
            "pid".to_string(),
            "mode".to_string(),
            "granted".to_string(),
            "fastpath".to_string(),
            "waitstart".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Get lock information (empty for SQLite as it doesn't have row-level locking)
        let locks = Self::get_lock_information();

        // Apply WHERE clause filtering if present
        let filtered_locks = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&locks, where_clause, &selected_columns)?
        } else {
            locks
        };

        // Build response
        let mut rows = Vec::new();
        for lock in filtered_locks {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = lock.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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

    fn get_lock_information() -> Vec<HashMap<String, Vec<u8>>> {
        // SQLite doesn't have row-level locking like PostgreSQL
        // Return empty result set for compatibility
        vec![]
    }

    fn apply_where_filter(
        locks: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for lock in locks {
            let mut string_data = HashMap::new();
            for (key, value) in lock {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(lock.clone());
            }
        }

        Ok(filtered)
    }
}
