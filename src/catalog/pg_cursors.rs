use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

/// Handler for pg_cursors view - shows open cursors
pub struct PgCursorsHandler;

impl PgCursorsHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_cursors query");

        // pg_cursors columns
        let all_columns = vec![
            "name".to_string(),
            "statement".to_string(),
            "is_holdable".to_string(),
            "is_binary".to_string(),
            "cursor_scope".to_string(),
            "status".to_string(),
            "creation_time".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Get cursor information (empty - would need session state tracking)
        let cursors = Self::get_cursors();

        // Apply WHERE clause filtering if present
        let filtered_cursors = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&cursors, where_clause, &selected_columns)?
        } else {
            cursors
        };

        // Build response
        let mut rows = Vec::new();
        for cursor in filtered_cursors {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = cursor.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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

    fn get_cursors() -> Vec<HashMap<String, Vec<u8>>> {
        // Return empty - in a full implementation, this would track
        // open cursors from the session state
        vec![]
    }

    fn apply_where_filter(
        cursors: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for cursor in cursors {
            let mut string_data = HashMap::new();
            for (key, value) in cursor {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(cursor.clone());
            }
        }

        Ok(filtered)
    }
}
