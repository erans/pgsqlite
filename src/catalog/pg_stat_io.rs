use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

/// Handler for pg_stat_io view - PostgreSQL 16+ I/O statistics
/// Provides information about I/O operations for the database
pub struct PgStatIoHandler;

impl PgStatIoHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_stat_io query");

        // pg_stat_io columns (PostgreSQL 16+)
        let all_columns = vec![
            "backend_type".to_string(),
            "reads".to_string(),
            "writes".to_string(),
            "write_bytes".to_string(),
            "opsize_reads".to_string(),
            "opsize_writes".to_string(),
            "opbytes_reads".to_string(),
            "opbytes_writes".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Get I/O statistics (simulated for SQLite)
        let stats = Self::get_io_statistics();

        // Apply WHERE clause filtering if present
        let filtered_stats = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&stats, where_clause, &selected_columns)?
        } else {
            stats
        };

        // Build response
        let mut rows = Vec::new();
        for stat in filtered_stats {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = stat.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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

    fn get_io_statistics() -> Vec<HashMap<String, Vec<u8>>> {
        // Simulate I/O statistics for SQLite
        // In a real implementation, this would track actual I/O operations
        vec![
            {
                let mut map = HashMap::new();
                map.insert("backend_type".to_string(), b"client backend".to_vec());
                map.insert("reads".to_string(), b"0".to_vec());
                map.insert("writes".to_string(), b"0".to_vec());
                map.insert("write_bytes".to_string(), b"0".to_vec());
                map.insert("opsize_reads".to_string(), b"0".to_vec());
                map.insert("opsize_writes".to_string(), b"0".to_vec());
                map.insert("opbytes_reads".to_string(), b"0".to_vec());
                map.insert("opbytes_writes".to_string(), b"0".to_vec());
                map
            },
            {
                let mut map = HashMap::new();
                map.insert("backend_type".to_string(), b"background writer".to_vec());
                map.insert("reads".to_string(), b"0".to_vec());
                map.insert("writes".to_string(), b"0".to_vec());
                map.insert("write_bytes".to_string(), b"0".to_vec());
                map.insert("opsize_reads".to_string(), b"0".to_vec());
                map.insert("opsize_writes".to_string(), b"0".to_vec());
                map.insert("opbytes_reads".to_string(), b"0".to_vec());
                map.insert("opbytes_writes".to_string(), b"0".to_vec());
                map
            },
            {
                let mut map = HashMap::new();
                map.insert("backend_type".to_string(), b"checkpointer".to_vec());
                map.insert("reads".to_string(), b"0".to_vec());
                map.insert("writes".to_string(), b"0".to_vec());
                map.insert("write_bytes".to_string(), b"0".to_vec());
                map.insert("opsize_reads".to_string(), b"0".to_vec());
                map.insert("opsize_writes".to_string(), b"0".to_vec());
                map.insert("opbytes_reads".to_string(), b"0".to_vec());
                map.insert("opbytes_writes".to_string(), b"0".to_vec());
                map
            },
            {
                let mut map = HashMap::new();
                map.insert("backend_type".to_string(), b"walwriter".to_vec());
                map.insert("reads".to_string(), b"0".to_vec());
                map.insert("writes".to_string(), b"0".to_vec());
                map.insert("write_bytes".to_string(), b"0".to_vec());
                map.insert("opsize_reads".to_string(), b"0".to_vec());
                map.insert("opsize_writes".to_string(), b"0".to_vec());
                map.insert("opbytes_reads".to_string(), b"0".to_vec());
                map.insert("opbytes_writes".to_string(), b"0".to_vec());
                map
            },
        ]
    }

    fn apply_where_filter(
        stats: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for stat in stats {
            let mut string_data = HashMap::new();
            for (key, value) in stat {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(stat.clone());
            }
        }

        Ok(filtered)
    }
}
