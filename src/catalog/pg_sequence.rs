use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgSequenceHandler;

impl PgSequenceHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_sequence query");

        let all_columns = vec![
            "seqrelid".to_string(),
            "seqtypid".to_string(),
            "seqstart".to_string(),
            "seqincrement".to_string(),
            "seqmax".to_string(),
            "seqmin".to_string(),
            "seqcache".to_string(),
            "seqcycle".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        let sequences = Self::get_sequences(db).await?;

        let filtered_sequences = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&sequences, where_clause)?
        } else {
            sequences
        };

        let mut rows = Vec::new();
        for sequence in filtered_sequences {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = sequence.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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
                SelectItem::UnnamedExpr(Expr::Function(_)) => {
                    selected.push("count".to_string());
                }
                SelectItem::ExprWithAlias { expr: Expr::Function(_), alias } => {
                    selected.push(alias.value.clone());
                }
                _ => {}
            }
        }

        selected
    }

    async fn get_sequences(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut sequences = Vec::new();

        let conn = rusqlite::Connection::open(&db.db_path).map_err(PgSqliteError::Sqlite)?;

        let query = "SELECT name, seq FROM sqlite_sequence";

        let mut stmt = match conn.prepare(query) {
            Ok(stmt) => stmt,
            Err(e) => {
                if e.to_string().contains("no such table") {
                    debug!("sqlite_sequence table doesn't exist (no AUTOINCREMENT columns)");
                    return Ok(sequences);
                }
                return Err(PgSqliteError::Sqlite(e));
            }
        };

        let rows = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let seq: i64 = row.get(1)?;
            Ok((name, seq))
        }).map_err(PgSqliteError::Sqlite)?;

        for row_result in rows.flatten() {
            let (table_name, current_value) = row_result;

            let table_oid = Self::generate_table_oid(&table_name);

            let mut sequence = HashMap::new();

            sequence.insert("seqrelid".to_string(), table_oid.to_string().into_bytes());
            sequence.insert("seqtypid".to_string(), b"20".to_vec()); // int8 (BIGINT) type OID
            sequence.insert("seqstart".to_string(), b"1".to_vec()); // SQLite AUTOINCREMENT starts at 1
            sequence.insert("seqincrement".to_string(), b"1".to_vec()); // SQLite increments by 1

            // Max value for BIGINT: 9223372036854775807
            sequence.insert("seqmax".to_string(), b"9223372036854775807".to_vec());

            // Min value for sequences: 1
            sequence.insert("seqmin".to_string(), b"1".to_vec());

            // Cache size (SQLite doesn't have caching concept, default to 1)
            sequence.insert("seqcache".to_string(), b"1".to_vec());

            // Cycle behavior (SQLite doesn't cycle, default to false)
            sequence.insert("seqcycle".to_string(), b"f".to_vec());

            debug!("Found sequence for table {} with current value {}", table_name, current_value);
            sequences.push(sequence);
        }

        Ok(sequences)
    }

    fn generate_table_oid(table_name: &str) -> u32 {
        let mut hash = 0u32;
        for byte in table_name.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
        }
        16384 + (hash % 65536)
    }

    fn apply_where_filter(
        sequences: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for sequence in sequences {
            let mut string_data = HashMap::new();
            for (key, value) in sequence {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(sequence.clone());
            }
        }

        Ok(filtered)
    }
}
