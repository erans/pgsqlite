use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgTriggerHandler;

impl PgTriggerHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_trigger query");

        let all_columns = vec![
            "oid".to_string(),
            "tgrelid".to_string(),
            "tgparentid".to_string(),
            "tgname".to_string(),
            "tgfoid".to_string(),
            "tgtype".to_string(),
            "tgenabled".to_string(),
            "tgisinternal".to_string(),
            "tgconstrrelid".to_string(),
            "tgconstrindid".to_string(),
            "tgconstraint".to_string(),
            "tgdeferrable".to_string(),
            "tginitdeferred".to_string(),
            "tgnargs".to_string(),
            "tgattr".to_string(),
            "tgargs".to_string(),
            "tgqual".to_string(),
            "tgoldtable".to_string(),
            "tgnewtable".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        let triggers = Self::get_triggers(db).await?;

        let mut filtered_triggers = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&triggers, where_clause)?
        } else {
            triggers
        };

        filtered_triggers.sort_by(|a, b| {
            let name_a = a.get("tgname").and_then(|v| String::from_utf8(v.clone()).ok()).unwrap_or_default();
            let name_b = b.get("tgname").and_then(|v| String::from_utf8(v.clone()).ok()).unwrap_or_default();
            name_a.cmp(&name_b)
        });

        let mut rows = Vec::new();
        for trigger in filtered_triggers {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = trigger.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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

    async fn get_triggers(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut triggers = Vec::new();

        let conn = rusqlite::Connection::open(&db.db_path).map_err(PgSqliteError::Sqlite)?;

        let query = "SELECT name, tbl_name, sql FROM sqlite_master WHERE type = 'trigger'";

        let mut stmt = conn.prepare(query).map_err(PgSqliteError::Sqlite)?;

        let rows = stmt.query_map([], |row| {
            let name: String = row.get(0)?;
            let tbl_name: String = row.get(1)?;
            let sql: String = row.get(2)?;
            Ok((name, tbl_name, sql))
        }).map_err(PgSqliteError::Sqlite)?;

        for row_result in rows.flatten() {
            let (trigger_name, table_name, trigger_sql) = row_result;

            let (timing, event, _orientation) = Self::parse_trigger_sql(&trigger_sql);

            let trigger_oid = Self::generate_trigger_oid(&trigger_name);
            let table_oid = Self::generate_table_oid(&table_name);
            let tgtype = Self::calculate_tgtype(&timing, &event);

            let mut trigger = HashMap::new();

            trigger.insert("oid".to_string(), trigger_oid.to_string().into_bytes());
            trigger.insert("tgrelid".to_string(), table_oid.to_string().into_bytes());
            trigger.insert("tgparentid".to_string(), b"0".to_vec());
            trigger.insert("tgname".to_string(), trigger_name.clone().into_bytes());
            trigger.insert("tgfoid".to_string(), b"0".to_vec());
            trigger.insert("tgtype".to_string(), tgtype.to_string().into_bytes());
            trigger.insert("tgenabled".to_string(), b"O".to_vec());
            trigger.insert("tgisinternal".to_string(), b"f".to_vec());
            trigger.insert("tgconstrrelid".to_string(), b"0".to_vec());
            trigger.insert("tgconstrindid".to_string(), b"0".to_vec());
            trigger.insert("tgconstraint".to_string(), b"0".to_vec());
            trigger.insert("tgdeferrable".to_string(), b"f".to_vec());
            trigger.insert("tginitdeferred".to_string(), b"f".to_vec());
            trigger.insert("tgnargs".to_string(), b"0".to_vec());
            trigger.insert("tgattr".to_string(), b"".to_vec());
            trigger.insert("tgargs".to_string(), b"".to_vec());
            trigger.insert("tgqual".to_string(), b"".to_vec());
            trigger.insert("tgoldtable".to_string(), b"".to_vec());
            trigger.insert("tgnewtable".to_string(), b"".to_vec());

            debug!("Found trigger {} on table {} with type {}", trigger_name, table_name, tgtype);
            triggers.push(trigger);
        }

        Ok(triggers)
    }

    fn parse_trigger_sql(sql: &str) -> (String, String, String) {
        let sql_upper = sql.to_uppercase();

        let timing = if sql_upper.contains("BEFORE") {
            "BEFORE".to_string()
        } else if sql_upper.contains("AFTER") {
            "AFTER".to_string()
        } else if sql_upper.contains("INSTEAD OF") {
            "INSTEAD OF".to_string()
        } else {
            "BEFORE".to_string()
        };

        let event = if let Some(on_pos) = sql_upper.find(" ON ") {
            let before_on = &sql_upper[..on_pos];

            if before_on.contains(" DELETE") || before_on.ends_with("DELETE") {
                "DELETE".to_string()
            } else if before_on.contains(" UPDATE") || before_on.ends_with("UPDATE") {
                "UPDATE".to_string()
            } else {
                "INSERT".to_string()
            }
        } else {
            if sql_upper.contains("DELETE") {
                "DELETE".to_string()
            } else if sql_upper.contains("UPDATE") {
                "UPDATE".to_string()
            } else {
                "INSERT".to_string()
            }
        };

        let orientation = "ROW".to_string();

        (timing, event, orientation)
    }

    fn calculate_tgtype(timing: &str, event: &str) -> i16 {
        let mut tgtype: i16 = 0;

        tgtype |= 1;

        if timing == "BEFORE" {
            tgtype |= 2;
        } else if timing == "INSTEAD OF" {
            tgtype |= 64;
        }

        if event == "INSERT" {
            tgtype |= 4;
        } else if event == "DELETE" {
            tgtype |= 8;
        } else if event == "UPDATE" {
            tgtype |= 16;
        }

        tgtype
    }

    fn generate_trigger_oid(trigger_name: &str) -> u32 {
        let mut hash = 0u32;
        for byte in trigger_name.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
        }
        16384 + (hash % 65536)
    }

    fn generate_table_oid(table_name: &str) -> u32 {
        let mut hash = 0u32;
        for byte in table_name.bytes() {
            hash = hash.wrapping_mul(31).wrapping_add(byte as u32);
        }
        16384 + (hash % 65536)
    }

    fn apply_where_filter(
        triggers: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for trigger in triggers {
            let mut string_data = HashMap::new();
            for (key, value) in trigger {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(trigger.clone());
            }
        }

        Ok(filtered)
    }
}
