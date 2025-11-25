use crate::session::db_handler::DbResponse;
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgSettingsHandler;

impl PgSettingsHandler {
    pub fn handle_query(select: &Select) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_settings query");

        let all_columns = vec![
            "name".to_string(),
            "setting".to_string(),
            "unit".to_string(),
            "category".to_string(),
            "short_desc".to_string(),
            "extra_desc".to_string(),
            "context".to_string(),
            "vartype".to_string(),
            "source".to_string(),
            "min_val".to_string(),
            "max_val".to_string(),
            "enumvals".to_string(),
            "boot_val".to_string(),
            "reset_val".to_string(),
            "sourcefile".to_string(),
            "sourceline".to_string(),
            "pending_restart".to_string(),
        ];

        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        let settings = Self::get_all_settings();

        let filtered_settings = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&settings, where_clause)?
        } else {
            settings
        };

        let mut rows = Vec::new();
        for setting in filtered_settings {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = setting.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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

    fn get_all_settings() -> Vec<HashMap<String, Vec<u8>>> {
        let settings_data: Vec<(&str, &str, &str, &str, &str, &str, &str)> = vec![
            ("server_version", "16.0", "", "Preset Options", "PostgreSQL version string", "internal", "string"),
            ("server_version_num", "160000", "", "Preset Options", "PostgreSQL version number", "internal", "integer"),
            ("server_encoding", "UTF8", "", "Client Connection Defaults", "Server encoding", "internal", "string"),
            ("client_encoding", "UTF8", "", "Client Connection Defaults", "Client encoding", "user", "string"),
            ("DateStyle", "ISO, MDY", "", "Client Connection Defaults", "Date display format", "user", "string"),
            ("TimeZone", "UTC", "", "Client Connection Defaults", "Time zone", "user", "string"),
            ("timezone_abbreviations", "Default", "", "Client Connection Defaults", "Time zone abbreviations", "user", "string"),
            ("extra_float_digits", "1", "", "Client Connection Defaults", "Extra float digits", "user", "integer"),
            ("integer_datetimes", "on", "", "Preset Options", "Integer datetimes", "internal", "bool"),
            ("max_connections", "100", "", "Connections and Authentication", "Maximum connections", "postmaster", "integer"),
            ("superuser_reserved_connections", "3", "", "Connections and Authentication", "Reserved for superuser", "postmaster", "integer"),
            ("shared_buffers", "128MB", "8kB", "Resource Usage / Memory", "Shared memory buffers", "postmaster", "integer"),
            ("work_mem", "4MB", "kB", "Resource Usage / Memory", "Work memory", "user", "integer"),
            ("maintenance_work_mem", "64MB", "kB", "Resource Usage / Memory", "Maintenance work memory", "user", "integer"),
            ("effective_cache_size", "4GB", "8kB", "Query Tuning / Planner Cost Constants", "Effective cache size", "user", "integer"),
            ("random_page_cost", "4", "", "Query Tuning / Planner Cost Constants", "Random page cost", "user", "real"),
            ("seq_page_cost", "1", "", "Query Tuning / Planner Cost Constants", "Sequential page cost", "user", "real"),
            ("standard_conforming_strings", "on", "", "Client Connection Defaults", "Standard conforming strings", "user", "bool"),
            ("escape_string_warning", "on", "", "Client Connection Defaults", "Escape string warning", "user", "bool"),
            ("bytea_output", "hex", "", "Client Connection Defaults", "Bytea output format", "user", "enum"),
            ("search_path", "\"$user\", public", "", "Client Connection Defaults", "Schema search path", "user", "string"),
            ("log_statement", "none", "", "Reporting and Logging", "Log statements", "superuser", "enum"),
            ("log_min_duration_statement", "-1", "ms", "Reporting and Logging", "Min duration to log", "superuser", "integer"),
            ("lc_collate", "en_US.UTF-8", "", "Client Connection Defaults", "Collation locale", "internal", "string"),
            ("lc_ctype", "en_US.UTF-8", "", "Client Connection Defaults", "Character type locale", "internal", "string"),
            ("lc_messages", "en_US.UTF-8", "", "Client Connection Defaults", "Messages locale", "superuser", "string"),
            ("lc_monetary", "en_US.UTF-8", "", "Client Connection Defaults", "Monetary locale", "user", "string"),
            ("lc_numeric", "en_US.UTF-8", "", "Client Connection Defaults", "Numeric locale", "user", "string"),
            ("lc_time", "en_US.UTF-8", "", "Client Connection Defaults", "Time locale", "user", "string"),
            ("default_transaction_isolation", "read committed", "", "Client Connection Defaults", "Default isolation level", "user", "enum"),
            ("default_transaction_read_only", "off", "", "Client Connection Defaults", "Default read only", "user", "bool"),
            ("transaction_isolation", "read committed", "", "Client Connection Defaults", "Transaction isolation", "user", "enum"),
            ("transaction_read_only", "off", "", "Client Connection Defaults", "Transaction read only", "user", "bool"),
            ("application_name", "", "", "Client Connection Defaults", "Application name", "user", "string"),
            ("ssl", "off", "", "Connections and Authentication", "SSL enabled", "sighup", "bool"),
            ("wal_level", "replica", "", "Write-Ahead Log", "WAL level", "postmaster", "enum"),
            ("max_wal_senders", "10", "", "Replication", "Max WAL senders", "postmaster", "integer"),
            ("autovacuum", "on", "", "Autovacuum", "Autovacuum enabled", "sighup", "bool"),
            ("statement_timeout", "0", "ms", "Client Connection Defaults", "Statement timeout", "user", "integer"),
            ("lock_timeout", "0", "ms", "Client Connection Defaults", "Lock timeout", "user", "integer"),
            ("idle_in_transaction_session_timeout", "0", "ms", "Client Connection Defaults", "Idle transaction timeout", "user", "integer"),
        ];

        settings_data
            .into_iter()
            .map(|(name, setting, unit, category, short_desc, context, vartype)| {
                let mut setting_map = HashMap::new();

                setting_map.insert("name".to_string(), name.as_bytes().to_vec());
                setting_map.insert("setting".to_string(), setting.as_bytes().to_vec());
                if unit.is_empty() {
                    setting_map.insert("unit".to_string(), b"".to_vec());
                } else {
                    setting_map.insert("unit".to_string(), unit.as_bytes().to_vec());
                }
                setting_map.insert("category".to_string(), category.as_bytes().to_vec());
                setting_map.insert("short_desc".to_string(), short_desc.as_bytes().to_vec());
                setting_map.insert("extra_desc".to_string(), b"".to_vec());
                setting_map.insert("context".to_string(), context.as_bytes().to_vec());
                setting_map.insert("vartype".to_string(), vartype.as_bytes().to_vec());
                setting_map.insert("source".to_string(), b"default".to_vec());
                setting_map.insert("min_val".to_string(), b"".to_vec());
                setting_map.insert("max_val".to_string(), b"".to_vec());
                setting_map.insert("enumvals".to_string(), b"".to_vec());
                setting_map.insert("boot_val".to_string(), setting.as_bytes().to_vec());
                setting_map.insert("reset_val".to_string(), setting.as_bytes().to_vec());
                setting_map.insert("sourcefile".to_string(), b"".to_vec());
                setting_map.insert("sourceline".to_string(), b"".to_vec());
                setting_map.insert("pending_restart".to_string(), b"f".to_vec());

                setting_map
            })
            .collect()
    }

    fn apply_where_filter(
        settings: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for setting in settings {
            let mut string_data = HashMap::new();
            for (key, value) in setting {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new();
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(setting.clone());
            }
        }

        Ok(filtered)
    }
}
