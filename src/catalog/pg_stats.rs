use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgStatsHandler;

impl PgStatsHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_stats query");
        eprintln!("🔍 PgStatsHandler::handle_query called");

        // Define all available columns for PostgreSQL pg_stats view
        let all_columns = vec![
            "schemaname".to_string(),
            "tablename".to_string(),
            "attname".to_string(),
            "inherited".to_string(),
            "null_frac".to_string(),
            "n_distinct".to_string(),
            "most_common_vals".to_string(),
            "most_common_freqs".to_string(),
            "histogram_bounds".to_string(),
            "correlation".to_string(),
            "most_common_elems".to_string(),
            "most_common_elem_freqs".to_string(),
            "elem_count_histogram".to_string(),
        ];

        // Determine which columns to return
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Build statistics from actual SQLite tables
        let stats = Self::get_table_statistics(db).await?;
        eprintln!("🔍 Generated {} statistics rows", stats.len());

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
                    // For qualified wildcard like pg_stats.*, return all columns
                    selected.extend_from_slice(all_columns);
                    break;
                }
                SelectItem::UnnamedExpr(Expr::Function(_)) => {
                    // For functions like COUNT(*), we need to return a placeholder column
                    // The actual function will be handled differently
                    selected.push("count".to_string());
                }
                SelectItem::ExprWithAlias { expr: Expr::Function(_), alias } => {
                    // For aliased functions
                    selected.push(alias.value.clone());
                }
                _ => {}
            }
        }

        selected
    }

    async fn get_table_statistics(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut stats = Vec::new();

        // Get all tables from SQLite
        let tables = Self::get_all_tables(db).await?;
        debug!("Found {} tables for statistics generation", tables.len());

        for table_name in tables {
            debug!("Processing table: {}", table_name);
            // Get columns for each table
            let columns = Self::get_table_columns(db, &table_name).await?;
            debug!("Found {} columns for table {}", columns.len(), table_name);

            for column_info in columns {
                let mut stat = HashMap::new();

                // Basic table/column info
                stat.insert("schemaname".to_string(), b"public".to_vec());
                stat.insert("tablename".to_string(), table_name.as_bytes().to_vec());
                stat.insert("attname".to_string(), column_info.name.as_bytes().to_vec());
                stat.insert("inherited".to_string(), b"f".to_vec()); // false

                // Generate realistic statistics based on column type and name
                let column_stats = Self::generate_column_statistics(&table_name, &column_info);

                stat.insert("null_frac".to_string(), column_stats.null_frac.as_bytes().to_vec());
                stat.insert("n_distinct".to_string(), column_stats.n_distinct.as_bytes().to_vec());
                stat.insert("most_common_vals".to_string(), column_stats.most_common_vals.as_bytes().to_vec());
                stat.insert("most_common_freqs".to_string(), column_stats.most_common_freqs.as_bytes().to_vec());
                stat.insert("histogram_bounds".to_string(), column_stats.histogram_bounds.as_bytes().to_vec());
                stat.insert("correlation".to_string(), column_stats.correlation.as_bytes().to_vec());
                stat.insert("most_common_elems".to_string(), b"".to_vec()); // NULL for non-arrays
                stat.insert("most_common_elem_freqs".to_string(), b"".to_vec()); // NULL for non-arrays
                stat.insert("elem_count_histogram".to_string(), b"".to_vec()); // NULL for non-arrays

                stats.push(stat);
            }
        }

        Ok(stats)
    }

    async fn get_all_tables(db: &DbHandler) -> Result<Vec<String>, PgSqliteError> {
        let mut tables = Vec::new();

        // Create a temporary connection to read the tables
        // This is safe because we're just reading metadata, not user data
        let conn = rusqlite::Connection::open(&db.db_path).map_err(PgSqliteError::Sqlite)?;

        let query = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE '__pgsqlite_%'";

        let mut stmt = conn.prepare(query).map_err(PgSqliteError::Sqlite)?;
        let rows = stmt.query_map([], |row| {
            row.get::<_, String>(0)
        }).map_err(PgSqliteError::Sqlite)?;

        debug!("get_all_tables direct query");
        for table_name in rows.flatten() {
            debug!("Found table: {}", table_name);
            tables.push(table_name);
        }

        Ok(tables)
    }

    async fn get_table_columns(db: &DbHandler, table_name: &str) -> Result<Vec<ColumnInfo>, PgSqliteError> {
        let mut columns = Vec::new();

        // Create a temporary connection to read the columns
        // This is safe because we're just reading metadata, not user data
        let conn = rusqlite::Connection::open(&db.db_path).map_err(PgSqliteError::Sqlite)?;

        let query = format!("PRAGMA table_info({})", table_name);

        let mut stmt = conn.prepare(&query).map_err(PgSqliteError::Sqlite)?;
        let rows = stmt.query_map([], |row| {
            let name: String = row.get(1)?;
            let data_type: String = row.get(2)?;
            Ok(ColumnInfo { name, data_type })
        }).map_err(PgSqliteError::Sqlite)?;

        for column_info in rows.flatten() {
            columns.push(column_info);
        }

        Ok(columns)
    }

    fn generate_column_statistics(_table_name: &str, column_info: &ColumnInfo) -> ColumnStats {
        let column_name = &column_info.name.to_lowercase();
        let data_type = &column_info.data_type.to_uppercase();

        // Generate realistic statistics based on column type and name patterns
        let (null_frac, n_distinct, correlation) = match data_type.as_str() {
            "INTEGER" | "INT" | "BIGINT" => {
                if column_name.contains("id") || column_name == "rowid" {
                    // Primary keys - unique, no nulls, high correlation
                    ("0.0", "-1", "1.0") // n_distinct = -1 means unique
                } else if column_name.contains("count") || column_name.contains("quantity") {
                    // Counts - some variety, few nulls
                    ("0.05", "50", "0.8")
                } else {
                    // Regular integers
                    ("0.1", "100", "0.3")
                }
            }
            "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" | "DECIMAL" => {
                if column_name.contains("price") || column_name.contains("amount") || column_name.contains("cost") {
                    // Monetary values - many distinct, some nulls
                    ("0.15", "500", "0.2")
                } else {
                    // Other numeric values
                    ("0.1", "200", "0.4")
                }
            }
            "TEXT" | "VARCHAR" | "CHAR" => {
                if column_name.contains("name") || column_name.contains("title") {
                    // Names/titles - many distinct values
                    ("0.05", "1000", "0.1")
                } else if column_name.contains("email") {
                    // Emails - unique or near-unique
                    ("0.02", "-0.9", "0.05") // n_distinct = -0.9 means 90% unique
                } else if column_name.contains("status") || column_name.contains("type") || column_name.contains("category") {
                    // Categorical data - few distinct values
                    ("0.1", "10", "0.7")
                } else {
                    // General text
                    ("0.2", "300", "0.2")
                }
            }
            "BOOLEAN" | "BOOL" => {
                // Boolean - only 2 distinct values
                ("0.05", "2", "0.5")
            }
            "DATE" | "DATETIME" | "TIMESTAMP" => {
                // Dates - many distinct, good correlation with insert order
                ("0.1", "365", "0.9")
            }
            _ => {
                // Default for unknown types
                ("0.1", "100", "0.3")
            }
        };

        // Generate most common values and frequencies based on type
        let (most_common_vals, most_common_freqs) = Self::generate_common_values(column_name, data_type);

        // Generate histogram bounds
        let histogram_bounds = Self::generate_histogram_bounds(data_type);

        ColumnStats {
            null_frac: null_frac.to_string(),
            n_distinct: n_distinct.to_string(),
            most_common_vals,
            most_common_freqs,
            histogram_bounds,
            correlation: correlation.to_string(),
        }
    }

    fn generate_common_values(column_name: &str, data_type: &str) -> (String, String) {
        match data_type {
            "BOOLEAN" | "BOOL" => {
                ("{t,f}".to_string(), "{0.6,0.4}".to_string())
            }
            "TEXT" | "VARCHAR" | "CHAR" => {
                if column_name.contains("status") {
                    ("{active,inactive,pending}".to_string(), "{0.7,0.2,0.1}".to_string())
                } else if column_name.contains("type") || column_name.contains("category") {
                    ("{standard,premium,basic}".to_string(), "{0.5,0.3,0.2}".to_string())
                } else {
                    // Names and other text fields typically don't have common values
                    ("".to_string(), "".to_string())
                }
            }
            "INTEGER" | "INT" | "BIGINT" => {
                if column_name.contains("count") || column_name.contains("quantity") {
                    ("{1,2,3,4,5}".to_string(), "{0.3,0.25,0.2,0.15,0.1}".to_string())
                } else {
                    // Most integer columns don't have obvious common values
                    ("".to_string(), "".to_string())
                }
            }
            _ => {
                ("".to_string(), "".to_string())
            }
        }
    }

    fn generate_histogram_bounds(data_type: &str) -> String {
        match data_type {
            "INTEGER" | "INT" | "BIGINT" => {
                "{1,10,25,50,100,250,500,1000,2500,5000,10000}".to_string()
            }
            "REAL" | "FLOAT" | "DOUBLE" | "NUMERIC" | "DECIMAL" => {
                "{0.01,1.0,5.0,10.0,25.0,50.0,100.0,250.0,500.0,1000.0}".to_string()
            }
            "DATE" | "DATETIME" | "TIMESTAMP" => {
                "{2020-01-01,2020-04-01,2020-07-01,2020-10-01,2021-01-01,2021-04-01,2021-07-01,2021-10-01,2022-01-01}".to_string()
            }
            "TEXT" | "VARCHAR" | "CHAR" => {
                // Text histogram bounds (lexicographic)
                "{A,E,I,M,Q,T,X,Z}".to_string()
            }
            _ => {
                "".to_string()
            }
        }
    }

    fn apply_where_filter(
        stats: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for stat in stats {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in stat {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(stat.clone());
            }
        }

        Ok(filtered)
    }
}

#[derive(Debug)]
struct ColumnInfo {
    name: String,
    data_type: String,
}

#[derive(Debug)]
struct ColumnStats {
    null_frac: String,
    n_distinct: String,
    most_common_vals: String,
    most_common_freqs: String,
    histogram_bounds: String,
    correlation: String,
}