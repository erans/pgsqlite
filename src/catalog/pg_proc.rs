use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgProcHandler;

impl PgProcHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_proc query");

        // Define all available columns for PostgreSQL pg_proc table
        let all_columns = vec![
            "oid".to_string(),
            "proname".to_string(),
            "pronamespace".to_string(),
            "proowner".to_string(),
            "prolang".to_string(),
            "procost".to_string(),
            "prorows".to_string(),
            "provariadic".to_string(),
            "prosupport".to_string(),
            "prokind".to_string(),
            "prosecdef".to_string(),
            "proleakproof".to_string(),
            "proisstrict".to_string(),
            "proretset".to_string(),
            "provolatile".to_string(),
            "proparallel".to_string(),
            "pronargs".to_string(),
            "pronargdefaults".to_string(),
            "prorettype".to_string(),
            "proargtypes".to_string(),
            "proallargtypes".to_string(),
            "proargmodes".to_string(),
            "proargnames".to_string(),
            "proargdefaults".to_string(),
            "protrftypes".to_string(),
            "prosrc".to_string(),
            "probin".to_string(),
            "prosqlbody".to_string(),
            "proconfig".to_string(),
            "proacl".to_string(),
        ];

        // Determine which columns to return
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Build function metadata - we'll populate with SQLite built-in functions
        // and pgsqlite-specific functions
        let functions = Self::get_system_functions();

        // Apply WHERE clause filtering if present
        let filtered_functions = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&functions, where_clause, &selected_columns)?
        } else {
            functions
        };

        // Build response
        let mut rows = Vec::new();
        for func in filtered_functions {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = func.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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
                    // For qualified wildcard like pg_proc.*, return all columns
                    selected.extend_from_slice(all_columns);
                    break;
                }
                _ => {}
            }
        }

        selected
    }

    fn get_system_functions() -> Vec<HashMap<String, Vec<u8>>> {
        let mut functions = Vec::new();

        // Built-in SQL functions
        let sql_functions = vec![
            // String functions
            ("length", "11", "23", "f", "i", false, false), // length(text) -> int4
            ("lower", "11", "25", "f", "i", true, false),   // lower(text) -> text
            ("upper", "11", "25", "f", "i", true, false),   // upper(text) -> text
            ("substr", "11", "25", "f", "i", true, false),  // substr(text, int, int) -> text
            ("replace", "11", "25", "f", "i", true, false), // replace(text, text, text) -> text

            // Math functions
            ("abs", "11", "23", "f", "i", true, false),     // abs(int) -> int
            ("round", "11", "1700", "f", "i", true, false), // round(numeric) -> numeric
            ("ceil", "11", "1700", "f", "i", true, false),  // ceil(numeric) -> numeric
            ("floor", "11", "1700", "f", "i", true, false), // floor(numeric) -> numeric

            // Aggregate functions
            ("count", "11", "20", "f", "v", false, true),   // count(*) -> bigint
            ("sum", "11", "1700", "f", "v", false, true),   // sum(numeric) -> numeric
            ("avg", "11", "1700", "f", "v", false, true),   // avg(numeric) -> numeric
            ("max", "11", "2283", "f", "v", false, true),   // max(any) -> any
            ("min", "11", "2283", "f", "v", false, true),   // min(any) -> any

            // Date/time functions
            ("now", "11", "1184", "f", "v", false, false),  // now() -> timestamptz
            ("date", "11", "1082", "f", "i", true, false),  // date(text) -> date

            // JSON functions
            ("json_agg", "11", "114", "f", "v", false, true),     // json_agg(any) -> json
            ("jsonb_agg", "11", "3802", "f", "v", false, true),   // jsonb_agg(any) -> jsonb
            ("json_object_agg", "11", "114", "f", "v", false, true), // json_object_agg -> json

            // Array functions
            ("array_agg", "11", "2277", "f", "v", false, true),   // array_agg(any) -> anyarray
            ("unnest", "11", "2283", "f", "i", false, true),      // unnest(anyarray) -> setof any

            // UUID functions
            ("uuid_generate_v4", "11", "2950", "f", "v", false, false), // uuid_generate_v4() -> uuid

            // System functions
            ("version", "11", "25", "f", "s", false, false),      // version() -> text
            ("current_database", "11", "19", "f", "s", false, false), // current_database() -> name
            ("current_user", "11", "19", "f", "s", false, false), // current_user -> name
        ];

        for (i, (name, namespace, return_type, kind, volatility, is_strict, returns_set)) in sql_functions.iter().enumerate() {
            let oid = (16384 + i) as u32; // Start from 16384 for user functions

            let mut func = HashMap::new();
            func.insert("oid".to_string(), oid.to_string().into_bytes());
            func.insert("proname".to_string(), name.as_bytes().to_vec());
            func.insert("pronamespace".to_string(), namespace.as_bytes().to_vec());
            func.insert("proowner".to_string(), b"10".to_vec()); // postgres user OID
            func.insert("prolang".to_string(), b"12".to_vec()); // SQL language OID
            func.insert("procost".to_string(), b"1".to_vec());
            func.insert("prorows".to_string(), b"0".to_vec());
            func.insert("provariadic".to_string(), b"0".to_vec());
            func.insert("prosupport".to_string(), b"0".to_vec());
            func.insert("prokind".to_string(), kind.as_bytes().to_vec());
            func.insert("prosecdef".to_string(), b"f".to_vec());
            func.insert("proleakproof".to_string(), b"f".to_vec());
            func.insert("proisstrict".to_string(), if *is_strict { b"t" } else { b"f" }.to_vec());
            func.insert("proretset".to_string(), if *returns_set { b"t" } else { b"f" }.to_vec());
            func.insert("provolatile".to_string(), volatility.as_bytes().to_vec());
            func.insert("proparallel".to_string(), b"s".to_vec()); // safe for parallel
            func.insert("pronargs".to_string(), b"0".to_vec()); // simplified - would need proper arg counting
            func.insert("pronargdefaults".to_string(), b"0".to_vec());
            func.insert("prorettype".to_string(), return_type.as_bytes().to_vec());
            func.insert("proargtypes".to_string(), b"".to_vec()); // simplified
            func.insert("proallargtypes".to_string(), b"".to_vec());
            func.insert("proargmodes".to_string(), b"".to_vec());
            func.insert("proargnames".to_string(), b"".to_vec());
            func.insert("proargdefaults".to_string(), b"".to_vec());
            func.insert("protrftypes".to_string(), b"".to_vec());
            func.insert("prosrc".to_string(), b"".to_vec());
            func.insert("probin".to_string(), b"".to_vec());
            func.insert("prosqlbody".to_string(), b"".to_vec());
            func.insert("proconfig".to_string(), b"".to_vec());
            func.insert("proacl".to_string(), b"".to_vec());

            functions.push(func);
        }

        functions
    }

    fn apply_where_filter(
        functions: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for func in functions {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in func {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(func.clone());
            }
        }

        Ok(filtered)
    }
}