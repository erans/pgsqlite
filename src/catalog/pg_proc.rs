use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use sqlparser::ast::{FunctionArgExpr, Ident};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;
use crate::session::SessionState;
use std::sync::Arc;

pub struct PgProcHandler;

impl PgProcHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
        session: Option<&Arc<SessionState>>,
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

        // Aggregate shortcut: SELECT count(*) FROM pg_proc ...
        if Self::is_count_star_query(select) {
            let mut functions = Self::get_system_functions();
            if let Some(session) = session
                && let Ok(user_funcs) = Self::load_user_functions(db, session).await {
                    functions.extend(user_funcs);
                }

            let filtered = if let Some(where_clause) = &select.selection {
                Self::apply_where_filter(&functions, where_clause, &all_columns)?
            } else {
                functions
            };

            let count = filtered.len().to_string().into_bytes();
            return Ok(DbResponse {
                columns: vec!["count".to_string()],
                rows: vec![vec![Some(count)]],
                rows_affected: 1,
            });
        }

        // Determine which columns to return
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Build function metadata - we'll populate with SQLite built-in functions
        // and pgsqlite-specific functions
        let mut functions = Self::get_system_functions();

        // Add persisted SQL-language user functions if available
        if let Some(session) = session
            && let Ok(user_funcs) = Self::load_user_functions(db, session).await {
                functions.extend(user_funcs);
            }

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

    fn is_count_star_query(select: &Select) -> bool {
        if select.projection.len() != 1 {
            return false;
        }
        let SelectItem::UnnamedExpr(Expr::Function(func)) = &select.projection[0] else {
            return false;
        };

        // name could be "count" or pg_catalog.count; we only check last ident.
        let func_name = match func.name.0.last() {
            Some(sqlparser::ast::ObjectNamePart::Identifier(Ident { value, .. })) => value.to_lowercase(),
            None => return false,
        };
        if func_name != "count" {
            return false;
        }
        let args = match &func.args {
            sqlparser::ast::FunctionArguments::List(list) => &list.args,
            _ => return false,
        };
        if args.len() != 1 {
            return false;
        }
        matches!(&args[0], sqlparser::ast::FunctionArg::Unnamed(FunctionArgExpr::Wildcard))
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
            ("uuid_generate_v1", "11", "2950", "f", "v", false, false), // uuid_generate_v1() -> uuid
            ("uuid_generate_v1mc", "11", "2950", "f", "v", false, false), // uuid_generate_v1mc() -> uuid
            ("uuid_generate_v3", "11", "2950", "f", "i", true, false), // uuid_generate_v3(uuid,text) -> uuid
            ("uuid_generate_v5", "11", "2950", "f", "i", true, false), // uuid_generate_v5(uuid,text) -> uuid
            ("uuid_nil", "11", "2950", "f", "v", false, false), // uuid_nil() -> uuid
            ("uuid_ns_dns", "11", "2950", "f", "v", false, false), // uuid_ns_dns() -> uuid
            ("uuid_ns_url", "11", "2950", "f", "v", false, false), // uuid_ns_url() -> uuid
            ("uuid_ns_oid", "11", "2950", "f", "v", false, false), // uuid_ns_oid() -> uuid
            ("uuid_ns_x500", "11", "2950", "f", "v", false, false), // uuid_ns_x500() -> uuid

            // unaccent extension functions
            ("unaccent", "11", "25", "f", "s", true, false), // unaccent(text) -> text

            // System functions
            ("version", "11", "25", "f", "s", false, false),      // version() -> text
            ("current_database", "11", "19", "f", "s", false, false), // current_database() -> name
            ("current_user", "11", "19", "f", "s", false, false), // current_user -> name

            // PG16-era common introspection/system functions
            ("current_setting", "11", "25", "f", "s", false, false), // current_setting(text[, bool]) -> text
            ("current_schema", "11", "19", "f", "s", false, false),  // current_schema() -> name
            ("current_schemas", "11", "1003", "f", "s", false, true), // current_schemas(bool) -> setof name[] (approx)
            ("session_user", "11", "19", "f", "s", false, false), // session_user -> name
            ("user", "11", "19", "f", "s", false, false),         // user -> name
            ("pg_backend_pid", "11", "23", "f", "v", false, false), // pg_backend_pid() -> int4
            ("pg_is_in_recovery", "11", "16", "f", "s", false, false), // pg_is_in_recovery() -> bool
            ("pg_postmaster_start_time", "11", "1184", "f", "s", false, false), // pg_postmaster_start_time() -> timestamptz
            ("pg_conf_load_time", "11", "1184", "f", "s", false, false), // pg_conf_load_time() -> timestamptz
            ("pg_database_size", "11", "20", "f", "s", false, false), // pg_database_size(name) -> int8
            ("inet_client_addr", "11", "869", "f", "s", false, false), // inet_client_addr() -> inet
            ("inet_client_port", "11", "23", "f", "s", false, false), // inet_client_port() -> int4
            ("inet_server_addr", "11", "869", "f", "s", false, false), // inet_server_addr() -> inet
            ("inet_server_port", "11", "23", "f", "s", false, false), // inet_server_port() -> int4
            ("pg_has_role", "11", "16", "f", "s", false, false), // pg_has_role(...) -> bool
            ("has_database_privilege", "11", "16", "f", "s", false, false), // has_database_privilege(...) -> bool
            ("has_schema_privilege", "11", "16", "f", "s", false, false), // has_schema_privilege(...) -> bool
            ("has_table_privilege", "11", "16", "f", "s", false, false), // has_table_privilege(...) -> bool
            ("pg_get_userbyid", "11", "19", "f", "s", false, false), // pg_get_userbyid(oid) -> name
            ("obj_description", "11", "25", "f", "s", false, false), // obj_description(...) -> text
            ("col_description", "11", "25", "f", "s", false, false), // col_description(oid, int4) -> text
            ("pg_size_pretty", "11", "25", "f", "s", false, false), // pg_size_pretty(int8) -> text
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

    async fn load_user_functions(
        db: &DbHandler,
        session: &Arc<SessionState>,
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut out = Vec::new();

        let rows = db
            .with_session_connection(&session.id, |conn| {
                let mut stmt = conn.prepare(
                    "SELECT schema_name, func_name, func_nargs, func_kind, func_strict, func_retset, func_volatile, func_rettype FROM __pgsqlite_user_functions",
                )?;
                let mut rows = Vec::new();
                let mut iter = stmt.query([])?;
                while let Some(row) = iter.next()? {
                    let schema_name: String = row.get(0)?;
                    let func_name: String = row.get(1)?;
                    let func_nargs: i64 = row.get(2)?;
                    let func_kind: String = row.get(3)?;
                    let func_strict: String = row.get(4)?;
                    let func_retset: String = row.get(5)?;
                    let func_volatile: String = row.get(6)?;
                    let func_rettype: i64 = row.get(7)?;
                    rows.push((schema_name, func_name, func_nargs, func_kind, func_strict, func_retset, func_volatile, func_rettype));
                }
                Ok(rows)
            })
            .await;

        let Ok(rows) = rows else {
            return Ok(out);
        };

        for (i, (schema_name, func_name, func_nargs, func_kind, func_strict, func_retset, func_volatile, func_rettype)) in
            rows.into_iter().enumerate()
        {
            let oid = crate::utils::oid_generator::generate_oid(&format!("{schema_name}.{func_name}/{func_nargs}")) + (i as u32);
            let mut func = HashMap::new();
            func.insert("oid".to_string(), oid.to_string().into_bytes());
            func.insert("proname".to_string(), func_name.as_bytes().to_vec());
            let nsp = match schema_name.as_str() {
                "pg_catalog" => 11,
                "public" => 2200,
                "information_schema" => 13445,
                _ => 11,
            };
            func.insert("pronamespace".to_string(), nsp.to_string().into_bytes());
            func.insert("proowner".to_string(), b"10".to_vec());
            func.insert("prolang".to_string(), b"12".to_vec());
            func.insert("procost".to_string(), b"1".to_vec());
            func.insert("prorows".to_string(), b"0".to_vec());
            func.insert("provariadic".to_string(), b"0".to_vec());
            func.insert("prosupport".to_string(), b"0".to_vec());
            func.insert("prokind".to_string(), func_kind.into_bytes());
            func.insert("prosecdef".to_string(), b"f".to_vec());
            func.insert("proleakproof".to_string(), b"f".to_vec());
            func.insert("proisstrict".to_string(), func_strict.into_bytes());
            func.insert("proretset".to_string(), func_retset.into_bytes());
            func.insert("provolatile".to_string(), func_volatile.into_bytes());
            func.insert("proparallel".to_string(), b"s".to_vec());
            func.insert("pronargs".to_string(), func_nargs.to_string().into_bytes());
            func.insert("pronargdefaults".to_string(), b"0".to_vec());
            func.insert("prorettype".to_string(), func_rettype.to_string().into_bytes());
            func.insert("proargtypes".to_string(), b"".to_vec());
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
            out.push(func);
        }

        Ok(out)
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
