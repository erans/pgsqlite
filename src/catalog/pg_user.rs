use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgUserHandler;

impl PgUserHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_user query");

        // Define all available columns for PostgreSQL pg_user view
        let all_columns = vec![
            "usename".to_string(),
            "usesysid".to_string(),
            "usecreatedb".to_string(),
            "usesuper".to_string(),
            "userepl".to_string(),
            "usebypassrls".to_string(),
            "passwd".to_string(),
            "valuntil".to_string(),
            "useconfig".to_string(),
        ];

        // Determine which columns to return
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Build default users (since SQLite doesn't have user management)
        let users = Self::get_default_users();

        // Apply WHERE clause filtering if present
        let filtered_users = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&users, where_clause, &selected_columns)?
        } else {
            users
        };

        // Build response
        let mut rows = Vec::new();
        for user in filtered_users {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = user.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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
                    // For qualified wildcard like pg_user.*, return all columns
                    selected.extend_from_slice(all_columns);
                    break;
                }
                _ => {}
            }
        }

        selected
    }

    fn get_default_users() -> Vec<HashMap<String, Vec<u8>>> {
        let mut users = Vec::new();

        // Default superuser (corresponds to postgres role)
        let mut postgres_user = HashMap::new();
        postgres_user.insert("usename".to_string(), b"postgres".to_vec());
        postgres_user.insert("usesysid".to_string(), b"10".to_vec()); // Standard postgres user OID
        postgres_user.insert("usecreatedb".to_string(), b"t".to_vec()); // true
        postgres_user.insert("usesuper".to_string(), b"t".to_vec()); // true
        postgres_user.insert("userepl".to_string(), b"t".to_vec()); // true
        postgres_user.insert("usebypassrls".to_string(), b"t".to_vec()); // true
        postgres_user.insert("passwd".to_string(), b"********".to_vec()); // hidden
        postgres_user.insert("valuntil".to_string(), b"".to_vec()); // NULL
        postgres_user.insert("useconfig".to_string(), b"".to_vec()); // NULL
        users.push(postgres_user);

        // Default current user (corresponds to pgsqlite_user role)
        let mut current_user = HashMap::new();
        current_user.insert("usename".to_string(), b"pgsqlite_user".to_vec());
        current_user.insert("usesysid".to_string(), b"100".to_vec()); // Default user OID
        current_user.insert("usecreatedb".to_string(), b"t".to_vec()); // true
        current_user.insert("usesuper".to_string(), b"t".to_vec()); // true for simplicity
        current_user.insert("userepl".to_string(), b"f".to_vec()); // false
        current_user.insert("usebypassrls".to_string(), b"t".to_vec()); // true
        current_user.insert("passwd".to_string(), b"********".to_vec()); // hidden
        current_user.insert("valuntil".to_string(), b"".to_vec()); // NULL
        current_user.insert("useconfig".to_string(), b"".to_vec()); // NULL
        users.push(current_user);

        users
    }

    fn apply_where_filter(
        users: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for user in users {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in user {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(user.clone());
            }
        }

        Ok(filtered)
    }
}