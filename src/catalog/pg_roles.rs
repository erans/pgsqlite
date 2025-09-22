use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgRolesHandler;

impl PgRolesHandler {
    pub async fn handle_query(
        select: &Select,
        _db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_roles query");

        // Define all available columns for PostgreSQL pg_roles view
        let all_columns = vec![
            "oid".to_string(),
            "rolname".to_string(),
            "rolsuper".to_string(),
            "rolinherit".to_string(),
            "rolcreaterole".to_string(),
            "rolcreatedb".to_string(),
            "rolcanlogin".to_string(),
            "rolreplication".to_string(),
            "rolconnlimit".to_string(),
            "rolpassword".to_string(),
            "rolvaliduntil".to_string(),
            "rolbypassrls".to_string(),
            "rolconfig".to_string(),
        ];

        // Determine which columns to return
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Build default roles (since SQLite doesn't have role management)
        let roles = Self::get_default_roles();

        // Apply WHERE clause filtering if present
        let filtered_roles = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&roles, where_clause, &selected_columns)?
        } else {
            roles
        };

        // Build response
        let mut rows = Vec::new();
        for role in filtered_roles {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = role.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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
                    // For qualified wildcard like pg_roles.*, return all columns
                    selected.extend_from_slice(all_columns);
                    break;
                }
                _ => {}
            }
        }

        selected
    }

    fn get_default_roles() -> Vec<HashMap<String, Vec<u8>>> {
        let mut roles = Vec::new();

        // Default superuser role (simulating PostgreSQL's postgres role)
        let mut postgres_role = HashMap::new();
        postgres_role.insert("oid".to_string(), b"10".to_vec()); // Standard postgres role OID
        postgres_role.insert("rolname".to_string(), b"postgres".to_vec());
        postgres_role.insert("rolsuper".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolinherit".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolcreaterole".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolcreatedb".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolcanlogin".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolreplication".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolconnlimit".to_string(), b"-1".to_vec()); // unlimited
        postgres_role.insert("rolpassword".to_string(), b"********".to_vec()); // hidden
        postgres_role.insert("rolvaliduntil".to_string(), b"".to_vec()); // NULL
        postgres_role.insert("rolbypassrls".to_string(), b"t".to_vec()); // true
        postgres_role.insert("rolconfig".to_string(), b"".to_vec()); // NULL
        roles.push(postgres_role);

        // Default public role (for compatibility)
        let mut public_role = HashMap::new();
        public_role.insert("oid".to_string(), b"0".to_vec()); // Public role OID
        public_role.insert("rolname".to_string(), b"public".to_vec());
        public_role.insert("rolsuper".to_string(), b"f".to_vec()); // false
        public_role.insert("rolinherit".to_string(), b"t".to_vec()); // true
        public_role.insert("rolcreaterole".to_string(), b"f".to_vec()); // false
        public_role.insert("rolcreatedb".to_string(), b"f".to_vec()); // false
        public_role.insert("rolcanlogin".to_string(), b"f".to_vec()); // false
        public_role.insert("rolreplication".to_string(), b"f".to_vec()); // false
        public_role.insert("rolconnlimit".to_string(), b"-1".to_vec()); // unlimited
        public_role.insert("rolpassword".to_string(), b"".to_vec()); // NULL
        public_role.insert("rolvaliduntil".to_string(), b"".to_vec()); // NULL
        public_role.insert("rolbypassrls".to_string(), b"f".to_vec()); // false
        public_role.insert("rolconfig".to_string(), b"".to_vec()); // NULL
        roles.push(public_role);

        // Default current user role (matches connection user)
        let mut current_user_role = HashMap::new();
        current_user_role.insert("oid".to_string(), b"100".to_vec()); // Default user OID
        current_user_role.insert("rolname".to_string(), b"pgsqlite_user".to_vec());
        current_user_role.insert("rolsuper".to_string(), b"t".to_vec()); // true for simplicity
        current_user_role.insert("rolinherit".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolcreaterole".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolcreatedb".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolcanlogin".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolreplication".to_string(), b"f".to_vec()); // false
        current_user_role.insert("rolconnlimit".to_string(), b"-1".to_vec()); // unlimited
        current_user_role.insert("rolpassword".to_string(), b"********".to_vec()); // hidden
        current_user_role.insert("rolvaliduntil".to_string(), b"".to_vec()); // NULL
        current_user_role.insert("rolbypassrls".to_string(), b"t".to_vec()); // true
        current_user_role.insert("rolconfig".to_string(), b"".to_vec()); // NULL
        roles.push(current_user_role);

        roles
    }

    fn apply_where_filter(
        roles: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for role in roles {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in role {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(role.clone());
            }
        }

        Ok(filtered)
    }
}