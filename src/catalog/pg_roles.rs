use super::where_evaluator::WhereEvaluator;
use crate::PgSqliteError;
use crate::session::db_handler::{DbHandler, DbResponse};
use sqlparser::ast::{Expr, Select, SelectItem};
use std::collections::HashMap;
use tracing::debug;

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

        // Determine which output columns to return and which source columns supply values
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Build default roles (since SQLite doesn't have role management)
        let roles = Self::get_default_roles();

        // Apply WHERE clause filtering if present
        let filtered_roles = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&roles, where_clause)?
        } else {
            roles
        };

        if selected_columns.is_empty() && Self::is_count_star_projection(&select.projection) {
            return Ok(DbResponse {
                columns: vec!["count".to_string()],
                rows: vec![vec![Some(filtered_roles.len().to_string().into_bytes())]],
                rows_affected: 1,
            });
        }

        // Build response
        let mut rows = Vec::new();
        for role in filtered_roles {
            let mut row = Vec::new();
            for (_, source_column) in &selected_columns {
                let value = role
                    .get(source_column)
                    .cloned()
                    .unwrap_or_else(|| b"".to_vec());
                row.push(Some(value));
            }
            rows.push(row);
        }

        let rows_count = rows.len();
        Ok(DbResponse {
            columns: selected_columns
                .into_iter()
                .map(|(output_column, _)| output_column)
                .collect(),
            rows,
            rows_affected: rows_count,
        })
    }

    fn get_selected_columns(
        projection: &[SelectItem],
        all_columns: &[String],
    ) -> Vec<(String, String)> {
        let mut selected = Vec::new();

        for item in projection {
            match item {
                SelectItem::Wildcard(_) => {
                    selected.extend(
                        all_columns
                            .iter()
                            .map(|column| (column.clone(), column.clone())),
                    );
                }
                SelectItem::UnnamedExpr(expr) => {
                    if let Some(col_name) = Self::extract_source_column(expr)
                        && all_columns.contains(&col_name)
                    {
                        selected.push((col_name.clone(), col_name));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    if let Some(col_name) = Self::extract_source_column(expr)
                        && all_columns.contains(&col_name)
                    {
                        selected.push((Self::alias_output_name(alias), col_name));
                    }
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    // For qualified wildcard like pg_roles.*, return all columns
                    selected.extend(
                        all_columns
                            .iter()
                            .map(|column| (column.clone(), column.clone())),
                    );
                }
            }
        }

        selected
    }

    fn extract_source_column(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|ident| ident.value.to_lowercase()),
            Expr::Cast { expr, .. } => Self::extract_source_column(expr),
            Expr::Nested(expr) => Self::extract_source_column(expr),
            Expr::Function(function) => Self::function_name(function),
            _ => None,
        }
    }

    fn alias_output_name(alias: &sqlparser::ast::Ident) -> String {
        if alias.quote_style.is_some() {
            alias.value.clone()
        } else {
            alias.value.to_lowercase()
        }
    }

    fn function_name(function: &sqlparser::ast::Function) -> Option<String> {
        function
            .name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.to_lowercase())
    }

    fn is_count_star_projection(projection: &[SelectItem]) -> bool {
        if projection.len() != 1 {
            return false;
        }

        let expr = match &projection[0] {
            SelectItem::UnnamedExpr(expr) => expr,
            SelectItem::ExprWithAlias { expr, .. } => expr,
            _ => return false,
        };

        let Expr::Function(function) = expr else {
            return false;
        };

        if Self::function_name(function).as_deref() != Some("count") {
            return false;
        }

        matches!(
            &function.args,
            sqlparser::ast::FunctionArguments::List(arg_list)
                if arg_list.args.len() == 1
                    && matches!(
                        &arg_list.args[0],
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Wildcard
                        )
                    )
        )
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
