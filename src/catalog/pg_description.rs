use crate::session::db_handler::{DbHandler, DbResponse};
use crate::PgSqliteError;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::debug;
use std::collections::HashMap;
use super::where_evaluator::WhereEvaluator;

pub struct PgDescriptionHandler;

impl PgDescriptionHandler {
    pub async fn handle_query(
        select: &Select,
        db: &DbHandler,
    ) -> Result<DbResponse, PgSqliteError> {
        debug!("Handling pg_description query");

        // Define all available columns for PostgreSQL pg_description table
        let all_columns = vec![
            "objoid".to_string(),
            "classoid".to_string(),
            "objsubid".to_string(),
            "description".to_string(),
        ];

        // Determine which columns to return
        let selected_columns = Self::get_selected_columns(&select.projection, &all_columns);

        // Build descriptions from pgsqlite comment system
        let descriptions = Self::get_object_descriptions(db).await?;

        // Apply WHERE clause filtering if present
        let filtered_descriptions = if let Some(where_clause) = &select.selection {
            Self::apply_where_filter(&descriptions, where_clause, &selected_columns)?
        } else {
            descriptions
        };

        // Build response
        let mut rows = Vec::new();
        for desc in filtered_descriptions {
            let mut row = Vec::new();
            for column in &selected_columns {
                let value = desc.get(column).cloned().unwrap_or_else(|| b"".to_vec());
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
                    // For qualified wildcard like pg_description.*, return all columns
                    selected.extend_from_slice(all_columns);
                    break;
                }
                _ => {}
            }
        }

        selected
    }

    async fn get_object_descriptions(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut descriptions = Vec::new();

        // Get table comments from __pgsqlite_comments table
        let table_comments = Self::get_table_comments(db).await?;
        descriptions.extend(table_comments);

        // Get column comments from __pgsqlite_comments table
        let column_comments = Self::get_column_comments(db).await?;
        descriptions.extend(column_comments);

        // Get function comments (if any exist in the future)
        let function_comments = Self::get_function_comments(db).await?;
        descriptions.extend(function_comments);

        Ok(descriptions)
    }

    async fn get_table_comments(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut comments = Vec::new();

        // Query table comments from __pgsqlite_comments
        let query = r#"
            SELECT object_oid, catalog_name, comment_text
            FROM __pgsqlite_comments
            WHERE catalog_name = 'pg_class' AND subobject_id = 0
        "#;

        match db.query(query).await {
            Ok(response) => {
                for row in response.rows {
                    if row.len() >= 3
                        && let (Some(object_oid_bytes), Some(comment_bytes)) = (&row[0], &row[2]) {
                        let object_oid = String::from_utf8_lossy(object_oid_bytes);
                        let comment_text = String::from_utf8_lossy(comment_bytes);

                        let mut desc = HashMap::new();
                        desc.insert("objoid".to_string(), object_oid.as_bytes().to_vec());
                        desc.insert("classoid".to_string(), b"1259".to_vec()); // pg_class OID
                        desc.insert("objsubid".to_string(), b"0".to_vec());    // 0 for table itself
                        desc.insert("description".to_string(), comment_text.as_bytes().to_vec());

                        comments.push(desc);
                    }
                }
            }
            Err(_) => {
                // __pgsqlite_comments table might not exist yet, that's OK
                debug!("No comments table found or accessible");
            }
        }

        Ok(comments)
    }

    async fn get_column_comments(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut comments = Vec::new();

        // Query column comments from __pgsqlite_comments
        let query = r#"
            SELECT object_oid, catalog_name, subobject_id, comment_text
            FROM __pgsqlite_comments
            WHERE catalog_name = 'pg_class' AND subobject_id > 0
        "#;

        match db.query(query).await {
            Ok(response) => {
                for row in response.rows {
                    if row.len() >= 4
                        && let (Some(object_oid_bytes), Some(subobject_id_bytes), Some(comment_bytes)) =
                               (&row[0], &row[2], &row[3]) {
                        let object_oid = String::from_utf8_lossy(object_oid_bytes);
                        let subobject_id = String::from_utf8_lossy(subobject_id_bytes);
                        let comment_text = String::from_utf8_lossy(comment_bytes);

                        let mut desc = HashMap::new();
                        desc.insert("objoid".to_string(), object_oid.as_bytes().to_vec());
                        desc.insert("classoid".to_string(), b"1259".to_vec()); // pg_class OID
                        desc.insert("objsubid".to_string(), subobject_id.as_bytes().to_vec());
                        desc.insert("description".to_string(), comment_text.as_bytes().to_vec());

                        comments.push(desc);
                    }
                }
            }
            Err(_) => {
                // __pgsqlite_comments table might not exist yet, that's OK
                debug!("No comments table found or accessible");
            }
        }

        Ok(comments)
    }

    async fn get_function_comments(db: &DbHandler) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut comments = Vec::new();

        // Query function comments from __pgsqlite_comments (if any exist)
        let query = r#"
            SELECT object_oid, catalog_name, comment_text
            FROM __pgsqlite_comments
            WHERE catalog_name = 'pg_proc' AND subobject_id = 0
        "#;

        match db.query(query).await {
            Ok(response) => {
                for row in response.rows {
                    if row.len() >= 3
                        && let (Some(object_oid_bytes), Some(comment_bytes)) = (&row[0], &row[2]) {
                        let object_oid = String::from_utf8_lossy(object_oid_bytes);
                        let comment_text = String::from_utf8_lossy(comment_bytes);

                        let mut desc = HashMap::new();
                        desc.insert("objoid".to_string(), object_oid.as_bytes().to_vec());
                        desc.insert("classoid".to_string(), b"1255".to_vec()); // pg_proc OID
                        desc.insert("objsubid".to_string(), b"0".to_vec());    // 0 for function itself
                        desc.insert("description".to_string(), comment_text.as_bytes().to_vec());

                        comments.push(desc);
                    }
                }
            }
            Err(_) => {
                // Function comments not available, that's OK
                debug!("No function comments found");
            }
        }

        Ok(comments)
    }


    fn apply_where_filter(
        descriptions: &[HashMap<String, Vec<u8>>],
        where_clause: &Expr,
        _selected_columns: &[String],
    ) -> Result<Vec<HashMap<String, Vec<u8>>>, PgSqliteError> {
        let mut filtered = Vec::new();

        for desc in descriptions {
            // Convert Vec<u8> to String for WhereEvaluator
            let mut string_data = HashMap::new();
            for (key, value) in desc {
                if let Ok(string_val) = String::from_utf8(value.clone()) {
                    string_data.insert(key.clone(), string_val);
                }
            }

            let column_mapping = HashMap::new(); // Empty mapping for now
            if WhereEvaluator::evaluate(where_clause, &string_data, &column_mapping) {
                filtered.push(desc.clone());
            }
        }

        Ok(filtered)
    }
}