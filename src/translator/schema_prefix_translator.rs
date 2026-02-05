use once_cell::sync::Lazy;
use regex::Regex;
use sqlparser::ast::{ObjectName, ObjectNamePart, Query, SetExpr, Statement, TableFactor};
use tracing::debug;

/// Translator that removes schema prefixes from table names
/// PostgreSQL queries often use schema.table syntax (e.g., pg_catalog.pg_class)
/// but SQLite doesn't support schemas, so we need to strip the prefix
pub struct SchemaPrefixTranslator;

static SCHEMA_QUALIFIED_TABLE_REF: Lazy<Regex> = Lazy::new(|| {
    // Match common table reference positions where schema qualification is legal.
    // We intentionally avoid rewriting arbitrary A.B (could be column refs).
    Regex::new(
        r#"(?i)\b(from|join|into|update|table|references|truncate)\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)"#,
    )
    .expect("regex compiles")
});

static CREATE_INDEX_ON_SCHEMA_TABLE: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r#"(?i)\bon\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*\.\s*("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)"#,
    )
    .expect("regex compiles")
});

static SCHEMA_QUALIFIED_FUNCTION_CALL: Lazy<Regex> = Lazy::new(|| {
    // Only rewrite a very small allowlist of schema-qualified functions we implement.
    Regex::new(
        r#"(?i)\b("public"|public)\s*\.\s*("unaccent"|unaccent|"unaccent_immutable"|unaccent_immutable)\s*\("#,
    )
    .expect("regex compiles")
});

static CREATE_FUNCTION_STMT: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r#"(?is)(^|;)\s*create\s+(?:or\s+replace\s+)?function\b"#).expect("regex compiles")
});

impl SchemaPrefixTranslator {
    /// Translate a query string by removing schema prefixes
    pub fn translate_query(query: &str) -> String {
        if CREATE_FUNCTION_STMT.is_match(query) {
            return query.to_string();
        }

        // Simple string replacement approach for known pg_catalog tables
        let mut result = query.to_string();

        // List of known pg_catalog tables that we have views for
        let catalog_tables = [
            "pg_class",
            "pg_namespace",
            "pg_attribute",
            "pg_type",
            "pg_constraint",
            "pg_index",
            "pg_attrdef",
            "pg_am",
            "pg_enum",
            "pg_range",
        ];

        for table in &catalog_tables {
            // Replace pg_catalog.table with just table
            result = result.replace(&format!("pg_catalog.{table}"), table);
            // Also handle uppercase
            result = result.replace(&format!("PG_CATALOG.{}", table.to_uppercase()), table);
        }

        // Also remove schema prefix from functions
        let catalog_functions = [
            "pg_table_is_visible",
            "pg_get_userbyid",
            "pg_get_constraintdef",
            "format_type",
            "pg_get_expr",
            "pg_get_indexdef",
            "version",
            "current_database",
            "current_schema",
            "current_user",
            "session_user",
            "pg_backend_pid",
            "pg_is_in_recovery",
            "current_schemas",
        ];

        for func in &catalog_functions {
            result = result.replace(&format!("pg_catalog.{func}"), func);
            result = result.replace(&format!("PG_CATALOG.{}", func.to_uppercase()), func);
        }

        // Also remove schema prefix from a small allowlist of public functions.
        // SQLite doesn't support schema-qualified function calls.
        result = SCHEMA_QUALIFIED_FUNCTION_CALL
            .replace_all(&result, |caps: &regex::Captures| {
                let func = caps.get(2).map(|m| m.as_str()).unwrap_or("");
                let func = func.trim_matches('"');
                format!("{func}(")
            })
            .to_string();

        // Rewrite schema-qualified table references (non-catalog schemas) into a stable
        // single-table namespace by mapping schema.table -> schema__table.
        result = SCHEMA_QUALIFIED_TABLE_REF
            .replace_all(&result, |caps: &regex::Captures| {
                let keyword = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                let schema_token = caps.get(2).map(|m| m.as_str()).unwrap_or("");
                let table_token = caps.get(3).map(|m| m.as_str()).unwrap_or("");

                let (schema_name, schema_quoted) = parse_ident_token(schema_token);
                let (table_name, table_quoted) = parse_ident_token(table_token);

                let schema_lc = schema_name.to_lowercase();
                if schema_lc == "information_schema" {
                    return caps.get(0).unwrap().as_str().to_string();
                }
                if schema_lc == "pg_catalog" {
                    return format!("{keyword} {table_token}");
                }

                let combined = format!("{schema_name}__{table_name}");
                let needs_quotes = schema_quoted || table_quoted;
                let rendered = if needs_quotes {
                    format!("\"{}\"", combined.replace('"', "\"\""))
                } else {
                    combined
                };

                format!("{keyword} {rendered}")
            })
            .to_string();

        // CREATE INDEX ... ON schema.table (...) is another place where schema-qualified table
        // references occur, but we must not rewrite arbitrary "ON a.b" in JOIN conditions.
        // Apply this only for CREATE INDEX statements.
        if result.trim_start().to_uppercase().starts_with("CREATE")
            && result.to_uppercase().contains("INDEX")
            && result.to_uppercase().contains(" ON ")
        {
            result = CREATE_INDEX_ON_SCHEMA_TABLE
                .replace_all(&result, |caps: &regex::Captures| {
                    let schema_token = caps.get(1).map(|m| m.as_str()).unwrap_or("");
                    let table_token = caps.get(2).map(|m| m.as_str()).unwrap_or("");

                    let (schema_name, schema_quoted) = parse_ident_token(schema_token);
                    let (table_name, table_quoted) = parse_ident_token(table_token);
                    let schema_lc = schema_name.to_lowercase();
                    if schema_lc == "information_schema" {
                        return caps.get(0).unwrap().as_str().to_string();
                    }
                    if schema_lc == "pg_catalog" {
                        return format!("ON {table_token}");
                    }

                    let combined = format!("{schema_name}__{table_name}");
                    let rendered = if schema_quoted || table_quoted {
                        format!("\"{}\"", combined.replace('"', "\"\""))
                    } else {
                        combined
                    };

                    format!("ON {rendered}")
                })
                .to_string();
        }

        debug!("Schema prefix translation: {} -> {}", query, result);
        result
    }

    /// Translate an AST by removing schema prefixes
    pub fn translate_statement(stmt: &mut Statement) -> Result<(), sqlparser::parser::ParserError> {
        match stmt {
            Statement::Query(query) => Self::translate_query_ast(query),
            _ => Ok(()),
        }
    }

    fn translate_query_ast(query: &mut Query) -> Result<(), sqlparser::parser::ParserError> {
        if let SetExpr::Select(select) = &mut *query.body {
            // Translate table names in FROM clause
            for table_ref in &mut select.from {
                Self::translate_table_factor(&mut table_ref.relation)?;

                // Also handle JOINs
                for join in &mut table_ref.joins {
                    Self::translate_table_factor(&mut join.relation)?;
                }
            }
        }
        Ok(())
    }

    fn translate_table_factor(
        factor: &mut TableFactor,
    ) -> Result<(), sqlparser::parser::ParserError> {
        if let TableFactor::Table { name, .. } = factor {
            Self::translate_object_name(name);
        }
        Ok(())
    }

    fn translate_object_name(name: &mut ObjectName) {
        // If the name has 2 parts (schema.table), remove the schema part
        if name.0.len() == 2 {
            let schema = &name.0[0];
            let table = &name.0[1];

            // Check if it's a pg_catalog schema
            let schema_name = match schema {
                ObjectNamePart::Identifier(ident) => ident.value.to_lowercase(),
            };

            if schema_name == "pg_catalog" {
                // Replace with just the table name
                name.0 = vec![table.clone()];
            }
            // Don't remove information_schema prefix - it's handled by query interceptor
            else if schema_name != "information_schema" {
                let schema_raw = match schema {
                    ObjectNamePart::Identifier(ident) => ident.value.clone(),
                };
                let table_raw = match table {
                    ObjectNamePart::Identifier(ident) => ident.value.clone(),
                };
                name.0 = vec![ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
                    format!("{schema_raw}__{table_raw}"),
                ))];
            }
        }
    }
}

fn parse_ident_token(token: &str) -> (String, bool) {
    let t = token.trim();
    if t.starts_with('"') && t.ends_with('"') && t.len() >= 2 {
        let inner = &t[1..t.len() - 1];
        return (inner.replace("\"\"", "\""), true);
    }
    (t.to_string(), false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_prefix_removal() {
        let query = "SELECT * FROM pg_catalog.pg_class WHERE relname = 'test'";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(translated, "SELECT * FROM pg_class WHERE relname = 'test'");
    }

    #[test]
    fn test_function_prefix_removal() {
        let query = "SELECT pg_catalog.pg_table_is_visible(oid) FROM pg_catalog.pg_class";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(translated, "SELECT pg_table_is_visible(oid) FROM pg_class");
    }

    #[test]
    fn test_join_prefix_removal() {
        let query = "SELECT * FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(
            translated,
            "SELECT * FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid"
        );
    }

    #[test]
    fn test_create_function_skips_translation() {
        let query = "CREATE OR REPLACE FUNCTION public.unaccent_immutable(input text) RETURNS text LANGUAGE sql AS $$ SELECT public.unaccent('public.unaccent'::regdictionary, input) $$;";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(translated, query);
    }
}
