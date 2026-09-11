use sqlparser::ast::{Statement, Query, SetExpr, TableFactor, ObjectName, ObjectNamePart};
use tracing::debug;

/// Translator that removes schema prefixes from table names
/// PostgreSQL queries often use schema.table syntax (e.g., pg_catalog.pg_class)
/// but SQLite doesn't support schemas, so we need to strip the prefix
pub struct SchemaPrefixTranslator;

impl SchemaPrefixTranslator {
    /// Translate a query string by removing schema prefixes
    pub fn translate_query(query: &str) -> String {
        // === PATCH v5: generic `pg_catalog.` stripping ===
        // The previous implementation matched a hard-coded whitelist of 10 tables
        // and 14 functions, so every other catalog object (pg_proc, pg_description,
        // pg_settings, pg_roles, pg_database, pg_depend, pg_trigger, ...) reached
        // SQLite still qualified and failed with "no such table". SQLite has no
        // schema namespace at all, so the qualifier can always be dropped.
        let result = Self::strip_pg_catalog_prefix(query);
        let result = Self::rewrite_information_schema_views(&result);

        // === PATCH v22: PostgreSQL LIMIT/OFFSET semantics ===
        // Covers the paths that do not go through execute_select:
        // query_interceptor.rs, unified_processor.rs and lazy_processor.rs all
        // funnel through translate_query. Rewriting twice is harmless (idempotent).
        let result = match crate::translator::LimitTranslator::translate(&result) {
            Some(rewritten) => rewritten,
            None => result,
        };
        
        // === PATCH v27: DBeaver qualifies tables with `public.` ===
        // SQLite has no schema namespace; `public.bath_records` raises
        // "no such table". Drop the qualifier everywhere it is not inside a
        // literal / quoted identifier (e.g. nspname = 'public' is untouched).
        let result = Self::strip_public_prefix(&result);

        // === PATCH v27: DBeaver index query uses JOIN LATERAL unnest ===
        let result = match crate::translator::UnnestTranslator::translate_unnest(&result) {
            Ok(rewritten) => rewritten,
            Err(_) => result,
        };

        debug!("Schema prefix translation: {} -> {}", query, result);
        result
    }

    /// Drop every `public.` qualifier that is not inside a string literal or a
    /// quoted identifier. `public.bath_records` -> `bath_records`; the literal
    /// `'public'` in `nspname = 'public'` is left alone.
    /// Drop every `public.` qualifier that is not inside a string literal or a
    /// quoted identifier. `public.bath_records` -> `bath_records`; the literal
    /// `'public'` in `nspname = 'public'` is left alone.
    /// NOTE: cannot reuse replace_ident_outside_literals -- that helper refuses
    /// to replace when the next char is an identifier char, and `public.<table>`
    /// always has one (public.bath_records). This mirrors strip_pg_catalog_prefix.
    pub fn strip_public_prefix(query: &str) -> String {
        let chars: Vec<char> = query.chars().collect();
        let n = chars.len();
        let mut out = String::with_capacity(query.len());
        let mut i = 0usize;
        let mut prev_ident = false;
        let dq = 0x22 as char;
        let sq = 0x27 as char;
        let target: [char; 6] = ['p', 'u', 'b', 'l', 'i', 'c'];

        while i < n {
            let c = chars[i];

            // 1) single-quoted string literal: copy verbatim
            if c == sq {
                out.push(c);
                i += 1;
                while i < n {
                    let lc = chars[i];
                    out.push(lc);
                    i += 1;
                    if lc == sq {
                        break;
                    }
                }
                prev_ident = false;
                continue;
            }

            // 2) double-quoted identifier
            if c == dq {
                let mut j = i + 1;
                let mut ident = String::new();
                let mut closed = false;
                while j < n {
                    if chars[j] == dq {
                        if j + 1 < n && chars[j + 1] == dq {
                            ident.push(dq);
                            j += 2;
                            continue;
                        }
                        closed = true;
                        break;
                    }
                    ident.push(chars[j]);
                    j += 1;
                }
                if closed && ident.eq_ignore_ascii_case("public") {
                    let mut k = j + 1;
                    while k < n && chars[k].is_whitespace() {
                        k += 1;
                    }
                    if k < n && chars[k] == '.' {
                        k += 1;
                        while k < n && chars[k].is_whitespace() {
                            k += 1;
                        }
                        i = k;
                        prev_ident = false;
                        continue;
                    }
                }
                let end = if closed { j + 1 } else { n };
                for ci in i..end {
                    out.push(chars[ci]);
                }
                i = end;
                prev_ident = true;
                continue;
            }

            // 3) bare public followed by optional ws + '.'
            if !prev_ident && (c == 'p' || c == 'P') && i + 6 <= n {
                let mut matched = true;
                for (o, tc) in target.iter().enumerate() {
                    if chars[i + o].to_ascii_lowercase() != *tc {
                        matched = false;
                        break;
                    }
                }
                if matched {
                    let mut k = i + 6;
                    while k < n && chars[k].is_whitespace() {
                        k += 1;
                    }
                    if k < n && chars[k] == '.' {
                        k += 1;
                        while k < n && chars[k].is_whitespace() {
                            k += 1;
                        }
                        i = k;
                        prev_ident = false;
                        continue;
                    }
                }
            }

            out.push(c);
            prev_ident = c.is_alphanumeric() || c == '_';
            i += 1;
        }

        out
    }
    
    /// Remove every `pg_catalog.` qualifier that is not inside a string literal
    /// or a quoted identifier, and that is not part of a longer identifier
    /// (e.g. `my_pg_catalog.foo` is left untouched). Case-insensitive.

    /// === PATCH v19 ===
    /// SQLite has no schema namespace, so `information_schema.foo` never resolves and
    /// raises "no such table", which aborts the whole transaction and takes the GUI
    /// metadata scan down with it.
    ///
    /// Objects still served by the Rust catalog handler MUST keep the dotted form --
    /// rewriting them would silently swap a populated handler result for an empty
    /// SQLite view (that is exactly the v18 regression on table_constraints /
    /// key_column_usage, 33 rows -> 0 rows). Only the objects that migration v33
    /// materialised as real views are rewritten here.
    const V33_ISCHEMA_VIEWS: &[&str] = &[
        "applicable_roles",
        "character_sets",
        "collations",
        "column_privileges",
        "column_udt_usage",
        "constraint_column_usage",
        "domain_constraints",
        "domains",
        "element_types",
        "enabled_roles",
        "information_schema_catalog_name",
        "parameters",
        "role_table_grants",
        "sequences",
        "table_privileges",
        "view_column_usage",
        "view_table_usage",

        // === PATCH v23: v14-era information_schema views ===
        // Migration v14 created real SQLite views for these six objects, but
        // the rewrite whitelist never included them, so
        // information_schema.tables & co. were never rewritten to
        // information_schema_tables & co. Aggregate / grouped / ordered
        // queries over them were therefore hijacked by the single-table
        // handlers (count(*) -> N rows of NULL, ORDER BY dropped). Keep in
        // sync with ISCHEMA_SQLITE_RESOLVABLE in query_interceptor.rs.
        "tables",
        "columns",
        "schemata",
        "key_column_usage",
        "table_constraints",
        "referential_constraints"
    ];

    fn rewrite_information_schema_views(query: &str) -> String {
        let mut out = query.to_string();
        for obj in Self::V33_ISCHEMA_VIEWS {
            let needle = format!("information_schema.{obj}");
            let repl = format!("information_schema_{obj}");
            out = Self::replace_ident_outside_literals(&out, &needle, &repl);
        }
        out
    }

    /// Replace `needle` with `repl` only when it appears as a standalone identifier:
    /// not inside a string literal or quoted identifier, and not glued to surrounding
    /// identifier characters on either side.
    fn replace_ident_outside_literals(query: &str, needle: &str, repl: &str) -> String {
        let mut out = String::with_capacity(query.len());
        let mut in_single = false;
        let mut in_double = false;
        let mut prev_ident_char = false;
        let mut skip_until = 0usize;

        for (idx, c) in query.char_indices() {
            if idx < skip_until {
                continue;
            }
            if in_single {
                out.push(c);
                if c == '\'' {
                    in_single = false;
                }
                continue;
            }
            if in_double {
                out.push(c);
                if c == '"' {
                    in_double = false;
                }
                continue;
            }
            match c {
                '\'' => {
                    in_single = true;
                    out.push(c);
                    prev_ident_char = false;
                }
                '"' => {
                    in_double = true;
                    out.push(c);
                    prev_ident_char = false;
                }
                _ => {
                    // NOTE: every slice below is guarded by is_char_boundary first --
                    // slicing on a non-boundary would panic on multi-byte input.
                    let end = idx + needle.len();
                    let matched = !prev_ident_char
                        && end <= query.len()
                        && query.is_char_boundary(end)
                        && query[idx..end].eq_ignore_ascii_case(needle)
                        && query[end..]
                            .chars()
                            .next()
                            .map(|n| !(n.is_alphanumeric() || n == '_'))
                            .unwrap_or(true);
                    if matched {
                        out.push_str(repl);
                        skip_until = end;
                        prev_ident_char = true;
                        continue;
                    }
                    out.push(c);
                    prev_ident_char = c.is_alphanumeric() || c == '_';
                }
            }
        }
        out
    }

    fn strip_pg_catalog_prefix(query: &str) -> String {
        const PREFIX: &str = "pg_catalog.";
        let mut out = String::with_capacity(query.len());
        let mut in_single = false;
        let mut in_double = false;
        let mut prev_ident_char = false;
        let mut skip_until = 0usize;
        
        for (idx, c) in query.char_indices() {
            if idx < skip_until {
                continue;
            }
            if in_single {
                out.push(c);
                if c == '\'' {
                    in_single = false;
                }
                continue;
            }
            if in_double {
                out.push(c);
                if c == '"' {
                    in_double = false;
                }
                continue;
            }
            match c {
                '\'' => {
                    in_single = true;
                    out.push(c);
                    prev_ident_char = false;
                }
                '"' => {
                    in_double = true;
                    out.push(c);
                    prev_ident_char = false;
                }
                _ => {
                    let end = idx + PREFIX.len();
                    if !prev_ident_char
                        && end <= query.len()
                        && query.is_char_boundary(end)
                        && query[idx..end].eq_ignore_ascii_case(PREFIX)
                    {
                        skip_until = end;
                        prev_ident_char = false;
                        continue;
                    }
                    out.push(c);
                    prev_ident_char = c.is_ascii_alphanumeric() || c == '_';
                }
            }
        }
        out
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
    
    fn translate_table_factor(factor: &mut TableFactor) -> Result<(), sqlparser::parser::ParserError> {
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
        }
    }
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
        assert_eq!(translated, "SELECT * FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid");
    }

    // === PATCH v27: public. schema prefix
    #[test]
    fn v27_strip_public_prefix_from_table() {
        assert_eq!(SchemaPrefixTranslator::strip_public_prefix(
            "SELECT * FROM public.bath_records WHERE id = 1"),
            "SELECT * FROM bath_records WHERE id = 1");
    }

    #[test]
    fn v27_public_prefix_keeps_literals() {
        assert_eq!(SchemaPrefixTranslator::strip_public_prefix(
            "SELECT * FROM users WHERE nspname = 'public'"),
            "SELECT * FROM users WHERE nspname = 'public'");
    }

    #[test]
    fn v27_public_prefix_join_and_alias() {
        assert_eq!(SchemaPrefixTranslator::strip_public_prefix(
            "SELECT u.id FROM public.users u JOIN public.orders o ON o.user_id = u.id"),
            "SELECT u.id FROM users u JOIN orders o ON o.user_id = u.id");
    }

    #[test]
    fn v27_public_prefix_quoted_identifier_safe() {
        assert_eq!(SchemaPrefixTranslator::strip_public_prefix(
            "SELECT * FROM \"public.t\" WHERE x = 1"),
            "SELECT * FROM \"public.t\" WHERE x = 1");
    }
}

#[cfg(test)]
mod v23_ischema_rewrite_tests {
    use super::*;

    #[test]
    fn v23_tables_rewritten_to_underscore_view() {
        let translated = SchemaPrefixTranslator::translate_query(
            "SELECT count(*) FROM information_schema.tables",
        );
        assert_eq!(translated, "SELECT count(*) FROM information_schema_tables");
    }

    #[test]
    fn v23_columns_rewritten_to_underscore_view() {
        let translated = SchemaPrefixTranslator::translate_query(
            "SELECT count(*) FROM information_schema.columns",
        );
        assert_eq!(translated, "SELECT count(*) FROM information_schema_columns");
    }

    #[test]
    fn v23_routines_not_rewritten() {
        // routines has no SQLite view (handler only) -> stays on handler path.
        let translated = SchemaPrefixTranslator::translate_query(
            "SELECT count(*) FROM information_schema.routines",
        );
        assert_eq!(translated, "SELECT count(*) FROM information_schema.routines");
    }
}


#[cfg(test)]
mod v29_public_quoted_tests {
    use super::*;

    #[test]
    fn strips_quoted_public_prefix() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix(r#"SELECT * FROM "public"."bath_records" LIMIT 100"#),
            r#"SELECT * FROM "bath_records" LIMIT 100"#
        );
    }

    #[test]
    fn leaves_quoted_public_as_column_alone() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix(r#"SELECT "public" FROM t"#),
            r#"SELECT "public" FROM t"#
        );
    }

    #[test]
    fn strips_unquoted_public_prefix_still() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix("SELECT * FROM public.bath_records"),
            "SELECT * FROM bath_records"
        );
    }

    #[test]
    fn leaves_public_literal_alone() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix("SELECT * FROM t WHERE nspname = 'public'"),
            "SELECT * FROM t WHERE nspname = 'public'"
        );
    }

    #[test]
    fn strips_quoted_public_with_unquoted_table() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix(r#"SELECT * FROM "public".bath_records"#),
            "SELECT * FROM bath_records"
        );
    }

    #[test]
    fn strips_unquoted_public_with_quoted_table() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix("SELECT * FROM public.\"bath_records\" LIMIT 5"),
            "SELECT * FROM \"bath_records\" LIMIT 5"
        );
    }

    #[test]
    fn strips_unquoted_public_with_limit() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix("SELECT * FROM public.bath_records LIMIT 5"),
            "SELECT * FROM bath_records LIMIT 5"
        );
    }

    #[test]
    fn strips_multiple_public_prefixes_in_join() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix(
                "SELECT a.id FROM public.a a JOIN \"public\".\"b\" b ON a.id = b.id"
            ),
            "SELECT a.id FROM a a JOIN \"b\" b ON a.id = b.id"
        );
    }

    #[test]
    fn leaves_republic_word_alone() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix("SELECT republic.id FROM republic"),
            "SELECT republic.id FROM republic"
        );
    }

    #[test]
    fn leaves_public_literal_containing_dot_alone() {
        assert_eq!(
            SchemaPrefixTranslator::strip_public_prefix("SELECT * FROM t WHERE x = 'public.bath_records'"),
            "SELECT * FROM t WHERE x = 'public.bath_records'"
        );
    }
}
