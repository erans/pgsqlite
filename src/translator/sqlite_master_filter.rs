use std::borrow::Cow;
use std::ops::ControlFlow;

use sqlparser::ast::{
    Ident, ObjectNamePart, Statement, TableAlias, TableFactor, VisitMut, VisitorMut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::debug;

/// The relation a client `sqlite_master` reference is replaced with.
///
/// `substr(...)` rather than `LIKE '__pgsqlite_%'` because `_` is a
/// single-character wildcard in LIKE, which would also match unrelated names.
/// The `tbl_name` half is what hides the index rows whose own names carry no
/// `__pgsqlite_` prefix (`idx_enum_values_label`, `sqlite_autoindex___pgsqlite_schema_1`).
const FILTERED_RELATION_SQL: &str = "SELECT * FROM sqlite_master \
     WHERE substr(name, 1, 11) <> '__pgsqlite_' \
     AND (tbl_name IS NULL OR substr(tbl_name, 1, 11) <> '__pgsqlite_')";

/// Rewrites client references to `sqlite_master` / `sqlite_schema` so that
/// pgsqlite's own `__pgsqlite_*` objects are not listed.
///
/// This type is deliberately pure: it does not read configuration. Callers gate
/// it on `crate::config::hide_internal_tables()`, and it must only ever be
/// invoked on queries that arrived from a client over the wire.
pub struct SqliteMasterFilter;

impl SqliteMasterFilter {
    /// Cheap allocation-free gate: is there any point parsing this query?
    pub fn needs_translation(query: &str) -> bool {
        contains_ignore_ascii_case(query, "sqlite_master")
            || contains_ignore_ascii_case(query, "sqlite_schema")
    }

    /// Returns the rewritten query, or the input unchanged if there is nothing
    /// to do or the query cannot be handled. Never returns an error: failing to
    /// hide a row is cosmetic, rejecting a client's query is not.
    pub fn translate(query: &str) -> Cow<'_, str> {
        if !Self::needs_translation(query) {
            return Cow::Borrowed(query);
        }

        let mut statements = match Parser::parse_sql(&PostgreSqlDialect {}, query) {
            Ok(statements) => statements,
            Err(e) => {
                debug!("sqlite_master filter: parse failed, passing through: {e}");
                return Cow::Borrowed(query);
            }
        };

        let mut visitor = RelationReplacer { replaced: 0 };
        for statement in &mut statements {
            let _ = statement.visit(&mut visitor);
        }

        if visitor.replaced == 0 {
            return Cow::Borrowed(query);
        }

        let rewritten = statements
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; ");
        debug!("sqlite_master filter: {query} -> {rewritten}");
        Cow::Owned(rewritten)
    }
}

struct RelationReplacer {
    replaced: usize,
}

impl VisitorMut for RelationReplacer {
    type Break = ();

    /// Replacement happens in `post_visit` rather than `pre_visit`: the visitor
    /// descends into a node's children between the two, so replacing in
    /// `pre_visit` would recurse into the `sqlite_master` reference inside the
    /// relation we just substituted, forever.
    fn post_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<()> {
        let TableFactor::Table { name, alias, .. } = table_factor else {
            return ControlFlow::Continue(());
        };

        let relation = match name.0.last() {
            Some(ObjectNamePart::Identifier(ident)) => ident.value.to_ascii_lowercase(),
            _ => return ControlFlow::Continue(()),
        };
        if relation != "sqlite_master" && relation != "sqlite_schema" {
            return ControlFlow::Continue(());
        }

        // Only `main.` and `temp.` qualify the SQLite catalog. Anything else
        // (e.g. an attached database) is left alone.
        if name.0.len() > 1 {
            match name.0.first() {
                Some(ObjectNamePart::Identifier(qualifier)) => {
                    let qualifier = qualifier.value.to_ascii_lowercase();
                    if qualifier != "main" && qualifier != "temp" {
                        return ControlFlow::Continue(());
                    }
                }
                _ => return ControlFlow::Continue(()),
            }
        }

        // Keep the client's own alias if it had one, so `m.name` still resolves;
        // otherwise alias to the spelling the client used, so `sqlite_schema.name` does.
        let effective_alias = alias.clone().unwrap_or(TableAlias {
            name: Ident::new(relation),
            columns: vec![],
        });

        let Ok(statements) = Parser::parse_sql(&PostgreSqlDialect {}, FILTERED_RELATION_SQL) else {
            return ControlFlow::Continue(());
        };
        let Some(Statement::Query(subquery)) = statements.into_iter().next() else {
            return ControlFlow::Continue(());
        };

        *table_factor = TableFactor::Derived {
            lateral: false,
            subquery,
            alias: Some(effective_alias),
        };
        self.replaced += 1;
        ControlFlow::Continue(())
    }
}

/// Allocation-free case-insensitive substring test. This runs on every client
/// query while the flag is on, so it must not allocate.
fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let haystack = haystack.as_bytes();
    let needle = needle.as_bytes();
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .windows(needle.len())
        .any(|window| window.eq_ignore_ascii_case(needle))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The filtered relation as sqlparser renders it back out (note: uppercase SUBSTR).
    const FILTERED: &str = "(SELECT * FROM sqlite_master WHERE SUBSTR(name, 1, 11) <> '__pgsqlite_' AND (tbl_name IS NULL OR SUBSTR(tbl_name, 1, 11) <> '__pgsqlite_'))";

    fn rewritten(query: &str) -> String {
        SqliteMasterFilter::translate(query).into_owned()
    }

    #[test]
    fn rewrites_bare_relation_and_keeps_client_predicate() {
        let out = rewritten("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name");
        assert_eq!(
            out,
            format!("SELECT name FROM {FILTERED} AS sqlite_master WHERE type = 'table' ORDER BY name")
        );
    }

    #[test]
    fn rewrites_aggregate() {
        let out = rewritten("SELECT count(*) FROM sqlite_master");
        assert_eq!(out, format!("SELECT count(*) FROM {FILTERED} AS sqlite_master"));
    }

    #[test]
    fn preserves_client_alias_in_join() {
        let out = rewritten("SELECT m.name FROM sqlite_schema m JOIN foo f ON f.n = m.name");
        assert_eq!(
            out,
            format!("SELECT m.name FROM {FILTERED} AS m JOIN foo AS f ON f.n = m.name")
        );
    }

    #[test]
    fn rewrites_schema_qualified_relation() {
        let out = rewritten("SELECT name FROM main.sqlite_master");
        assert_eq!(out, format!("SELECT name FROM {FILTERED} AS sqlite_master"));
    }

    #[test]
    fn rewrites_inside_cte() {
        let out = rewritten("WITH t AS (SELECT name FROM sqlite_master) SELECT * FROM t");
        assert_eq!(
            out,
            format!("WITH t AS (SELECT name FROM {FILTERED} AS sqlite_master) SELECT * FROM t")
        );
    }

    #[test]
    fn rewrites_inside_exists_subquery() {
        let out = rewritten("SELECT * FROM foo WHERE EXISTS (SELECT 1 FROM sqlite_master WHERE name = foo.n)");
        assert_eq!(
            out,
            format!("SELECT * FROM foo WHERE EXISTS (SELECT 1 FROM {FILTERED} AS sqlite_master WHERE name = foo.n)")
        );
    }

    #[test]
    fn rewrites_select_sql_projection() {
        let out = rewritten("SELECT sql FROM sqlite_master WHERE type = 'table'");
        assert_eq!(
            out,
            format!("SELECT sql FROM {FILTERED} AS sqlite_master WHERE type = 'table'")
        );
    }

    #[test]
    fn does_not_corrupt_complex_predicates() {
        // This is the shape that broke the earlier query-rewriting attempt (PR #82).
        let out = rewritten(r"SELECT name FROM sqlite_master WHERE name NOT IN ('a', 'b') AND name LIKE 'x%' ESCAPE '\'");
        assert_eq!(
            out,
            format!(r"SELECT name FROM {FILTERED} AS sqlite_master WHERE name NOT IN ('a', 'b') AND name LIKE 'x%' ESCAPE '\'")
        );
    }

    #[test]
    fn leaves_unrelated_queries_borrowed() {
        assert!(matches!(
            SqliteMasterFilter::translate("SELECT * FROM customers"),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn leaves_unparseable_input_borrowed() {
        assert!(matches!(
            SqliteMasterFilter::translate("SELECT FROM WHERE sqlite_master ((("),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn filters_listing_queries_that_name_an_internal_table() {
        // A listing query is filtered the same way regardless of what its WHERE
        // clause happens to test for; naming an internal table in a predicate
        // doesn't opt the query out of the rewrite.
        let out = rewritten("SELECT name FROM sqlite_master WHERE name = '__pgsqlite_schema'");
        assert_eq!(
            out,
            format!("SELECT name FROM {FILTERED} AS sqlite_master WHERE name = '__pgsqlite_schema'")
        );
    }

    #[test]
    fn filters_despite_internal_prefix_in_a_comment() {
        // Regression guard: the internal-prefix literal appearing anywhere in the
        // query text (e.g. a trailing comment) must never disable the rewrite.
        // sqlparser drops comments on the round-trip, so assert on the rewritten
        // relation rather than on the comment surviving.
        let out = rewritten("SELECT name FROM sqlite_master -- __pgsqlite_");
        assert!(out.contains(FILTERED));
    }

    #[test]
    fn does_not_match_unrelated_table_named_like_a_wildcard_match() {
        // 'abpgsqliteX' matches LIKE '__pgsqlite_%' but not substr(name,1,11).
        // Guard that we generate the substr form, not the LIKE form.
        let out = rewritten("SELECT name FROM sqlite_master");
        assert!(out.contains("SUBSTR(name, 1, 11) <> '__pgsqlite_'"));
        assert!(!out.contains("LIKE '__pgsqlite_"));
    }
}
