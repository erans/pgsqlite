use std::borrow::Cow;
use std::ops::ControlFlow;
use std::sync::LazyLock;

use sqlparser::ast::{
    Ident, ObjectNamePart, Query, Statement, TableAlias, TableFactor, VisitMut, VisitorMut,
};
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};
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

/// [`FILTERED_RELATION_SQL`] parsed once, so each rewrite and each recognition
/// check reuses the same shape.
fn parse_filtered_relation() -> Option<Box<Query>> {
    let statements = Parser::parse_sql(&PostgreSqlDialect {}, FILTERED_RELATION_SQL).ok()?;
    match statements.into_iter().next() {
        Some(Statement::Query(query)) => Some(query),
        _ => None,
    }
}

/// How sqlparser renders [`FILTERED_RELATION_SQL`] back out. Comparing against
/// this is what lets us recognize our own generated relation in a query that
/// has already been rewritten and re-parsed.
static FILTERED_RELATION_RENDERED: LazyLock<String> =
    LazyLock::new(|| parse_filtered_relation().map(|q| q.to_string()).unwrap_or_default());

/// Is `query` the subquery this translator substitutes for a client
/// `sqlite_master` reference?
///
/// The SQL injection detector uses this so that pgsqlite's own wrapper is not
/// counted as attacker-supplied subquery nesting. Recognizing it cannot be
/// abused: a client that reproduces this exact subquery gets the filtered
/// relation, which is precisely what the flag exists to hand them.
pub(crate) fn is_generated_filter_subquery(query: &Query) -> bool {
    let rendered = &*FILTERED_RELATION_RENDERED;
    !rendered.is_empty() && query.to_string() == *rendered
}

/// Rewrites client references to `sqlite_master` / `sqlite_schema` so that
/// pgsqlite's own `__pgsqlite_*` objects are not listed.
///
/// This type is deliberately pure: it does not read configuration. Callers gate
/// it on `crate::config::hide_internal_tables()`, and it must only ever be
/// invoked on queries that arrived from a client over the wire.
pub struct SqliteMasterFilter;

impl SqliteMasterFilter {
    /// Cheap allocation-free gate: is there any point parsing this query?
    fn needs_translation(query: &str) -> bool {
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
            // PostgreSQL has no `CREATE VIEW IF NOT EXISTS`; SQLite does, and a
            // client talking to pgsqlite may well use it. Without this retry the
            // parse fails, we fail open, and the view is stored unfiltered —
            // issue #86 verbatim, one keyword away. The `replaced == 0` early
            // return below confines any rendering differences between the two
            // dialects to statements that genuinely referenced `sqlite_master`.
            Err(pg_err) => match Parser::parse_sql(&SQLiteDialect {}, query) {
                Ok(statements) => statements,
                Err(sqlite_err) => {
                    debug!(
                        "sqlite_master filter: parse failed, passing through: \
                         postgres dialect: {pg_err}; sqlite dialect: {sqlite_err}"
                    );
                    return Cow::Borrowed(query);
                }
            },
        };

        let mut visitor = RelationReplacer { replaced: 0 };
        for statement in &mut statements {
            // Rewrite where the filtered rows are read back; never where
            // filtering would change which rows a write touches.
            //
            // `UPDATE`/`DELETE` name their target relation with the same
            // `TableFactor::Table`, and substituting a derived table there
            // produces syntactically invalid SQL whose error text would leak
            // the `__pgsqlite_` prefix straight back to the client. Writes to
            // `sqlite_master` are SQLite's to reject, unmodified. Their
            // subqueries are left alone too: filtering a `DELETE ... WHERE
            // name IN (SELECT ... FROM sqlite_master)` would silently delete
            // fewer rows than the client's SQL says.
            //
            // This stays an allowlist. A statement kind we miss is merely
            // unfiltered; a write target we fail to recognize is invalid SQL.
            match statement {
                Statement::Query(query) => {
                    let _ = query.visit(&mut visitor);
                }
                Statement::Insert(insert) => {
                    if let Some(source) = insert.source.as_mut() {
                        let _ = source.visit(&mut visitor);
                    }
                }
                // SQLite persists the literal CREATE VIEW text and expands it
                // on every read, so creation time is the only chance to filter.
                // Covers MATERIALIZED and TEMP views: same variant, same field.
                Statement::CreateView { query, .. } => {
                    let _ = query.visit(&mut visitor);
                }
                // `CREATE TABLE ... AS SELECT`. `query` is `None` for ordinary
                // CREATE TABLE, which is then left untouched.
                //
                // A CTAS carrying an explicit column list
                // (`CREATE TABLE t (name TEXT) AS SELECT ...`) is skipped: the
                // rewritten DDL is valid SQL, but downstream
                // `CreateTableTranslator`'s greedy CREATE_TABLE_REGEX swallows
                // the `AS SELECT` clause once a parenthesized subquery follows
                // the column list, and the resulting SQLite error embeds the
                // whole statement text — pasting `__pgsqlite_` back to the very
                // client the flag exists to shield. Leaving it borrowed means
                // the statement behaves exactly as it does with the flag off.
                Statement::CreateTable(create_table) if create_table.columns.is_empty() => {
                    if let Some(query) = create_table.query.as_mut() {
                        let _ = query.visit(&mut visitor);
                    }
                }
                _ => {}
            }
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

        // Only `main.` names the catalog this filter substitutes for. `temp.`
        // is a *different* relation listing only temp objects, and
        // FILTERED_RELATION_SQL hardcodes an unqualified `FROM sqlite_master`,
        // so rewriting it would silently drop the qualifier and hand back
        // main's catalog. Left alone, exactly like an ATTACHed database's.
        if name.0.len() > 1 {
            match name.0.first() {
                Some(ObjectNamePart::Identifier(qualifier)) => {
                    if !qualifier.value.eq_ignore_ascii_case("main") {
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

        let Some(subquery) = parse_filtered_relation() else {
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

    #[test]
    fn leaves_attached_database_qualifier_alone() {
        // Only `main.` names the catalog we substitute for; `otherdb.sqlite_master`
        // belongs to an ATTACHed database and is none of our business.
        assert!(matches!(
            SqliteMasterFilter::translate("SELECT name FROM otherdb.sqlite_master"),
            Cow::Borrowed(_)
        ));
        assert_eq!(
            rewritten("SELECT name FROM otherdb.sqlite_master"),
            "SELECT name FROM otherdb.sqlite_master"
        );
    }

    #[test]
    fn leaves_temp_qualified_relation_borrowed() {
        // `temp.sqlite_master` is a *different* relation, listing only temp
        // objects. FILTERED_RELATION_SQL hardcodes an unqualified
        // `FROM sqlite_master`, so rewriting would drop the qualifier and hand
        // the client main's catalog instead — wrong rows, not merely unfiltered
        // ones. Leave it alone.
        for sql in [
            "SELECT name FROM temp.sqlite_master",
            "SELECT name FROM TEMP.sqlite_schema",
            "CREATE VIEW v AS SELECT name FROM temp.sqlite_master",
        ] {
            assert!(
                matches!(SqliteMasterFilter::translate(sql), Cow::Borrowed(_)),
                "temp-qualified reference was rewritten: {sql}"
            );
        }
    }

    #[test]
    fn leaves_delete_untouched() {
        // Substituting a derived table for the DELETE target yields invalid SQL
        // whose error text would leak `__pgsqlite_` to the client. Let SQLite
        // reject the write itself, with its own clear diagnostic.
        assert!(matches!(
            SqliteMasterFilter::translate("DELETE FROM sqlite_master WHERE name = 'zzz'"),
            Cow::Borrowed(_)
        ));
        assert_eq!(
            rewritten("DELETE FROM sqlite_master WHERE name = 'zzz'"),
            "DELETE FROM sqlite_master WHERE name = 'zzz'"
        );
    }

    #[test]
    fn leaves_update_untouched() {
        assert!(matches!(
            SqliteMasterFilter::translate("UPDATE sqlite_master SET name = 'x'"),
            Cow::Borrowed(_)
        ));
        assert_eq!(
            rewritten("UPDATE sqlite_master SET name = 'x'"),
            "UPDATE sqlite_master SET name = 'x'"
        );
    }

    #[test]
    fn rewrites_insert_select_source() {
        // The read half of an INSERT ... SELECT is still a listing.
        let out = rewritten("INSERT INTO t SELECT name FROM sqlite_master");
        assert_eq!(
            out,
            format!("INSERT INTO t SELECT name FROM {FILTERED} AS sqlite_master")
        );
    }

    #[test]
    fn rewrites_create_view_body() {
        // The gap in #86: a view body is read back on every SELECT against the
        // view, and the client's later `SELECT * FROM v` never mentions
        // sqlite_master, so this is the only chance to filter it.
        let out = rewritten("CREATE VIEW v AS SELECT name FROM sqlite_master");
        assert_eq!(
            out,
            format!("CREATE VIEW v AS SELECT name FROM {FILTERED} AS sqlite_master")
        );
    }

    #[test]
    fn rewrites_create_table_as_select_source() {
        let out = rewritten("CREATE TABLE snapshot AS SELECT name FROM sqlite_master");
        assert_eq!(
            out,
            format!("CREATE TABLE snapshot AS SELECT name FROM {FILTERED} AS sqlite_master")
        );
    }

    #[test]
    fn rewrites_materialized_view() {
        // Same Statement::CreateView variant, same `query` field: free.
        let out = rewritten("CREATE MATERIALIZED VIEW mv AS SELECT name FROM sqlite_master");
        assert_eq!(
            out,
            format!("CREATE MATERIALIZED VIEW mv AS SELECT name FROM {FILTERED} AS sqlite_master")
        );
    }

    #[test]
    fn leaves_create_table_without_query_borrowed() {
        // Passes the cheap substring gate on the *table name*, parses to a
        // CreateTable with `query: None`, replaces nothing, and must come back
        // untouched. Guards the new arm against perturbing ordinary DDL.
        assert!(matches!(
            SqliteMasterFilter::translate("CREATE TABLE sqlite_master_backup (id INT)"),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn leaves_create_view_over_attached_db_borrowed() {
        // Only `main.` names the catalog we substitute for.
        assert!(matches!(
            SqliteMasterFilter::translate(
                "CREATE VIEW v AS SELECT name FROM otherdb.sqlite_master"
            ),
            Cow::Borrowed(_)
        ));
    }

    #[test]
    fn rewrites_create_view_if_not_exists() {
        // PostgreSQL has no `CREATE VIEW IF NOT EXISTS`, so the PostgreSqlDialect
        // parse fails and we used to fail open — storing the view unfiltered,
        // which is issue #86 verbatim one keyword away. The SQLiteDialect retry
        // is what catches it.
        let out = rewritten("CREATE VIEW IF NOT EXISTS v AS SELECT name FROM sqlite_master");
        assert!(
            out.contains("SUBSTR(name, 1, 11) <> '__pgsqlite_'"),
            "CREATE VIEW IF NOT EXISTS was not filtered: {out}"
        );
        assert!(!out.contains("LIKE '__pgsqlite_"));
    }

    #[test]
    fn leaves_create_table_as_select_with_column_list_borrowed() {
        // `CREATE TABLE t (name TEXT) AS SELECT ...` is valid PostgreSQL, and
        // the rewrite of its body is valid SQL — but the rewritten DDL then
        // meets CreateTableTranslator's greedy CREATE_TABLE_REGEX, which
        // swallows the `AS SELECT` once a parenthesized subquery follows the
        // column list. The broken statement's text is echoed back in SQLite's
        // error, leaking `__pgsqlite_` to the client. Skipping the rewrite
        // makes the statement behave exactly as with the flag off.
        for sql in [
            "CREATE TABLE t (name TEXT) AS SELECT name FROM sqlite_master",
            "CREATE TABLE IF NOT EXISTS t (name TEXT) AS SELECT name FROM sqlite_master",
        ] {
            assert!(
                matches!(SqliteMasterFilter::translate(sql), Cow::Borrowed(_)),
                "CTAS with an explicit column list was rewritten: {sql}"
            );
        }
    }

    #[test]
    fn leaves_merge_untouched() {
        // The allowlist boundary. `Statement::Merge`'s target is itself a
        // `TableFactor`, so any future refactor to a denylist that visits
        // statements wholesale would substitute a derived table for the MERGE
        // target and emit invalid SQL whose error text pastes `__pgsqlite_`
        // back to the client. This test pins the boundary.
        //
        // Both forms below parse under sqlparser 0.57's PostgreSqlDialect, so
        // today this test really does exercise the allowlist rather than the
        // fail-open path. That is not guaranteed forever: if a future sqlparser
        // stopped parsing this syntax the assertion would still hold, but only
        // via fail-open, and the test would quietly become weaker evidence than
        // it looks. It is a regression guard against a denylist refactor, not
        // proof that MERGE parses.
        for sql in [
            "MERGE INTO sqlite_master USING src ON src.name = sqlite_master.name \
             WHEN MATCHED THEN UPDATE SET tbl_name = src.tbl_name",
            "MERGE INTO t USING sqlite_master AS s ON s.name = t.name \
             WHEN MATCHED THEN UPDATE SET n = s.name",
        ] {
            assert!(
                matches!(SqliteMasterFilter::translate(sql), Cow::Borrowed(_)),
                "MERGE was rewritten: {sql}"
            );
        }
    }

    #[test]
    fn recognizes_its_own_generated_subquery() {
        // What the SQL injection detector keys on. Parse the rewritten form back
        // and confirm the derived table it contains is recognized as ours.
        let out = rewritten("SELECT name FROM sqlite_master WHERE type = 'table'");
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, &out).unwrap();
        let Some(Statement::Query(query)) = statements.into_iter().next() else {
            panic!("expected a query");
        };
        let sqlparser::ast::SetExpr::Select(select) = &*query.body else {
            panic!("expected a select");
        };
        let TableFactor::Derived { subquery, .. } = &select.from[0].relation else {
            panic!("expected a derived table");
        };
        assert!(is_generated_filter_subquery(subquery));
    }

    #[test]
    fn does_not_recognize_a_client_written_subquery() {
        let statements =
            Parser::parse_sql(&PostgreSqlDialect {}, "SELECT * FROM (SELECT name FROM sqlite_master) z")
                .unwrap();
        let Some(Statement::Query(query)) = statements.into_iter().next() else {
            panic!("expected a query");
        };
        let sqlparser::ast::SetExpr::Select(select) = &*query.body else {
            panic!("expected a select");
        };
        let TableFactor::Derived { subquery, .. } = &select.from[0].relation else {
            panic!("expected a derived table");
        };
        assert!(!is_generated_filter_subquery(subquery));
    }
}
