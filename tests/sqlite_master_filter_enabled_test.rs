mod common;
use common::*;
use tokio_postgres::SimpleQueryMessage;

async fn table_names(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    client
        .simple_query(sql)
        .await
        .unwrap()
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn hides_internal_objects_from_simple_protocol() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            db.execute("CREATE INDEX idx_customers_name ON customers(name)").await?;
            // Matches LIKE '__pgsqlite_%' but not substr(name, 1, 11) — must stay visible.
            db.execute("CREATE TABLE abpgsqliteX (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    })
    .await;

    let names = table_names(
        &server.client,
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
    )
    .await;
    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked: {names:?}"
    );
    assert!(names.iter().any(|n| n == "customers"), "user table missing: {names:?}");
    assert!(names.iter().any(|n| n == "abpgsqliteX"), "LIKE-wildcard false positive: {names:?}");

    // Indexes owned by internal tables carry no __pgsqlite_ prefix of their own
    // and must be caught via tbl_name.
    let indexes = table_names(
        &server.client,
        "SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name",
    )
    .await;
    assert!(
        !indexes.iter().any(|n| n.starts_with("idx_enum_") || n.contains("__pgsqlite_")),
        "internal indexes leaked: {indexes:?}"
    );
    assert!(
        indexes.iter().any(|n| n == "idx_customers_name"),
        "user index missing: {indexes:?}"
    );

    // The DDL projection must not dump internal CREATE TABLE statements.
    let ddl = table_names(&server.client, "SELECT sql FROM sqlite_master WHERE type = 'table'").await;
    assert!(
        !ddl.iter().any(|s| s.contains("__pgsqlite_")),
        "internal DDL leaked"
    );

    // sqlite_schema is an alias for the same relation.
    let via_alias = table_names(
        &server.client,
        "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name",
    )
    .await;
    assert!(!via_alias.iter().any(|n| n.starts_with("__pgsqlite_")));

    // Aggregates must count only visible rows.
    let counts = table_names(&server.client, "SELECT count(*) FROM sqlite_master WHERE type = 'table'").await;
    assert_eq!(counts.len(), 1);
    let visible: usize = counts[0].parse().unwrap();
    assert_eq!(visible, names.len(), "count disagrees with the listing");
}

#[tokio::test]
async fn hides_internal_objects_from_extended_protocol() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total TEXT)").await?;
            Ok(())
        })
    })
    .await;

    // client.query() goes through Parse/Bind/Execute, not simple query.
    let rows = server
        .client
        .query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name", &[])
        .await
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked over extended protocol: {names:?}"
    );
    assert!(names.iter().any(|n| n == "orders"), "user table missing: {names:?}");
}

#[tokio::test]
async fn write_attempts_on_sqlite_master_keep_sqlites_own_error() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|_| Box::pin(async move { Ok(()) })).await;

    // The rewrite is a read-context rewrite. Substituting a derived table for a
    // DELETE/UPDATE target produces invalid SQL whose error text would paste
    // `__pgsqlite_` in front of the very user the flag exists to shield.
    for sql in [
        "DELETE FROM sqlite_master WHERE name = 'zzz'",
        "UPDATE sqlite_master SET name = 'x'",
    ] {
        let err = server
            .client
            .simple_query(sql)
            .await
            .expect_err("writing to sqlite_master must fail");
        let text = err.to_string();
        assert!(
            !text.contains("__pgsqlite_"),
            "internal prefix leaked into a client-facing error for `{sql}`: {text}"
        );
        assert!(
            text.contains("may not be modified"),
            "expected SQLite's own diagnostic for `{sql}`, got: {text}"
        );
    }
}

#[tokio::test]
async fn hides_internal_objects_through_a_view() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    })
    .await;

    // The issue's reproduction. The SELECT below never mentions sqlite_master,
    // so the view body is the only place the filter can be applied.
    server
        .client
        .simple_query("CREATE VIEW schema_names AS SELECT name FROM sqlite_master")
        .await
        .expect("CREATE VIEW over sqlite_master should succeed");

    let names = table_names(&server.client, "SELECT name FROM schema_names ORDER BY name").await;
    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked through a view: {names:?}"
    );
    assert!(names.iter().any(|n| n == "customers"), "user table missing: {names:?}");

    // Accepted trade-off, asserted so a future reader finds it recorded rather
    // than discovering it as a surprise: SQLite persists the literal CREATE VIEW
    // text, so rewriting the body bakes the filter predicate — internal prefix
    // and all — into the *user's own* DDL. It surfaces here and in
    // information_schema.views.view_definition. Deliberately a weak assertion:
    // sqlparser's exact rendering is not pinned, only that the predicate is in
    // there somewhere.
    let view_ddl = table_names(&server.client, "SELECT sql FROM sqlite_master WHERE type = 'view'").await;
    let stored = view_ddl.join("\n").to_ascii_lowercase();
    assert!(
        stored.contains("__pgsqlite_") && stored.contains("substr"),
        "the filter predicate is expected to be baked into the persisted view DDL \
         (see docs/configuration.md); got: {view_ddl:?}"
    );
}

#[tokio::test]
async fn hides_internal_objects_through_a_view_over_extended_protocol() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    })
    .await;

    // The extended-protocol hook (src/query/extended.rs) is a physically
    // separate call site from the simple-protocol one, and many drivers send
    // DDL through Parse/Bind/Execute by default. client.execute()/client.query()
    // take that path; simple_query() does not.
    server
        .client
        .execute("CREATE VIEW ext_schema_names AS SELECT name FROM sqlite_master", &[])
        .await
        .expect("CREATE VIEW over sqlite_master should succeed on the extended protocol");

    let rows = server
        .client
        .query("SELECT name FROM ext_schema_names ORDER BY name", &[])
        .await
        .unwrap();
    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked through a view created over the extended protocol: {names:?}"
    );
    assert!(names.iter().any(|n| n == "customers"), "user table missing: {names:?}");
}

#[tokio::test]
async fn hides_internal_objects_through_a_create_view_if_not_exists() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    })
    .await;

    // PostgreSQL has no `CREATE VIEW IF NOT EXISTS`; SQLite does. The filter
    // parsed with PostgreSqlDialect only, so this shape failed to parse, failed
    // open, and stored the view unfiltered — issue #86 verbatim, one keyword
    // away. The SQLiteDialect retry closes it.
    server
        .client
        .simple_query("CREATE VIEW IF NOT EXISTS maybe_names AS SELECT name FROM sqlite_master")
        .await
        .expect("CREATE VIEW IF NOT EXISTS over sqlite_master should succeed");

    let names = table_names(&server.client, "SELECT name FROM maybe_names ORDER BY name").await;
    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked through CREATE VIEW IF NOT EXISTS: {names:?}"
    );
    assert!(names.iter().any(|n| n == "customers"), "user table missing: {names:?}");
}

#[tokio::test]
async fn hides_internal_objects_through_create_table_as_select() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total TEXT)").await?;
            Ok(())
        })
    })
    .await;

    server
        .client
        .simple_query("CREATE TABLE catalog_snapshot AS SELECT name FROM sqlite_master")
        .await
        .expect("CTAS over sqlite_master should succeed");

    let names = table_names(
        &server.client,
        "SELECT name FROM catalog_snapshot ORDER BY name",
    )
    .await;
    assert!(
        !names.iter().any(|n| n.starts_with("__pgsqlite_")),
        "internal tables leaked into a CTAS snapshot: {names:?}"
    );
    assert!(names.iter().any(|n| n == "orders"), "user table missing: {names:?}");
}

#[tokio::test]
async fn ctas_with_explicit_column_list_never_leaks_internal_prefix() {
    pgsqlite::config::set_hide_internal_tables(true);

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total TEXT)").await?;
            Ok(())
        })
    })
    .await;

    // `CREATE TABLE t (name TEXT) AS SELECT ...` is valid PostgreSQL. Rewriting
    // its body produced valid SQL, but downstream CreateTableTranslator's greedy
    // CREATE_TABLE_REGEX swallowed the `AS SELECT` once a parenthesized subquery
    // followed the column list; rusqlite embeds the whole statement in its error
    // and pgsqlite propagates it verbatim, so the client got a message
    // containing `__pgsqlite_` — the exact opposite of what the flag promises.
    //
    // The filter now skips this shape, so it behaves as with the flag off. This
    // test does not care whether the statement succeeds or fails; it only
    // requires that nothing pastes the internal prefix at the client.
    //
    // Verified against the unguarded code, which fails this assertion with:
    //   SQLite error: near "SELECT": syntax error in CREATE TABLE
    //   typed_snapshot (name TEXT AS SELECT name FROM (SELECT * FROM
    //   sqlite_master WHERE SUBSTR(name, 1, 11) <> '__pgsqlite_' ...
    let sql = "CREATE TABLE typed_snapshot (name TEXT) AS SELECT name FROM sqlite_master";
    if let Err(err) = server.client.simple_query(sql).await {
        let text = err.to_string();
        assert!(
            !text.contains("__pgsqlite_"),
            "internal prefix leaked into a client-facing error for `{sql}`: {text}"
        );
        return;
    }

    // If it succeeded, the table must still not expose internal rows. Note that
    // CreateTableTranslator's greedy CREATE_TABLE_REGEX drops the `AS SELECT`
    // clause outright for this shape, so the table comes out empty. That is a
    // pre-existing bug in the translator, unrelated to this flag and out of
    // scope here (it reproduces with --hide-internal-tables off); this test
    // only pins that we do not make it *worse* by leaking the prefix.
    let names = table_names(&server.client, "SELECT name FROM typed_snapshot ORDER BY name").await;
    assert!(
        !names.iter().any(|n| n.contains("__pgsqlite_")),
        "internal tables leaked into a typed CTAS snapshot: {names:?}"
    );
}
