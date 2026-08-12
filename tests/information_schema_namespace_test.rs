mod common;
use common::setup_test_server;

/// Issue #88: information_schema.tables listed pgsqlite's own catalog relations
/// as user tables in public, so Django inspectdb and SQLAlchemy automap would
/// generate models for pg_constraint and friends.
#[tokio::test]
async fn internal_relations_are_not_reported_in_public() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .unwrap();

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["customers".to_string()]);
}

#[tokio::test]
async fn internal_relations_report_their_real_schema() {
    let server = setup_test_server().await;
    let client = &server.client;

    let rows = client
        .query(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = $1",
            &[&"pg_class"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "pg_catalog");

    let rows = client
        .query(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = $1",
            &[&"information_schema_tables"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "information_schema");
}

/// The deleted handler ignored ORDER BY entirely.
#[tokio::test]
async fn order_by_is_respected() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("CREATE TABLE zebra (id INTEGER)").await.unwrap();
    client.simple_query("CREATE TABLE alpha (id INTEGER)").await.unwrap();
    client.simple_query("CREATE TABLE middle (id INTEGER)").await.unwrap();

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["alpha".to_string(), "middle".to_string(), "zebra".to_string()]);
}

/// The deleted handler returned zero rows for any aggregate.
#[tokio::test]
async fn aggregates_work() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("CREATE TABLE a (id INTEGER)").await.unwrap();
    client.simple_query("CREATE TABLE b (id INTEGER)").await.unwrap();

    let row = client
        .query_one(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 2);
}

/// The deleted handler supported equality on table_name and nothing else.
#[tokio::test]
async fn non_table_name_predicates_filter() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("CREATE TABLE t (id INTEGER)").await.unwrap();
    client.simple_query("CREATE VIEW v AS SELECT id FROM t").await.unwrap();

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_type = 'VIEW'",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["v".to_string()]);
}

/// #102 divergence, asserted so it is documented rather than discovered.
/// pg_class still uses v28's LIKE 'pg\_%' heuristic, so a user table named
/// pg_* is misfiled there while information_schema.tables gets it right.
/// When #102 lands, the second assertion flips to "public".
#[tokio::test]
async fn user_table_named_like_a_catalog_relation_is_visible() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query("CREATE TABLE pg_myreport (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();

    let rows = client
        .query(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = $1",
            &[&"pg_myreport"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "public", "#88: exact-name matching");

    let rows = client
        .query(
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = $1",
            &[&"pg_myreport"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, &str>(0),
        "pg_catalog",
        "known divergence until #102 lands; flip this to 'public' there"
    );
}

/// The type fidelity table from the spec. The deleted handler got id, ts and
/// tags wrong; the pre-v29 view got six of eight wrong, including reporting
/// `integer` for TIMESTAMPTZ, which leaked the INTEGER datetime storage.
#[tokio::test]
async fn column_types_are_reported_faithfully() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query(
            "CREATE TABLE fidelity (
                id SERIAL PRIMARY KEY,
                amount NUMERIC(10,2),
                uid UUID,
                doc JSONB,
                tags TEXT[],
                ts TIMESTAMPTZ,
                flag BOOLEAN,
                nick VARCHAR(50)
            )",
        )
        .await
        .unwrap();

    let rows = client
        .query(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = 'fidelity' ORDER BY ordinal_position",
            &[],
        )
        .await
        .unwrap();

    let got: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect();

    assert_eq!(
        got,
        vec![
            ("id".to_string(), "integer".to_string()),
            ("amount".to_string(), "numeric".to_string()),
            ("uid".to_string(), "uuid".to_string()),
            ("doc".to_string(), "jsonb".to_string()),
            ("tags".to_string(), "ARRAY".to_string()),
            ("ts".to_string(), "timestamp with time zone".to_string()),
            ("flag".to_string(), "boolean".to_string()),
            ("nick".to_string(), "character varying".to_string()),
        ]
    );
}

/// The four columns the handler populated and the pre-v29 view left NULL.
/// Django and SQLAlchemy both read these.
#[tokio::test]
async fn column_modifiers_and_defaults_are_reported() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query(
            "CREATE TABLE widgets (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                active BOOLEAN DEFAULT true
            )",
        )
        .await
        .unwrap();

    let row = client
        .query_one(
            "SELECT character_maximum_length FROM information_schema.columns \
             WHERE table_name = 'widgets' AND column_name = 'name'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<i32>>(0), Some(50));

    let row = client
        .query_one(
            "SELECT numeric_precision, numeric_scale FROM information_schema.columns \
             WHERE table_name = 'widgets' AND column_name = 'price'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<i32>>(0), Some(10));
    assert_eq!(row.get::<_, Option<i32>>(1), Some(2));

    let row = client
        .query_one(
            "SELECT column_default FROM information_schema.columns \
             WHERE table_name = 'widgets' AND column_name = 'active'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.get::<_, Option<String>>(0).is_some(),
        "column_default should be populated for DEFAULT true"
    );

    // INTEGER PRIMARY KEY is NOT NULL in PostgreSQL. pragma_table_info reports
    // notnull=0 for it, so the view must also consider pk.
    let row = client
        .query_one(
            "SELECT is_nullable FROM information_schema.columns \
             WHERE table_name = 'widgets' AND column_name = 'id'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "NO");
}

#[tokio::test]
async fn columns_of_internal_relations_are_not_in_public() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("CREATE TABLE t (id INTEGER)").await.unwrap();

    let rows = client
        .query(
            "SELECT DISTINCT table_name FROM information_schema.columns \
             WHERE table_schema = 'public' ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["t".to_string()]);
}

/// The internal-relation list is maintained by hand, so it can drift when a
/// migration adds a catalog relation. Fail here rather than silently leaking
/// the new relation into information_schema.tables as a user table.
#[test]
fn internal_relation_list_matches_migrated_database() {
    use pgsqlite::catalog::internal_relations::{
        INTERNAL_INFORMATION_SCHEMA_RELATIONS, INTERNAL_PG_CATALOG_RELATIONS,
    };
    use pgsqlite::migration::MigrationRunner;
    use rusqlite::Connection;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drift.db");
    let conn = Connection::open(&db_path).unwrap();
    let mut runner = MigrationRunner::new(conn);
    runner.run_pending_migrations().unwrap();
    let conn = runner.into_connection();

    let mut in_database: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE name NOT LIKE 'sqlite_%' AND substr(name, 1, 11) <> '__pgsqlite_' \
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    in_database.sort();

    let mut in_list: Vec<String> = INTERNAL_PG_CATALOG_RELATIONS
        .iter()
        .chain(INTERNAL_INFORMATION_SCHEMA_RELATIONS.iter())
        .map(|s| s.to_string())
        .collect();
    in_list.sort();

    let missing: Vec<&String> = in_database.iter().filter(|n| !in_list.contains(n)).collect();
    assert!(
        missing.is_empty(),
        "migrations create relations absent from src/catalog/internal_relations.rs: {missing:?}. \
         Add them, or they will be reported as user tables in information_schema.tables."
    );

    let stale: Vec<&String> = in_list.iter().filter(|n| !in_database.contains(n)).collect();
    assert!(
        stale.is_empty(),
        "src/catalog/internal_relations.rs lists relations no migration creates: {stale:?}"
    );
}

fn simple_query_column(messages: &[tokio_postgres::SimpleQueryMessage]) -> Vec<String> {
    let mut values: Vec<String> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(r) => Some(r.get(0).unwrap().to_string()),
            _ => None,
        })
        .collect();
    values.sort();
    values
}

/// The catalog interceptor gates on a lowercased query, so a mixed-case
/// qualifier enters the catalog path. Before the case-insensitive rewrite it
/// fell through untranslated and died with "no such table". The Rust handler
/// this branch deleted dispatched on a lowercased name, so every spelling
/// worked; JDBC-derived tooling emits `INFORMATION_SCHEMA.tables`.
#[tokio::test]
async fn mixed_case_schema_qualifiers_route_to_the_views() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query("CREATE TABLE gadgets (id INTEGER PRIMARY KEY, label TEXT)")
        .await
        .unwrap();

    let baseline = simple_query_column(
        &client
            .simple_query(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            )
            .await
            .unwrap(),
    );
    assert_eq!(baseline, vec!["gadgets".to_string()]);

    for query in [
        "SELECT table_name FROM Information_Schema.Tables WHERE table_schema = 'public'",
        "SELECT table_name FROM information_schema.TABLES WHERE table_schema = 'public'",
        "SELECT table_name FROM INFORMATION_SCHEMA.tables WHERE table_schema = 'public'",
        "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = 'public'",
    ] {
        let messages = client
            .simple_query(query)
            .await
            .unwrap_or_else(|e| panic!("{query} failed: {e}"));
        assert_eq!(simple_query_column(&messages), baseline, "mismatch for {query}");
    }

    let baseline = simple_query_column(
        &client
            .simple_query(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'gadgets'",
            )
            .await
            .unwrap(),
    );
    assert_eq!(baseline, vec!["id".to_string(), "label".to_string()]);

    for query in [
        "SELECT column_name FROM Information_Schema.Columns WHERE table_name = 'gadgets'",
        "SELECT column_name FROM information_schema.COLUMNS WHERE table_name = 'gadgets'",
        "SELECT column_name FROM INFORMATION_SCHEMA.columns WHERE table_name = 'gadgets'",
    ] {
        let messages = client
            .simple_query(query)
            .await
            .unwrap_or_else(|e| panic!("{query} failed: {e}"));
        assert_eq!(simple_query_column(&messages), baseline, "mismatch for {query}");
    }
}

/// The schema-prefix translator used a blind `str::replace`, which rewrote
/// matches inside string literals. Any table holding SQL text -- an audit log,
/// a migration history, a notes table -- got corrupted values and silently
/// wrong equality results.
#[tokio::test]
async fn string_literals_containing_schema_qualifiers_survive() {
    let server = setup_test_server().await;
    let client = &server.client;

    for literal in [
        "information_schema.tables",
        "information_schema.columns",
        "pg_catalog.pg_class",
    ] {
        let row = client
            .query_one(&format!("SELECT '{literal}' AS s"), &[])
            .await
            .unwrap_or_else(|e| panic!("selecting '{literal}' failed: {e}"));
        assert_eq!(row.get::<_, &str>(0), literal);
    }

    client
        .simple_query("CREATE TABLE notes (id INTEGER PRIMARY KEY, msg TEXT)")
        .await
        .unwrap();

    for (id, msg) in [
        (1i32, "see information_schema.tables for details"),
        (2i32, "see pg_catalog.pg_class for details"),
    ] {
        client
            .execute("INSERT INTO notes (id, msg) VALUES ($1, $2)", &[&id, &msg])
            .await
            .unwrap();

        // simple_query, not query_one: the catalog interceptor's gate is a
        // substring test over the whole query text, so a literal mentioning
        // pg_catalog sends even this count down the catalog path, which types
        // the result as text. Pre-existing and orthogonal to literal handling.
        let messages = client
            .simple_query(&format!("SELECT count(*) FROM notes WHERE msg = '{msg}'"))
            .await
            .unwrap();
        assert_eq!(
            simple_query_column(&messages),
            vec!["1".to_string()],
            "round-trip failed for {msg:?}"
        );

        let row = client
            .query_one("SELECT msg FROM notes WHERE id = $1", &[&id])
            .await
            .unwrap();
        assert_eq!(row.get::<_, &str>(0), msg);
    }

    // The SQL-standard doubled-quote escape: 'it''s ...' is one literal.
    let row = client
        .query_one("SELECT 'it''s information_schema.tables' AS s", &[])
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "it's information_schema.tables");
}

/// PostgreSQL reports radix 2 for the binary-precision numeric types and 10
/// only for numeric/decimal. v29 first reported 10 for every typed precision.
#[tokio::test]
async fn numeric_precision_radix_matches_postgresql() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query(
            "CREATE TABLE radix_probe (
                i INTEGER,
                b BIGINT,
                s SMALLINT,
                d DOUBLE PRECISION,
                n NUMERIC(10,2),
                t TEXT
            )",
        )
        .await
        .unwrap();

    let rows = client
        .query(
            "SELECT column_name, numeric_precision, numeric_precision_radix \
             FROM information_schema.columns WHERE table_name = 'radix_probe' \
             ORDER BY ordinal_position",
            &[],
        )
        .await
        .unwrap();

    let got: Vec<(String, Option<i32>, Option<i32>)> = rows
        .iter()
        .map(|r| {
            (
                r.get::<_, String>(0),
                r.get::<_, Option<i32>>(1),
                r.get::<_, Option<i32>>(2),
            )
        })
        .collect();

    assert_eq!(
        got,
        vec![
            ("i".to_string(), Some(32), Some(2)),
            ("b".to_string(), Some(64), Some(2)),
            ("s".to_string(), Some(16), Some(2)),
            ("d".to_string(), Some(53), Some(2)),
            ("n".to_string(), Some(10), Some(10)),
            ("t".to_string(), None, None),
        ]
    );
}
