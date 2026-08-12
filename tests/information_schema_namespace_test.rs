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
