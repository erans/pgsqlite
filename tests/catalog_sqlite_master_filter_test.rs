mod common;
use common::setup_test_server_with_init;

/// pgsqlite keeps its own bookkeeping in tables named `__pgsqlite_*`. A client
/// that lists `sqlite_master` (or its `sqlite_schema` alias) directly should not
/// see those internal tables, only its own. These tests exercise the filtering
/// added to the catalog interceptor.
async fn setup() -> common::TestServer {
    setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(
                "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL, balance REAL)",
            )
            .await?;
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)").await?;
            db.execute("CREATE INDEX idx_customers_name ON customers(name)").await?;
            db.execute("CREATE VIEW customer_names AS SELECT name FROM customers").await?;
            // Guarantee at least one internal-prefixed table exists so the
            // filter has something to hide even if pgsqlite's own metadata
            // tables are named differently across versions.
            db.execute("CREATE TABLE __pgsqlite_probe (k TEXT)").await?;
            // A user table whose name is NOT one of the four materialized
            // pg_catalog tables. It must still be returned, proving the filter
            // is an exact-set match and not a `pg_%` wildcard.
            db.execute("CREATE TABLE pg_indexes (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    })
    .await
}

fn names(rows: &[tokio_postgres::Row]) -> Vec<String> {
    rows.iter().map(|r| r.get::<_, String>(0)).collect()
}

/// The four pg_catalog tables pgsqlite materializes as real SQLite tables
/// (source of truth: src/migration/registry.rs). None of these should leak.
const MATERIALIZED_PG_CATALOG_TABLES: [&str; 4] =
    ["pg_attrdef", "pg_constraint", "pg_depend", "pg_index"];

fn assert_no_internal(names: &[String]) {
    for name in names {
        assert!(
            !name.starts_with("__pgsqlite_"),
            "internal table leaked to client: {name} (all: {names:?})"
        );
        assert!(
            !MATERIALIZED_PG_CATALOG_TABLES.contains(&name.as_str()),
            "materialized pg_catalog table leaked to client: {name} (all: {names:?})"
        );
        // Indexes pgsqlite creates on its internal enum bookkeeping table.
        assert!(
            !name.starts_with("idx_enum_values_"),
            "internal index leaked to client: {name} (all: {names:?})"
        );
    }
}

#[tokio::test]
async fn plain_sqlite_master_hides_internal_tables() {
    let server = setup().await;
    let rows = server
        .client
        .query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name", &[])
        .await
        .expect("sqlite_master query should succeed");
    let names = names(&rows);

    assert_no_internal(&names);
    // User tables are still returned.
    assert!(names.contains(&"customers".to_string()), "customers missing: {names:?}");
    assert!(names.contains(&"orders".to_string()), "orders missing: {names:?}");
    // A user table named `pg_indexes` (NOT one of the four materialized
    // pg_catalog tables) is still returned: the filter is an exact-set match,
    // not a `pg_%` wildcard.
    assert!(names.contains(&"pg_indexes".to_string()), "pg_indexes missing: {names:?}");

    server.abort();
}

#[tokio::test]
async fn aliased_sqlite_master_hides_internal_tables() {
    let server = setup().await;
    let rows = server
        .client
        .query(
            "SELECT m.name FROM sqlite_master m WHERE m.type = 'table' ORDER BY m.name",
            &[],
        )
        .await
        .expect("aliased sqlite_master query should succeed");
    let names = names(&rows);

    assert_no_internal(&names);
    assert!(names.contains(&"customers".to_string()), "customers missing: {names:?}");

    server.abort();
}

#[tokio::test]
async fn sqlite_schema_alias_hides_internal_tables() {
    let server = setup().await;
    let rows = server
        .client
        .query("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name", &[])
        .await
        .expect("sqlite_schema query should succeed");
    let names = names(&rows);

    assert_no_internal(&names);
    assert!(names.contains(&"customers".to_string()), "customers missing: {names:?}");

    server.abort();
}

#[tokio::test]
async fn indexes_and_views_are_returned_without_internal_leak() {
    let server = setup().await;
    let rows = server
        .client
        .query(
            "SELECT name FROM sqlite_master WHERE type IN ('index', 'view') ORDER BY name",
            &[],
        )
        .await
        .expect("sqlite_master index/view query should succeed");
    let names = names(&rows);

    // assert_no_internal also rejects any idx_enum_values_* internal index,
    // which pgsqlite creates on __pgsqlite_enum_values, so a raw index scan
    // stays clean.
    assert_no_internal(&names);
    assert!(
        names.contains(&"idx_customers_name".to_string()),
        "user index missing: {names:?}"
    );
    assert!(
        names.contains(&"customer_names".to_string()),
        "user view missing: {names:?}"
    );

    server.abort();
}

#[tokio::test]
async fn select_star_hides_internal_tables_and_their_indexes() {
    let server = setup().await;
    // An unqualified scan (no `type = 'table'` filter) still comes back clean:
    // the tbl_name predicate hides indexes/triggers owned by internal tables.
    let rows = server
        .client
        .query("SELECT name FROM sqlite_master ORDER BY name", &[])
        .await
        .expect("SELECT * style sqlite_master query should succeed");
    let names = names(&rows);

    assert_no_internal(&names);
    // User objects across every type are still present.
    assert!(names.contains(&"customers".to_string()), "customers missing: {names:?}");
    assert!(names.contains(&"pg_indexes".to_string()), "pg_indexes missing: {names:?}");
    assert!(
        names.contains(&"idx_customers_name".to_string()),
        "user index missing: {names:?}"
    );
    assert!(
        names.contains(&"customer_names".to_string()),
        "user view missing: {names:?}"
    );

    server.abort();
}

#[tokio::test]
async fn non_sqlite_master_query_is_untouched() {
    let server = setup().await;
    server
        .client
        .execute("INSERT INTO customers (id, name, balance) VALUES (1, 'Ada', 10.0)", &[])
        .await
        .expect("insert should succeed");
    let rows = server
        .client
        .query("SELECT name FROM customers ORDER BY id", &[])
        .await
        .expect("plain user-table query should succeed");
    let names = names(&rows);

    assert_eq!(names, vec!["Ada".to_string()]);

    server.abort();
}
