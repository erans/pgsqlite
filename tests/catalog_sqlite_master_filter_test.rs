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
            // A user VIEW whose name is `pg_*` but is NOT one of the reserved
            // materialized pg_catalog / information_schema views. It must still
            // be returned, proving the view filter is exact-set, not `pg_%`.
            db.execute("CREATE VIEW pg_my_report AS SELECT id FROM customers").await?;
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

/// The pg_catalog / information_schema compatibility views pgsqlite materializes
/// as real SQLite views (source of truth: src/migration/registry.rs, the only
/// place these are `CREATE VIEW`d). None of these should leak. Mirrors the
/// private `PG_CATALOG_INTERNAL_VIEWS` const in the interceptor.
const MATERIALIZED_PG_CATALOG_VIEWS: [&str; 24] = [
    "information_schema_columns",
    "information_schema_key_column_usage",
    "information_schema_referential_constraints",
    "information_schema_schemata",
    "information_schema_table_constraints",
    "information_schema_tables",
    "pg_am",
    "pg_attribute",
    "pg_class",
    "pg_database",
    "pg_description",
    "pg_enum",
    "pg_foreign_data_wrapper",
    "pg_namespace",
    "pg_proc",
    "pg_roles",
    "pg_stat_activity",
    "pg_stat_all_indexes",
    "pg_stat_all_tables",
    "pg_stat_database",
    "pg_stat_user_indexes",
    "pg_stat_user_tables",
    "pg_type",
    "pg_user",
];

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
        assert!(
            !MATERIALIZED_PG_CATALOG_VIEWS.contains(&name.as_str()),
            "materialized pg_catalog view leaked to client: {name} (all: {names:?})"
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
    // stays clean. It also rejects every materialized pg_catalog view, so a
    // type='view' scan shows only the user's own views.
    assert_no_internal(&names);
    assert!(
        names.contains(&"idx_customers_name".to_string()),
        "user index missing: {names:?}"
    );
    assert!(
        names.contains(&"customer_names".to_string()),
        "user view missing: {names:?}"
    );
    // A user view named `pg_*` that is NOT reserved is still returned.
    assert!(
        names.contains(&"pg_my_report".to_string()),
        "user pg_* view missing: {names:?}"
    );

    server.abort();
}

#[tokio::test]
async fn view_scan_hides_materialized_pg_catalog_views() {
    let server = setup().await;
    // A plain type='view' scan is exactly what an external client uses to list
    // views. Every reserved pg_catalog / information_schema view must be hidden;
    // only the user's own views come back.
    let rows = server
        .client
        .query("SELECT name FROM sqlite_master WHERE type = 'view' ORDER BY name", &[])
        .await
        .expect("sqlite_master view scan should succeed");
    let names = names(&rows);

    assert_no_internal(&names);
    // Belt-and-suspenders: assert each reserved view name is absent by name.
    for reserved in MATERIALIZED_PG_CATALOG_VIEWS {
        assert!(
            !names.contains(&reserved.to_string()),
            "reserved view {reserved} leaked: {names:?}"
        );
    }
    // The user's own views are present, including the exact-set proof view.
    assert!(names.contains(&"customer_names".to_string()), "user view missing: {names:?}");
    assert!(names.contains(&"pg_my_report".to_string()), "user pg_* view missing: {names:?}");

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
    assert!(
        names.contains(&"pg_my_report".to_string()),
        "user pg_* view missing: {names:?}"
    );

    server.abort();
}

/// The exact shape of the web console's list-tables query that broke in
/// production: a `type='table'` scan with a `name NOT LIKE '\_%' ESCAPE '\'`
/// clause plus a large `name NOT IN (...)` list of pg_* names, ordered by name.
/// The previous AST re-serialization returned ZERO rows once the NOT IN list
/// grew past ~16 entries; the splice-based rewrite must return the user's table.
#[tokio::test]
async fn console_list_tables_query_returns_user_tables() {
    let server = setup().await;

    // 32 pg_* names - well past the N>=17 threshold where the old code broke.
    // Deliberately EXCLUDES the four names pgsqlite materializes as real tables
    // (pg_attrdef, pg_constraint, pg_depend, pg_index). Those do not start with
    // '_', so the client's own `\_%` filter cannot hide them: only our rewrite
    // can. If the rewrite regressed to fail-open, they would leak here and
    // assert_no_internal would catch it - so this test is strictly discriminating.
    let pg_names = [
        "pg_aggregate", "pg_am", "pg_amop", "pg_amproc", "pg_attribute",
        "pg_authid", "pg_auth_members", "pg_cast", "pg_class", "pg_collation",
        "pg_conversion", "pg_database", "pg_description",
        "pg_enum", "pg_event_trigger", "pg_extension", "pg_foreign_data_wrapper",
        "pg_foreign_server", "pg_foreign_table", "pg_inherits",
        "pg_language", "pg_namespace", "pg_opclass", "pg_operator", "pg_proc",
        "pg_range", "pg_rewrite", "pg_shdepend", "pg_statistic", "pg_tablespace",
        "pg_trigger", "pg_type",
    ];
    let in_list = pg_names
        .iter()
        .map(|n| format!("'{n}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT name FROM sqlite_master WHERE type = 'table' \
         AND name NOT LIKE '\\_%' ESCAPE '\\' \
         AND name NOT IN ({in_list}) ORDER BY name"
    );

    let rows = server
        .client
        .query(query.as_str(), &[])
        .await
        .expect("console list-tables query should succeed");
    let names = names(&rows);

    // The regression: this MUST return the user's tables, not zero rows.
    assert!(
        names.contains(&"customers".to_string()),
        "customers missing (the production regression): {names:?}"
    );
    assert!(names.contains(&"orders".to_string()), "orders missing: {names:?}");
    // Internals stay hidden, and a pg_* name NOT in the client's own NOT IN
    // list (pg_indexes) is still returned - the rewrite did not corrupt the
    // client's own exclusions either.
    assert_no_internal(&names);
    assert!(names.contains(&"pg_indexes".to_string()), "pg_indexes missing: {names:?}");

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
