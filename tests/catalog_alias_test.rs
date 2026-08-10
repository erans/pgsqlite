use pgsqlite::catalog::CatalogInterceptor;
use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;

async fn catalog_query(query: &str) -> pgsqlite::session::db_handler::DbResponse {
    let db = Arc::new(DbHandler::new(":memory:").unwrap());
    CatalogInterceptor::intercept_query(query, db, None)
        .await
        .expect("query should be intercepted")
        .expect("catalog query should succeed")
}

fn text_cell(row: &[Option<Vec<u8>>], index: usize) -> String {
    String::from_utf8(row[index].clone().expect("cell should not be NULL")).unwrap()
}

#[tokio::test]
async fn test_pg_catalog_database_alias_projection() {
    let response = catalog_query("SELECT oid AS did, datname FROM pg_catalog.pg_database").await;

    assert_eq!(response.columns, vec!["did", "datname"]);
    assert_eq!(response.rows.len(), 1);
    assert_eq!(text_cell(&response.rows[0], 0), "1");
    assert_eq!(text_cell(&response.rows[0], 1), "main");
}

#[tokio::test]
async fn test_pg_catalog_roles_alias_projection_uses_source_column() {
    let response = catalog_query("SELECT rolname AS rolsuper FROM pg_catalog.pg_roles").await;

    assert_eq!(response.columns, vec!["rolsuper"]);
    assert_eq!(response.rows.len(), 3);
    let role_names: Vec<String> = response.rows.iter().map(|row| text_cell(row, 0)).collect();
    assert_eq!(role_names, vec!["postgres", "public", "pgsqlite_user"]);
}

#[tokio::test]
async fn test_pg_catalog_namespace_alias_projection() {
    // pg_namespace is now served directly from SQLite (migration v28) instead of being
    // intercepted in Rust, so this alias-projection behavior is exercised against pg_roles
    // instead, which remains a Rust-intercepted catalog.
    let response = catalog_query("SELECT oid AS did FROM pg_catalog.pg_roles").await;

    assert_eq!(response.columns, vec!["did"]);
    assert_eq!(response.rows.len(), 3);
    assert_eq!(text_cell(&response.rows[0], 0), "10");
    assert_eq!(text_cell(&response.rows[1], 0), "0");
    assert_eq!(text_cell(&response.rows[2], 0), "100");
}

#[tokio::test]
async fn test_catalog_unquoted_aliases_fold_to_lowercase_and_quoted_aliases_preserve_case() {
    let response = catalog_query(
        "SELECT oid AS MixedAlias, rolname AS \"SchemaName\" FROM pg_catalog.pg_roles",
    )
    .await;

    assert_eq!(response.columns, vec!["mixedalias", "SchemaName"]);
    assert_eq!(response.rows.len(), 3);
    assert_eq!(text_cell(&response.rows[0], 0), "10");
    assert_eq!(text_cell(&response.rows[0], 1), "postgres");
}

#[tokio::test]
async fn test_catalog_wildcard_keeps_trailing_projection_items() {
    // pg_namespace is now served directly from SQLite (migration v28), so the wildcard half
    // of this test is exercised against pg_database instead, which remains Rust-intercepted.
    let database = catalog_query("SELECT *, oid FROM pg_catalog.pg_database").await;

    assert_eq!(database.columns.len(), 19);
    assert_eq!(database.columns[0], "oid");
    assert_eq!(database.columns[18], "oid");
    assert_eq!(database.rows.len(), 1);
    assert_eq!(text_cell(&database.rows[0], 0), "1");
    assert_eq!(text_cell(&database.rows[0], 18), "1");

    let roles = catalog_query("SELECT *, oid FROM pg_catalog.pg_roles").await;

    assert_eq!(roles.columns.len(), 14);
    assert_eq!(roles.columns[0], "oid");
    assert_eq!(roles.columns[13], "oid");
    assert_eq!(roles.rows.len(), 3);
    assert_eq!(text_cell(&roles.rows[0], 0), "10");
    assert_eq!(text_cell(&roles.rows[0], 13), "10");
}

#[tokio::test]
async fn test_catalog_count_star_returns_static_dataset_count() {
    // pg_namespace is now served directly from SQLite (migration v28), so the unfiltered
    // count(*) half of this test is exercised against pg_roles instead (still Rust-intercepted),
    // using a distinct query from the filtered pg_roles count below.
    let roles_all = catalog_query("SELECT count(*) FROM pg_catalog.pg_roles").await;

    assert_eq!(roles_all.columns, vec!["count"]);
    assert_eq!(roles_all.rows.len(), 1);
    assert_eq!(text_cell(&roles_all.rows[0], 0), "3");

    let roles =
        catalog_query("SELECT count(*) FROM pg_catalog.pg_roles WHERE rolcanlogin = 't'").await;

    assert_eq!(roles.columns, vec!["count"]);
    assert_eq!(roles.rows.len(), 1);
    assert_eq!(text_cell(&roles.rows[0], 0), "2");
}

#[tokio::test]
async fn test_pg_roles_unquoted_aliases_fold_to_lowercase_and_quoted_aliases_preserve_case() {
    let response = catalog_query(
        "SELECT rolname AS DisplayName, oid AS \"RoleOid\" FROM pg_roles",
    )
    .await;

    assert_eq!(response.columns, vec!["displayname", "RoleOid"]);
    assert_eq!(response.rows.len(), 3);
    assert_eq!(text_cell(&response.rows[0], 0), "postgres");
    assert_eq!(text_cell(&response.rows[0], 1), "10");
}

#[tokio::test]
async fn test_pg_namespace_cast_projection_uses_inner_source_column() {
    // pg_namespace is now served directly from SQLite (migration v28), so cast-projection
    // behavior is exercised against pg_roles instead, which remains Rust-intercepted.
    let response = catalog_query("SELECT CAST(oid AS text) AS o FROM pg_catalog.pg_roles").await;

    assert_eq!(response.columns, vec!["o"]);
    assert_eq!(response.rows.len(), 3);
    assert_eq!(text_cell(&response.rows[0], 0), "10");
    assert_eq!(text_cell(&response.rows[1], 0), "0");
    assert_eq!(text_cell(&response.rows[2], 0), "100");
}

#[tokio::test]
async fn test_pg_namespace_nested_projection_uses_inner_source_column() {
    // pg_namespace is now served directly from SQLite (migration v28), so nested-projection
    // behavior is exercised against pg_roles instead, which remains Rust-intercepted.
    let response = catalog_query("SELECT (oid) AS o FROM pg_catalog.pg_roles").await;

    assert_eq!(response.columns, vec!["o"]);
    assert_eq!(response.rows.len(), 3);
    assert_eq!(response.rows[0][0].as_deref(), Some(b"10".as_ref()));
}

#[tokio::test]
async fn test_pg_namespace_compound_identifier_projection_uses_leaf_column() {
    // pg_namespace is now served directly from SQLite (migration v28), so
    // compound-identifier projection is exercised against pg_roles instead, which remains
    // Rust-intercepted.
    let response = catalog_query("SELECT r.rolname FROM pg_catalog.pg_roles AS r").await;

    assert_eq!(response.columns, vec!["rolname"]);
    assert_eq!(response.rows.len(), 3);
    assert_eq!(text_cell(&response.rows[0], 0), "postgres");
    assert_eq!(text_cell(&response.rows[1], 0), "public");
    assert_eq!(text_cell(&response.rows[2], 0), "pgsqlite_user");
}

#[tokio::test]
async fn test_pg_roles_where_filter_with_alias_free_projection() {
    let response = catalog_query("SELECT rolname FROM pg_roles WHERE rolname = 'postgres'").await;

    assert_eq!(response.columns, vec!["rolname"]);
    assert_eq!(response.rows.len(), 1);
    assert_eq!(text_cell(&response.rows[0], 0), "postgres");
}

#[tokio::test]
async fn test_pg_namespace_unknown_source_column_projects_no_columns() {
    // pg_namespace is now served directly from SQLite (migration v28), so unknown-column
    // projection behavior is exercised against pg_roles instead, which remains
    // Rust-intercepted.
    let response = catalog_query("SELECT nonexistent FROM pg_catalog.pg_roles").await;

    assert!(response.columns.is_empty());
    assert_eq!(response.rows.len(), 3);
    assert!(response.rows.iter().all(Vec::is_empty));
}

#[tokio::test]
async fn test_information_schema_schemata_alias_projection() {
    let response = catalog_query("SELECT catalog_name AS x FROM information_schema.schemata").await;

    assert_eq!(response.columns, vec!["x"]);
    assert_eq!(response.rows.len(), 3);
    for row in &response.rows {
        assert_eq!(text_cell(row, 0), "main");
    }
}
