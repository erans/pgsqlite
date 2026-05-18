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
    let response = catalog_query("SELECT oid AS did FROM pg_catalog.pg_namespace").await;

    assert_eq!(response.columns, vec!["did"]);
    assert_eq!(response.rows.len(), 2);
    assert_eq!(text_cell(&response.rows[0], 0), "11");
    assert_eq!(text_cell(&response.rows[1], 0), "2200");
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
