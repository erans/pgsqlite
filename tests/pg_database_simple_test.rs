mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_pg_database_datname_basic() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test the most important query: SELECT datname FROM pg_database
    let rows = client.query("SELECT datname FROM pg_database", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let datname: &str = rows[0].get(0);
    assert_eq!(datname, "main");

    println!("✓ pg_database.datname query successful: {}", datname);
}

#[tokio::test]
async fn test_pg_database_essential_columns() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test essential columns that ORMs typically use
    let rows = client.query("SELECT oid, datname, datdba FROM pg_database", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    let oid: i32 = row.get(0);
    let datname: &str = row.get(1);
    let datdba: i32 = row.get(2);

    assert_eq!(oid, 1);
    assert_eq!(datname, "main");
    assert_eq!(datdba, 10);

    println!("✓ Essential pg_database columns: oid={}, datname={}, datdba={}", oid, datname, datdba);
}

#[tokio::test]
async fn test_pg_catalog_pg_database() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test fully qualified name
    let rows = client.query("SELECT datname FROM pg_catalog.pg_database", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let datname: &str = rows[0].get(0);
    assert_eq!(datname, "main");

    println!("✓ pg_catalog.pg_database query successful: {}", datname);
}

#[tokio::test]
async fn test_database_existence_check() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Common pattern: check if database exists
    let rows = client.query("SELECT 1 FROM pg_database WHERE datname = 'main'", &[]).await.unwrap();

    // This should return at least one row since 'main' database exists
    assert!(!rows.is_empty());

    println!("✓ Database existence check successful");
}

#[tokio::test]
async fn test_sqlalchemy_compatibility() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // SQLAlchemy often runs this type of query
    let rows = client.query(
        "SELECT datname, datcollate, datctype FROM pg_database ORDER BY datname",
        &[]
    ).await.unwrap();

    assert_eq!(rows.len(), 1);
    let datname: &str = rows[0].get(0);
    let datcollate: &str = rows[0].get(1);
    let datctype: &str = rows[0].get(2);

    assert_eq!(datname, "main");
    assert_eq!(datcollate, "en_US.UTF-8");
    assert_eq!(datctype, "en_US.UTF-8");

    println!("✓ SQLAlchemy-style query successful: {} ({}, {})", datname, datcollate, datctype);
}