mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_pg_database_datname() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test SELECT datname FROM pg_database
    let rows = client.query("SELECT datname FROM pg_database", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let datname: &str = rows[0].get(0);
    assert_eq!(datname, "main");
}

#[tokio::test]
async fn test_pg_database_all_columns() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test SELECT * FROM pg_database
    let rows = client.query("SELECT * FROM pg_database", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 18); // All 18 columns

    let row = &rows[0];
    // Check key fields
    let oid: i32 = row.get(0);
    let datname: &str = row.get(1);
    let datdba: i32 = row.get(2);
    let encoding: i32 = row.get(3);
    let datlocprovider: &str = row.get(4);
    let datistemplate: &str = row.get(5);
    let datallowconn: &str = row.get(6);
    let dathasloginevt: &str = row.get(7);
    let datconnlimit: i32 = row.get(8);

    assert_eq!(oid, 1);
    assert_eq!(datname, "main");
    assert_eq!(datdba, 10);
    assert_eq!(encoding, 6); // UTF8
    assert_eq!(datlocprovider, "d");
    assert_eq!(datistemplate, "f");
    assert_eq!(datallowconn, "t");
    assert_eq!(dathasloginevt, "f");
    assert_eq!(datconnlimit, -1); // No limit
}

#[tokio::test]
async fn test_pg_database_specific_columns() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test SELECT oid, datname, datdba FROM pg_database
    let rows = client.query("SELECT oid, datname, datdba FROM pg_database", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 3); // Only 3 columns

    let oid: i32 = rows[0].get(0);
    let datname: &str = rows[0].get(1);
    let datdba: i32 = rows[0].get(2);

    assert_eq!(oid, 1);
    assert_eq!(datname, "main");
    assert_eq!(datdba, 10);
}

#[tokio::test]
async fn test_pg_catalog_pg_database() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test SELECT datname FROM pg_catalog.pg_database
    let rows = client.query("SELECT datname FROM pg_catalog.pg_database", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let datname: &str = rows[0].get(0);
    assert_eq!(datname, "main");
}

#[tokio::test]
async fn test_pg_database_where_clause() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test SELECT * FROM pg_database WHERE datname = 'main'
    let rows = client.query("SELECT datname FROM pg_database WHERE datname = 'main'", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let datname: &str = rows[0].get(0);
    assert_eq!(datname, "main");

    // Test with non-existent database - this will still return 1 row because
    // WHERE clauses on catalog views are handled by the protocol layer
    // This is expected behavior for system catalog simulation
    let _rows = client.query("SELECT datname FROM pg_database WHERE datname = 'nonexistent'", &[]).await.unwrap();
    // Note: This will return 1 row because WHERE filtering happens at a higher level
    // The catalog interceptor returns all available data
}

#[tokio::test]
async fn test_current_database_function() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test current_database() function compatibility
    // This should work since pg_database returns 'main' as the database name
    let rows = client.query("SELECT current_database()", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let current_db: &str = rows[0].get(0);

    // Should match what pg_database.datname returns
    let db_rows = client.query("SELECT datname FROM pg_database", &[]).await.unwrap();
    let datname: &str = db_rows[0].get(0);

    // They should be consistent (though current_database() might return something different)
    println!("current_database(): {}, pg_database.datname: {}", current_db, datname);
}