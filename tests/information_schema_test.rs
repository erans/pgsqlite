mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_information_schema_schemata() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test SELECT * FROM information_schema.schemata
    let rows = client.query("SELECT * FROM information_schema.schemata", &[]).await.unwrap();

    // Should return 3 schemas: public, pg_catalog, information_schema
    assert_eq!(rows.len(), 3);

    // Check that we have the expected schemas
    let schema_names: Vec<&str> = rows.iter()
        .map(|row| row.get::<_, &str>(1)) // schema_name is column 1
        .collect();
    assert!(schema_names.contains(&"public"));
    assert!(schema_names.contains(&"pg_catalog"));
    assert!(schema_names.contains(&"information_schema"));
}

#[tokio::test]
async fn test_information_schema_schemata_specific_columns() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test SELECT schema_name FROM information_schema.schemata
    let rows = client.query("SELECT schema_name FROM information_schema.schemata", &[]).await.unwrap();

    assert_eq!(rows.len(), 3);
    // Check that we have the expected schemas
    let schema_names: Vec<&str> = rows.iter()
        .map(|row| row.get::<_, &str>(0)) // schema_name is the only column
        .collect();
    assert!(schema_names.contains(&"public"));
    assert!(schema_names.contains(&"pg_catalog"));
    assert!(schema_names.contains(&"information_schema"));
}

#[tokio::test]
async fn test_information_schema_tables() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = common::setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create a test table and view
            db.execute("CREATE TABLE test_table (id INTEGER, name TEXT)").await?;
            db.execute("CREATE VIEW test_view AS SELECT id FROM test_table").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test SELECT table_name, table_type FROM information_schema.tables
    let rows = client.query("SELECT table_name, table_type FROM information_schema.tables", &[]).await.unwrap();

    // Should return at least our test table and view
    assert!(rows.len() >= 2);

    // Check that our table and view are present with correct types
    let table_info: Vec<(&str, &str)> = rows.iter()
        .map(|row| (row.get::<_, &str>(0), row.get::<_, &str>(1)))
        .collect();

    assert!(table_info.contains(&("test_table", "BASE TABLE")));
    assert!(table_info.contains(&("test_view", "VIEW")));
}

#[tokio::test]
async fn test_information_schema_tables_all_columns() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = common::setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test_table (id INTEGER)").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test SELECT * FROM information_schema.tables WHERE table_name = 'test_table'
    let rows = client.query("SELECT * FROM information_schema.tables WHERE table_name = 'test_table'", &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].len(), 12); // All 12 columns

    let row = &rows[0];
    // Check specific values
    assert_eq!(row.get::<_, &str>(0), "main"); // table_catalog
    assert_eq!(row.get::<_, &str>(1), "public"); // table_schema
    assert_eq!(row.get::<_, &str>(2), "test_table"); // table_name
    assert_eq!(row.get::<_, &str>(3), "BASE TABLE"); // table_type
    assert_eq!(row.get::<_, &str>(9), "YES"); // is_insertable_into
    assert_eq!(row.get::<_, &str>(10), "NO"); // is_typed
}

#[tokio::test]
async fn test_information_schema_tables_view_insertable() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = common::setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test_table (id INTEGER)").await?;
            db.execute("CREATE VIEW test_view AS SELECT id FROM test_table").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test that views are marked as not insertable
    let rows = client.query(
        "SELECT table_name, table_type, is_insertable_into FROM information_schema.tables WHERE table_name IN ('test_table', 'test_view')",
        &[]
    ).await.unwrap();

    assert_eq!(rows.len(), 2);

    for row in &rows {
        let table_name: &str = row.get(0);
        let table_type: &str = row.get(1);
        let is_insertable: &str = row.get(2);

        if table_name == "test_table" {
            assert_eq!(table_type, "BASE TABLE");
            assert_eq!(is_insertable, "YES");
        } else if table_name == "test_view" {
            assert_eq!(table_type, "VIEW");
            assert_eq!(is_insertable, "NO");
        }
    }
}