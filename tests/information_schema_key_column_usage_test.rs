mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_information_schema_key_column_usage_basic() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create test tables with constraints
            db.execute(r#"
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) UNIQUE
                )
            "#).await?;

            db.execute(r#"
                CREATE TABLE orders (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    amount DECIMAL(10,2)
                )
            "#).await?;

            Ok(())
        })
    }).await;
    let client = &server.client;

    // Query information_schema.key_column_usage
    let rows = client.query(
        "SELECT constraint_name, table_name, column_name, ordinal_position FROM information_schema.key_column_usage ORDER BY table_name, constraint_name, ordinal_position",
        &[]
    ).await.unwrap();

    // Should have constraints for primary keys and foreign keys
    assert!(!rows.is_empty(), "Should have key column usage entries");

    let mut found_constraints = std::collections::HashSet::new();
    for row in &rows {
        let constraint_name: &str = row.get(0);
        let table_name: &str = row.get(1);
        let column_name: &str = row.get(2);
        let ordinal_position: i32 = row.get(3);

        found_constraints.insert(format!("{}:{}:{}", table_name, constraint_name, column_name));

        // Ordinal position should be >= 1
        assert!(ordinal_position >= 1, "Ordinal position should be 1-based");
    }

    // We should have at least primary key constraints
    assert!(found_constraints.len() >= 2, "Should have at least primary key constraints for both tables");
}

#[tokio::test]
async fn test_information_schema_key_column_usage_with_filter() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(r#"
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    code VARCHAR(10) UNIQUE,
                    description TEXT
                )
            "#).await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Query with table filter
    let rows = client.query(
        "SELECT constraint_name, table_name, column_name FROM information_schema.key_column_usage WHERE table_name = 'test_table'",
        &[]
    ).await.unwrap();

    // Should have constraints only for test_table
    for row in &rows {
        let table_name: &str = row.get(1);
        assert_eq!(table_name, "test_table");
    }

    // Should have at least primary key constraint
    assert!(!rows.is_empty(), "Should have key constraints for test_table");
}

#[tokio::test]
async fn test_information_schema_key_column_usage_wildcard() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(r#"
                CREATE TABLE simple_pk (
                    id INTEGER PRIMARY KEY,
                    name TEXT
                )
            "#).await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Query with wildcard
    let rows = client.query("SELECT * FROM information_schema.key_column_usage WHERE table_name = 'simple_pk'", &[]).await.unwrap();

    // Should return all 9 standard columns
    if !rows.is_empty() {
        assert_eq!(rows[0].len(), 9, "Should have all 9 standard columns");
    }
}