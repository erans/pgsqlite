mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_information_schema_table_constraints_basic() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create test tables with various constraints
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

    // Query information_schema.table_constraints
    let rows = client.query(
        "SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints ORDER BY table_name, constraint_name",
        &[]
    ).await.unwrap();

    // Should have constraints for primary keys and foreign keys
    assert!(!rows.is_empty(), "Should have constraint entries");

    let mut found_constraints = std::collections::HashSet::new();
    for row in &rows {
        let constraint_name: &str = row.get(0);
        let table_name: &str = row.get(1);
        let constraint_type: &str = row.get(2);

        found_constraints.insert(format!("{}:{}:{}", table_name, constraint_name, constraint_type));

        // Constraint type should be one of the standard types
        assert!(
            matches!(constraint_type, "PRIMARY KEY" | "UNIQUE" | "FOREIGN KEY" | "CHECK"),
            "Invalid constraint type: {}", constraint_type
        );
    }

    // We should have at least primary key constraints
    assert!(found_constraints.len() >= 2, "Should have at least primary key constraints for both tables");
}

#[tokio::test]
async fn test_information_schema_table_constraints_with_filter() {
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
        "SELECT constraint_name, table_name, constraint_type FROM information_schema.table_constraints WHERE table_name = 'test_table'",
        &[]
    ).await.unwrap();

    // Should have constraints only for test_table
    for row in &rows {
        let table_name: &str = row.get(1);
        assert_eq!(table_name, "test_table");
    }

    // Should have at least primary key constraint
    assert!(!rows.is_empty(), "Should have constraints for test_table");
}

#[tokio::test]
async fn test_information_schema_table_constraints_wildcard() {
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
    let rows = client.query("SELECT * FROM information_schema.table_constraints WHERE table_name = 'simple_pk'", &[]).await.unwrap();

    // Should return all 11 standard columns
    if !rows.is_empty() {
        assert_eq!(rows[0].len(), 11, "Should have all 11 standard columns");
    }
}

#[tokio::test]
async fn test_information_schema_table_constraints_types() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(r#"
                CREATE TABLE constraint_test (
                    id INTEGER PRIMARY KEY,
                    unique_col VARCHAR(50) UNIQUE,
                    parent_id INTEGER REFERENCES constraint_test(id)
                )
            "#).await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Query for different constraint types
    let rows = client.query(
        "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = 'constraint_test' ORDER BY constraint_type",
        &[]
    ).await.unwrap();

    let constraint_types: Vec<&str> = rows.iter()
        .map(|row| row.get::<_, &str>(0))
        .collect();

    // Should have at least PRIMARY KEY constraint
    assert!(constraint_types.contains(&"PRIMARY KEY"), "Should have PRIMARY KEY constraint");

    // May have other constraint types depending on how they're parsed
    for constraint_type in constraint_types {
        assert!(
            matches!(constraint_type, "PRIMARY KEY" | "UNIQUE" | "FOREIGN KEY" | "CHECK"),
            "Invalid constraint type: {}", constraint_type
        );
    }
}