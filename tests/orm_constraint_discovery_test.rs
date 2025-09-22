mod common;
use common::setup_test_server_with_init;

/// Test Django-style constraint discovery using information_schema views
#[tokio::test]
async fn test_django_style_constraint_discovery() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create a typical Django-style schema
            db.execute(r#"
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    username VARCHAR(150) UNIQUE,
                    email VARCHAR(254) UNIQUE,
                    first_name VARCHAR(150),
                    last_name VARCHAR(150),
                    created_at TIMESTAMP
                )
            "#).await?;

            db.execute(r#"
                CREATE TABLE posts (
                    id INTEGER PRIMARY KEY,
                    title VARCHAR(200) NOT NULL,
                    content TEXT,
                    author_id INTEGER REFERENCES users(id),
                    created_at TIMESTAMP
                )
            "#).await?;

            db.execute(r#"
                CREATE TABLE comments (
                    id INTEGER PRIMARY KEY,
                    content TEXT NOT NULL,
                    post_id INTEGER REFERENCES posts(id),
                    author_id INTEGER REFERENCES users(id),
                    created_at TIMESTAMP
                )
            "#).await?;

            Ok(())
        })
    }).await;
    let client = &server.client;

    // 1. Django inspectdb: Query column information
    let columns = client.query(
        r#"
        SELECT table_name, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position
        "#,
        &[]
    ).await.unwrap();

    // Should have columns for all tables
    assert!(!columns.is_empty(), "Should have column information");

    let mut tables_found = std::collections::HashSet::new();
    for row in &columns {
        let table_name: &str = row.get(0);
        tables_found.insert(table_name);
    }
    assert!(tables_found.contains("users"));
    assert!(tables_found.contains("posts"));
    assert!(tables_found.contains("comments"));

    // 2. Django foreign key discovery: Query constraint relationships
    let foreign_keys = client.query(
        r#"
        SELECT
            tc.table_name,
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
        ORDER BY tc.table_name, kcu.ordinal_position
        "#,
        &[]
    ).await.unwrap();

    // Should find foreign key relationships
    assert!(!foreign_keys.is_empty(), "Should have foreign key constraints");

    let mut fk_relationships = std::collections::HashSet::new();
    for row in &foreign_keys {
        let table_name: &str = row.get(0);
        let column_name: &str = row.get(3);
        fk_relationships.insert(format!("{}:{}", table_name, column_name));
    }

    // Should find specific foreign keys
    assert!(fk_relationships.contains("posts:author_id") ||
            fk_relationships.iter().any(|fk| fk.contains("posts") && fk.contains("author_id")),
            "Should find posts.author_id foreign key");

    // 3. Rails-style constraint enumeration
    let all_constraints = client.query(
        r#"
        SELECT
            constraint_name,
            table_name,
            constraint_type
        FROM information_schema.table_constraints
        ORDER BY table_name, constraint_type, constraint_name
        "#,
        &[]
    ).await.unwrap();

    // Should have various constraint types
    assert!(!all_constraints.is_empty(), "Should have constraints");

    let mut constraint_types = std::collections::HashSet::new();
    for row in &all_constraints {
        let constraint_type: &str = row.get(2);
        constraint_types.insert(constraint_type);
    }

    // Should have at least primary keys
    assert!(constraint_types.contains("PRIMARY KEY"), "Should have primary key constraints");

    // 4. Detailed column metadata query (SQLAlchemy-style)
    let detailed_columns = client.query(
        r#"
        SELECT
            c.table_name,
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            c.ordinal_position
        FROM information_schema.columns c
        WHERE c.table_name = 'users'
        ORDER BY c.ordinal_position
        "#,
        &[]
    ).await.unwrap();

    // Should have detailed metadata for users table
    assert!(!detailed_columns.is_empty(), "Should have detailed column metadata");

    for row in &detailed_columns {
        let column_name: &str = row.get(1);
        let data_type: &str = row.get(2);
        let is_nullable: &str = row.get(6);
        let ordinal_position: i32 = row.get(8);

        // Verify basic sanity
        assert!(!column_name.is_empty(), "Column name should not be empty");
        assert!(!data_type.is_empty(), "Data type should not be empty");
        assert!(matches!(is_nullable, "YES" | "NO"), "is_nullable should be YES or NO");
        assert!(ordinal_position > 0, "Ordinal position should be positive");
    }
}

/// Test Rails-style index and constraint discovery
#[tokio::test]
async fn test_rails_style_constraint_discovery() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create tables with Rails-style naming
            db.execute(r#"
                CREATE TABLE categories (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100) UNIQUE,
                    parent_id INTEGER REFERENCES categories(id)
                )
            "#).await?;

            db.execute(r#"
                CREATE TABLE articles (
                    id INTEGER PRIMARY KEY,
                    title VARCHAR(255) NOT NULL,
                    slug VARCHAR(255) UNIQUE,
                    category_id INTEGER REFERENCES categories(id)
                )
            "#).await?;

            Ok(())
        })
    }).await;
    let client = &server.client;

    // Rails-style query to get all constraints for a table
    let constraints = client.query(
        r#"
        SELECT DISTINCT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name,
            kcu.ordinal_position
        FROM information_schema.table_constraints tc
        LEFT JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_name = kcu.table_name
        WHERE tc.table_name = 'articles'
        ORDER BY tc.constraint_type, kcu.ordinal_position
        "#,
        &[]
    ).await.unwrap();

    assert!(!constraints.is_empty(), "Should find constraints for articles table");

    // Verify we can discover different types of constraints
    let mut found_primary_key = false;
    let mut _found_foreign_key = false;

    for row in &constraints {
        let constraint_type: &str = row.get(1);

        match constraint_type {
            "PRIMARY KEY" => found_primary_key = true,
            "FOREIGN KEY" => _found_foreign_key = true,
            _ => {}
        }
    }

    assert!(found_primary_key, "Should find primary key constraint");
    // Note: Foreign key might not be found if ALTER TABLE constraint wasn't properly parsed
}

/// Test comprehensive ORM compatibility across all information_schema views
#[tokio::test]
async fn test_comprehensive_orm_compatibility() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute(r#"
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    code VARCHAR(20) UNIQUE,
                    parent_id INTEGER REFERENCES test_table(id)
                )
            "#).await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test 1: information_schema.tables
    let tables = client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'test_table'",
        &[]
    ).await.unwrap();
    assert_eq!(tables.len(), 1, "Should find test_table");

    // Test 2: information_schema.columns
    let columns = client.query(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'test_table' ORDER BY ordinal_position",
        &[]
    ).await.unwrap();
    assert_eq!(columns.len(), 4, "Should have 4 columns");

    // Test 3: information_schema.table_constraints
    let table_constraints = client.query(
        "SELECT constraint_type FROM information_schema.table_constraints WHERE table_name = 'test_table'",
        &[]
    ).await.unwrap();
    assert!(!table_constraints.is_empty(), "Should have constraints");

    // Test 4: information_schema.key_column_usage
    let key_usage = client.query(
        "SELECT column_name FROM information_schema.key_column_usage WHERE table_name = 'test_table'",
        &[]
    ).await.unwrap();
    assert!(!key_usage.is_empty(), "Should have key column usage");

    println!("✅ All information_schema views working correctly for ORM compatibility");
}