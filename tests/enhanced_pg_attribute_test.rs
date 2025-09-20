mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_enhanced_pg_attribute_defaults() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table with various default types
            db.execute("CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name TEXT DEFAULT 'Unnamed Product',
                price REAL DEFAULT 0.0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                description TEXT
            )").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test default expression extraction
    let rows = client.query("
        SELECT attname, atthasdef, attidentity
        FROM pg_attribute
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'products')
        ORDER BY attnum
    ", &[]).await.unwrap();

    println!("Column defaults for 'products' table:");
    for row in &rows {
        let attname: String = row.get(0);
        let atthasdef: bool = row.get(1);
        let attidentity: String = row.get(2);

        println!("  {}: hasdef={}, identity='{}'",
                 attname, atthasdef, attidentity);

        match attname.as_str() {
            "id" => {
                assert!(!atthasdef, "id should not have explicit default");
                assert_eq!(attidentity, "d", "id should be identity column (SERIAL-like)");
            },
            "name" => {
                assert!(atthasdef, "name should have default");
            },
            "price" => {
                assert!(atthasdef, "price should have default");
            },
            "created_at" => {
                assert!(atthasdef, "created_at should have default");
            },
            "is_active" => {
                assert!(atthasdef, "is_active should have default");
            },
            "description" => {
                assert!(!atthasdef, "description should not have default");
            },
            _ => panic!("Unexpected column: {}", attname),
        }
    }

    assert_eq!(rows.len(), 6, "Should have 6 columns");
}

#[tokio::test]
async fn test_enhanced_pg_attribute_constraints() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table with various constraint combinations
            db.execute("CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                email TEXT NOT NULL UNIQUE,
                username TEXT NOT NULL DEFAULT 'anonymous',
                age INTEGER,
                bio TEXT DEFAULT NULL
            )").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test constraint detection
    let rows = client.query("
        SELECT attname, attnotnull, atthasdef, attidentity
        FROM pg_attribute
        WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 'users')
        ORDER BY attnum
    ", &[]).await.unwrap();

    println!("Column constraints for 'users' table:");
    for row in &rows {
        let attname: String = row.get(0);
        let attnotnull: bool = row.get(1);
        let atthasdef: bool = row.get(2);
        let attidentity: String = row.get(3);

        println!("  {}: notnull={}, hasdef={}, identity='{}'",
                 attname, attnotnull, atthasdef, attidentity);

        match attname.as_str() {
            "id" => {
                assert!(attnotnull, "id should be NOT NULL (PRIMARY KEY)");
                assert!(!atthasdef, "id should not have explicit default");
                assert_eq!(attidentity, "d", "id should be identity column");
            },
            "email" => {
                assert!(attnotnull, "email should be NOT NULL");
                assert!(!atthasdef, "email should not have default");
                assert_eq!(attidentity, "", "email should not be identity");
            },
            "username" => {
                assert!(attnotnull, "username should be NOT NULL");
                assert!(atthasdef, "username should have default");
            },
            "age" => {
                assert!(!attnotnull, "age should allow NULL");
                assert!(!atthasdef, "age should not have default");
            },
            "bio" => {
                assert!(!attnotnull, "bio should allow NULL");
                // Note: DEFAULT NULL might not be detected as having a default
                // This is expected behavior since NULL is the implicit default
            },
            _ => panic!("Unexpected column: {}", attname),
        }
    }

    assert_eq!(rows.len(), 5, "Should have 5 columns");
}

#[tokio::test]
async fn test_enhanced_pg_attribute_orm_compatibility() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table similar to what ORMs might generate
            db.execute("CREATE TABLE articles (
                id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                slug TEXT NOT NULL UNIQUE,
                published_at TEXT DEFAULT CURRENT_TIMESTAMP,
                view_count INTEGER DEFAULT 0,
                author_id INTEGER
            )").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test ORM-style query that checks for column metadata
    let rows = client.query("
        SELECT
            a.attname,
            a.attnotnull,
            a.atthasdef,
            a.attidentity,
            t.typname
        FROM pg_attribute a
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE a.attrelid = (SELECT oid FROM pg_class WHERE relname = 'articles')
        AND a.attnum > 0
        ORDER BY a.attnum
    ", &[]).await.unwrap();

    println!("ORM-style metadata for 'articles' table:");
    for row in &rows {
        let attname: String = row.get(0);
        let attnotnull: bool = row.get(1);
        let atthasdef: bool = row.get(2);
        let attidentity: String = row.get(3);
        let typname: String = row.get(4);

        println!("  {}: type={}, notnull={}, hasdef={}, identity='{}'",
                 attname, typname, attnotnull, atthasdef, attidentity);
    }

    // Verify that the enhanced metadata is accessible via standard PostgreSQL patterns
    assert!(rows.len() >= 6, "Should have at least 6 columns");

    // Check that we can find the identity column (common ORM pattern)
    let identity_cols: Vec<_> = rows.iter()
        .filter(|row| {
            let attidentity: String = row.get(3);
            !attidentity.is_empty()
        })
        .collect();

    assert_eq!(identity_cols.len(), 1, "Should have exactly one identity column");

    let id_row = identity_cols[0];
    let id_name: String = id_row.get(0);
    assert_eq!(id_name, "id", "Identity column should be 'id'");
}

#[tokio::test]
async fn test_enhanced_pg_attribute_with_joins() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table to test JOIN queries with pg_attribute
            db.execute("CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                customer_name TEXT NOT NULL DEFAULT 'Unknown',
                order_date TEXT DEFAULT CURRENT_TIMESTAMP,
                total_amount REAL DEFAULT 0.00
            )").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test complex JOIN query similar to what Django/Rails might use
    let rows = client.query("
        SELECT
            c.relname as table_name,
            a.attname as column_name,
            a.attnotnull as not_null,
            a.atthasdef as has_default,
            t.typname as data_type
        FROM pg_class c
        JOIN pg_attribute a ON c.oid = a.attrelid
        JOIN pg_type t ON a.atttypid = t.oid
        WHERE c.relname = 'orders'
        AND a.attnum > 0
        ORDER BY a.attnum
    ", &[]).await.unwrap();

    println!("JOIN query results for enhanced pg_attribute:");
    let mut found_defaults = 0;

    for row in &rows {
        let table_name: String = row.get(0);
        let column_name: String = row.get(1);
        let not_null: bool = row.get(2);
        let has_default: bool = row.get(3);
        let data_type: String = row.get(4);

        println!("  {}.{}: type={}, not_null={}, has_default={}",
                 table_name, column_name, data_type, not_null, has_default);

        if has_default {
            found_defaults += 1;
        }
    }

    assert_eq!(rows.len(), 4, "Should have 4 columns");
    assert!(found_defaults >= 2, "Should find at least 2 columns with defaults");
}