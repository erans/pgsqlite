mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_pg_constraint_basic() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Enable foreign keys in SQLite
            db.execute("PRAGMA foreign_keys = ON").await?;

            // Create tables with constraints
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE)").await?;
            db.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT, FOREIGN KEY(user_id) REFERENCES users(id))").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test basic constraint query
    let rows = client.query("SELECT conname, contype FROM pg_constraint", &[]).await.unwrap();

    println!("Found {} constraints", rows.len());
    for row in &rows {
        let conname: &str = row.get(0);
        let contype: &str = row.get(1);
        println!("  Constraint: {} (type: {})", conname, contype);
    }

    // Should have at least primary key constraints
    assert!(rows.len() >= 2, "Should have at least 2 constraints (primary keys)");

    // Check constraint types
    let constraint_types: Vec<&str> = rows.iter()
        .map(|row| row.get::<_, &str>(1))
        .collect();

    assert!(constraint_types.contains(&"p"), "Should have primary key constraints");
}

#[tokio::test]
async fn test_pg_constraint_foreign_keys() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Enable foreign keys in SQLite
            db.execute("PRAGMA foreign_keys = ON").await?;

            // Create tables with foreign key
            db.execute("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)").await?;
            db.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, dept_id INTEGER, name TEXT, FOREIGN KEY(dept_id) REFERENCES departments(id))").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test foreign key discovery - common ORM query pattern
    let rows = client.query(
        "SELECT conname, contype, conrelid, confrelid, conkey, confkey FROM pg_constraint WHERE contype = 'f'",
        &[]
    ).await.unwrap();

    println!("Found {} foreign key constraints", rows.len());
    for row in &rows {
        let conname: &str = row.get(0);
        let contype: &str = row.get(1);
        let conrelid: &str = row.get(2);  // conrelid is TEXT in schema
        let confrelid: i32 = row.get(3);  // confrelid is INTEGER in schema
        let conkey: &str = row.get(4);
        let confkey: &str = row.get(5);

        println!("  FK: {} (type: {}, from: {} to: {}, columns: {} -> {})",
                 conname, contype, conrelid, confrelid, conkey, confkey);
    }

    // Should have at least one foreign key
    assert!(rows.len() >= 1, "Should have at least 1 foreign key constraint");

    // Check that it's actually a foreign key
    let fk_row = &rows[0];
    let contype: &str = fk_row.get(1);
    assert_eq!(contype, "f", "Should be foreign key type");

    // Check that referenced table is different from source table
    let conrelid: &str = fk_row.get(2);  // TEXT type
    let confrelid: i32 = fk_row.get(3);  // INTEGER type
    // Convert to same type for comparison
    let conrelid_int: i32 = conrelid.parse().unwrap_or(0);
    assert_ne!(conrelid_int, confrelid, "Source and target tables should be different");
}

#[tokio::test]
async fn test_pg_constraint_primary_keys() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Enable foreign keys in SQLite
            db.execute("PRAGMA foreign_keys = ON").await?;

            // Create table with primary key
            db.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test primary key discovery - common Rails/Django pattern
    let rows = client.query(
        "SELECT conname, contype, conkey FROM pg_constraint WHERE contype = 'p'",
        &[]
    ).await.unwrap();

    println!("Found {} primary key constraints", rows.len());
    for row in &rows {
        let conname: &str = row.get(0);
        let contype: &str = row.get(1);
        let conkey: &str = row.get(2);

        println!("  PK: {} (type: {}, columns: {})", conname, contype, conkey);
    }

    // Should have at least one primary key
    assert!(rows.len() >= 1, "Should have at least 1 primary key constraint");

    // Check that it's actually a primary key
    let pk_row = &rows[0];
    let contype: &str = pk_row.get(1);
    assert_eq!(contype, "p", "Should be primary key type");

    // Check constraint name pattern
    let conname: &str = pk_row.get(0);
    assert!(conname.contains("pkey"), "Primary key name should contain 'pkey'");
}

#[tokio::test]
async fn test_pg_constraint_sqlalchemy_pattern() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Enable foreign keys in SQLite
            db.execute("PRAGMA foreign_keys = ON").await?;

            // Create SQLAlchemy-style tables
            db.execute("CREATE TABLE account (id INTEGER PRIMARY KEY, name TEXT)").await?;
            db.execute("CREATE TABLE user_account (id INTEGER PRIMARY KEY, account_id INTEGER, FOREIGN KEY(account_id) REFERENCES account(id))").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // SQLAlchemy constraint discovery query pattern
    let rows = client.query(
        "SELECT conname, contype, condeferrable, condeferred, convalidated FROM pg_constraint ORDER BY conname",
        &[]
    ).await.unwrap();

    println!("Found {} constraints for SQLAlchemy pattern", rows.len());
    for row in &rows {
        let conname: &str = row.get(0);
        let contype: &str = row.get(1);
        let condeferrable: bool = row.get(2);  // BOOLEAN type
        let condeferred: bool = row.get(3);    // BOOLEAN type
        let convalidated: bool = row.get(4);   // BOOLEAN type

        println!("  Constraint: {} (type: {}, deferrable: {}, deferred: {}, validated: {})",
                 conname, contype, condeferrable, condeferred, convalidated);
    }

    // Should have at least 2 constraints (2 primary keys)
    assert!(rows.len() >= 2, "Should have at least 2 constraints");

    // Check that all constraints are validated
    for row in &rows {
        let convalidated: bool = row.get(4);  // BOOLEAN type
        assert_eq!(convalidated, true, "All constraints should be validated");
    }
}

#[tokio::test]
async fn test_pg_constraint_django_pattern() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Enable foreign keys in SQLite
            db.execute("PRAGMA foreign_keys = ON").await?;

            // Create Django-style tables
            db.execute("CREATE TABLE blog_category (id INTEGER PRIMARY KEY, name TEXT)").await?;
            db.execute("CREATE TABLE blog_post (id INTEGER PRIMARY KEY, category_id INTEGER, title TEXT, FOREIGN KEY(category_id) REFERENCES blog_category(id))").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Django foreign key discovery pattern
    let rows = client.query(
        "SELECT conname, contype, confupdtype, confdeltype, confmatchtype FROM pg_constraint WHERE contype = 'f'",
        &[]
    ).await.unwrap();

    println!("Found {} foreign keys for Django pattern", rows.len());
    for row in &rows {
        let conname: &str = row.get(0);
        let contype: &str = row.get(1);
        let confupdtype: &str = row.get(2);
        let confdeltype: &str = row.get(3);
        let confmatchtype: &str = row.get(4);

        println!("  FK: {} (type: {}, upd: {}, del: {}, match: {})",
                 conname, contype, confupdtype, confdeltype, confmatchtype);
    }

    // Should have at least one foreign key
    assert!(rows.len() >= 1, "Should have at least 1 foreign key");

    // Check FK action types (should be defaults)
    let fk_row = &rows[0];
    let confupdtype: &str = fk_row.get(2);
    let confdeltype: &str = fk_row.get(3);
    let confmatchtype: &str = fk_row.get(4);

    assert_eq!(confupdtype, "a", "Should default to NO ACTION (a) for updates");
    assert_eq!(confdeltype, "a", "Should default to NO ACTION (a) for deletes");
    assert_eq!(confmatchtype, "s", "Should default to SIMPLE (s) match");
}