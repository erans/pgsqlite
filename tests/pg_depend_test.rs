mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_pg_depend_basic() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table with INTEGER PRIMARY KEY (acts like SERIAL)
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test basic pg_depend query
    let rows = client.query("SELECT classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype FROM pg_depend", &[]).await.unwrap();

    println!("Found {} dependencies", rows.len());
    for row in &rows {
        let classid: &str = row.get(0);
        let objid: &str = row.get(1);
        let objsubid: i32 = row.get(2);
        let refclassid: &str = row.get(3);
        let refobjid: &str = row.get(4);
        let refobjsubid: i32 = row.get(5);
        let deptype: &str = row.get(6);

        println!("  Dependency: classid={}, objid={}, objsubid={}, refclassid={}, refobjid={}, refobjsubid={}, deptype={}",
                 classid, objid, objsubid, refclassid, refobjid, refobjsubid, deptype);
    }

    // Should have at least one dependency for the SERIAL-like column
    assert!(rows.len() >= 1, "Should have at least 1 dependency for INTEGER PRIMARY KEY");

    // Check dependency properties
    let dep_row = &rows[0];
    let classid: &str = dep_row.get(0);
    let deptype: &str = dep_row.get(6);
    let objsubid: i32 = dep_row.get(2);
    let refobjsubid: i32 = dep_row.get(5);

    assert_eq!(classid, "1259", "classid should be pg_class OID (1259)");
    assert_eq!(deptype, "a", "deptype should be automatic (a)");
    assert_eq!(objsubid, 0, "objsubid should be 0 for sequences");
    assert_eq!(refobjsubid, 1, "refobjsubid should be 1 for first column");
}

#[tokio::test]
async fn test_pg_depend_rails_sequence_pattern() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create Rails-style table with SERIAL-like primary key
            db.execute("CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, author_id INTEGER)").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test the exact Rails sequence discovery query pattern (simplified)
    // Rails uses this to find sequences for primary key columns
    let query = "
        SELECT dep.classid, dep.objid, dep.objsubid, dep.refclassid, dep.refobjid, dep.refobjsubid, dep.deptype
        FROM pg_depend dep
        WHERE dep.refclassid = '1259'
        AND dep.refobjsubid = 1
        AND dep.deptype = 'a'
    ";

    let rows = client.query(query, &[]).await.unwrap();

    println!("Rails sequence discovery found {} dependencies", rows.len());
    for row in &rows {
        let classid: &str = row.get(0);
        let objid: &str = row.get(1);
        let objsubid: i32 = row.get(2);
        let refclassid: &str = row.get(3);
        let refobjid: &str = row.get(4);
        let refobjsubid: i32 = row.get(5);
        let deptype: &str = row.get(6);

        println!("  Sequence dependency: classid={}, objid={}, refclassid={}, refobjid={}, refobjsubid={}, deptype={}",
                 classid, objid, refclassid, refobjid, refobjsubid, deptype);

        // Validate Rails expectations
        assert_eq!(classid, "1259", "Sequence should be in pg_class");
        assert_eq!(refclassid, "1259", "Table should be in pg_class");
        assert_eq!(objsubid, 0, "Sequences have objsubid=0");
        assert_eq!(refobjsubid, 1, "First column should have refobjsubid=1");
        assert_eq!(deptype, "a", "Should be automatic dependency");
    }

    // Should find dependencies for INTEGER PRIMARY KEY columns
    assert!(!rows.is_empty(), "Should find at least one sequence dependency for Rails");
}

#[tokio::test]
async fn test_pg_depend_multiple_tables() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create multiple tables with SERIAL-like columns
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await?;
            db.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)").await?;
            db.execute("CREATE TABLE comments (id INTEGER PRIMARY KEY, post_id INTEGER, content TEXT)").await?;

            // Create a table without SERIAL (should not have dependencies)
            db.execute("CREATE TABLE categories (name TEXT PRIMARY KEY, description TEXT)").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Get all automatic dependencies
    let rows = client.query("SELECT * FROM pg_depend WHERE deptype = 'a'", &[]).await.unwrap();

    println!("Found {} automatic dependencies across multiple tables", rows.len());

    // Should have exactly 3 dependencies (one for each INTEGER PRIMARY KEY)
    assert_eq!(rows.len(), 3, "Should have 3 dependencies for 3 INTEGER PRIMARY KEY columns");

    // Check that all dependencies are for the first column of their respective tables
    for row in &rows {
        let refobjsubid: i32 = row.get(5);
        assert_eq!(refobjsubid, 1, "All dependencies should be for the first column (PRIMARY KEY)");
    }
}

#[tokio::test]
async fn test_pg_depend_non_serial_primary_key() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table with non-INTEGER primary key (should not create dependencies)
            db.execute("CREATE TABLE products (sku TEXT PRIMARY KEY, name TEXT, price REAL)").await?;

            // Create table with compound primary key (should not create dependencies)
            db.execute("CREATE TABLE order_items (order_id INTEGER, product_id INTEGER, quantity INTEGER, PRIMARY KEY (order_id, product_id))").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Check for dependencies
    let rows = client.query("SELECT * FROM pg_depend", &[]).await.unwrap();

    println!("Found {} dependencies for non-SERIAL tables", rows.len());

    // Should have no dependencies since no INTEGER PRIMARY KEY columns
    assert_eq!(rows.len(), 0, "Should have no dependencies for non-INTEGER PRIMARY KEY tables");
}

#[tokio::test]
async fn test_pg_depend_mixed_columns() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table with INTEGER PRIMARY KEY in different positions
            db.execute("CREATE TABLE mixed1 (name TEXT, id INTEGER PRIMARY KEY, email TEXT)").await?;
            db.execute("CREATE TABLE mixed2 (title TEXT, author TEXT, post_id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Get dependencies and check column positions
    let rows = client.query("SELECT refobjsubid FROM pg_depend WHERE deptype = 'a' ORDER BY refobjid", &[]).await.unwrap();

    println!("Found {} dependencies for mixed column positions", rows.len());
    assert_eq!(rows.len(), 2, "Should have 2 dependencies");

    // Collect column positions
    let positions: Vec<i32> = rows.iter().map(|row| {
        let pos: i32 = row.get(0);
        pos
    }).collect();

    println!("Column positions found: {:?}", positions);

    // Check that we have positions 2 and 3 (regardless of order)
    assert!(positions.contains(&2), "Should have dependency for column position 2 (mixed1.id)");
    assert!(positions.contains(&3), "Should have dependency for column position 3 (mixed2.post_id)");
}

#[tokio::test]
async fn test_pg_depend_wildcard_query() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create test table
            db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, data TEXT)").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    // Test wildcard SELECT
    let rows = client.query("SELECT * FROM pg_depend", &[]).await.unwrap();

    println!("Wildcard query returned {} dependencies", rows.len());
    assert!(rows.len() >= 1, "Should have at least one dependency");

    // Verify all 7 columns are returned
    let first_row = &rows[0];
    assert_eq!(first_row.len(), 7, "Should return all 7 pg_depend columns");
}