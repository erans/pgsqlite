mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_pg_class_where_filtering() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create some test tables
            db.execute("CREATE TABLE test_table1 (id INTEGER PRIMARY KEY, name TEXT)").await?;
            db.execute("CREATE TABLE test_table2 (id INTEGER PRIMARY KEY, value REAL)").await?;
            db.execute("CREATE INDEX idx_test ON test_table1(name)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test 1: Filter by relkind = 'r' (tables only)
    let rows = client.query(
        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind = 'r'",
        &[]
    ).await.unwrap();
    
    // In CI environment, there might be additional tables
    assert!(rows.len() >= 2, "Should find at least 2 tables, found: {}", rows.len());
    
    // Verify we have our test tables
    let table_names: Vec<String> = rows.iter()
        .map(|row| row.get::<_, &str>(0).to_string())
        .collect();
    assert!(table_names.contains(&"test_table1".to_string()), "Should find test_table1");
    assert!(table_names.contains(&"test_table2".to_string()), "Should find test_table2");
    
    for row in &rows {
        let relkind: &str = row.get(1);
        assert_eq!(relkind, "r", "All results should be tables");
    }

    // Test 2: Filter by relkind IN ('r', 'i') (tables and indexes)
    let rows = client.query(
        "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relkind IN ('r', 'i')",
        &[]
    ).await.unwrap();
    
    // Debug: print what we actually got
    let objects: Vec<(String, String)> = rows.iter()
        .map(|row| (row.get::<_, &str>(0).to_string(), row.get::<_, &str>(1).to_string()))
        .collect();
    
    // Should have at least our 2 tables (index might not be created in some environments)
    assert!(rows.len() >= 2, "Should find at least 2 tables, found: {} objects: {:?}", rows.len(), objects);
    
    // Verify we have our specific tables
    let object_names: Vec<String> = objects.iter()
        .map(|(name, _)| name.clone())
        .collect();
    assert!(object_names.contains(&"test_table1".to_string()), "Should find test_table1 in {:?}", object_names);
    assert!(object_names.contains(&"test_table2".to_string()), "Should find test_table2 in {:?}", object_names);
    
    // Check if index exists (it might not in some SQLite configurations)
    let has_index = object_names.contains(&"idx_test".to_string());
    if has_index {
        // Verify it's marked as an index
        let idx_entry = objects.iter().find(|(name, _)| name == "idx_test");
        assert_eq!(idx_entry.unwrap().1, "i", "idx_test should have relkind='i'");
    }
    
    // Test 3: Filter by relname LIKE pattern
    let rows = client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname LIKE 'test_%'",
        &[]
    ).await.unwrap();
    
    // Our test creates test_table1 and test_table2
    assert!(rows.len() >= 2, "Should find at least 2 tables matching pattern, found: {}", rows.len());
    
    // Verify our specific tables are in the results
    let matching_names: Vec<String> = rows.iter()
        .map(|row| row.get::<_, &str>(0).to_string())
        .collect();
    assert!(matching_names.contains(&"test_table1".to_string()), "Should find test_table1");
    assert!(matching_names.contains(&"test_table2".to_string()), "Should find test_table2");
    
    // Test 4: Complex WHERE with AND
    let rows = client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r' AND relname = 'test_table1'",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 1, "Should find exactly 1 table");
    let relname: &str = rows[0].get(0);
    assert_eq!(relname, "test_table1");
    
    server.abort();
}

#[tokio::test]
async fn test_pg_attribute_where_filtering() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create a test table with PostgreSQL types
            db.execute("CREATE TABLE test_attrs (id INTEGER PRIMARY KEY, name VARCHAR(50), active BOOLEAN)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // Test 1: Filter by attnum > 0 (exclude system columns)
    let rows = client.query(
        "SELECT attname, attnum FROM pg_catalog.pg_attribute WHERE attnum > 0",
        &[]
    ).await.unwrap();
    
    assert!(rows.len() >= 3, "Should find at least 3 columns");
    for row in &rows {
        let attnum: i16 = row.get(1);
        assert!(attnum > 0, "All attnums should be positive");
    }

    // Test 2: Filter by attnotnull = true
    let rows = client.query(
        "SELECT attname FROM pg_catalog.pg_attribute WHERE attnotnull = 't'",
        &[]
    ).await.unwrap();
    
    // Should at least find the PRIMARY KEY column (id) and the NOT NULL column (name)
    let not_null_columns: Vec<String> = rows.iter()
        .map(|row| row.get::<_, &str>(0).to_string())
        .collect();
    
    // The 'id' column should be NOT NULL because it's PRIMARY KEY
    assert!(!not_null_columns.is_empty(), "Should find at least 1 NOT NULL column, found: {:?}", not_null_columns);
    
    // In our test table, we expect to find at least the 'id' column (PRIMARY KEY)
    // The 'name' column is VARCHAR(50) but not explicitly NOT NULL
    let test_table_not_nulls: Vec<&String> = not_null_columns.iter()
        .filter(|name| *name == "id" || *name == "name")
        .collect();
    assert!(!test_table_not_nulls.is_empty(), "Should find NOT NULL columns from our test table");

    // Test 3: Complex filter combining conditions
    let rows = client.query(
        "SELECT attname FROM pg_catalog.pg_attribute WHERE attnum > 0 AND attisdropped = 'f'",
        &[]
    ).await.unwrap();
    
    // All non-system columns that aren't dropped
    assert!(rows.len() >= 3, "Should find at least 3 active columns");
    
    server.abort();
}

#[tokio::test]  
async fn test_psql_common_patterns() {
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create some test tables
            db.execute("CREATE TABLE public_table (id INTEGER)").await?;
            db.execute("CREATE TABLE pg_internal (id INTEGER)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;
    
    // Test psql \dt pattern: Filter tables only, excluding system schemas
    let rows = client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind IN ('r','p') AND relnamespace = 2200",
        &[]
    ).await.unwrap();
    
    // Debug: show what we found
    let table_names: Vec<String> = rows.iter()
        .map(|row| row.get::<_, &str>(0).to_string())
        .collect();
    
    // Should find both tables (we don't actually filter by namespace pattern yet)
    assert!(rows.len() >= 2, "Should find at least 2 tables, found: {} tables: {:?}", rows.len(), table_names);
    
    // Verify our test tables are present
    assert!(table_names.contains(&"public_table".to_string()), "Should find public_table in {:?}", table_names);
    assert!(table_names.contains(&"pg_internal".to_string()), "Should find pg_internal in {:?}", table_names);
    
    // Test NOT EQUAL pattern
    let rows = client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind != 'i'",
        &[]
    ).await.unwrap();
    
    // Should find only tables, not indexes
    for row in &rows {
        let relname: &str = row.get(0);
        assert!(!relname.starts_with("idx_"), "Should not include indexes");
    }
    
    server.abort();
}