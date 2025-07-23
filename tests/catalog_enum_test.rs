mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_pg_attribute_enum_types() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type using simple query
    client.simple_query("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')")
        .await
        .expect("Failed to create ENUM type");
    
    // Create a table with an ENUM column using simple query
    client.simple_query("CREATE TABLE test_enum_table (id INTEGER PRIMARY KEY, current_status status)")
        .await
        .expect("Failed to create table with ENUM column");
    
    // Check if table was created
    let tables = client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r' AND relname = 'test_enum_table'",
        &[]
    ).await.unwrap();
    
    assert_eq!(tables.len(), 1, "Table test_enum_table should exist");
    eprintln!("Table created successfully");
    
    // Debug: Check the actual SQLite table structure
    let sqlite_info = client.query(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='test_enum_table'",
        &[]
    ).await.unwrap();
    
    if !sqlite_info.is_empty() {
        let sql: &str = sqlite_info[0].get(0);
        eprintln!("SQLite table definition: {sql}");
    }
    
    // Debug: Check what tables exist in pg_class
    let all_tables = client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relkind = 'r'",
        &[]
    ).await.unwrap();
    
    eprintln!("Tables in pg_class:");
    for row in &all_tables {
        let name: &str = row.get(0);
        eprintln!("  {name}");
    }
    
    // First get the table OID as text
    let table_oid_rows = client.query(
        "SELECT CAST(oid AS TEXT) FROM pg_catalog.pg_class WHERE relname = 'test_enum_table'",
        &[]
    ).await.unwrap();
    
    assert_eq!(table_oid_rows.len(), 1, "Should find test_enum_table in pg_class");
    let table_oid: &str = table_oid_rows[0].get(0);
    eprintln!("Table OID: {table_oid}");
    
    // Query pg_attribute to check the type OID
    eprintln!("Querying pg_attribute for table OID {table_oid} and column 'current_status'");
    let rows = client.query(
        &format!("SELECT attname, CAST(atttypid AS TEXT) FROM pg_catalog.pg_attribute WHERE attrelid = {table_oid} AND attname = 'current_status'"),
        &[]
    ).await.unwrap();
    eprintln!("pg_attribute query returned {} rows", rows.len());
    
    // Debug: Check all columns if we don't find the specific one
    if rows.is_empty() {
        let all_rows = client.query(
            &format!("SELECT attname, atttypid FROM pg_catalog.pg_attribute WHERE attrelid = {table_oid}"),
            &[]
        ).await.unwrap();
        
        eprintln!("Found {} columns for test_enum_table:", all_rows.len());
        for row in &all_rows {
            let name: &str = row.get(0);
            let typeid: &str = row.try_get(1).unwrap_or("ERROR");
            eprintln!("  Column: {name} (type OID: {typeid})");
        }
        
        // Debug: Check if pg_attribute returns anything at all
        let any_attrs = client.query(
            "SELECT COUNT(*) FROM pg_catalog.pg_attribute",
            &[]
        ).await.unwrap();
        
        if !any_attrs.is_empty() {
            let count: i64 = any_attrs[0].get(0);
            eprintln!("Total rows in pg_attribute: {count}");
        }
    }
    
    assert_eq!(rows.len(), 1, "Should find the current_status column");
    
    let attname: &str = rows[0].get(0);
    let atttypid_str: &str = rows[0].get(1);
    let atttypid: u32 = atttypid_str.parse().expect("Failed to parse atttypid as u32");
    
    assert_eq!(attname, "current_status");
    // The OID should be >= 10000 (our ENUM type OID offset)
    assert!(atttypid >= 10000, "ENUM type should have custom OID >= 10000, got {atttypid}");
    
    eprintln!("Looking for type with OID: {atttypid}");
    
    // Debug: Check what enum types exist
    let all_enum_types = client.query(
        "SELECT CAST(oid AS TEXT), typname FROM pg_catalog.pg_type WHERE typtype = 'e'",
        &[]
    ).await.unwrap();
    
    eprintln!("All enum types in pg_type:");
    for row in &all_enum_types {
        let oid_str: &str = row.get(0);
        let name: &str = row.get(1);
        eprintln!("  {oid_str} - {name}");
    }
    
    // Verify the type is in pg_type
    let atttypid_str = atttypid.to_string();
    let type_rows = client.query(
        "SELECT typname, typtype FROM pg_catalog.pg_type WHERE oid = $1",
        &[&atttypid_str]
    ).await.unwrap();
    
    assert_eq!(type_rows.len(), 1, "Should find the ENUM type in pg_type");
    
    let typname: &str = type_rows[0].get(0);
    let typtype: &str = type_rows[0].get(1);
    
    assert_eq!(typname, "status");
    assert_eq!(typtype, "e", "Type should be 'e' for enum");
    
    // Verify pg_enum has the values
    let enum_rows = client.query(
        "SELECT enumlabel FROM pg_catalog.pg_enum WHERE enumtypid = $1 ORDER BY enumsortorder",
        &[&atttypid_str]
    ).await.unwrap();
    
    assert_eq!(enum_rows.len(), 3, "Should find 3 enum values");
    
    let labels: Vec<String> = enum_rows.iter()
        .map(|row| row.get::<_, &str>(0).to_string())
        .collect();
    
    assert_eq!(labels, vec!["active", "inactive", "pending"]);
    
    server.abort();
}

#[tokio::test]
async fn test_pg_enum_filtering() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create multiple ENUM types using simple query
    client.simple_query("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
        .await
        .expect("Failed to create color ENUM");
    
    client.simple_query("CREATE TYPE size AS ENUM ('small', 'medium', 'large', 'extra_large')")
        .await
        .expect("Failed to create size ENUM");
    
    // Get the type OIDs as text
    let type_rows = client.query(
        "SELECT CAST(oid AS TEXT), typname FROM pg_catalog.pg_type WHERE typtype = 'e' ORDER BY typname",
        &[]
    ).await.unwrap();
    
    // Debug: Show all enum types found
    eprintln!("Found {} enum types in pg_type:", type_rows.len());
    for row in &type_rows {
        let oid: &str = row.get(0);
        let typname: &str = row.get(1);
        eprintln!("  ENUM type: {typname} (OID: {oid})");
    }
    
    // Check if our types are in there
    let type_names: Vec<String> = type_rows.iter()
        .map(|row| row.get::<_, &str>(1).to_string())
        .collect();
    
    assert!(type_names.contains(&"color".to_string()), "Should find color enum");
    assert!(type_names.contains(&"size".to_string()), "Should find size enum");
    
    // Filter to just our test types
    let our_types: Vec<_> = type_rows.iter()
        .filter(|row| {
            let name: &str = row.get(1);
            name == "color" || name == "size"
        })
        .collect();
    
    assert_eq!(our_types.len(), 2, "Should find 2 enum types");
    
    let color_oid: &str = if our_types[0].get::<_, &str>(1) == "color" {
        our_types[0].get(0)
    } else {
        our_types[1].get(0)
    };
    
    // Test filtering pg_enum by type OID
    let color_values = client.query(
        "SELECT enumlabel FROM pg_catalog.pg_enum WHERE enumtypid = $1 ORDER BY enumsortorder",
        &[&color_oid]
    ).await.unwrap();
    
    assert_eq!(color_values.len(), 3, "Should find 3 color values");
    
    let labels: Vec<String> = color_values.iter()
        .map(|row| row.get::<_, &str>(0).to_string())
        .collect();
    
    assert_eq!(labels, vec!["red", "green", "blue"]);
    
    server.abort();
}