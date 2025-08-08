mod common;
use common::*;

/// Simple integration test for basic binary protocol functionality
#[tokio::test]
async fn test_basic_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create test table with fully supported binary types only
    client.execute(
        "CREATE TABLE basic_binary_test (
            id INTEGER PRIMARY KEY,
            bool_val BOOLEAN,
            int4_val INTEGER,
            int8_val BIGINT,
            float8_val DOUBLE PRECISION,
            text_val TEXT,
            bytea_val BYTEA,
            json_val TEXT
        )",
        &[]
    ).await.unwrap();
    
    // Test data
    let bytea_val = vec![1u8, 2, 3, 4, 5];
    let json_str = r#"{"test": "binary", "value": 42}"#;
    
    // Insert using prepared statement (uses binary protocol when beneficial)
    client.execute(
        "INSERT INTO basic_binary_test 
        (id, bool_val, int4_val, int8_val, float8_val, text_val, bytea_val, json_val) 
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        &[
            &1i32,
            &true,
            &12345i32,
            &9876543210i64,
            &3.14159f64,
            &"Binary Protocol Test",
            &bytea_val,
            &json_str
        ]
    ).await.unwrap();
    
    println!("✅ Inserted data using binary protocol");
    
    // Query data back
    let row = client.query_one(
        "SELECT * FROM basic_binary_test WHERE id = $1",
        &[&1i32]
    ).await.unwrap();
    
    // Verify data integrity
    let id: i32 = row.get("id");
    let bool_val: bool = row.get("bool_val");
    let int4_val: i32 = row.get("int4_val");
    let int8_val: i64 = row.get("int8_val");
    let float8_val: f64 = row.get("float8_val");
    let text_val: String = row.get("text_val");
    let bytea_result: Vec<u8> = row.get("bytea_val");
    let json_val: String = row.get("json_val");
    
    assert_eq!(id, 1);
    assert!(bool_val);
    assert_eq!(int4_val, 12345);
    assert_eq!(int8_val, 9876543210);
    assert!((float8_val - 3.14159).abs() < 0.00001);
    assert_eq!(text_val, "Binary Protocol Test");
    assert_eq!(bytea_result, bytea_val);
    assert_eq!(json_val, r#"{"test": "binary", "value": 42}"#);
    
    println!("✅ Data integrity verified:");
    println!("  Boolean: {bool_val}");
    println!("  Integer: {int4_val}");
    println!("  Bigint: {int8_val}");
    println!("  Float: {float8_val:.5}");
    println!("  Text: '{text_val}'");
    println!("  Bytea: {bytea_result:?}");
    println!("  JSON: {json_val}");
    
    server.abort();
    println!("🎉 Basic binary protocol test completed successfully!");
}

/// Test binary protocol with array types
#[tokio::test]
#[ignore] // Array types require proper binary encoding which is not yet implemented
async fn test_array_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create test table with array columns
    client.execute(
        "CREATE TABLE array_binary_test (
            id INTEGER PRIMARY KEY,
            int_array INTEGER[],
            text_array TEXT[],
            bool_array BOOLEAN[]
        )",
        &[]
    ).await.unwrap();
    
    // Insert array data (arrays are stored as JSON strings in pgsqlite)
    // Arrays must be passed as JSON strings
    let int_array_str = "[1, 2, 3, 4, 5]";
    let text_array_str = r#"["hello", "world", "test"]"#;
    let bool_array_str = "[true, false, true]";
    
    client.execute(
        "INSERT INTO array_binary_test (id, int_array, text_array, bool_array) VALUES ($1, $2, $3, $4)",
        &[
            &1i32,
            &int_array_str,
            &text_array_str,
            &bool_array_str
        ]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT * FROM array_binary_test WHERE id = $1",
        &[&1i32]
    ).await.unwrap();
    
    let int_array: String = row.get("int_array");
    let text_array: String = row.get("text_array");
    let bool_array: String = row.get("bool_array");
    
    assert_eq!(int_array, "[1, 2, 3, 4, 5]");
    assert_eq!(text_array, r#"["hello", "world", "test"]"#);
    assert_eq!(bool_array, "[true, false, true]");
    
    println!("✅ Array binary protocol test:");
    println!("  Int array: {int_array}");
    println!("  Text array: {text_array}");
    println!("  Bool array: {bool_array}");
    
    server.abort();
    println!("🎉 Array binary protocol test completed successfully!");
}

/// Test binary protocol with network types
#[tokio::test]
#[ignore] // Network types require proper binary encoding which is not yet implemented
async fn test_network_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create test table with network columns
    client.execute(
        "CREATE TABLE network_binary_test (
            id INTEGER PRIMARY KEY,
            inet_val INET,
            cidr_val CIDR,
            mac_val MACADDR
        )",
        &[]
    ).await.unwrap();
    
    // Insert network data (network types are stored as strings in pgsqlite)
    let inet_str = "192.168.1.1";
    let cidr_str = "192.168.1.0/24";
    let mac_str = "08:00:2b:01:02:03";
    
    client.execute(
        "INSERT INTO network_binary_test (id, inet_val, cidr_val, mac_val) VALUES ($1, $2, $3, $4)",
        &[
            &1i32,
            &inet_str,
            &cidr_str,
            &mac_str
        ]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT * FROM network_binary_test WHERE id = $1",
        &[&1i32]
    ).await.unwrap();
    
    let inet_val: String = row.get("inet_val");
    let cidr_val: String = row.get("cidr_val");
    let mac_val: String = row.get("mac_val");
    
    assert_eq!(inet_val, "192.168.1.1");
    assert_eq!(cidr_val, "192.168.1.0/24");
    assert_eq!(mac_val, "08:00:2b:01:02:03");
    
    println!("✅ Network binary protocol test:");
    println!("  INET: {inet_val}");
    println!("  CIDR: {cidr_val}");
    println!("  MACADDR: {mac_val}");
    
    server.abort();
    println!("🎉 Network binary protocol test completed successfully!");
}

/// Test binary protocol NULL handling
#[tokio::test]
async fn test_null_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create test table
    client.execute(
        "CREATE TABLE null_binary_test (
            id INTEGER PRIMARY KEY,
            nullable_text TEXT,
            nullable_int INTEGER,
            nullable_bool BOOLEAN
        )",
        &[]
    ).await.unwrap();
    
    // Insert NULL values
    client.execute(
        "INSERT INTO null_binary_test (id, nullable_text, nullable_int, nullable_bool) VALUES ($1, $2, $3, $4)",
        &[
            &1i32,
            &None::<String>,
            &Some(42i32),
            &None::<bool>
        ]
    ).await.unwrap();
    
    let row = client.query_one(
        "SELECT * FROM null_binary_test WHERE id = $1",
        &[&1i32]
    ).await.unwrap();
    
    // Check NULL handling
    assert!(row.try_get::<_, String>("nullable_text").is_err());
    let nullable_int: i32 = row.get("nullable_int");
    assert!(row.try_get::<_, bool>("nullable_bool").is_err());
    
    assert_eq!(nullable_int, 42);
    
    println!("✅ NULL binary protocol test:");
    println!("  nullable_text: NULL (correctly handled)");
    println!("  nullable_int: {nullable_int}");
    println!("  nullable_bool: NULL (correctly handled)");
    
    server.abort();
    println!("🎉 NULL binary protocol test completed successfully!");
}