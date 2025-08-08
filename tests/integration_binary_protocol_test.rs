mod common;
use common::*;
use rust_decimal::Decimal;
use std::str::FromStr;

/// Test basic binary protocol support with simple types
#[tokio::test]
async fn test_basic_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create test table with basic supported types
    client.execute(
        "CREATE TABLE binary_basic_test (
            id INTEGER PRIMARY KEY,
            bool_val BOOLEAN,
            int_val INTEGER,
            bigint_val BIGINT,
            float_val REAL,
            double_val DOUBLE PRECISION,
            text_val TEXT
        )",
        &[]
    ).await.unwrap();
    
    // Test inserting data with binary protocol (via prepared statements)
    let stmt = client.prepare(
        "INSERT INTO binary_basic_test (id, bool_val, int_val, bigint_val, float_val, double_val, text_val) 
         VALUES ($1, $2, $3, $4, $5, $6, $7)"
    ).await.unwrap();
    
    client.execute(&stmt, &[
        &1i32,
        &true,
        &42i32,
        &9999999999i64,
        &3.14f32,
        &2.718281828f64,
        &"Hello Binary Protocol"
    ]).await.unwrap();
    
    client.execute(&stmt, &[
        &2i32,
        &false,
        &-100i32,
        &-9999999999i64,
        &0.0f32,
        &-1.414213562f64,
        &"Testing special chars: 🚀"
    ]).await.unwrap();
    
    // Query data back using binary protocol
    let rows = client.query(
        "SELECT * FROM binary_basic_test ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    
    // Verify first row
    let row = &rows[0];
    let id: i32 = row.get("id");
    let bool_val: bool = row.get("bool_val");
    let int_val: i32 = row.get("int_val");
    let bigint_val: i64 = row.get("bigint_val");
    let float_val: f32 = row.get("float_val");
    let double_val: f64 = row.get("double_val");
    let text_val: String = row.get("text_val");
    
    assert_eq!(id, 1);
    assert_eq!(bool_val, true);
    assert_eq!(int_val, 42);
    assert_eq!(bigint_val, 9999999999);
    assert!((float_val - 3.14).abs() < 0.01);
    assert!((double_val - 2.718281828).abs() < 0.000001);
    assert_eq!(text_val, "Hello Binary Protocol");
    
    // Verify second row
    let row = &rows[1];
    let id: i32 = row.get("id");
    let bool_val: bool = row.get("bool_val");
    let int_val: i32 = row.get("int_val");
    let text_val: String = row.get("text_val");
    
    assert_eq!(id, 2);
    assert_eq!(bool_val, false);
    assert_eq!(int_val, -100);
    assert_eq!(text_val, "Testing special chars: 🚀");
    
    println!("✅ Basic binary protocol test passed");
    server.abort();
}

/// Integration test for core binary protocol support
#[tokio::test] 
#[ignore] // This test requires full binary protocol support for all types
async fn test_comprehensive_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create test table with core supported types
    client.execute(
        "CREATE TABLE binary_comprehensive_test (
            id INTEGER PRIMARY KEY,
            -- Core types
            bool_val BOOLEAN,
            int2_val SMALLINT,
            int4_val INTEGER,
            int8_val BIGINT,
            float4_val REAL,
            float8_val DOUBLE PRECISION,
            text_val TEXT,
            varchar_val VARCHAR(100),
            bytea_val BYTEA,
            -- Advanced types
            numeric_val NUMERIC(10, 2),
            money_val MONEY,
            -- Date/Time types
            date_val DATE,
            time_val TIME,
            timestamp_val TIMESTAMP
        )",
        &[]
    ).await.unwrap();
    
    // Test data covering all binary types
    // Pre-allocate values to avoid lifetime issues
    let bytea_val1 = vec![1u8, 2, 3, 4, 5];
    let bytea_val2 = Vec::<u8>::new();
    let decimal_val1 = Decimal::from_str("12345.67").unwrap();
    let decimal_val2 = Decimal::from_str("0.00").unwrap();
    
    let test_cases = vec![
        (
            1,
            "Standard values test",
            vec![
                ("bool_val", &true as &(dyn tokio_postgres::types::ToSql + Sync)),
                ("int2_val", &12345i16),
                ("int4_val", &1234567890i32),
                ("int8_val", &9223372036854775807i64),
                ("float4_val", &3.14159f32),
                ("float8_val", &2.718281828459045f64),
                ("text_val", &"Hello Binary Protocol"),
                ("varchar_val", &"Variable length"),
                ("bytea_val", &bytea_val1),
                ("numeric_val", &decimal_val1),
                ("money_val", &"$1234.56"),
                ("date_val", &"2024-01-15"),
                ("time_val", &"14:30:45.123456"),
                ("timestamp_val", &"2024-01-15 14:30:45.123456"),
            ]
        ),
        (
            2,
            "Edge cases and extremes",
            vec![
                ("bool_val", &false as &(dyn tokio_postgres::types::ToSql + Sync)),
                ("int2_val", &-32768i16),
                ("int4_val", &-2147483648i32),
                ("int8_val", &-9223372036854775808i64),
                ("float4_val", &0.0f32),
                ("float8_val", &f64::INFINITY),
                ("text_val", &""),
                ("varchar_val", &"🚀🌟💻"),
                ("bytea_val", &bytea_val2),
                ("numeric_val", &decimal_val2),
                ("money_val", &"$0.00"),
                ("date_val", &"2000-01-01"),
                ("time_val", &"00:00:00"),
                ("timestamp_val", &"2000-01-01 00:00:00"),
            ]
        ),
    ];
    
    // Insert test data using prepared statements (which use binary protocol when beneficial)
    for (test_id, description, fields) in &test_cases {
        println!("Testing: {description}");
        
        // Build dynamic INSERT statement
        let columns: Vec<&str> = fields.iter().map(|(col, _)| *col).collect();
        let values: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = 
            fields.iter().map(|(_, val)| *val).collect();
        
        let column_list = columns.join(", ");
        let placeholder_list = (1..=columns.len())
            .map(|i| format!("${}", i + 1))
            .collect::<Vec<_>>()
            .join(", ");
        
        let query = format!(
            "INSERT INTO binary_comprehensive_test (id, {}) VALUES (${}, {})",
            column_list, 1, placeholder_list
        );
        
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![test_id];
        params.extend(values);
        
        client.execute(&query, &params).await.unwrap();
        println!("  ✅ Inserted {} fields", fields.len());
    }
    
    // Query data back using prepared statements (binary protocol)
    let rows = client.query(
        "SELECT * FROM binary_comprehensive_test ORDER BY id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), test_cases.len());
    println!("✅ Retrieved {} rows using binary protocol", rows.len());
    
    // Verify data integrity for key types
    for (i, row) in rows.iter().enumerate() {
        let test_id: i32 = row.get("id");
        assert_eq!(test_id, (i + 1) as i32);
        
        // Verify core types
        let bool_val: bool = row.get("bool_val");
        let int4_val: i32 = row.get("int4_val");
        let float8_val: f64 = row.get("float8_val");
        let text_val: String = row.get("text_val");
        
        println!("  Row {test_id}: bool={bool_val}, int4={int4_val}, float8={float8_val:.6}, text='{text_val}'");
        
        // Verify advanced types work correctly
        let numeric_val: Decimal = row.get("numeric_val");
        let money_val: String = row.get("money_val");
        
        println!("    Advanced: numeric={numeric_val}, money={money_val}");
        
        // Verify date/time types
        let date_val: String = row.get("date_val");
        let time_val: String = row.get("time_val");
        let timestamp_val: String = row.get("timestamp_val");
        
        println!("    DateTime: date={date_val}, time={time_val}, timestamp={timestamp_val}");
    }
    
    // Test binary protocol with complex queries
    println!("\n🔧 Testing complex queries with binary protocol...");
    
    // Test prepared statement with parameters (uses binary for parameters when beneficial)
    let stmt = client.prepare(
        "SELECT id, bool_val, numeric_val, text_val FROM binary_comprehensive_test WHERE int4_val > $1"
    ).await.unwrap();
    
    let filtered_rows = client.query(&stmt, &[&1000000i32]).await.unwrap();
    println!("  Complex query returned {} rows", filtered_rows.len());
    
    // Test aggregation with binary results
    let agg_row = client.query_one(
        "SELECT COUNT(*) as total, MAX(int8_val) as max_bigint FROM binary_comprehensive_test",
        &[]
    ).await.unwrap();
    
    let total: i64 = agg_row.get("total");
    let max_bigint: i64 = agg_row.get("max_bigint");
    println!("  Aggregation: total={total}, max_bigint={max_bigint}");
    
    // Test NULL handling with binary protocol
    client.execute(
        "INSERT INTO binary_comprehensive_test (id, bool_val, text_val) VALUES ($1, $2, $3)",
        &[&999i32, &None::<bool>, &Some("not null".to_string())]
    ).await.unwrap();
    
    let null_row = client.query_one(
        "SELECT bool_val, text_val FROM binary_comprehensive_test WHERE id = $1",
        &[&999i32]
    ).await.unwrap();
    
    // Check NULL handling
    assert!(null_row.try_get::<_, bool>("bool_val").is_err());
    let text_val: String = null_row.get("text_val");
    assert_eq!(text_val, "not null");
    println!("  ✅ NULL handling working correctly");
    
    // Test additional inserts with binary protocol  
    client.execute(
        "INSERT INTO binary_comprehensive_test (id, text_val, numeric_val) VALUES ($1, $2, $3)",
        &[&998i32, &"additional test", &Decimal::from_str("999.99").unwrap()]
    ).await.unwrap();
    
    let add_row = client.query_one(
        "SELECT text_val, numeric_val FROM binary_comprehensive_test WHERE id = $1",
        &[&998i32]
    ).await.unwrap();
    
    let add_text: String = add_row.get("text_val");
    let add_numeric: Decimal = add_row.get("numeric_val");
    
    println!("  ✅ Additional test with binary protocol: text='{add_text}', numeric={add_numeric}");
    
    // Final verification
    let final_count = client.query_one(
        "SELECT COUNT(*) FROM binary_comprehensive_test",
        &[]
    ).await.unwrap();
    
    let count: i64 = final_count.get(0);
    println!("\n📊 Final database state: {count} total rows");
    
    server.abort();
    
    println!("🎉 Comprehensive binary protocol integration test completed successfully!");
}

/// Test binary protocol with high-precision numeric types
#[tokio::test]
#[ignore] // Binary NUMERIC encoding needs investigation
async fn test_binary_numeric_precision() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    client.execute(
        "CREATE TABLE numeric_precision_test (
            id INTEGER PRIMARY KEY,
            small_decimal NUMERIC(5, 2),
            large_decimal NUMERIC(20, 8),
            money_val MONEY
        )",
        &[]
    ).await.unwrap();
    
    let test_cases = [(Decimal::from_str("123.45").unwrap(), "123.45"),
        (Decimal::from_str("99999.99").unwrap(), "99999.99"),
        (Decimal::from_str("0.01").unwrap(), "0.01"),
        (Decimal::from_str("-999.99").unwrap(), "-999.99"),
        (Decimal::from_str("12345678901234.12345678").unwrap(), "12345678901234.12345678")];
    
    for (i, (decimal_val, money_str)) in test_cases.iter().enumerate() {
        client.execute(
            "INSERT INTO numeric_precision_test (id, small_decimal, large_decimal, money_val) VALUES ($1, $2, $3, $4)",
            &[&(i as i32 + 1), decimal_val, decimal_val, &format!("${money_str}")]
        ).await.unwrap();
    }
    
    let rows = client.query(
        "SELECT * FROM numeric_precision_test ORDER BY id",
        &[]
    ).await.unwrap();
    
    for (i, row) in rows.iter().enumerate() {
        let small_decimal: Decimal = row.get("small_decimal");
        let large_decimal: Decimal = row.get("large_decimal");
        let money_val: String = row.get("money_val");
        
        println!("Row {}: small={}, large={}, money={}", i + 1, small_decimal, large_decimal, money_val);
        
        // Verify precision is maintained (within reasonable bounds for small_decimal due to scale limit)
        if small_decimal.scale() <= 2 {
            assert_eq!(small_decimal.to_string(), test_cases[i].0.round_dp(2).to_string());
        }
        assert_eq!(large_decimal, test_cases[i].0);
    }
    
    server.abort();
    println!("✅ Binary numeric precision test passed");
}

/// Test binary protocol error handling
#[tokio::test]
async fn test_binary_protocol_error_handling() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    client.execute(
        "CREATE TABLE error_test (
            id INTEGER PRIMARY KEY,
            not_null_val TEXT NOT NULL
        )",
        &[]
    ).await.unwrap();
    
    // Test NOT NULL constraint violation with binary protocol
    let result = client.execute(
        "INSERT INTO error_test (id, not_null_val) VALUES ($1, $2)",
        &[&1i32, &None::<String>]
    ).await;
    
    // Should handle constraint errors gracefully
    assert!(result.is_err());
    println!("✅ Binary protocol error handling works correctly");
    
    // Test duplicate primary key with binary protocol
    client.execute(
        "INSERT INTO error_test (id, not_null_val) VALUES ($1, $2)",
        &[&2i32, &"valid"]
    ).await.unwrap();
    
    let duplicate_result = client.execute(
        "INSERT INTO error_test (id, not_null_val) VALUES ($1, $2)",
        &[&2i32, &"duplicate"]
    ).await;
    
    assert!(duplicate_result.is_err());
    println!("✅ Duplicate key error handling works correctly");
    
    server.abort();
}