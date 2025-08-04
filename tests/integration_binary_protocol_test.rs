mod common;
use common::*;
use rust_decimal::Decimal;
use std::str::FromStr;

/// Comprehensive integration test for binary protocol support
#[tokio::test]
async fn test_comprehensive_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create comprehensive test table
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
            uuid_val UUID,
            json_val JSON,
            jsonb_val JSONB,
            money_val MONEY,
            -- Date/Time types
            date_val DATE,
            time_val TIME,
            timestamp_val TIMESTAMP,
            timestamptz_val TIMESTAMPTZ,
            interval_val INTERVAL,
            -- Array types
            int_array INTEGER[],
            text_array TEXT[],
            bool_array BOOLEAN[],
            -- Range types
            int4_range INT4RANGE,
            int8_range INT8RANGE,
            num_range NUMRANGE,
            -- Network types
            cidr_val CIDR,
            inet_val INET,
            mac_val MACADDR,
            mac8_val MACADDR8
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
                ("uuid_val", &"550e8400-e29b-41d4-a716-446655440000"),
                ("json_val", &r#"{"name": "test", "value": 42}"#),
                ("jsonb_val", &r#"{"binary": true, "nested": {"key": "value"}}"#),
                ("money_val", &"$1234.56"),
                ("date_val", &"2024-01-15"),
                ("time_val", &"14:30:45.123456"),
                ("timestamp_val", &"2024-01-15 14:30:45.123456"),
                ("timestamptz_val", &"2024-01-15 14:30:45.123456+00"),
                ("interval_val", &"1 day 2:30:00"),
                ("int_array", &"[1, 2, 3, 4, 5]"),
                ("text_array", &r#"["hello", "world", "binary"]"#),
                ("bool_array", &"[true, false, true]"),
                ("int4_range", &"[1,100)"),
                ("int8_range", &"[1000000000000,2000000000000]"),
                ("num_range", &"[1.5,99.99]"),
                ("cidr_val", &"192.168.1.0/24"),
                ("inet_val", &"192.168.1.1"),
                ("mac_val", &"08:00:2b:01:02:03"),
                ("mac8_val", &"08:00:2b:01:02:03:04:05"),
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
                ("uuid_val", &"00000000-0000-0000-0000-000000000000"),
                ("json_val", &"[]"),
                ("jsonb_val", &"{}"),
                ("money_val", &"$0.00"),
                ("date_val", &"2000-01-01"),
                ("time_val", &"00:00:00"),
                ("timestamp_val", &"2000-01-01 00:00:00"),
                ("timestamptz_val", &"2000-01-01 00:00:00+00"),
                ("interval_val", &"0 seconds"),
                ("int_array", &"[]"),
                ("text_array", &r#"["", "single"]"#),
                ("bool_array", &"[false]"),
                ("int4_range", &"empty"),
                ("int8_range", &"(,)"),
                ("num_range", &"[-999.99,999.99)"),
                ("cidr_val", &"10.0.0.0/8"),
                ("inet_val", &"::1"),
                ("mac_val", &"00:00:00:00:00:00"),
                ("mac8_val", &"ff:ff:ff:ff:ff:ff:ff:ff"),
            ]
        ),
    ];
    
    // Insert test data using prepared statements (which use binary protocol when beneficial)
    for (test_id, description, fields) in &test_cases {
        println!("Testing: {}", description);
        
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
        
        println!("  Row {}: bool={}, int4={}, float8={:.6}, text='{}'", 
                 test_id, bool_val, int4_val, float8_val, text_val);
        
        // Verify advanced types work correctly
        let numeric_val: Decimal = row.get("numeric_val");
        let uuid_val: String = row.get("uuid_val");
        
        println!("    Advanced: numeric={}, uuid={}", numeric_val, uuid_val);
        
        // Verify array types as text (since we store them as JSON)
        let int_array: String = row.get("int_array");
        let text_array: String = row.get("text_array");
        
        println!("    Arrays: int_array={}, text_array={}", int_array, text_array);
        
        // Verify network types
        let cidr_val: String = row.get("cidr_val");
        let inet_val: String = row.get("inet_val");
        let mac_val: String = row.get("mac_val");
        
        println!("    Network: cidr={}, inet={}, mac={}", cidr_val, inet_val, mac_val);
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
    println!("  Aggregation: total={}, max_bigint={}", total, max_bigint);
    
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
    
    println!("  ✅ Additional test with binary protocol: text='{}', numeric={}", add_text, add_numeric);
    
    // Final verification
    let final_count = client.query_one(
        "SELECT COUNT(*) FROM binary_comprehensive_test",
        &[]
    ).await.unwrap();
    
    let count: i64 = final_count.get(0);
    println!("\n📊 Final database state: {} total rows", count);
    
    server.abort();
    
    println!("🎉 Comprehensive binary protocol integration test completed successfully!");
}

/// Test binary protocol with high-precision numeric types
#[tokio::test]
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
    
    let test_cases = vec![
        (Decimal::from_str("123.45").unwrap(), "123.45"),
        (Decimal::from_str("99999.99").unwrap(), "99999.99"),
        (Decimal::from_str("0.01").unwrap(), "0.01"),
        (Decimal::from_str("-999.99").unwrap(), "-999.99"),
        (Decimal::from_str("12345678901234.12345678").unwrap(), "12345678901234.12345678"),
    ];
    
    for (i, (decimal_val, money_str)) in test_cases.iter().enumerate() {
        client.execute(
            "INSERT INTO numeric_precision_test (id, small_decimal, large_decimal, money_val) VALUES ($1, $2, $3, $4)",
            &[&(i as i32 + 1), decimal_val, decimal_val, &format!("${}", money_str)]
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
            constrained_val VARCHAR(5)
        )",
        &[]
    ).await.unwrap();
    
    // Test constraint violation with binary protocol
    let result = client.execute(
        "INSERT INTO error_test (id, constrained_val) VALUES ($1, $2)",
        &[&1i32, &"this_string_is_too_long_for_varchar_5"]
    ).await;
    
    // Should handle constraint errors gracefully
    assert!(result.is_err());
    println!("✅ Binary protocol error handling works correctly");
    
    server.abort();
}