mod common;
use common::*;

#[tokio::test]
async fn test_datetime_types_stored_as_integer() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with all datetime type variants
    client.execute(
        "CREATE TABLE datetime_storage_test (
            id INTEGER PRIMARY KEY,
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP,
            timestamptz_col TIMESTAMPTZ,
            timetz_col TIMETZ,
            interval_col INTERVAL,
            datetime_col DATETIME
        )",
        &[]
    ).await.unwrap();
    
    // Use direct SQLite query to check storage types via PRAGMA table_info
    let rows = client.query(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='main.datetime_storage_test'",
        &[]
    ).await.unwrap();
    
    // First, let's see what the actual CREATE TABLE statement looks like
    if !rows.is_empty() {
        let sql: String = rows[0].get(0);
        println!("Actual CREATE TABLE SQL: {}", sql);
    }
    
    // Now check the actual column types using PRAGMA
    let pragma_rows = client.query(
        "SELECT cid, name, type FROM pragma_table_info('datetime_storage_test') ORDER BY cid",
        &[]
    ).await.unwrap();
    
    println!("PRAGMA table_info results:");
    for row in &pragma_rows {
        let cid: i32 = row.get(0);
        let name: String = row.get(1);
        let sqlite_type: String = row.get(2);
        println!("  Column {}: {} -> {}", cid, name, sqlite_type);
        
        // All datetime columns should be stored as INTEGER
        if name != "id" {
            assert_eq!(sqlite_type, "INTEGER", 
                "Column {} should be stored as INTEGER, but got {}", name, sqlite_type);
        }
    }
    
    // Verify we have the expected number of columns
    assert_eq!(pragma_rows.len(), 8, "Should have 8 columns total");
    
    server.abort();
}

#[tokio::test]
async fn test_datetime_type_aliases_stored_as_integer() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test various type aliases and case variations
    client.execute(
        "CREATE TABLE datetime_aliases_test (
            id INTEGER PRIMARY KEY,
            ts_with_tz TIMESTAMP WITH TIME ZONE,
            ts_without_tz TIMESTAMP WITHOUT TIME ZONE,
            time_with_tz TIME WITH TIME ZONE,
            time_without_tz TIME WITHOUT TIME ZONE,
            mixed_case_date DaTe,
            mixed_case_time TiMe,
            mixed_case_timestamp TiMeStAmP
        )",
        &[]
    ).await.unwrap();
    
    // Check storage types
    let pragma_rows = client.query(
        "SELECT name, type FROM pragma_table_info('datetime_aliases_test') WHERE name != 'id' ORDER BY cid",
        &[]
    ).await.unwrap();
    
    println!("Testing datetime aliases:");
    for row in &pragma_rows {
        let name: String = row.get(0);
        let sqlite_type: String = row.get(1);
        println!("  Column {} -> {}", name, sqlite_type);
        
        assert_eq!(sqlite_type, "INTEGER", 
            "Column {} should be stored as INTEGER, but got {}", name, sqlite_type);
    }
    
    server.abort();
}

#[tokio::test]
async fn test_pgsqlite_schema_metadata() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table with datetime types
    client.execute(
        "CREATE TABLE datetime_metadata_test (
            id INTEGER PRIMARY KEY,
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP,
            timestamptz_col TIMESTAMPTZ,
            timetz_col TIMETZ,
            interval_col INTERVAL
        )",
        &[]
    ).await.unwrap();
    
    // Check __pgsqlite_schema metadata
    let schema_rows = client.query(
        "SELECT column_name, pg_type, sqlite_type 
         FROM __pgsqlite_schema 
         WHERE table_name = 'datetime_metadata_test' 
         AND column_name != 'id'
         ORDER BY column_name",
        &[]
    ).await.unwrap();
    
    println!("__pgsqlite_schema metadata:");
    for row in &schema_rows {
        let column_name: String = row.get(0);
        let pg_type: String = row.get(1);
        let sqlite_type: String = row.get(2);
        println!("  {} -> pg_type: {}, sqlite_type: {}", column_name, pg_type, sqlite_type);
        
        // All datetime types should be stored as INTEGER in SQLite
        assert_eq!(sqlite_type, "INTEGER", 
            "Column {} should have sqlite_type INTEGER, but got {}", column_name, sqlite_type);
        
        // Verify the PostgreSQL type is preserved correctly
        match column_name.as_str() {
            "date_col" => assert!(pg_type.eq_ignore_ascii_case("date"), "Expected date, got {}", pg_type),
            "time_col" => assert!(pg_type.eq_ignore_ascii_case("time"), "Expected time, got {}", pg_type),
            "timestamp_col" => assert!(pg_type.eq_ignore_ascii_case("timestamp"), "Expected timestamp, got {}", pg_type),
            "timestamptz_col" => assert!(pg_type.eq_ignore_ascii_case("timestamptz"), "Expected timestamptz, got {}", pg_type),
            "timetz_col" => assert!(pg_type.eq_ignore_ascii_case("timetz"), "Expected timetz, got {}", pg_type),
            "interval_col" => assert!(pg_type.eq_ignore_ascii_case("interval"), "Expected interval, got {}", pg_type),
            _ => panic!("Unexpected column: {}", column_name),
        }
    }
    
    // Verify we have metadata for all datetime columns
    assert_eq!(schema_rows.len(), 6, "Should have metadata for 6 datetime columns");
    
    server.abort();
}

#[tokio::test]
async fn test_datetime_value_storage_and_retrieval() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create table and insert datetime values
    client.execute(
        "CREATE TABLE datetime_values_test (
            id INTEGER PRIMARY KEY,
            date_col DATE,
            time_col TIME,
            timestamp_col TIMESTAMP,
            timestamptz_col TIMESTAMPTZ,
            timetz_col TIMETZ,
            interval_col INTERVAL
        )",
        &[]
    ).await.unwrap();
    
    // Insert test values
    client.execute(
        "INSERT INTO datetime_values_test VALUES (
            1,
            '2024-01-15',
            '14:30:00',
            '2024-01-15 14:30:00',
            '2024-01-15 14:30:00+00',
            '14:30:00+00',
            '1 day 2 hours 30 minutes'
        )",
        &[]
    ).await.unwrap();
    
    // Query raw SQLite values to verify INTEGER storage
    let raw_rows = client.query(
        "SELECT 
            typeof(date_col) as date_type,
            typeof(time_col) as time_type,
            typeof(timestamp_col) as timestamp_type,
            typeof(timestamptz_col) as timestamptz_type,
            typeof(timetz_col) as timetz_type,
            typeof(interval_col) as interval_type,
            date_col as date_raw,
            time_col as time_raw,
            timestamp_col as timestamp_raw,
            timestamptz_col as timestamptz_raw,
            timetz_col as timetz_raw,
            interval_col as interval_raw
         FROM datetime_values_test WHERE id = 1",
        &[]
    ).await.unwrap();
    
    if !raw_rows.is_empty() {
        let row = &raw_rows[0];
        
        // Check SQLite storage types - all should be 'integer'
        println!("SQLite typeof() results:");
        for i in 0..6 {
            let col_type: String = row.get(i);
            let col_name = match i {
                0 => "date",
                1 => "time", 
                2 => "timestamp",
                3 => "timestamptz",
                4 => "timetz",
                5 => "interval",
                _ => unreachable!()
            };
            println!("  {} column: {}", col_name, col_type);
            assert_eq!(col_type, "integer", 
                "{} column should have SQLite type 'integer', but got '{}'", col_name, col_type);
        }
        
        // Also verify the raw values are integers
        println!("\nRaw INTEGER values:");
        let date_raw: i64 = row.get(6);
        let time_raw: i64 = row.get(7);
        let timestamp_raw: i64 = row.get(8);
        let timestamptz_raw: i64 = row.get(9);
        let timetz_raw: i64 = row.get(10);
        let interval_raw: i64 = row.get(11);
        
        println!("  date: {} (days since epoch)", date_raw);
        println!("  time: {} (microseconds since midnight)", time_raw);
        println!("  timestamp: {} (microseconds since epoch)", timestamp_raw);
        println!("  timestamptz: {} (microseconds since epoch)", timestamptz_raw);
        println!("  timetz: {} (microseconds since midnight)", timetz_raw);
        println!("  interval: {} (microseconds)", interval_raw);
        
        // Basic sanity checks
        assert!(date_raw > 0, "Date should be positive days since epoch");
        assert!(time_raw > 0, "Time should be positive microseconds");
        assert!(timestamp_raw > 0, "Timestamp should be positive microseconds");
        assert!(timestamptz_raw > 0, "Timestamptz should be positive microseconds");
        assert!(timetz_raw > 0, "Timetz should be positive microseconds");
        assert!(interval_raw > 0, "Interval should be positive microseconds");
    }
    
    // Also verify we can retrieve values correctly through PostgreSQL interface
    let pg_row = client.query_one(
        "SELECT date_col, time_col, timestamp_col FROM datetime_values_test WHERE id = 1",
        &[]
    ).await.unwrap();
    
    let date_val: chrono::NaiveDate = pg_row.get(0);
    let time_val: chrono::NaiveTime = pg_row.get(1);
    let timestamp_val: chrono::NaiveDateTime = pg_row.get(2);
    
    assert_eq!(date_val.to_string(), "2024-01-15");
    assert_eq!(time_val.to_string(), "14:30:00");
    assert_eq!(timestamp_val.to_string(), "2024-01-15 14:30:00");
    
    server.abort();
}

#[tokio::test]
async fn test_create_table_with_constraints_and_defaults() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test that datetime types with constraints and defaults are still stored as INTEGER
    client.execute(
        "CREATE TABLE datetime_constraints_test (
            id INTEGER PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            birth_date DATE CHECK (birth_date > '1900-01-01'),
            work_start TIME DEFAULT '09:00:00',
            meeting_time TIMETZ
        )",
        &[]
    ).await.unwrap();
    
    // Check storage types
    let pragma_rows = client.query(
        "SELECT name, type FROM pragma_table_info('datetime_constraints_test') 
         WHERE name != 'id' ORDER BY cid",
        &[]
    ).await.unwrap();
    
    println!("Testing datetime types with constraints:");
    for row in &pragma_rows {
        let name: String = row.get(0);
        let sqlite_type: String = row.get(1);
        println!("  Column {} -> {}", name, sqlite_type);
        
        assert_eq!(sqlite_type, "INTEGER", 
            "Column {} should be stored as INTEGER despite constraints, but got {}", name, sqlite_type);
    }
    
    server.abort();
}

#[tokio::test]
async fn test_datetime_edge_cases() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test 1: Array types (should fail gracefully as arrays not supported yet)
    let array_result = client.execute(
        "CREATE TABLE datetime_array_test (
            id INTEGER PRIMARY KEY,
            dates DATE[]
        )",
        &[]
    ).await;
    
    // Arrays aren't supported yet, so this should fail
    if array_result.is_ok() {
        println!("Array types seem to be supported now - test may need updating");
    }
    
    // Test 2: Domain types based on datetime
    let domain_result = client.execute(
        "CREATE DOMAIN positive_date AS DATE CHECK (VALUE > '2000-01-01')",
        &[]
    ).await;
    
    if let Err(e) = domain_result {
        println!("CREATE DOMAIN not supported: {}", e);
    }
    
    // Test 3: Complex type combinations
    client.execute(
        "CREATE TABLE datetime_complex_test (
            id INTEGER PRIMARY KEY,
            nullable_date DATE NULL,
            not_null_time TIME NOT NULL,
            unique_timestamp TIMESTAMP UNIQUE,
            primary_key_date DATE PRIMARY KEY
        )",
        &[]
    ).await.unwrap_err(); // Should fail due to multiple primary keys
    
    // Test 4: Very long column names with datetime types
    client.execute(
        "CREATE TABLE datetime_long_names_test (
            id INTEGER PRIMARY KEY,
            this_is_a_very_long_column_name_for_a_date_field DATE,
            another_extremely_long_column_name_for_timestamp_with_timezone TIMESTAMPTZ
        )",
        &[]
    ).await.unwrap();
    
    // Verify long column names still use INTEGER storage
    let pragma_rows = client.query(
        "SELECT name, type FROM pragma_table_info('datetime_long_names_test') 
         WHERE name != 'id' ORDER BY cid",
        &[]
    ).await.unwrap();
    
    for row in &pragma_rows {
        let name: String = row.get(0);
        let sqlite_type: String = row.get(1);
        assert_eq!(sqlite_type, "INTEGER", 
            "Long column name {} should still use INTEGER storage", name);
    }
    
    // Test 5: Quoted identifiers with datetime types
    client.execute(
        r#"CREATE TABLE "DateTime Special Table" (
            "ID Column" INTEGER PRIMARY KEY,
            "Date Column!" DATE,
            "Time (with parens)" TIME,
            "Timestamp @ Special" TIMESTAMP
        )"#,
        &[]
    ).await.unwrap();
    
    // Verify quoted identifiers still use INTEGER storage
    let quoted_rows = client.query(
        r#"SELECT name, type FROM pragma_table_info('"DateTime Special Table"') 
           WHERE name != 'ID Column' ORDER BY cid"#,
        &[]
    ).await.unwrap();
    
    println!("Testing quoted identifiers:");
    for row in &quoted_rows {
        let name: String = row.get(0);
        let sqlite_type: String = row.get(1);
        println!("  '{}' -> {}", name, sqlite_type);
        assert_eq!(sqlite_type, "INTEGER", 
            "Quoted column {} should use INTEGER storage", name);
    }
    
    server.abort();
}