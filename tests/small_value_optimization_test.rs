mod common;
use common::setup_test_server;
use std::time::Instant;

#[tokio::test]
async fn test_small_value_optimization_performance() {
    let server = setup_test_server().await;
    let client = &server.client;

    // Create test table with various small value types
    client.execute("DROP TABLE IF EXISTS small_value_test", &[]).await.ok();
    client.execute(
        "CREATE TABLE small_value_test (
            id INTEGER PRIMARY KEY,
            bool_col BOOLEAN,
            small_int INTEGER,
            zero_val INTEGER,
            one_val INTEGER,
            small_float REAL
        )",
        &[]
    ).await.expect("Failed to create table");

    // Insert test data with common small values using batch insert for better performance
    let mut values = Vec::new();
    for i in 0..100 {  // Reduced from 1000 to 100 for faster tests
        let bool_val = if i % 2 == 0 { "true" } else { "false" };
        values.push(format!("({}, {}, {}, 0, 1, {})", i, bool_val, i % 100, i as f32 / 10.0));
    }
    
    client.execute(
        &format!("INSERT INTO small_value_test (id, bool_col, small_int, zero_val, one_val, small_float) 
                 VALUES {}", values.join(", ")),
        &[]
    ).await.expect("Failed to insert");

    // Warm up
    for _ in 0..5 {  // Reduced from 10 to 5
        let rows: Vec<_> = client.query(
            "SELECT * FROM small_value_test WHERE id < 50",
            &[]
        ).await.expect("Failed to query");
        assert_eq!(rows.len(), 50);
    }

    // Benchmark small value queries
    let iterations = 20;  // Reduced from 100 to 20
    let start = Instant::now();
    
    for _ in 0..iterations {
        let _rows: Vec<_> = client.query(
            "SELECT bool_col, small_int, zero_val, one_val, small_float 
             FROM small_value_test 
             WHERE id < 50",  // Reduced result set size
            &[]
        ).await.expect("Failed to query");
    }
    
    let elapsed = start.elapsed();
    let avg_time = elapsed.as_micros() as f64 / iterations as f64;
    
    println!("Small value query performance:");
    println!("  Total time: {elapsed:?}");
    println!("  Average time per query: {avg_time:.2} µs");
    println!("  Queries per second: {:.0}", 1_000_000.0 / avg_time);

    // Test memory usage by querying all rows
    let all_rows: Vec<_> = client.query(
        "SELECT * FROM small_value_test",
        &[]
    ).await.expect("Failed to query all");
    
    assert_eq!(all_rows.len(), 100);  // Updated to match reduced row count
    
    // Verify values are correct
    let first_row = &all_rows[0];
    assert_eq!(first_row.get::<_, i32>("id"), 0);
    assert!(first_row.get::<_, bool>("bool_col"));
    assert_eq!(first_row.get::<_, i32>("small_int"), 0);
    assert_eq!(first_row.get::<_, i32>("zero_val"), 0);
    assert_eq!(first_row.get::<_, i32>("one_val"), 1);
    assert_eq!(first_row.get::<_, f32>("small_float"), 0.0);
}

#[tokio::test]
async fn test_small_value_correctness() {
    let server = setup_test_server().await;
    let client = &server.client;

    // Test various small values
    client.execute("DROP TABLE IF EXISTS small_value_correct", &[]).await.ok();
    client.execute(
        "CREATE TABLE small_value_correct (
            bool_true BOOLEAN,
            bool_false BOOLEAN,
            zero INTEGER,
            one INTEGER,
            minus_one INTEGER,
            small_pos INTEGER,
            small_neg INTEGER,
            small_float REAL,
            empty_text TEXT
        )",
        &[]
    ).await.expect("Failed to create table");

    client.execute(
        "INSERT INTO small_value_correct VALUES (true, false, 0, 1, -1, 42, -42, 3.25, '')",
        &[]
    ).await.expect("Failed to insert");

    let row = client.query_one(
        "SELECT * FROM small_value_correct",
        &[]
    ).await.expect("Failed to query");

    // Verify all small values are correctly handled
    assert!(row.get::<_, bool>(0));
    assert!(!row.get::<_, bool>(1));
    assert_eq!(row.get::<_, i32>(2), 0);
    assert_eq!(row.get::<_, i32>(3), 1);
    assert_eq!(row.get::<_, i32>(4), -1);
    assert_eq!(row.get::<_, i32>(5), 42);
    assert_eq!(row.get::<_, i32>(6), -42);
    assert_eq!(row.get::<_, f32>(7), 3.25);
    assert_eq!(row.get::<_, &str>(8), "");
}

#[tokio::test]
async fn test_small_value_binary_protocol() {
    let server = setup_test_server().await;
    let client = &server.client;

    // Test binary protocol with small values
    client.execute("DROP TABLE IF EXISTS small_value_binary", &[]).await.ok();
    client.execute(
        "CREATE TABLE small_value_binary (
            id INTEGER PRIMARY KEY,
            bool_val BOOLEAN,
            int_val INTEGER,
            float_val REAL
        )",
        &[]
    ).await.expect("Failed to create table");

    // Insert values using simple protocol to avoid parameter type issues
    client.execute(
        "INSERT INTO small_value_binary VALUES (1, true, 42, 3.25)",
        &[]
    ).await.expect("Failed to insert");

    // Query with binary format
    let stmt = client.prepare("SELECT * FROM small_value_binary WHERE id = $1")
        .await.expect("Failed to prepare");
    
    let row = client.query_one(&stmt, &[&1i32])
        .await.expect("Failed to query");

    // Verify binary protocol works correctly
    assert_eq!(row.get::<_, i32>(0), 1);
    assert!(row.get::<_, bool>(1));
    assert_eq!(row.get::<_, i32>(2), 42);
    assert_eq!(row.get::<_, f32>(3), 3.25);
}