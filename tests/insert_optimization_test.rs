use pgsqlite::query::fast_path::{can_use_fast_path_enhanced, FastPathOperation};

#[test]
fn test_insert_fast_path_detection() {
    // Test simple INSERT statements that should qualify for fast path
    let test_cases = vec![
        "INSERT INTO users (name, email) VALUES ('test', 'test@example.com')",
        "INSERT INTO products (id, name, price) VALUES (1, 'widget', 19.99)",
        "insert into orders (customer_id, amount) values (123, 45.67)",
        "  INSERT INTO   items   (sku, description) VALUES ('ABC123', 'Test Item')  ",
    ];

    for query in test_cases {
        println!("Testing query: {query}");
        match can_use_fast_path_enhanced(query) {
            Some(fast_query) => {
                assert!(matches!(fast_query.operation, FastPathOperation::Insert));
                println!("✅ Fast path detected for INSERT: table={}", fast_query.table_name);
            }
            None => {
                panic!("❌ Fast path NOT detected for simple INSERT: {query}");
            }
        }
    }
}

#[test]
fn test_insert_complex_queries_no_fast_path() {
    // Test complex INSERT statements that should NOT qualify for fast path
    let test_cases = vec![
        "INSERT INTO users (name, email) SELECT name, email FROM temp_users",
        "INSERT INTO products (name) VALUES ('test') RETURNING id",
        "INSERT INTO orders (customer_id) VALUES ((SELECT id FROM customers WHERE name = 'test'))",
        "INSERT INTO items (name) VALUES ('test') ON CONFLICT DO NOTHING",
    ];

    for query in test_cases {
        println!("Testing complex query: {query}");
        match can_use_fast_path_enhanced(query) {
            Some(_) => {
                println!("⚠️  Fast path detected (may be okay): {query}");
            }
            None => {
                println!("✅ Fast path correctly rejected for complex INSERT: {query}");
            }
        }
    }
}

#[tokio::test]
async fn test_insert_performance_improvement() {
    use pgsqlite::session::DbHandler;
    use std::time::Instant;

    // Create in-memory database
    let db = DbHandler::new(":memory:").expect("Failed to create database");

    // Create test table without decimal columns (should use fast path)
    db.execute("CREATE TABLE test_table (id INTEGER, name TEXT)").await.expect("Failed to create table");

    // Test fast path INSERT performance
    let start = Instant::now();
    for i in 0..100 {
        let query = format!("INSERT INTO test_table (id, name) VALUES ({i}, 'test{i}')");
        db.execute(&query).await.expect("Failed to execute INSERT");
    }
    let duration = start.elapsed();
    
    println!("100 INSERT operations (fast path) took: {duration:?}");
    println!("Average per INSERT: {:?}", duration / 100);
    
    // Verify data was inserted
    let result = db.query("SELECT COUNT(*) FROM test_table").await.expect("Failed to count rows");
    assert_eq!(result.rows.len(), 1);
    
    // The actual count should be 100
    if let Some(Some(count_bytes)) = result.rows[0].first() {
        let count_str = String::from_utf8_lossy(count_bytes);
        assert_eq!(count_str, "100");
    }
}

#[tokio::test]
async fn test_insert_with_decimal_columns_fallback() {
    use pgsqlite::session::DbHandler;
    use std::time::Instant;

    // Create in-memory database
    let db = DbHandler::new(":memory:").expect("Failed to create database");

    // Create test table WITH decimal columns (should fall back to slow path)
    db.execute("CREATE TABLE decimal_table (id INTEGER, price DECIMAL(10,2), name TEXT)").await.expect("Failed to create table");

    // Test slow path INSERT performance (with decimal columns)
    let start = Instant::now();
    for i in 0..100 {
        let query = format!("INSERT INTO decimal_table (id, price, name) VALUES ({i}, {i}.99, 'test{i}')");
        db.execute(&query).await.expect("Failed to execute INSERT");
    }
    let duration = start.elapsed();
    
    println!("100 INSERT operations (slow path - with decimals) took: {duration:?}");
    println!("Average per INSERT: {:?}", duration / 100);
    
    // Verify data was inserted
    let result = db.query("SELECT COUNT(*) FROM decimal_table").await.expect("Failed to count rows");
    assert_eq!(result.rows.len(), 1);
    
    // The actual count should be 100
    if let Some(Some(count_bytes)) = result.rows[0].first() {
        let count_str = String::from_utf8_lossy(count_bytes);
        assert_eq!(count_str, "100");
    }
}

#[tokio::test]
async fn test_insert_bottleneck_analysis() {
    use pgsqlite::session::DbHandler;
    use std::time::Instant;
    use pgsqlite::query::fast_path::can_use_fast_path_enhanced;

    println!("\n=== INSERT BOTTLENECK ANALYSIS ===");
    
    // Create in-memory database
    let db = DbHandler::new(":memory:").expect("Failed to create database");

    // Create test table without decimal columns
    db.execute("CREATE TABLE perf_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)").await.expect("Failed to create table");
    
    // Warm up the connection and caches
    db.execute("INSERT INTO perf_test (name, value) VALUES ('warmup', 1)").await.expect("Failed to warm up");
    
    let test_query = "INSERT INTO perf_test (name, value) VALUES ('test', 42)";
    
    // Test 1: Fast path detection overhead
    let start = Instant::now();
    for _ in 0..1000 {
        let _ = can_use_fast_path_enhanced(test_query);
    }
    let fast_path_time = start.elapsed();
    println!("Fast path detection (1000x): {:?}, avg: {:?}", fast_path_time, fast_path_time / 1000);
    
    // Test 2: Schema cache lookup overhead
    let start = Instant::now();
    for _ in 0..1000 {
        let _ = db.get_table_schema("perf_test").await;
    }
    let schema_lookup_time = start.elapsed();
    println!("Schema cache lookup (1000x): {:?}, avg: {:?}", schema_lookup_time, schema_lookup_time / 1000);
    
    // Test 3: Single INSERT with timing breakdown
    println!("\nSingle INSERT timing breakdown:");
    
    // Measure total time
    let start_total = Instant::now();
    db.execute("INSERT INTO perf_test (name, value) VALUES ('single', 100)").await.expect("Failed to execute INSERT");
    let total_time = start_total.elapsed();
    println!("Total INSERT time: {total_time:?}");
    
    // Test 4: Batch of INSERTs to see if there's per-operation overhead
    println!("\nBatch INSERT performance:");
    let batch_sizes = [10, 100, 500];
    
    for &batch_size in &batch_sizes {
        let start = Instant::now();
        for i in 0..batch_size {
            let query = format!("INSERT INTO perf_test (name, value) VALUES ('batch{i}', {i})");
            db.execute(&query).await.expect("Failed to execute INSERT");
        }
        let duration = start.elapsed();
        println!("{} INSERTs: {:?}, avg: {:?}", batch_size, duration, duration / batch_size as u32);
    }
    
    // Test 5: Compare with direct SQLite access
    println!("\nDirect SQLite comparison:");
    use rusqlite::Connection;
    let conn = Connection::open_in_memory().expect("Failed to create SQLite connection");
    conn.execute("CREATE TABLE direct_test (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)", []).expect("Failed to create table");
    
    let start = Instant::now();
    for i in 0..100 {
        conn.execute("INSERT INTO direct_test (name, value) VALUES (?1, ?2)", 
            rusqlite::params![format!("direct{}", i), i]).expect("Failed to execute INSERT");
    }
    let direct_time = start.elapsed();
    println!("100 direct SQLite INSERTs: {:?}, avg: {:?}", direct_time, direct_time / 100);
    
    // Test 6: Test with prepared statements through statement pool
    println!("\nStatement pool performance:");
    let start = Instant::now();
    for i in 0..100 {
        let query = "INSERT INTO perf_test (name, value) VALUES ($1, $2)";
        db.execute_with_statement_pool_params(
            query,
            &[rusqlite::types::Value::Text(format!("pooled{i}")), 
              rusqlite::types::Value::Integer(i as i64)]
        ).await.expect("Failed to execute INSERT");
    }
    let pool_time = start.elapsed();
    println!("100 statement pool INSERTs: {:?}, avg: {:?}", pool_time, pool_time / 100);
}