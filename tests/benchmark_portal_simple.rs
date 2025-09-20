use std::time::{Instant, Duration};
use tokio_postgres::NoTls;
use std::process::{Command, Child};

#[tokio::test]
#[ignore] // Run with: cargo test benchmark_portal_simple -- --ignored --nocapture
async fn benchmark_simple_portal_demo() {
    println!("\n🚀 === Simple Portal Management Demo ===");
    println!("Demonstrating portal management benefits with basic operations\n");
    
    #[allow(clippy::zombie_processes)]
    let mut server = start_server();
    
    // Wait for server to be ready with retry logic
    let (client, connection) = connect_with_retry().await;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Setup basic test data
    setup_test_data(&client).await;
    
    // Test 1: Basic Query Performance
    test_basic_queries(&client).await;
    
    // Test 2: Memory Efficiency Demo
    test_memory_efficiency(&client).await;
    
    // Test 3: Prepared Statement Benefits
    test_prepared_statements(&client).await;
    
    println!("✅ Portal Management Demo Complete\n");
    server.kill().expect("Failed to kill server");
}

async fn setup_test_data(client: &tokio_postgres::Client) {
    println!("🔧 Setting up test data...");
    
    // Create test table
    client.execute(
        "CREATE TABLE IF NOT EXISTS demo_table (
            id SERIAL PRIMARY KEY,
            name TEXT,
            value INTEGER,
            category TEXT
        )", &[]
    ).await.expect("Failed to create table");
    
    // Insert test records one by one for simplicity
    let record_count = 1000;
    
    for i in 0..record_count {
        let name = format!("Record {i}");
        let value = i * 2;
        let category = format!("Category {}", i % 10);
        
        client.execute(
            "INSERT INTO demo_table (name, value, category) VALUES ($1, $2, $3)",
            &[&name, &value, &category]
        ).await.expect("Failed to insert record");
        
        if i % 200 == 0 && i > 0 {
            println!("  Inserted {i} records...");
        }
    }
    
    println!("✅ Test data ready: {record_count} records\n");
}

async fn test_basic_queries(client: &tokio_postgres::Client) {
    println!("📊 === Basic Query Performance Test ===");
    
    // Test simple SELECT
    let start = Instant::now();
    let rows = client.query("SELECT * FROM demo_table WHERE value > 500 LIMIT 100", &[])
        .await.expect("Failed to execute query");
    let simple_time = start.elapsed();
    
    println!("  Simple query: {:?} ({} rows)", simple_time, rows.len());
    
    // Test with ORDER BY
    let start = Instant::now();
    let rows = client.query("SELECT * FROM demo_table ORDER BY value DESC LIMIT 50", &[])
        .await.expect("Failed to execute query");
    let ordered_time = start.elapsed();
    
    println!("  Ordered query: {:?} ({} rows)", ordered_time, rows.len());
    
    // Test aggregate query
    let start = Instant::now();
    let rows = client.query("SELECT category, COUNT(*), AVG(value) FROM demo_table GROUP BY category", &[])
        .await.expect("Failed to execute query");
    let aggregate_time = start.elapsed();
    
    println!("  Aggregate query: {:?} ({} rows)\n", aggregate_time, rows.len());
}

async fn test_memory_efficiency(client: &tokio_postgres::Client) {
    println!("💾 === Memory Efficiency Demo ===");
    
    // Traditional approach: Get all records
    println!("📋 Traditional: Fetch all records at once");
    let start = Instant::now();
    let all_rows = client.query("SELECT * FROM demo_table ORDER BY id", &[])
        .await.expect("Failed to fetch all records");
    let full_fetch_time = start.elapsed();
    
    let total_records = all_rows.len();
    println!("  Full fetch: {full_fetch_time:?} ({total_records} records)");
    println!("  Estimated memory: ~{} KB\n", total_records * 50 / 1024);
    
    // Portal approach: Fetch in chunks
    println!("📋 Portal approach: Fetch in chunks of 100");
    let chunk_size = 100;
    let mut total_chunk_time = Duration::ZERO;
    let mut chunks_processed = 0;
    let mut records_processed = 0;
    
    for offset in (0..total_records).step_by(chunk_size) {
        let start = Instant::now();
        let chunk_rows = client.query(
            "SELECT * FROM demo_table ORDER BY id LIMIT $1 OFFSET $2",
            &[&(chunk_size as i32), &(offset as i32)]
        ).await.expect("Failed to fetch chunk");
        
        let chunk_time = start.elapsed();
        total_chunk_time += chunk_time;
        chunks_processed += 1;
        records_processed += chunk_rows.len();
        
        if chunks_processed <= 3 || chunks_processed % 3 == 0 {
            println!("  Chunk {}: {} records in {:?}", 
                chunks_processed, chunk_rows.len(), chunk_time);
        }
        
        if chunk_rows.len() < chunk_size {
            break;
        }
    }
    
    println!("\n📈 Memory Efficiency Results:");
    println!("  Full fetch:    {:?} (~{} KB memory)", 
        full_fetch_time, total_records * 50 / 1024);
    println!("  Chunked fetch: {:?} (~{} KB peak memory)", 
        total_chunk_time, chunk_size * 50 / 1024);
    println!("  Memory savings: {:.1}%", 
        (1.0 - (chunk_size as f64 / total_records as f64)) * 100.0);
    println!("  Chunks processed: {chunks_processed} ({records_processed} total records)\n");
}

async fn test_prepared_statements(client: &tokio_postgres::Client) {
    println!("🔧 === Prepared Statement Benefits ===");
    
    let test_iterations = 50;
    
    // Test 1: Dynamic queries (no preparation)
    println!("📋 Dynamic queries (no preparation)");
    let start = Instant::now();
    for i in 0..test_iterations {
        let category = format!("Category {}", i % 10);
        let query = format!("SELECT * FROM demo_table WHERE category = '{category}' LIMIT 5");
        let _rows = client.query(&query, &[]).await.expect("Failed to execute dynamic query");
    }
    let dynamic_time = start.elapsed();
    
    println!("  {test_iterations} dynamic queries: {dynamic_time:?}");
    
    // Test 2: Prepared statements
    println!("📋 Prepared statements (portal approach)");
    let stmt = client.prepare("SELECT * FROM demo_table WHERE category = $1 LIMIT 5")
        .await.expect("Failed to prepare statement");
    
    let start = Instant::now();
    for i in 0..test_iterations {
        let category = format!("Category {}", i % 10);
        let _rows = client.query(&stmt, &[&category]).await.expect("Failed to execute prepared query");
    }
    let prepared_time = start.elapsed();
    
    println!("  {test_iterations} prepared queries: {prepared_time:?}");
    
    println!("\n📈 Prepared Statement Results:");
    println!("  Dynamic queries:  {:?} ({:.2}ms avg)", 
        dynamic_time, dynamic_time.as_millis() as f64 / test_iterations as f64);
    println!("  Prepared queries: {:?} ({:.2}ms avg)", 
        prepared_time, prepared_time.as_millis() as f64 / test_iterations as f64);
    
    if prepared_time < dynamic_time {
        println!("  🚀 Prepared statements are {:.1}x faster", 
            dynamic_time.as_secs_f64() / prepared_time.as_secs_f64());
    } else {
        println!("  ⚠️  Dynamic queries were {:.1}x faster", 
            prepared_time.as_secs_f64() / dynamic_time.as_secs_f64());
    }
    
    println!("  💡 Portal management enables efficient prepared statement reuse\n");
}

async fn connect_with_retry() -> (tokio_postgres::Client, tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>) {
    for attempt in 1..=10 {
        println!("  Connection attempt {attempt}...");
        
        match tokio_postgres::connect(
            "host=localhost port=5433 user=postgres dbname=test",
            NoTls,
        ).await {
            Ok((client, connection)) => {
                println!("  ✅ Connected to pgsqlite server");
                return (client, connection);
            }
            Err(e) => {
                println!("  ❌ Connection failed: {e}");
                if attempt < 10 {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        }
    }
    panic!("Failed to connect after 10 attempts");
}

#[allow(dead_code)]
async fn connect_to_server() -> (tokio_postgres::Client, tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>) {
    tokio_postgres::connect(
        "host=localhost port=5433 user=postgres dbname=test",
        NoTls,
    )
    .await
    .expect("Failed to connect to server")
}

fn start_server() -> Child {
    Command::new("cargo")
        .args(["run", "--bin", "pgsqlite", "--", "--port", "5433", "--in-memory"])
        .spawn()
        .expect("Failed to start server")
}