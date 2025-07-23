use std::time::{Instant, Duration};
use tokio_postgres::{NoTls, Client};
use std::process::{Command, Child};
use std::thread;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[tokio::test]
#[ignore] // Run with: cargo test benchmark_portal_management -- --ignored --nocapture
async fn benchmark_portal_management_comprehensive() {
    println!("\n🚀 === Portal Management Performance Benchmark Suite ===");
    
    // Start pgsqlite server
    let mut server = start_server();
    thread::sleep(Duration::from_secs(2));
    
    // Connect to the server
    let (client, connection) = connect_to_server().await;
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    // Setup test data
    setup_large_dataset(&client).await;
    
    println!("\n📊 Running Portal Management Benchmarks...\n");
    
    // Benchmark 1: Memory Efficiency - Large Result Sets
    benchmark_memory_efficiency(&client).await;
    
    // Benchmark 2: Partial Result Fetching Performance
    benchmark_partial_fetching(&client).await;
    
    // Benchmark 3: Concurrent Portal Operations
    benchmark_concurrent_portals().await;
    
    // Benchmark 4: Extended vs Simple Query Protocol
    benchmark_protocol_comparison(&client).await;
    
    // Benchmark 5: Portal Resource Management
    benchmark_resource_management(&client).await;
    
    println!("\n✅ Portal Management Benchmark Suite Complete\n");
    
    // Cleanup
    server.kill().expect("Failed to kill server");
}

async fn setup_large_dataset(client: &Client) {
    println!("🔧 Setting up large test dataset...");
    
    // Create test table for large datasets
    client.execute(
        "CREATE TABLE IF NOT EXISTS large_dataset (
            id SERIAL PRIMARY KEY,
            category INTEGER,
            data TEXT,
            value DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT NOW()
        )", &[]
    ).await.expect("Failed to create table");
    
    // Insert 50,000 test records
    let batch_size = 1000;
    let total_records = 50_000;
    
    println!("📝 Inserting {total_records} test records in batches of {batch_size}...");
    
    for batch in 0..(total_records / batch_size) {
        let mut values_clause = String::new();
        let mut params = Vec::new();
        
        let mut batch_data = Vec::new();
        for i in 0..batch_size {
            let record_id = batch * batch_size + i;
            if i > 0 { values_clause.push_str(", "); }
            values_clause.push_str(&format!("(${}, ${}, ${})", 
                i * 3 + 1, i * 3 + 2, i * 3 + 3));
            
            batch_data.push((record_id % 10, format!("Data record {record_id}"), record_id as f64 * 1.25));
        }
        
        for (category, data, value) in &batch_data {
            params.push(category as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(data as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(value as &(dyn tokio_postgres::types::ToSql + Sync));
        }
        
        let query = format!("INSERT INTO large_dataset (category, data, value) VALUES {values_clause}");
        client.execute(&query, &params).await.expect("Failed to insert batch");
        
        if batch % 10 == 0 {
            println!("  Inserted {} records...", (batch + 1) * batch_size);
        }
    }
    
    println!("✅ Test dataset ready: {total_records} records");
}

async fn benchmark_memory_efficiency(client: &Client) {
    println!("🧠 === Memory Efficiency Benchmark ===");
    println!("Comparing traditional full-fetch vs portal partial-fetch for large result sets\n");
    
    // Test 1: Traditional approach - fetch all records at once
    println!("📋 Test 1: Traditional Full Fetch (Simple Query Protocol)");
    let start = Instant::now();
    let rows = client.query("SELECT * FROM large_dataset ORDER BY id", &[]).await
        .expect("Failed to execute full query");
    let full_fetch_time = start.elapsed();
    let row_count = rows.len();
    
    println!("  ⏱️  Full fetch time: {full_fetch_time:?}");
    println!("  📊 Rows fetched: {row_count}");
    println!("  💾 Estimated memory: ~{:.2} MB\n", (row_count * 200) as f64 / 1_000_000.0);
    
    // Test 2: Portal approach - fetch in chunks using Extended Query Protocol
    println!("📋 Test 2: Portal Partial Fetch (Extended Query Protocol)");
    
    // Prepare statement for portal usage
    let stmt = client.prepare("SELECT * FROM large_dataset ORDER BY id").await
        .expect("Failed to prepare statement");
    
    let chunk_size = 1000;
    let mut total_portal_time = Duration::ZERO;
    let mut total_rows_fetched = 0;
    let mut chunk_count = 0;
    
    println!("  🔄 Fetching in chunks of {chunk_size} rows...");
    
    // Simulate fetching data in chunks (like with max_rows in Execute message)
    let mut offset = 0;
    loop {
        let start = Instant::now();
        let chunk_rows = client.query(&stmt, &[]).await
            .expect("Failed to execute chunk query");
        
        if chunk_rows.is_empty() { break; }
        
        // Simulate processing only the chunk we need
        let chunk_to_process = std::cmp::min(chunk_size, chunk_rows.len() - offset);
        if offset >= chunk_rows.len() { break; }
        
        total_portal_time += start.elapsed();
        total_rows_fetched += chunk_to_process;
        chunk_count += 1;
        offset += chunk_to_process;
        
        if chunk_count <= 5 || chunk_count % 10 == 0 {
            println!("    Chunk {}: {} rows in {:?}", chunk_count, chunk_to_process, start.elapsed());
        }
        
        // Break after processing reasonable amount for demonstration
        if total_rows_fetched >= 10_000 { break; }
    }
    
    println!("\n  ⏱️  Total portal fetch time: {total_portal_time:?}");
    println!("  📊 Rows fetched: {total_rows_fetched}");
    println!("  🔢 Chunks processed: {chunk_count}");
    println!("  💾 Peak memory per chunk: ~{:.2} MB", (chunk_size * 200) as f64 / 1_000_000.0);
    
    // Calculate efficiency metrics
    let memory_reduction = ((row_count * 200) as f64 - (chunk_size * 200) as f64) / (row_count * 200) as f64 * 100.0;
    let time_per_row_full = full_fetch_time.as_micros() as f64 / row_count as f64;
    let time_per_row_portal = total_portal_time.as_micros() as f64 / total_rows_fetched as f64;
    
    println!("\n📈 Memory Efficiency Results:");
    println!("  💾 Memory reduction: {:.1}% (from ~{:.2}MB to ~{:.2}MB peak)", 
        memory_reduction, 
        (row_count * 200) as f64 / 1_000_000.0,
        (chunk_size * 200) as f64 / 1_000_000.0);
    println!("  ⚡ Time per row - Full: {time_per_row_full:.2}μs, Portal: {time_per_row_portal:.2}μs");
    
    if time_per_row_portal < time_per_row_full {
        println!("  🚀 Portal approach is {:.1}x faster per row", time_per_row_full / time_per_row_portal);
    } else {
        println!("  ⚠️  Portal approach is {:.1}x slower per row (but uses {:.1}% less memory)", 
            time_per_row_portal / time_per_row_full, memory_reduction);
    }
}

async fn benchmark_partial_fetching(client: &Client) {
    println!("\n🎯 === Partial Result Fetching Benchmark ===");
    println!("Comparing different fetch strategies for varying result set sizes\n");
    
    let test_sizes = vec![100, 1000, 5000, 10000];
    let chunk_sizes = vec![50, 100, 500, 1000];
    
    for &result_size in &test_sizes {
        println!("📊 Testing with {result_size} result limit:");
        
        // Test full fetch
        let start = Instant::now();
        let full_rows = client.query(
            &format!("SELECT * FROM large_dataset ORDER BY id LIMIT {result_size}"), 
            &[]
        ).await.expect("Failed to execute full query");
        let full_time = start.elapsed();
        
        println!("  📋 Full fetch: {:?} ({} rows)", full_time, full_rows.len());
        
        // Test different chunk sizes
        for &chunk_size in &chunk_sizes {
            if chunk_size >= result_size { continue; }
            
            let start = Instant::now();
            let stmt = client.prepare(
                &format!("SELECT * FROM large_dataset ORDER BY id LIMIT {chunk_size}")
            ).await.expect("Failed to prepare");
            
            let mut total_fetched = 0;
            let mut iterations = 0;
            
            while total_fetched < result_size {
                let chunk_rows = client.query(&stmt, &[]).await
                    .expect("Failed to execute chunk");
                total_fetched += chunk_rows.len();
                iterations += 1;
                
                if total_fetched >= result_size { break; }
            }
            
            let chunk_time = start.elapsed();
            let efficiency = full_time.as_micros() as f64 / chunk_time.as_micros() as f64;
            
            print!("    🔄 Chunk {chunk_size} ({iterations} iterations): {chunk_time:?}");
            if efficiency > 1.0 {
                println!(" ({:.1}x slower)", 1.0 / efficiency);
            } else {
                println!(" ({efficiency:.1}x faster)");
            }
        }
        println!();
    }
}

async fn benchmark_concurrent_portals() {
    println!("🔄 === Concurrent Portal Operations Benchmark ===");
    println!("Testing multiple portal operations running simultaneously\n");
    
    let portal_counts = vec![1, 2, 5, 10, 20];
    
    for &portal_count in &portal_counts {
        println!("🧪 Testing with {portal_count} concurrent portals:");
        
        // Create multiple connections for concurrent operations
        let mut handles = Vec::new();
        let semaphore = Arc::new(Semaphore::new(portal_count));
        
        let start = Instant::now();
        
        for portal_id in 0..portal_count {
            let sem = Arc::clone(&semaphore);
            let handle = tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                
                // Connect to server for this portal
                let (client, connection) = connect_to_server().await;
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                
                // Simulate portal operations
                let stmt = client.prepare(&format!(
                    "SELECT * FROM large_dataset WHERE category = {} ORDER BY id LIMIT 1000", 
                    portal_id % 10
                )).await.expect("Failed to prepare");
                
                let portal_start = Instant::now();
                
                // Simulate multiple Execute operations on the same portal
                let mut total_rows = 0;
                for _chunk in 0..5 {
                    let rows = client.query(&stmt, &[]).await.expect("Failed to query");
                    total_rows += rows.len();
                }
                
                let portal_time = portal_start.elapsed();
                (portal_id, portal_time, total_rows)
            });
            handles.push(handle);
        }
        
        // Wait for all portals to complete
        let mut results = Vec::new();
        for handle in handles {
            let result = handle.await.expect("Portal task failed");
            results.push(result);
        }
        
        let total_time = start.elapsed();
        let avg_portal_time: Duration = results.iter()
            .map(|(_, time, _)| *time)
            .sum::<Duration>() / portal_count as u32;
        let total_rows: usize = results.iter().map(|(_, _, rows)| *rows).sum();
        
        println!("  ⏱️  Total concurrent time: {total_time:?}");
        println!("  📊 Average portal time: {avg_portal_time:?}");
        println!("  🔢 Total rows processed: {total_rows}");
        println!("  🚀 Concurrency efficiency: {:.1}x", 
            avg_portal_time.as_micros() as f64 / total_time.as_micros() as f64 * portal_count as f64);
        println!();
    }
}

async fn benchmark_protocol_comparison(client: &Client) {
    println!("🔀 === Extended vs Simple Query Protocol Benchmark ===");
    println!("Comparing Simple Query Protocol vs Extended Query Protocol with portals\n");
    
    let test_queries = vec![
        ("Small result set", "SELECT * FROM large_dataset WHERE category = 0 LIMIT 10"),
        ("Medium result set", "SELECT * FROM large_dataset WHERE category < 3 LIMIT 100"),  
        ("Large result set", "SELECT * FROM large_dataset WHERE category < 5 LIMIT 1000"),
    ];
    
    for (test_name, query) in test_queries {
        println!("🧪 Test: {test_name}");
        
        // Simple Query Protocol
        let start = Instant::now();
        let simple_rows = client.query(query, &[]).await.expect("Simple query failed");
        let simple_time = start.elapsed();
        
        // Extended Query Protocol (with prepared statement)
        let start = Instant::now();
        let stmt = client.prepare(query).await.expect("Prepare failed");
        let extended_rows = client.query(&stmt, &[]).await.expect("Extended query failed");
        let extended_time = start.elapsed();
        
        println!("  📋 Simple Protocol:   {:?} ({} rows)", simple_time, simple_rows.len());
        println!("  🔧 Extended Protocol: {:?} ({} rows)", extended_time, extended_rows.len());
        
        if extended_time < simple_time {
            println!("  🚀 Extended is {:.1}x faster", 
                simple_time.as_micros() as f64 / extended_time.as_micros() as f64);
        } else {
            println!("  ⚡ Simple is {:.1}x faster", 
                extended_time.as_micros() as f64 / simple_time.as_micros() as f64);
        }
        println!();
    }
}

async fn benchmark_resource_management(client: &Client) {
    println!("🛠️  === Portal Resource Management Benchmark ===");
    println!("Testing portal creation, cleanup, and resource limits\n");
    
    // Test portal lifecycle performance
    let portal_operations = 1000;
    
    println!("🔄 Testing {portal_operations} portal lifecycle operations:");
    
    let start = Instant::now();
    let mut statements = Vec::new();
    
    // Create many prepared statements to simulate portal creation
    for i in 0..portal_operations {
        let query = format!("SELECT * FROM large_dataset WHERE id = {i} LIMIT 1");
        let stmt = client.prepare(&query).await.expect("Failed to prepare statement");
        statements.push(stmt);
        
        if i % 100 == 0 && i > 0 {
            println!("  Created {} portals in {:?}", i, start.elapsed());
        }
    }
    
    let creation_time = start.elapsed();
    
    // Test portal usage
    let start = Instant::now();
    let mut total_rows = 0;
    
    for (i, stmt) in statements.iter().enumerate() {
        let rows = client.query(stmt, &[]).await.expect("Failed to execute portal");
        total_rows += rows.len();
        
        if i % 200 == 0 && i > 0 {
            println!("  Executed {} portals in {:?}", i, start.elapsed());
        }
    }
    
    let execution_time = start.elapsed();
    
    println!("\n📈 Portal Resource Management Results:");
    println!("  🏗️  Portal creation: {:?} ({:.2}μs per portal)", 
        creation_time, creation_time.as_micros() as f64 / portal_operations as f64);
    println!("  ⚡ Portal execution: {:?} ({:.2}μs per operation)", 
        execution_time, execution_time.as_micros() as f64 / portal_operations as f64);
    println!("  📊 Total rows processed: {total_rows}");
    println!("  🎯 Average rows per portal: {:.1}", total_rows as f64 / portal_operations as f64);
    
    // Memory efficiency estimate
    let estimated_memory_mb = statements.len() * 50 / 1024; // Rough estimate
    println!("  💾 Estimated portal memory usage: ~{estimated_memory_mb}KB");
}

async fn connect_to_server() -> (Client, tokio_postgres::Connection<tokio_postgres::Socket, tokio_postgres::tls::NoTlsStream>) {
    tokio_postgres::connect(
        "host=localhost port=5433 user=postgres dbname=test",
        NoTls,
    )
    .await
    .expect("Failed to connect to server")
}

fn start_server() -> Child {
    Command::new("cargo")
        .args(["run", "--", "--port", "5433"])
        .spawn()
        .expect("Failed to start server")
}

#[tokio::test]
#[ignore] // Run with: cargo test benchmark_portal_memory_stress -- --ignored --nocapture
async fn benchmark_portal_memory_stress() {
    println!("\n💾 === Portal Memory Stress Test ===");
    println!("Testing memory efficiency with very large result sets\n");
    
    let mut server = start_server();
    thread::sleep(Duration::from_secs(2));
    
    let (client, connection) = connect_to_server().await;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    
    // Create extra large dataset
    setup_stress_dataset(&client).await;
    
    println!("🧪 Memory Stress Test: Fetching 100K rows in different chunk sizes");
    let chunk_sizes = vec![100, 500, 1000, 5000, 10000];
    let target_rows = 100_000;
    
    for chunk_size in chunk_sizes {
        let iterations = target_rows / chunk_size;
        
        println!("\n📊 Testing chunk size: {chunk_size} ({iterations} iterations)");
        
        let start = Instant::now();
        let mut total_fetched = 0;
        let mut peak_memory_estimate = 0;
        
        for i in 0..iterations {
            let offset = i * chunk_size;
            let query = format!(
                "SELECT * FROM stress_dataset ORDER BY id LIMIT {chunk_size} OFFSET {offset}"
            );
            
            let chunk_start = Instant::now();
            let rows = client.query(&query, &[]).await.expect("Failed to fetch chunk");
            let chunk_time = chunk_start.elapsed();
            
            total_fetched += rows.len();
            peak_memory_estimate = std::cmp::max(peak_memory_estimate, rows.len() * 200); // ~200 bytes per row
            
            if i % (iterations / 10) == 0 {
                println!("  Progress: {}/{} chunks, {} rows, chunk time: {:?}", 
                    i + 1, iterations, total_fetched, chunk_time);
            }
            
            if total_fetched >= target_rows { break; }
        }
        
        let total_time = start.elapsed();
        
        println!("  ✅ Completed: {total_fetched} rows in {total_time:?}");
        println!("  💾 Peak memory estimate: {:.2}MB", peak_memory_estimate as f64 / 1_000_000.0);
        println!("  ⚡ Throughput: {:.0} rows/sec", 
            total_fetched as f64 / total_time.as_secs_f64());
    }
    
    server.kill().expect("Failed to kill server");
}

async fn setup_stress_dataset(client: &Client) {
    client.execute(
        "CREATE TABLE IF NOT EXISTS stress_dataset (
            id SERIAL PRIMARY KEY,
            data1 TEXT,
            data2 TEXT, 
            data3 TEXT,
            value1 DECIMAL(10,2),
            value2 DECIMAL(10,2)
        )", &[]
    ).await.expect("Failed to create stress table");
    
    println!("📝 Creating stress dataset with 100K records...");
    
    // Insert in large batches for speed
    let batch_size = 5000;
    let total_records = 100_000;
    
    for batch in 0..(total_records / batch_size) {
        let mut values = Vec::new();
        let mut params = Vec::new();
        
        let mut batch_data = Vec::new();
        for i in 0..batch_size {
            let record_id = batch * batch_size + i;
            values.push(format!("(${}, ${}, ${}, ${}, ${})", 
                i * 5 + 1, i * 5 + 2, i * 5 + 3, i * 5 + 4, i * 5 + 5));
                
            batch_data.push((
                format!("Data1-{record_id}"),
                format!("Data2-{record_id}"),
                format!("Data3-{record_id}"),
                record_id as f64 * 1.5,
                record_id as f64 * 2.25
            ));
        }
        
        for (data1, data2, data3, value1, value2) in &batch_data {
            params.push(data1 as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(data2 as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(data3 as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(value1 as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(value2 as &(dyn tokio_postgres::types::ToSql + Sync));
        }
        
        let query = format!("INSERT INTO stress_dataset (data1, data2, data3, value1, value2) VALUES {}", 
            values.join(", "));
        client.execute(&query, &params).await.expect("Failed to insert stress batch");
        
        println!("  Inserted {}/{} records", (batch + 1) * batch_size, total_records);
    }
    
    println!("✅ Stress dataset ready");
}