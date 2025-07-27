use std::time::{Instant, Duration};
use tokio_postgres::{NoTls, Client};
use std::process::{Command, Child};
use std::thread;

/// Realistic portal usage scenarios benchmark
/// Tests common patterns where portal management provides significant benefits
#[tokio::test]
#[ignore] // Run with: cargo test benchmark_portal_realistic -- --ignored --nocapture
async fn benchmark_realistic_portal_scenarios() {
    println!("\n🎯 === Realistic Portal Usage Scenarios Benchmark ===");
    println!("Testing real-world scenarios where portal management provides clear benefits\n");
    
    let mut server = start_server();
    thread::sleep(Duration::from_secs(2));
    
    let (client, connection) = connect_to_server().await;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    
    // Setup realistic dataset
    setup_realistic_dataset(&client).await;
    
    // Scenario 1: Data Export with Memory Constraints
    benchmark_data_export_scenario(&client).await;
    
    // Scenario 2: Paginated Web API Responses  
    benchmark_pagination_scenario(&client).await;
    
    // Scenario 3: Report Generation with Large Datasets
    benchmark_report_generation_scenario(&client).await;
    
    // Scenario 4: ETL Processing with Streaming
    benchmark_etl_streaming_scenario(&client).await;
    
    // Scenario 5: Multi-tenant Concurrent Access
    benchmark_multitenant_scenario().await;
    
    println!("✅ Realistic Portal Scenarios Benchmark Complete\n");
    server.kill().expect("Failed to kill server");
}

async fn setup_realistic_dataset(client: &Client) {
    println!("🔧 Setting up realistic business dataset...");
    
    // Orders table - typical e-commerce scenario
    client.execute(
        "CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            customer_id INTEGER,
            order_date TIMESTAMP DEFAULT NOW(),
            total_amount DECIMAL(12,2),
            status VARCHAR(50),
            shipping_address TEXT,
            items_json JSONB
        )", &[]
    ).await.expect("Failed to create orders table");
    
    // Products table
    client.execute(
        "CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            sku VARCHAR(100),
            name TEXT,
            category VARCHAR(100),
            price DECIMAL(10,2),
            description TEXT,
            inventory_count INTEGER
        )", &[]
    ).await.expect("Failed to create products table");
    
    // Customer table
    client.execute(
        "CREATE TABLE IF NOT EXISTS customers (
            id SERIAL PRIMARY KEY,
            email VARCHAR(255),
            name VARCHAR(255),
            registration_date TIMESTAMP DEFAULT NOW(),
            total_orders INTEGER DEFAULT 0,
            lifetime_value DECIMAL(12,2) DEFAULT 0
        )", &[]
    ).await.expect("Failed to create customers table");
    
    // Insert realistic test data
    println!("📝 Inserting realistic test data...");
    
    // Insert 10,000 customers
    for batch in 0..100 {
        let mut values = Vec::new();
        let mut params = Vec::new();
        
        let mut email_strings = Vec::new();
        let mut name_strings = Vec::new();
        let mut total_orders_values = Vec::new();
        
        for i in 0..100 {
            let customer_id = batch * 100 + i;
            values.push(format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
            
            email_strings.push(format!("customer{customer_id}@example.com"));
            name_strings.push(format!("Customer {customer_id}"));
            total_orders_values.push(customer_id * 5 + 100);
        }
        
        for i in 0..100 {
            params.push(&email_strings[i] as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(&name_strings[i] as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(&total_orders_values[i] as &(dyn tokio_postgres::types::ToSql + Sync));
        }
        
        let query = format!("INSERT INTO customers (email, name, total_orders) VALUES {}", values.join(", "));
        client.execute(&query, &params).await.expect("Failed to insert customers");
    }
    
    // Insert 5,000 products
    let categories = ["Electronics", "Books", "Clothing", "Home", "Sports"];
    for batch in 0..50 {
        let mut values = Vec::new();
        let mut params = Vec::new();
        
        let mut sku_strings = Vec::new();
        let mut name_strings = Vec::new();
        let mut category_refs = Vec::new();
        let mut price_values = Vec::new();
        
        for i in 0..100 {
            let product_id = batch * 100 + i;
            values.push(format!("(${}, ${}, ${}, ${})", i * 4 + 1, i * 4 + 2, i * 4 + 3, i * 4 + 4));
            
            sku_strings.push(format!("SKU-{product_id:06}"));
            name_strings.push(format!("Product {product_id}"));
            category_refs.push(&categories[product_id % categories.len()]);
            price_values.push((product_id % 500 + 10) as f64 * 1.99);
        }
        
        for i in 0..100 {
            params.push(&sku_strings[i] as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(&name_strings[i] as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(category_refs[i] as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(&price_values[i] as &(dyn tokio_postgres::types::ToSql + Sync));
        }
        
        let query = format!("INSERT INTO products (sku, name, category, price) VALUES {}", values.join(", "));
        client.execute(&query, &params).await.expect("Failed to insert products");
    }
    
    // Insert 50,000 orders
    for batch in 0..500 {
        let mut values = Vec::new();
        let mut params = Vec::new();
        
        let mut customer_ids = Vec::new();
        let mut total_amounts = Vec::new();
        let status = "completed";
        
        for i in 0..100 {
            let order_id = batch * 100 + i;
            values.push(format!("(${}, ${}, ${})", i * 3 + 1, i * 3 + 2, i * 3 + 3));
            
            customer_ids.push((order_id % 10000) + 1);
            total_amounts.push((order_id % 1000 + 50) as f64 * 2.99);
        }
        
        for i in 0..100 {
            params.push(&customer_ids[i] as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(&total_amounts[i] as &(dyn tokio_postgres::types::ToSql + Sync));
            params.push(&status as &(dyn tokio_postgres::types::ToSql + Sync));
        }
        
        let query = format!("INSERT INTO orders (customer_id, total_amount, status) VALUES {}", values.join(", "));
        client.execute(&query, &params).await.expect("Failed to insert orders");
        
        if batch % 50 == 0 {
            println!("  Inserted {} orders...", (batch + 1) * 100);
        }
    }
    
    println!("✅ Realistic dataset ready: 10K customers, 5K products, 50K orders");
}

async fn benchmark_data_export_scenario(client: &Client) {
    println!("\n📦 === Data Export Scenario ===");
    println!("Scenario: Exporting large customer dataset to CSV with memory constraints\n");
    
    // Scenario: Export all customers with their order history
    // Memory constraint: Can only hold 1000 records in memory at once
    
    let export_query = "
        SELECT c.id, c.name, c.email, c.total_orders, 
               COALESCE(SUM(o.total_amount), 0) as lifetime_value
        FROM customers c 
        LEFT JOIN orders o ON c.id = o.customer_id 
        GROUP BY c.id, c.name, c.email, c.total_orders
        ORDER BY c.id";
    
    println!("📋 Test 1: Traditional Export (Load all data into memory)");
    let start = Instant::now();
    let all_customers = client.query(export_query, &[]).await.expect("Failed to export all customers");
    let full_export_time = start.elapsed();
    let total_customers = all_customers.len();
    
    println!("  ⏱️  Full export time: {full_export_time:?}");
    println!("  📊 Customers exported: {total_customers}");
    println!("  💾 Memory used: ~{:.2} MB\n", (total_customers * 300) as f64 / 1_000_000.0);
    
    println!("📋 Test 2: Portal-based Streaming Export (1000 records at a time)");
    let chunk_size = 1000;
    let _stmt = client.prepare(export_query).await.expect("Failed to prepare export statement");
    
    let start = Instant::now();
    let mut exported_count = 0;
    let mut chunks_processed = 0;
    let mut total_processing_time = Duration::ZERO;
    
    // Simulate processing in chunks (like fetching with max_rows in Extended Protocol)
    loop {
        let chunk_start = Instant::now();
        
        // In real portal usage, this would be an Execute message with max_rows=1000
        let chunk_query = format!("{} LIMIT {} OFFSET {}", 
            export_query.replace('\n', " "), chunk_size, exported_count);
        let chunk_customers = client.query(&chunk_query, &[]).await
            .expect("Failed to fetch chunk");
        
        if chunk_customers.is_empty() { break; }
        
        // Simulate processing the chunk (writing to file, transforming data, etc.)
        let processing_start = Instant::now();
        thread::sleep(Duration::from_millis(10)); // Simulate processing time
        total_processing_time += processing_start.elapsed();
        
        exported_count += chunk_customers.len();
        chunks_processed += 1;
        let chunk_time = chunk_start.elapsed();
        
        if chunks_processed <= 5 || chunks_processed % 5 == 0 {
            println!("  Chunk {}: {} customers in {:?} (total: {})", 
                chunks_processed, chunk_customers.len(), chunk_time, exported_count);
        }
        
        if chunk_customers.len() < chunk_size { break; }
    }
    
    let streaming_export_time = start.elapsed();
    
    println!("\n📈 Data Export Comparison:");
    println!("  📦 Full Export:      {:?} ({} customers, ~{:.2}MB memory)", 
        full_export_time, total_customers, (total_customers * 300) as f64 / 1_000_000.0);
    println!("  🚀 Streaming Export: {:?} ({} customers, ~{:.2}MB peak memory)", 
        streaming_export_time, exported_count, (chunk_size * 300) as f64 / 1_000_000.0);
    println!("  💾 Memory reduction: {:.1}%", 
        (1.0 - (chunk_size as f64 / total_customers as f64)) * 100.0);
    println!("  ⚡ Processing time: {total_processing_time:?} ({chunks_processed} chunks)");
    
    if streaming_export_time < full_export_time {
        println!("  🎯 Streaming is {:.1}x faster overall", 
            full_export_time.as_secs_f64() / streaming_export_time.as_secs_f64());
    } else {
        println!("  ⚠️  Streaming is {:.1}x slower but uses {:.1}% less memory", 
            streaming_export_time.as_secs_f64() / full_export_time.as_secs_f64(),
            (1.0 - (chunk_size as f64 / total_customers as f64)) * 100.0);
    }
}

async fn benchmark_pagination_scenario(client: &Client) {
    println!("\n📄 === Pagination Scenario ===");
    println!("Scenario: Web API serving paginated product listings to multiple users\n");
    
    let page_size = 20;
    let total_pages = 10;
    
    println!("📋 Simulating {total_pages} API requests for paginated product listings (page size: {page_size})");
    
    let base_query = "SELECT id, name, category, price FROM products ORDER BY name";
    
    // Test traditional approach: Execute query for each page separately
    println!("\n🔄 Traditional Approach: Separate query per page");
    let start = Instant::now();
    let mut traditional_total_rows = 0;
    
    for page in 0..total_pages {
        let offset = page * page_size;
        let page_query = format!("{base_query} LIMIT {page_size} OFFSET {offset}");
        
        let page_start = Instant::now();
        let rows = client.query(&page_query, &[]).await.expect("Failed to fetch page");
        let page_time = page_start.elapsed();
        
        traditional_total_rows += rows.len();
        
        if page < 3 || page % 3 == 0 {
            println!("  Page {}: {} products in {:?}", page + 1, rows.len(), page_time);
        }
    }
    
    let traditional_time = start.elapsed();
    
    // Test portal approach: Prepare once, execute multiple times with different offsets
    println!("\n🚀 Portal Approach: Prepared statement with parameter binding");
    let stmt = client.prepare(&format!("{base_query} LIMIT $1 OFFSET $2"))
        .await.expect("Failed to prepare paginated query");
    
    let start = Instant::now();
    let mut portal_total_rows = 0;
    
    for page in 0..total_pages {
        let offset = page * page_size;
        
        let page_start = Instant::now();
        let rows = client.query(&stmt, &[&{ page_size }, &offset])
            .await.expect("Failed to fetch page with portal");
        let page_time = page_start.elapsed();
        
        portal_total_rows += rows.len();
        
        if page < 3 || page % 3 == 0 {
            println!("  Page {}: {} products in {:?}", page + 1, rows.len(), page_time);
        }
    }
    
    let portal_time = start.elapsed();
    
    println!("\n📈 Pagination Performance Comparison:");
    println!("  🔄 Traditional: {traditional_time:?} ({traditional_total_rows} total products)");
    println!("  🚀 Portal:      {portal_time:?} ({portal_total_rows} total products)");
    
    if portal_time < traditional_time {
        println!("  🎯 Portal approach is {:.1}x faster", 
            traditional_time.as_secs_f64() / portal_time.as_secs_f64());
    } else {
        println!("  ⚠️  Traditional approach is {:.1}x faster", 
            portal_time.as_secs_f64() / traditional_time.as_secs_f64());
    }
    
    let traditional_avg = traditional_time.as_micros() / total_pages as u128;
    let portal_avg = portal_time.as_micros() / total_pages as u128;
    println!("  📊 Average per page: Traditional {:.2}ms, Portal {:.2}ms", 
        traditional_avg as f64 / 1000.0, portal_avg as f64 / 1000.0);
}

async fn benchmark_report_generation_scenario(client: &Client) {
    println!("\n📊 === Report Generation Scenario ===");
    println!("Scenario: Generating business reports with large datasets and complex queries\n");
    
    // Complex analytical query for business reporting
    let report_query = "
        SELECT 
            DATE_TRUNC('month', o.order_date) as month,
            p.category,
            COUNT(o.id) as order_count,
            SUM(o.total_amount) as total_revenue,
            AVG(o.total_amount) as avg_order_value,
            COUNT(DISTINCT o.customer_id) as unique_customers
        FROM orders o
        JOIN products p ON p.id = (o.customer_id % 5000 + 1) -- Simulate product lookup
        WHERE o.order_date >= NOW() - INTERVAL '12 months'
        GROUP BY DATE_TRUNC('month', o.order_date), p.category
        ORDER BY month DESC, total_revenue DESC";
    
    println!("📈 Generating Monthly Revenue Report by Category");
    
    // Test full report generation
    println!("\n📋 Full Report Generation:");
    let start = Instant::now();
    let report_data = client.query(report_query, &[]).await
        .expect("Failed to generate report");
    let full_report_time = start.elapsed();
    
    println!("  ⏱️  Report generation time: {full_report_time:?}");
    println!("  📊 Report rows: {}", report_data.len());
    println!("  💾 Memory used: ~{:.2} MB\n", (report_data.len() * 250) as f64 / 1_000_000.0);
    
    // Test streaming report generation (useful for large reports)
    println!("📋 Streaming Report Generation (process in chunks):");
    let chunk_size = 50;
    let _stmt = client.prepare(report_query).await.expect("Failed to prepare report query");
    
    let start = Instant::now();
    let mut processed_rows = 0;
    let mut chunk_count = 0;
    
    // Simulate processing report data in chunks
    loop {
        let chunk_query = format!("{} LIMIT {} OFFSET {}", 
            report_query.replace('\n', " "), chunk_size, processed_rows);
        
        let chunk_start = Instant::now();
        let chunk_data = client.query(&chunk_query, &[]).await
            .expect("Failed to fetch report chunk");
        
        if chunk_data.is_empty() { break; }
        
        // Simulate processing chunk (formatting, calculations, writing to output)
        thread::sleep(Duration::from_millis(5));
        
        processed_rows += chunk_data.len();
        chunk_count += 1;
        let chunk_time = chunk_start.elapsed();
        
        println!("  Chunk {}: {} rows in {:?} (total: {})", 
            chunk_count, chunk_data.len(), chunk_time, processed_rows);
        
        if chunk_data.len() < chunk_size { break; }
    }
    
    let streaming_report_time = start.elapsed();
    
    println!("\n📈 Report Generation Comparison:");
    println!("  📊 Full Report:      {:?} ({} rows)", full_report_time, report_data.len());
    println!("  🚀 Streaming Report: {streaming_report_time:?} ({processed_rows} rows, {chunk_count} chunks)");
    println!("  💾 Memory reduction: {:.1}%", 
        (1.0 - (chunk_size as f64 / report_data.len() as f64)) * 100.0);
    
    // Calculate throughput
    let full_throughput = report_data.len() as f64 / full_report_time.as_secs_f64();
    let streaming_throughput = processed_rows as f64 / streaming_report_time.as_secs_f64();
    
    println!("  ⚡ Throughput: Full {full_throughput:.0} rows/sec, Streaming {streaming_throughput:.0} rows/sec");
}

async fn benchmark_etl_streaming_scenario(client: &Client) {
    println!("\n🔄 === ETL Streaming Scenario ===");
    println!("Scenario: ETL process streaming data transformation with limited memory\n");
    
    // Simulate ETL: Extract order data, transform, and load into summary table
    client.execute(
        "CREATE TABLE IF NOT EXISTS order_summary (
            customer_id INTEGER,
            total_orders INTEGER,
            total_spent DECIMAL(12,2),
            avg_order_value DECIMAL(10,2),
            last_order_date TIMESTAMP
        )", &[]
    ).await.expect("Failed to create summary table");
    
    let etl_query = "
        SELECT 
            customer_id,
            COUNT(*) as order_count,
            SUM(total_amount) as total_spent,
            AVG(total_amount) as avg_order_value,
            MAX(order_date) as last_order_date
        FROM orders 
        GROUP BY customer_id 
        ORDER BY customer_id";
    
    println!("📊 ETL Process: Orders -> Customer Summary");
    
    // Traditional ETL: Load all data, process, insert
    println!("\n📋 Traditional ETL (Load all data at once):");
    let start = Instant::now();
    
    let all_data = client.query(etl_query, &[]).await.expect("Failed to extract data");
    let _extract_time = start.elapsed();
    
    // Simulate transformation and loading
    let transform_start = Instant::now();
    let mut insert_values = Vec::new();
    let mut insert_params = Vec::new();
    
    let mut extracted_values: Vec<(i32, i64, rust_decimal::Decimal, rust_decimal::Decimal, chrono::DateTime<chrono::Utc>)> = Vec::new();
    
    for (i, row) in all_data.iter().enumerate() {
        insert_values.push(format!("(${}, ${}, ${}, ${}, ${})", 
            i * 5 + 1, i * 5 + 2, i * 5 + 3, i * 5 + 4, i * 5 + 5));
        
        let val0 = row.get::<_, i32>(0);
        let val1 = row.get::<_, i64>(1);
        let val2 = row.get::<_, rust_decimal::Decimal>(2);
        let val3 = row.get::<_, rust_decimal::Decimal>(3);
        let val4 = row.get::<_, chrono::DateTime<chrono::Utc>>(4);
        
        extracted_values.push((val0, val1, val2, val3, val4));
    }
    
    for (val0, val1, val2, val3, val4) in &extracted_values {
        insert_params.push(val0 as &(dyn tokio_postgres::types::ToSql + Sync));
        insert_params.push(val1 as &(dyn tokio_postgres::types::ToSql + Sync));
        insert_params.push(val2 as &(dyn tokio_postgres::types::ToSql + Sync));
        insert_params.push(val3 as &(dyn tokio_postgres::types::ToSql + Sync));
        insert_params.push(val4 as &(dyn tokio_postgres::types::ToSql + Sync));
    }
    
    if !insert_values.is_empty() {
        let insert_query = format!("INSERT INTO order_summary VALUES {}", insert_values.join(", "));
        client.execute(&insert_query, &insert_params).await.expect("Failed to load data");
    }
    
    let traditional_etl_time = start.elapsed();
    let _transform_load_time = transform_start.elapsed();
    
    println!("  ⏱️  Total ETL time: {traditional_etl_time:?}");
    println!("  📊 Records processed: {}", all_data.len());
    println!("  💾 Peak memory: ~{:.2} MB\n", (all_data.len() * 200) as f64 / 1_000_000.0);
    
    // Clean up for streaming test
    client.execute("DELETE FROM order_summary", &[]).await.expect("Failed to clean summary");
    
    // Streaming ETL: Process in chunks
    println!("📋 Streaming ETL (Process in chunks of 500):");
    let chunk_size = 500;
    
    let start = Instant::now();
    let mut processed_records = 0;
    let mut chunk_count = 0;
    
    loop {
        let chunk_query = format!("{} LIMIT {} OFFSET {}", 
            etl_query.replace('\n', " "), chunk_size, processed_records);
        
        let chunk_start = Instant::now();
        let chunk_data = client.query(&chunk_query, &[]).await.expect("Failed to extract chunk");
        
        if chunk_data.is_empty() { break; }
        
        // Transform and load chunk
        let mut chunk_values = Vec::new();
        let mut chunk_params = Vec::new();
        
        let mut chunk_extracted_values: Vec<(i32, i64, rust_decimal::Decimal, rust_decimal::Decimal, chrono::DateTime<chrono::Utc>)> = Vec::new();
        
        for (i, row) in chunk_data.iter().enumerate() {
            chunk_values.push(format!("(${}, ${}, ${}, ${}, ${})", 
                i * 5 + 1, i * 5 + 2, i * 5 + 3, i * 5 + 4, i * 5 + 5));
            
            let val0 = row.get::<_, i32>(0);
            let val1 = row.get::<_, i64>(1);
            let val2 = row.get::<_, rust_decimal::Decimal>(2);
            let val3 = row.get::<_, rust_decimal::Decimal>(3);
            let val4 = row.get::<_, chrono::DateTime<chrono::Utc>>(4);
            
            chunk_extracted_values.push((val0, val1, val2, val3, val4));
        }
        
        for (val0, val1, val2, val3, val4) in &chunk_extracted_values {
            chunk_params.push(val0 as &(dyn tokio_postgres::types::ToSql + Sync));
            chunk_params.push(val1 as &(dyn tokio_postgres::types::ToSql + Sync));
            chunk_params.push(val2 as &(dyn tokio_postgres::types::ToSql + Sync));
            chunk_params.push(val3 as &(dyn tokio_postgres::types::ToSql + Sync));
            chunk_params.push(val4 as &(dyn tokio_postgres::types::ToSql + Sync));
        }
        
        let chunk_insert = format!("INSERT INTO order_summary VALUES {}", chunk_values.join(", "));
        client.execute(&chunk_insert, &chunk_params).await.expect("Failed to load chunk");
        
        processed_records += chunk_data.len();
        chunk_count += 1;
        let chunk_time = chunk_start.elapsed();
        
        if chunk_count <= 5 || chunk_count % 5 == 0 {
            println!("  Chunk {}: {} records in {:?} (total: {})", 
                chunk_count, chunk_data.len(), chunk_time, processed_records);
        }
        
        if chunk_data.len() < chunk_size { break; }
    }
    
    let streaming_etl_time = start.elapsed();
    
    println!("\n📈 ETL Performance Comparison:");
    println!("  📊 Traditional ETL: {:?} ({} records)", traditional_etl_time, all_data.len());
    println!("  🚀 Streaming ETL:   {streaming_etl_time:?} ({processed_records} records, {chunk_count} chunks)");
    println!("  💾 Memory reduction: {:.1}%", 
        (1.0 - (chunk_size as f64 / all_data.len() as f64)) * 100.0);
    
    let traditional_throughput = all_data.len() as f64 / traditional_etl_time.as_secs_f64();
    let streaming_throughput = processed_records as f64 / streaming_etl_time.as_secs_f64();
    
    println!("  ⚡ Throughput: Traditional {traditional_throughput:.0} records/sec, Streaming {streaming_throughput:.0} records/sec");
}

async fn benchmark_multitenant_scenario() {
    println!("\n🏢 === Multi-tenant Concurrent Access Scenario ===");
    println!("Scenario: Multiple tenant applications accessing data concurrently\n");
    
    let tenant_count = 5;
    let queries_per_tenant = 20;
    
    println!("👥 Simulating {tenant_count} tenants each running {queries_per_tenant} queries concurrently");
    
    let mut handles = Vec::new();
    let start = Instant::now();
    
    for tenant_id in 0..tenant_count {
        let handle = tokio::spawn(async move {
            // Each tenant gets its own connection
            let (client, connection) = connect_to_server().await;
            tokio::spawn(async move {
                let _ = connection.await;
            });
            
            let tenant_start = Instant::now();
            let mut tenant_results = Vec::new();
            
            // Each tenant runs multiple queries (simulating concurrent user requests)
            for query_id in 0..queries_per_tenant {
                let query_start = Instant::now();
                
                // Different query types per tenant
                let result = match query_id % 4 {
                    0 => {
                        // Customer lookup
                        let rows = client.query(
                            &format!("SELECT * FROM customers WHERE id % {tenant_count} = {tenant_id} LIMIT 10"),
                            &[]
                        ).await.expect("Customer query failed");
                        rows.len()
                    },
                    1 => {
                        // Order history  
                        let rows = client.query(
                            &format!("SELECT * FROM orders WHERE customer_id % {tenant_count} = {tenant_id} ORDER BY order_date DESC LIMIT 20"),
                            &[]
                        ).await.expect("Orders query failed");
                        rows.len()
                    },
                    2 => {
                        // Product catalog
                        let rows = client.query("SELECT * FROM products WHERE price > $1 LIMIT 15", 
                            &[&((tenant_id * 10 + 50) as f64)]
                        ).await.expect("Products query failed");
                        rows.len()
                    },
                    _ => {
                        // Analytics query
                        let rows = client.query(
                            &format!("SELECT category, COUNT(*), AVG(price) FROM products WHERE id % {tenant_count} = {tenant_id} GROUP BY category"),
                            &[]
                        ).await.expect("Analytics query failed");
                        rows.len()
                    }
                };
                
                let query_time = query_start.elapsed();
                tenant_results.push((query_id, result, query_time));
                
                // Small delay to simulate real usage patterns
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            
            let tenant_total_time = tenant_start.elapsed();
            (tenant_id, tenant_results, tenant_total_time)
        });
        
        handles.push(handle);
    }
    
    // Wait for all tenants to complete
    let mut all_results = Vec::new();
    for handle in handles {
        let result = handle.await.expect("Tenant task failed");
        all_results.push(result);
    }
    
    let total_concurrent_time = start.elapsed();
    
    // Analyze results
    println!("\n📊 Multi-tenant Performance Results:");
    
    let mut total_queries = 0;
    let mut total_rows = 0;
    let mut fastest_tenant = Duration::MAX;
    let mut slowest_tenant = Duration::ZERO;
    let mut total_tenant_time = Duration::ZERO;
    
    for (tenant_id, results, tenant_time) in &all_results {
        let tenant_queries = results.len();
        let tenant_rows: usize = results.iter().map(|(_, rows, _)| *rows).sum();
        let avg_query_time = tenant_time.as_micros() / tenant_queries as u128;
        
        total_queries += tenant_queries;
        total_rows += tenant_rows;
        total_tenant_time += *tenant_time;
        fastest_tenant = fastest_tenant.min(*tenant_time);
        slowest_tenant = slowest_tenant.max(*tenant_time);
        
        println!("  Tenant {}: {} queries, {} rows, {:?} total ({:.1}μs avg/query)", 
            tenant_id, tenant_queries, tenant_rows, tenant_time, avg_query_time as f64);
    }
    
    println!("\n📈 Concurrency Analysis:");
    println!("  🏁 Total concurrent time: {total_concurrent_time:?}");
    println!("  ⚡ Total sequential time: {total_tenant_time:?}");
    println!("  🚀 Concurrency speedup: {:.1}x", 
        total_tenant_time.as_secs_f64() / total_concurrent_time.as_secs_f64());
    println!("  📊 Total queries: {total_queries}, Total rows: {total_rows}");
    println!("  ⏱️  Fastest tenant: {fastest_tenant:?}, Slowest: {slowest_tenant:?}");
    println!("  🎯 Throughput: {:.0} queries/sec, {:.0} rows/sec", 
        total_queries as f64 / total_concurrent_time.as_secs_f64(),
        total_rows as f64 / total_concurrent_time.as_secs_f64());
    
    // Portal efficiency metric
    let avg_queries_per_second = total_queries as f64 / total_concurrent_time.as_secs_f64();
    println!("  🎪 Portal efficiency: {avg_queries_per_second:.0} concurrent queries/sec across {tenant_count} tenants");
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