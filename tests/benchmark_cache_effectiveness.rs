use std::time::{Duration, Instant};
use pgsqlite::session::DbHandler;
use pgsqlite::session::GLOBAL_QUERY_CACHE;

/// Benchmark to measure the effectiveness of the query cache
/// Run with: cargo test benchmark_cache_effectiveness -- --ignored --nocapture
#[test]
#[ignore]
fn benchmark_cache_effectiveness() {
    println!("\n=== Query Cache Effectiveness Benchmark ===\n");
    
    // Initialize database
    let db = DbHandler::new(":memory:").expect("Failed to create DB handler");
    
    // Create test tables - mix of decimal and non-decimal
    let setup_queries = vec![
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)",
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL(10,2))",
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total DECIMAL(10,2))",
        "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)",
    ];
    
    for query in setup_queries {
        let _ = tokio::runtime::Runtime::new().unwrap().block_on(async {
            db.execute(query).await
        });
    }
    
    // Insert test data
    let insert_queries = vec![
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')",
        "INSERT INTO products (id, name, price) VALUES (1, 'Widget', 19.99), (2, 'Gadget', 29.99)",
        "INSERT INTO orders (id, user_id, total) VALUES (1, 1, 49.98), (2, 2, 19.99)",
        "INSERT INTO categories (id, name) VALUES (1, 'Electronics'), (2, 'Home')",
    ];
    
    for query in insert_queries {
        let _ = tokio::runtime::Runtime::new().unwrap().block_on(async {
            db.execute(query).await
        });
    }
    
    // Clear cache to start fresh
    GLOBAL_QUERY_CACHE.clear();
    
    // Test queries - mix of simple and complex, decimal and non-decimal
    let test_queries = vec![
        // Simple queries without decimal
        ("simple_select", "SELECT * FROM users WHERE id = 1"),
        ("simple_multi", "SELECT name, email FROM users WHERE id = 2"),
        ("simple_all", "SELECT * FROM categories"),
        
        // Queries with decimal tables (should trigger rewriting)
        ("decimal_select", "SELECT * FROM products WHERE price > 20"),
        ("decimal_join", "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id"),
        ("decimal_aggregate", "SELECT COUNT(*), SUM(price) FROM products"),
        
        // Complex queries
        ("complex_subquery", "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 20)"),
        ("complex_case", "SELECT name, CASE WHEN id = 1 THEN 'First' ELSE 'Other' END as position FROM users"),
    ];
    
    // Benchmark each query type
    println!("Query Type               | First Run | Avg Cached | Speedup | Hit Rate");
    println!("------------------------|-----------|------------|---------|----------");
    
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    for (query_name, query) in test_queries {
        // Clear metrics for this query
        let initial_metrics = GLOBAL_QUERY_CACHE.get_metrics();
        
        // First run (cache miss)
        let start = Instant::now();
        runtime.block_on(async {
            db.query(query).await.expect("Query failed");
        });
        let first_run_time = start.elapsed();
        
        // Subsequent runs (should hit cache)
        let mut cached_times = Vec::new();
        let runs = 10;
        
        for _ in 0..runs {
            let start = Instant::now();
            runtime.block_on(async {
                db.query(query).await.expect("Query failed");
            });
            cached_times.push(start.elapsed());
        }
        
        // Calculate average cached time
        let avg_cached_time = cached_times.iter().sum::<Duration>() / runs as u32;
        let speedup = first_run_time.as_secs_f64() / avg_cached_time.as_secs_f64();
        
        // Get cache metrics
        let final_metrics = GLOBAL_QUERY_CACHE.get_metrics();
        let queries_for_this_test = final_metrics.total_queries - initial_metrics.total_queries;
        let hits_for_this_test = final_metrics.cache_hits - initial_metrics.cache_hits;
        let hit_rate = if queries_for_this_test > 0 {
            (hits_for_this_test as f64 / queries_for_this_test as f64) * 100.0
        } else {
            0.0
        };
        
        println!("{:<24} | {:>9.3}ms | {:>10.3}ms | {:>7.1}x | {:>8.1}%",
            query_name,
            first_run_time.as_secs_f64() * 1000.0,
            avg_cached_time.as_secs_f64() * 1000.0,
            speedup,
            hit_rate
        );
    }
    
    // Overall cache statistics
    println!("\n=== Overall Cache Statistics ===");
    let final_metrics = GLOBAL_QUERY_CACHE.get_metrics();
    let overall_hit_rate = if final_metrics.total_queries > 0 {
        (final_metrics.cache_hits as f64 / final_metrics.total_queries as f64) * 100.0
    } else {
        0.0
    };
    
    println!("Total queries: {}", final_metrics.total_queries);
    println!("Cache hits: {}", final_metrics.cache_hits);
    println!("Cache misses: {}", final_metrics.cache_misses);
    println!("Overall hit rate: {:.1}%", overall_hit_rate);
    println!("Evictions: {}", final_metrics.evictions);
    
    // Test query normalization effectiveness
    println!("\n=== Query Normalization Test ===");
    
    // Clear cache for normalization test
    GLOBAL_QUERY_CACHE.clear();
    
    let normalized_queries = vec![
        "SELECT * FROM users WHERE id = 1",
        "select * from users where id = 1",
        "SELECT  *  FROM  users  WHERE  id = 1",
        "SeLeCt * FrOm users WhErE id = 1",
    ];
    
    let mut normalization_times = Vec::new();
    for (i, query) in normalized_queries.iter().enumerate() {
        let start = Instant::now();
        runtime.block_on(async {
            db.query(query).await.expect("Query failed");
        });
        let elapsed = start.elapsed();
        normalization_times.push(elapsed);
        
        let metrics = GLOBAL_QUERY_CACHE.get_metrics();
        println!("Query {}: {:>7.3}ms (Total hits: {})", 
            i + 1, 
            elapsed.as_secs_f64() * 1000.0,
            metrics.cache_hits
        );
    }
    
    // First query should be slowest, others should be fast
    let first_time = normalization_times[0];
    let avg_normalized_time = normalization_times[1..].iter().sum::<Duration>() / 3;
    let normalization_speedup = first_time.as_secs_f64() / avg_normalized_time.as_secs_f64();
    
    println!("\nNormalization effectiveness:");
    println!("First query: {:.3}ms", first_time.as_secs_f64() * 1000.0);
    println!("Avg normalized: {:.3}ms", avg_normalized_time.as_secs_f64() * 1000.0);
    println!("Speedup: {:.1}x", normalization_speedup);
}

/// Benchmark to compare performance with and without cache
#[test]
#[ignore]
fn benchmark_cache_disabled_comparison() {
    println!("\n=== Cache Enabled vs Disabled Comparison ===\n");
    
    let runtime = tokio::runtime::Runtime::new().unwrap();
    
    // Test queries
    let queries = vec![
        ("Simple SELECT", "SELECT * FROM test_table WHERE id = 1"),
        ("SELECT with JOIN", "SELECT t1.*, t2.value FROM test_table t1 JOIN test_table2 t2 ON t1.id = t2.id"),
        ("Aggregate", "SELECT COUNT(*), AVG(value) FROM test_table2"),
    ];
    
    // Run with cache enabled
    println!("With Cache Enabled:");
    println!("Query Type          | Iterations | Total Time | Avg Time");
    println!("--------------------|------------|------------|----------");
    
    for (query_name, query) in &queries {
        let db = DbHandler::new(":memory:").expect("Failed to create DB handler");
        
        // Setup
        runtime.block_on(async {
            db.execute("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
            db.execute("CREATE TABLE test_table2 (id INTEGER PRIMARY KEY, value DECIMAL(10,2))").await.unwrap();
            db.execute("INSERT INTO test_table VALUES (1, 'test'), (2, 'test2')").await.unwrap();
            db.execute("INSERT INTO test_table2 VALUES (1, 10.50), (2, 20.75)").await.unwrap();
        });
        
        GLOBAL_QUERY_CACHE.clear();
        
        let iterations = 100;
        let start = Instant::now();
        
        for _ in 0..iterations {
            runtime.block_on(async {
                db.query(query).await.expect("Query failed");
            });
        }
        
        let total_time = start.elapsed();
        let avg_time = total_time / iterations as u32;
        
        println!("{:<20} | {:>10} | {:>10.2}ms | {:>8.3}ms",
            query_name,
            iterations,
            total_time.as_secs_f64() * 1000.0,
            avg_time.as_secs_f64() * 1000.0
        );
    }
    
    // Get cache stats
    let metrics = GLOBAL_QUERY_CACHE.get_metrics();
    println!("\nCache statistics:");
    println!("Hit rate: {:.1}%", (metrics.cache_hits as f64 / metrics.total_queries as f64) * 100.0);
    
    // Note: To truly test with cache disabled, you would need to modify the code
    // to bypass the cache. This comparison shows the benefit of having the cache.
}