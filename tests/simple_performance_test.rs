use std::time::Instant;
use rusqlite::{params, Connection};

/// Test to validate ultra-fast path optimization
#[test]
fn test_ultra_fast_path() {
    // Create in-memory SQLite database
    let conn = Connection::open(":memory:").unwrap();
    
    // Create simple table
    conn.execute(
        "CREATE TABLE test_table (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            value INTEGER NOT NULL
        )",
        [],
    ).unwrap();
    
    // Insert test data
    for i in 0..100 {
        conn.execute(
            "INSERT INTO test_table (id, name, value) VALUES (?1, ?2, ?3)",
            params![i, format!("name_{}", i), i * 10],
        ).unwrap();
    }
    
    // Test queries
    let simple_queries = vec![
        "SELECT * FROM test_table WHERE id = 50",
        "SELECT * FROM test_table LIMIT 10",
        "INSERT INTO test_table (id, name, value) VALUES (1000, 'test', 100)",
        "UPDATE test_table SET value = 200 WHERE id = 50",
        "DELETE FROM test_table WHERE id = 1000",
    ];
    
    let complex_queries = vec![
        "SELECT * FROM test_table WHERE id::text = '50'",
        "SELECT * FROM test_table WHERE created_at > NOW()",
        "SELECT * FROM test_table JOIN other_table",
    ];
    
    // Check that simple queries are detected correctly
    for query in &simple_queries {
        assert!(
            pgsqlite::query::simple_query_detector::is_ultra_simple_query(query),
            "Query should be detected as ultra-simple: {}", query
        );
    }
    
    // Check that complex queries are not detected as simple
    for query in &complex_queries {
        assert!(
            !pgsqlite::query::simple_query_detector::is_ultra_simple_query(query),
            "Query should NOT be detected as ultra-simple: {}", query
        );
    }
    
    // Benchmark simple query execution
    let start = Instant::now();
    for _ in 0..1000 {
        let mut stmt = conn.prepare("SELECT * FROM test_table WHERE id = 50").unwrap();
        let _rows: Vec<(i32, String, i32)> = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    }
    let elapsed = start.elapsed();
    println!("SQLite direct: 1000 simple SELECTs in {:?} ({:.2}µs avg)", 
        elapsed, elapsed.as_micros() as f64 / 1000.0);
}