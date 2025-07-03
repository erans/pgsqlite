use std::time::Instant;

#[test]
fn test_query_type_detection_performance() {
    let queries = vec![
        "SELECT * FROM users WHERE id = $1",
        "INSERT INTO users (name, email) VALUES ($1, $2)",
        "UPDATE users SET name = $1 WHERE id = $2",
        "DELETE FROM users WHERE id = $1",
        "select * from products where price > $1",
        "insert into orders (user_id, total) values ($1, $2)",
        "update inventory set quantity = $1 where product_id = $2",
        "delete from sessions where expired = true",
    ];
    
    // Warm up
    for query in &queries {
        let _ = detect_query_type_old(query);
        let _ = detect_query_type_new(query);
    }
    
    // Benchmark old approach
    let start = Instant::now();
    for _ in 0..100_000 {
        for query in &queries {
            let _ = detect_query_type_old(query);
        }
    }
    let old_duration = start.elapsed();
    
    // Benchmark new approach
    let start = Instant::now();
    for _ in 0..100_000 {
        for query in &queries {
            let _ = detect_query_type_new(query);
        }
    }
    let new_duration = start.elapsed();
    
    println!("Old approach (to_uppercase): {:?}", old_duration);
    println!("New approach (byte comparison): {:?}", new_duration);
    println!("Speedup: {:.1}x", old_duration.as_secs_f64() / new_duration.as_secs_f64());
    
    // Verify they produce the same results
    for query in &queries {
        assert_eq!(detect_query_type_old(query), detect_query_type_new(query));
    }
}

#[derive(Debug, PartialEq)]
enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
    Other,
}

// Old approach with to_uppercase()
fn detect_query_type_old(query: &str) -> QueryType {
    let query_upper = query.trim().to_uppercase();
    if query_upper.starts_with("SELECT") {
        QueryType::Select
    } else if query_upper.starts_with("INSERT") {
        QueryType::Insert
    } else if query_upper.starts_with("UPDATE") {
        QueryType::Update
    } else if query_upper.starts_with("DELETE") {
        QueryType::Delete
    } else {
        QueryType::Other
    }
}

// New optimized approach
fn detect_query_type_new(query: &str) -> QueryType {
    let query_trimmed = query.trim();
    let query_bytes = query_trimmed.as_bytes();
    
    if query_bytes.len() >= 6 {
        match &query_bytes[0..6] {
            b"SELECT" | b"select" | b"Select" => return QueryType::Select,
            b"INSERT" | b"insert" | b"Insert" => return QueryType::Insert,
            b"UPDATE" | b"update" | b"Update" => return QueryType::Update,
            b"DELETE" | b"delete" | b"Delete" => return QueryType::Delete,
            _ => {}
        }
    }
    
    // Fallback for mixed case or shorter queries
    let query_start = &query_trimmed[..query_trimmed.len().min(6)];
    if query_start.eq_ignore_ascii_case("SELECT") {
        QueryType::Select
    } else if query_start.eq_ignore_ascii_case("INSERT") {
        QueryType::Insert
    } else if query_start.eq_ignore_ascii_case("UPDATE") {
        QueryType::Update
    } else if query_start.eq_ignore_ascii_case("DELETE") {
        QueryType::Delete
    } else {
        QueryType::Other
    }
}