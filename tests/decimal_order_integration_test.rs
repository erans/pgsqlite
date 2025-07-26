use rusqlite::Connection;
use pgsqlite::functions::register_all_functions;
use pgsqlite::metadata::TypeMetadata;

#[test]
fn test_decimal_ordering_integration() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata and functions
    TypeMetadata::init(&conn).unwrap();
    register_all_functions(&conn).unwrap();
    
    // Create a table with decimal values
    conn.execute(
        "CREATE TABLE prices (
            id INTEGER PRIMARY KEY,
            item TEXT,
            amount TEXT
        )",
        [],
    ).unwrap();
    
    // Register as DECIMAL type
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) 
         VALUES ('prices', 'amount', 'NUMERIC', 'DECIMAL')",
        [],
    ).unwrap();
    
    // Insert test data - these would sort incorrectly as text
    let test_data = vec![
        ("item1", "100"),
        ("item2", "20"),
        ("item3", "3"),
        ("item4", "1000"),
        ("item5", "99.99"),
        ("item6", "-5"),
        ("item7", "0.5"),
    ];
    
    for (item, amount) in test_data {
        conn.execute(
            "INSERT INTO prices (item, amount) VALUES (?1, ?2)",
            [item, amount],
        ).unwrap();
    }
    
    // Test ordering with CAST (simulating what our rewriter would do)
    let mut stmt = conn.prepare("SELECT item, amount FROM prices ORDER BY CAST(amount AS REAL)").unwrap();
    let results: Vec<(String, String)> = stmt.query_map([], |row| {
        Ok((row.get(0)?, row.get(1)?))
    }).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    
    // Verify correct numeric ordering
    assert_eq!(results[0].0, "item6"); // -5
    assert_eq!(results[1].0, "item7"); // 0.5
    assert_eq!(results[2].0, "item3"); // 3
    assert_eq!(results[3].0, "item2"); // 20
    assert_eq!(results[4].0, "item5"); // 99.99
    assert_eq!(results[5].0, "item1"); // 100
    assert_eq!(results[6].0, "item4"); // 1000
    
    // Test descending order
    let mut stmt_desc = conn.prepare("SELECT item, amount FROM prices ORDER BY CAST(amount AS REAL) DESC").unwrap();
    let results_desc: Vec<(String, String)> = stmt_desc.query_map([], |row| {
        Ok((row.get(0)?, row.get(1)?))
    }).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    
    assert_eq!(results_desc[0].0, "item4"); // 1000
    assert_eq!(results_desc[6].0, "item6"); // -5
}

#[test]
fn test_decimal_grouping_integration() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata and functions
    TypeMetadata::init(&conn).unwrap();
    register_all_functions(&conn).unwrap();
    
    // Create a table
    conn.execute(
        "CREATE TABLE transactions (
            id INTEGER PRIMARY KEY,
            category TEXT,
            amount TEXT
        )",
        [],
    ).unwrap();
    
    // Register as DECIMAL type
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) 
         VALUES ('transactions', 'amount', 'NUMERIC', 'DECIMAL')",
        [],
    ).unwrap();
    
    // Insert test data
    let test_data = vec![
        ("food", "10.50"),
        ("food", "10.50"),  // Duplicate to test grouping
        ("food", "25.00"),
        ("transport", "5.00"),
        ("transport", "5.00"),  // Duplicate
        ("transport", "15.00"),
    ];
    
    for (cat, amount) in test_data {
        conn.execute(
            "INSERT INTO transactions (category, amount) VALUES (?1, ?2)",
            [cat, amount],
        ).unwrap();
    }
    
    // Test GROUP BY with decimal column
    let mut stmt = conn.prepare(
        "SELECT category, amount, COUNT(*) as cnt 
         FROM transactions 
         GROUP BY category, amount 
         ORDER BY category, CAST(amount AS REAL)"
    ).unwrap();
    
    let results: Vec<(String, String, i32)> = stmt.query_map([], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    }).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
    
    // Debug: print results
    println!("GROUP BY results:");
    for (cat, amt, cnt) in &results {
        println!("  {cat} | {amt} | {cnt}");
    }
    
    // Verify grouping worked correctly
    assert_eq!(results.len(), 4); // Actually 4 unique category/amount combinations (not 5)
    
    // Check specific groups
    assert_eq!(results[0], ("food".to_string(), "10.50".to_string(), 2));
    assert_eq!(results[1], ("food".to_string(), "25.00".to_string(), 1));
    assert_eq!(results[2], ("transport".to_string(), "5.00".to_string(), 2));
}