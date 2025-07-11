use pgsqlite::rewriter::DecimalQueryRewriter;
use pgsqlite::metadata::TypeMetadata;
use rusqlite::Connection;
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;

fn setup_test_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata table
    TypeMetadata::init(&conn).unwrap();
    
    // Register decimal functions
    pgsqlite::functions::register_all_functions(&conn).unwrap();
    
    // Create test table with NUMERIC column
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price TEXT
        )",
        [],
    ).unwrap();
    
    // Insert metadata for NUMERIC column
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('products', 'price', 'NUMERIC', 'DECIMAL')",
        [],
    ).unwrap();
    
    conn
}

fn rewrite_query(conn: &Connection, sql: &str) -> Result<String, String> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| format!("Parse error: {}", e))?;
    
    if let Some(stmt) = statements.first_mut() {
        let mut rewriter = DecimalQueryRewriter::new(conn);
        rewriter.rewrite_statement(stmt)?;
        Ok(stmt.to_string())
    } else {
        Err("No statement found".to_string())
    }
}

#[test]
fn test_simple_column_selection() {
    let conn = setup_test_db();
    
    // Test 1: Simple column selection should NOT be wrapped
    let sql = "SELECT price FROM products";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Simple SELECT rewritten to: {}", result);
    
    // The simple column reference should NOT be wrapped in decimal_from_text
    assert_eq!(result, "SELECT price FROM products");
}

#[test]
fn test_arithmetic_needs_wrapping() {
    let conn = setup_test_db();
    
    // Test 2: Arithmetic operations SHOULD be wrapped
    let sql = "SELECT price * 1.1 FROM products";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Arithmetic SELECT rewritten to: {}", result);
    
    // Arithmetic operations should use decimal functions
    assert!(result.contains("decimal_mul"));
}

#[test]
fn test_comparison_needs_wrapping() {
    let conn = setup_test_db();
    
    // Test 3: Comparisons in WHERE clause SHOULD be wrapped
    let sql = "SELECT * FROM products WHERE price > 100";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("WHERE clause rewritten to: {}", result);
    
    // Comparisons should use decimal functions
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_aggregate_needs_wrapping() {
    let conn = setup_test_db();
    
    // Test 4: Aggregates on decimal columns SHOULD be wrapped
    let sql = "SELECT SUM(price) FROM products";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Aggregate rewritten to: {}", result);
    
    // Aggregates should wrap the column in decimal_from_text
    assert!(result.contains("decimal_from_text"));
}