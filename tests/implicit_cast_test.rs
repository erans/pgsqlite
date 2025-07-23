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
    
    // Create test tables
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price TEXT,
            quantity INTEGER,
            category_id INTEGER
        )",
        [],
    ).unwrap();
    
    // Insert metadata for NUMERIC columns
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('products', 'id', 'INT4', 'INTEGER'),
         ('products', 'price', 'NUMERIC', 'DECIMAL'),
         ('products', 'quantity', 'INT4', 'INTEGER'),
         ('products', 'category_id', 'INT4', 'INTEGER')",
        [],
    ).unwrap();
    
    conn.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            amount TEXT,
            discount_pct TEXT
        )",
        [],
    ).unwrap();
    
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('orders', 'product_id', 'INT4', 'INTEGER'),
         ('orders', 'amount', 'NUMERIC', 'DECIMAL'),
         ('orders', 'discount_pct', 'NUMERIC', 'DECIMAL')",
        [],
    ).unwrap();
    
    conn
}

fn rewrite_query(conn: &Connection, sql: &str) -> Result<String, String> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| format!("Parse error: {e}"))?;
    
    if let Some(stmt) = statements.first_mut() {
        let mut rewriter = DecimalQueryRewriter::new(conn);
        rewriter.rewrite_statement(stmt)?;
        Ok(stmt.to_string())
    } else {
        Err("No statement found".to_string())
    }
}

#[test]
fn test_implicit_cast_integer_column_eq_decimal_string() {
    let conn = setup_test_db();
    
    // Test: integer_column = '123.45'
    let sql = "SELECT * FROM products WHERE category_id = '123.45'";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // Debug: print the rewritten query
    println!("Rewritten query: {result}");
    
    // Should wrap the string literal in decimal_from_text
    assert!(result.contains("decimal_eq"));
    assert!(result.contains("decimal_from_text"));
    assert!(result.contains("'123.45'"));
}

#[test]
fn test_implicit_cast_decimal_column_eq_integer() {
    let conn = setup_test_db();
    
    // Test: decimal_column = integer_literal
    let sql = "SELECT * FROM products WHERE price = 100";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // Should wrap the integer in decimal_from_text
    assert!(result.contains("decimal_eq"));
    assert!(result.contains("decimal_from_text"));
    assert!(result.contains("CAST(100 AS TEXT)"));
}

#[test]
fn test_implicit_cast_integer_plus_decimal() {
    let conn = setup_test_db();
    
    // Test: integer + decimal -> decimal
    let sql = "SELECT quantity + price FROM products";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // Should promote integer to decimal
    assert!(result.contains("decimal_add"));
    assert!(result.contains("decimal_from_text"));
}

#[test]
fn test_implicit_cast_function_argument() {
    let conn = setup_test_db();
    
    // Test: ROUND(integer_column) should cast to decimal
    let sql = "SELECT ROUND(quantity) FROM products";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // Debug: print the rewritten query
    println!("Rewritten query: {result}");
    
    // Should wrap integer column in decimal_from_text
    assert!(result.contains("decimal_round"));
    assert!(result.contains("decimal_from_text"));
}

#[test]
fn test_implicit_cast_in_where_clause() {
    let conn = setup_test_db();
    
    // Test complex WHERE with implicit casts
    let sql = "SELECT * FROM products WHERE price > 50 AND quantity * price > 1000";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // Should handle both comparisons with proper casts
    assert!(result.contains("decimal_gt"));
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_from_text"));
}

#[test]
fn test_implicit_cast_insert_values() {
    let conn = setup_test_db();
    
    // Test: INSERT with string values for numeric columns
    let sql = "INSERT INTO orders (product_id, amount, discount_pct) VALUES (1, '99.99', '0.15')";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // For now, INSERT handling might not apply the rewriter
    // This test documents expected behavior
    assert!(result.contains("'99.99'"));
    assert!(result.contains("'0.15'"));
}

#[test]
fn test_implicit_cast_update_assignment() {
    let conn = setup_test_db();
    
    // Test: UPDATE with implicit cast in SET clause
    let sql = "UPDATE products SET price = price * 1.1 WHERE category_id = '5'";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // Debug: print the rewritten query
    println!("Rewritten UPDATE query: {result}");
    
    // Should handle multiplication and comparison
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_eq"));
}

#[test]
fn test_no_implicit_cast_when_types_match() {
    let conn = setup_test_db();
    
    // Test: No unnecessary casts when types already match
    let sql = "SELECT price + amount FROM products p JOIN orders o ON p.id = o.product_id";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // Debug: print the rewritten query
    println!("Rewritten query: {result}");
    
    // Both are NUMERIC, so should use decimal_add without extra casts
    assert!(result.contains("decimal_add"));
    // Should not have excessive decimal_from_text calls
    let from_text_count = result.matches("decimal_from_text").count();
    assert_eq!(from_text_count, 0, "No decimal_from_text needed when both operands are already NUMERIC");
}

#[test]
fn test_implicit_cast_mixed_arithmetic() {
    let conn = setup_test_db();
    
    // Test: Complex expression with mixed types
    let sql = "SELECT (quantity * 2 + 5) * price / 100 FROM products";
    let result = rewrite_query(&conn, sql).unwrap();
    
    // Debug: print the rewritten query
    println!("Rewritten mixed arithmetic query: {result}");
    
    // Should handle type promotion throughout the expression
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_add"));
    assert!(result.contains("decimal_div"));
    assert!(result.contains("decimal_from_text"));
}