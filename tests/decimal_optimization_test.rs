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
    
    // Create tables - some with decimal columns, some without
    
    // Table with only integer columns
    conn.execute(
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            active INTEGER
        )",
        [],
    ).unwrap();
    
    // Table with decimal columns
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            price TEXT,
            cost TEXT
        )",
        [],
    ).unwrap();
    
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('products', 'price', 'NUMERIC', 'DECIMAL'),
         ('products', 'cost', 'NUMERIC', 'DECIMAL')",
        [],
    ).unwrap();
    
    // Another table without decimal columns
    conn.execute(
        "CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name TEXT,
            parent_id INTEGER
        )",
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
fn test_skip_rewriting_for_non_decimal_tables() {
    let conn = setup_test_db();
    
    // Query on table with no decimal columns - should not be rewritten
    let sql = "SELECT * FROM users WHERE age > 25";
    let result = rewrite_query(&conn, sql).unwrap();
    assert_eq!(result, "SELECT * FROM users WHERE age > 25");
    
    // Another query with arithmetic on non-decimal columns
    let sql = "SELECT name, age * 2 as double_age FROM users";
    let result = rewrite_query(&conn, sql).unwrap();
    assert_eq!(result, "SELECT name, age * 2 AS double_age FROM users");
    
    // JOIN between non-decimal tables
    let sql = "SELECT u.name, c.name as category 
               FROM users u 
               JOIN categories c ON u.id = c.parent_id";
    let result = rewrite_query(&conn, sql).unwrap();
    // Should remain unchanged
    assert!(!result.contains("decimal_"));
}

#[test]
fn test_rewrite_queries_with_decimal_tables() {
    let conn = setup_test_db();
    
    // Query on table with decimal columns - should be rewritten
    let sql = "SELECT * FROM products WHERE price > 100";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Decimal table query rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
    
    // Arithmetic on decimal columns
    let sql = "SELECT name, price * 1.1 as new_price FROM products";
    let result = rewrite_query(&conn, sql).unwrap();
    assert!(result.contains("decimal_mul"));
}

#[test]
fn test_mixed_table_queries() {
    let conn = setup_test_db();
    
    // JOIN between decimal and non-decimal tables - should be rewritten
    let sql = "SELECT u.name, p.price 
               FROM users u 
               JOIN products p ON u.id = p.id 
               WHERE p.price > 50";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Mixed table query rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
    
    // But operations on non-decimal columns should not be wrapped
    assert!(!result.contains("decimal_from_text(u.id)"));
}

#[test]
fn test_subquery_optimization() {
    let conn = setup_test_db();
    
    // Subquery with only non-decimal tables
    let sql = "SELECT * FROM users WHERE id IN (SELECT parent_id FROM categories)";
    let result = rewrite_query(&conn, sql).unwrap();
    assert_eq!(result, "SELECT * FROM users WHERE id IN (SELECT parent_id FROM categories)");
    
    // Subquery with decimal table
    let sql = "SELECT * FROM users WHERE id IN (SELECT id FROM products WHERE price > 100)";
    let result = rewrite_query(&conn, sql).unwrap();
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_update_delete_optimization() {
    let conn = setup_test_db();
    
    // UPDATE on non-decimal table - should not be rewritten
    let sql = "UPDATE users SET age = age + 1 WHERE active = 1";
    let result = rewrite_query(&conn, sql).unwrap();
    assert_eq!(result, "UPDATE users SET age = age + 1 WHERE active = 1");
    
    // UPDATE on decimal table - should be rewritten
    let sql = "UPDATE products SET price = price * 1.05 WHERE cost < 50";
    let result = rewrite_query(&conn, sql).unwrap();
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_lt"));
    
    // DELETE on non-decimal table - should not be rewritten
    let sql = "DELETE FROM users WHERE age < 18";
    let result = rewrite_query(&conn, sql).unwrap();
    assert_eq!(result, "DELETE FROM users WHERE age < 18");
    
    // DELETE on decimal table - should be rewritten
    let sql = "DELETE FROM products WHERE price > 1000";
    let result = rewrite_query(&conn, sql).unwrap();
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_cte_optimization() {
    let conn = setup_test_db();
    
    // CTE with only non-decimal tables
    let sql = "WITH active_users AS (
                   SELECT * FROM users WHERE active = 1
               )
               SELECT * FROM active_users WHERE age > 30";
    let result = rewrite_query(&conn, sql).unwrap();
    // Should remain unchanged
    assert!(!result.contains("decimal_"));
    
    // CTE with decimal table
    let sql = "WITH expensive_products AS (
                   SELECT * FROM products WHERE price > 100
               )
               SELECT * FROM expensive_products";
    let result = rewrite_query(&conn, sql).unwrap();
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_union_optimization() {
    let conn = setup_test_db();
    
    // UNION of non-decimal tables
    let sql = "SELECT name FROM users 
               UNION 
               SELECT name FROM categories";
    let result = rewrite_query(&conn, sql).unwrap();
    assert!(!result.contains("decimal_"));
    
    // UNION involving decimal table
    let sql = "SELECT name FROM users 
               UNION 
               SELECT name FROM products WHERE price > 50";
    let result = rewrite_query(&conn, sql).unwrap();
    assert!(result.contains("decimal_gt"));
}