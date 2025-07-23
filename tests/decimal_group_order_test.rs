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
        "CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            product_id INTEGER,
            category TEXT,
            price TEXT,
            quantity INTEGER,
            discount TEXT
        )",
        [],
    ).unwrap();
    
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('sales', 'price', 'NUMERIC', 'DECIMAL'),
         ('sales', 'discount', 'NUMERIC', 'DECIMAL')",
        [],
    ).unwrap();
    
    conn.execute(
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT,
            cost TEXT,
            margin TEXT
        )",
        [],
    ).unwrap();
    
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('products', 'cost', 'DOUBLE PRECISION', 'DECIMAL'),
         ('products', 'margin', 'REAL', 'DECIMAL')",
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
fn test_group_by_decimal_column() {
    let conn = setup_test_db();
    
    // Test GROUP BY with decimal column
    let sql = "SELECT category, price, COUNT(*) 
               FROM sales 
               GROUP BY category, price";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("GROUP BY decimal column rewritten to: {result}");
    // For grouping, we might need to ensure consistent decimal representation
    // but SQLite should handle grouping text values correctly
}

#[test]
fn test_group_by_with_decimal_having() {
    let conn = setup_test_db();
    
    // Test GROUP BY with HAVING clause on decimal
    let sql = "SELECT category, SUM(price) as total_price 
               FROM sales 
               GROUP BY category 
               HAVING SUM(price) > 1000";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("GROUP BY with HAVING rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_order_by_decimal_column() {
    let conn = setup_test_db();
    
    // Test ORDER BY with decimal column
    let sql = "SELECT * FROM sales ORDER BY price DESC";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("ORDER BY decimal column rewritten to: {result}");
    // We need to wrap the decimal column for proper ordering
    // Since decimal values are stored as text, we need to convert them for proper numeric ordering
    assert!(result.contains("CAST") || result.contains("decimal_to_real"));
}

#[test]
fn test_order_by_decimal_expression() {
    let conn = setup_test_db();
    
    // Test ORDER BY with decimal expression
    let sql = "SELECT *, price * (1 - discount) as final_price 
               FROM sales 
               ORDER BY price * (1 - discount)";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("ORDER BY decimal expression rewritten to: {result}");
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_sub"));
}

#[test]
fn test_order_by_multiple_columns_with_decimal() {
    let conn = setup_test_db();
    
    // Test ORDER BY with multiple columns including decimal
    let sql = "SELECT * FROM sales ORDER BY category, price DESC, quantity";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("ORDER BY multiple columns rewritten to: {result}");
}

#[test]
fn test_order_by_aggregate_decimal() {
    let conn = setup_test_db();
    
    // Test ORDER BY with aggregate on decimal column
    let sql = "SELECT category, AVG(price) as avg_price 
               FROM sales 
               GROUP BY category 
               ORDER BY AVG(price) DESC";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("ORDER BY aggregate decimal rewritten to: {result}");
}

#[test]
fn test_complex_group_order_query() {
    let conn = setup_test_db();
    
    // Complex query with GROUP BY and ORDER BY on decimal columns
    let sql = "SELECT s.category, p.name, SUM(s.price * s.quantity) as revenue
               FROM sales s
               JOIN products p ON s.product_id = p.id
               WHERE s.discount < 0.2
               GROUP BY s.category, p.name, p.cost
               HAVING SUM(s.price * s.quantity) > 500
               ORDER BY SUM(s.price * s.quantity) DESC, p.cost ASC";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Complex GROUP BY/ORDER BY rewritten to: {result}");
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_lt"));
    assert!(result.contains("decimal_gt"));
}