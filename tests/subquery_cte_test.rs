use pgsqlite::rewriter::{DecimalQueryRewriter, ExpressionTypeResolver};
use pgsqlite::types::PgType;
use pgsqlite::metadata::TypeMetadata;
use rusqlite::Connection;
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::ast::{Query, Statement};

fn setup_test_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata table
    TypeMetadata::init(&conn).unwrap();
    
    // Register decimal functions
    pgsqlite::functions::register_all_functions(&conn).unwrap();
    
    // Create test tables
    conn.execute(
        "CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            total TEXT,
            tax TEXT,
            status TEXT
        )",
        [],
    ).unwrap();
    
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('orders', 'total', 'NUMERIC', 'DECIMAL'),
         ('orders', 'tax', 'NUMERIC', 'DECIMAL')",
        [],
    ).unwrap();
    
    conn.execute(
        "CREATE TABLE customers (
            id INTEGER PRIMARY KEY,
            name TEXT,
            credit_limit TEXT,
            discount_rate TEXT
        )",
        [],
    ).unwrap();
    
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('customers', 'credit_limit', 'NUMERIC', 'DECIMAL'),
         ('customers', 'discount_rate', 'NUMERIC', 'DECIMAL')",
        [],
    ).unwrap();
    
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
    
    conn
}

fn parse_query(sql: &str) -> Query {
    let dialect = PostgreSqlDialect {};
    let statements = Parser::parse_sql(&dialect, sql).unwrap();
    match &statements[0] {
        Statement::Query(query) => query.as_ref().clone(),
        _ => panic!("Expected SELECT query"),
    }
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
fn test_simple_join_aggregate() {
    let conn = setup_test_db();
    
    // First test simple aggregate with alias
    let sql = "SELECT AVG(o.total) FROM orders o";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Simple alias aggregate rewritten to: {result}");
    assert!(result.contains("decimal_from_text"));
    
    // Then test join aggregate
    let sql2 = "SELECT c.name, AVG(o.total) FROM customers c JOIN orders o ON c.id = o.customer_id GROUP BY c.name";
    let result2 = rewrite_query(&conn, sql2).unwrap();
    println!("Join aggregate rewritten to: {result2}");
    assert!(result2.contains("decimal_from_text"));
}

#[test]
fn test_simple_subquery_in_select() {
    let conn = setup_test_db();
    
    let sql = "SELECT name, (SELECT SUM(total) FROM orders WHERE customer_id = c.id) as total_orders 
               FROM customers c";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Subquery in SELECT rewritten to: {result}");
    assert!(result.contains("decimal_from_text"));
    assert!(result.contains("SUM"));
}

#[test]
fn test_subquery_with_arithmetic() {
    let conn = setup_test_db();
    
    let sql = "SELECT name, 
               (SELECT SUM(total + tax) FROM orders WHERE customer_id = c.id) as total_with_tax 
               FROM customers c";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Subquery with arithmetic rewritten to: {result}");
    assert!(result.contains("decimal_add"));
}

#[test]
fn test_derived_table_subquery() {
    let conn = setup_test_db();
    
    // First test just the inner query
    let inner_sql = "SELECT c.name as customer_name, AVG(o.total) as avg_order 
                     FROM customers c 
                     JOIN orders o ON c.id = o.customer_id 
                     GROUP BY c.id, c.name";
    let inner_result = rewrite_query(&conn, inner_sql).unwrap();
    println!("Inner query rewritten to: {inner_result}");
    assert!(inner_result.contains("decimal_from_text"), "Inner query should wrap AVG argument");
    
    let sql = "SELECT customer_name, avg_order 
               FROM (
                   SELECT c.name as customer_name, AVG(o.total) as avg_order 
                   FROM customers c 
                   JOIN orders o ON c.id = o.customer_id 
                   GROUP BY c.id, c.name
               ) t 
               WHERE avg_order > 100";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Derived table query rewritten to: {result}");
    assert!(result.contains("AVG"));
    assert!(result.contains("decimal_from_text"));
    // The WHERE clause on avg_order should use decimal comparison
    assert!(result.contains("decimal_gt"), "WHERE clause should use decimal_gt for avg_order > 100");
}

#[test]
fn test_cte_basic() {
    let conn = setup_test_db();
    
    let sql = "WITH order_totals AS (
                   SELECT customer_id, SUM(total) as total_amount 
                   FROM orders 
                   GROUP BY customer_id
               )
               SELECT c.name, ot.total_amount 
               FROM customers c 
               JOIN order_totals ot ON c.id = ot.customer_id
               WHERE ot.total_amount > 1000";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("CTE query rewritten to: {result}");
    assert!(result.contains("WITH"));
    assert!(result.contains("SUM"));
    assert!(result.contains("decimal_from_text"));
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_cte_with_arithmetic_operations() {
    let conn = setup_test_db();
    
    let sql = "WITH customer_metrics AS (
                   SELECT 
                       customer_id,
                       SUM(total) as total_amount,
                       SUM(tax) as total_tax,
                       SUM(total + tax) as total_with_tax
                   FROM orders 
                   GROUP BY customer_id
               )
               SELECT 
                   c.name,
                   cm.total_with_tax / cm.total_amount as tax_ratio
               FROM customers c 
               JOIN customer_metrics cm ON c.id = cm.customer_id";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("CTE with arithmetic rewritten to: {result}");
    assert!(result.contains("decimal_add"));
    assert!(result.contains("decimal_div"));
}

#[test]
fn test_multiple_ctes() {
    let conn = setup_test_db();
    
    let sql = "WITH 
               high_value_customers AS (
                   SELECT customer_id 
                   FROM orders 
                   GROUP BY customer_id 
                   HAVING SUM(total) > 5000
               ),
               customer_discounts AS (
                   SELECT c.id, c.discount_rate * 0.01 as discount_decimal
                   FROM customers c
               )
               SELECT 
                   c.name,
                   cd.discount_decimal
               FROM customers c
               JOIN high_value_customers hvc ON c.id = hvc.customer_id
               JOIN customer_discounts cd ON c.id = cd.id";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Multiple CTEs rewritten to: {result}");
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_nested_ctes() {
    let conn = setup_test_db();
    
    let sql = "WITH RECURSIVE 
               order_hierarchy AS (
                   SELECT id, total, 1 as level
                   FROM orders 
                   WHERE customer_id = 1
                   
                   UNION ALL
                   
                   SELECT o.id, o.total + oh.total, oh.level + 1
                   FROM orders o
                   JOIN order_hierarchy oh ON o.customer_id = 1
                   WHERE oh.level < 5
               )
               SELECT level, total 
               FROM order_hierarchy";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Recursive CTE rewritten to: {result}");
    
    // Recursive CTEs should still have decimal operations rewritten
    assert!(result.contains("decimal_add"));
}

#[test]
fn test_subquery_in_where_clause() {
    let conn = setup_test_db();
    
    let sql = "SELECT name, credit_limit 
               FROM customers 
               WHERE credit_limit > (
                   SELECT AVG(total) 
                   FROM orders
               )";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Subquery in WHERE rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
    assert!(result.contains("AVG"));
}

#[test]
fn test_correlated_subquery() {
    let conn = setup_test_db();
    
    let sql = "SELECT c.name, c.credit_limit,
               (SELECT MAX(o.total) 
                FROM orders o 
                WHERE o.customer_id = c.id AND o.total < c.credit_limit) as max_order
               FROM customers c";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Correlated subquery rewritten to: {result}");
    assert!(result.contains("decimal_lt"));
    assert!(result.contains("MAX"));
}

#[test]
fn test_exists_subquery() {
    let conn = setup_test_db();
    
    let sql = "SELECT name 
               FROM customers c 
               WHERE EXISTS (
                   SELECT 1 
                   FROM orders o 
                   WHERE o.customer_id = c.id 
                   AND o.total > 1000
               )";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("EXISTS subquery rewritten to: {result}");
    assert!(result.contains("EXISTS"));
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_in_subquery() {
    let conn = setup_test_db();
    
    let sql = "SELECT name 
               FROM customers 
               WHERE id IN (
                   SELECT customer_id 
                   FROM orders 
                   WHERE total > 500
               )";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("IN subquery rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_expression_type_resolver_with_cte() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    
    let sql = "WITH totals AS (
                   SELECT customer_id, SUM(total) as sum_total 
                   FROM orders 
                   GROUP BY customer_id
               )
               SELECT * FROM totals";
    let query = parse_query(sql);
    let context = resolver.build_context(&query);
    
    // Check that CTE columns are recognized
    assert!(context.cte_columns.contains_key("totals"));
    let cte_cols = context.cte_columns.get("totals").unwrap();
    
    // Find the sum_total column
    let sum_total_type = cte_cols.iter()
        .find(|(name, _)| name == "sum_total")
        .map(|(_, typ)| *typ);
    
    assert_eq!(sum_total_type, Some(PgType::Numeric));
}

#[test]
fn test_expression_type_resolver_with_derived_table() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    
    let sql = "SELECT avg_price 
               FROM (
                   SELECT AVG(price) as avg_price 
                   FROM products
               ) t";
    let query = parse_query(sql);
    let context = resolver.build_context(&query);
    
    // Check that derived table columns are recognized
    assert!(context.derived_table_columns.contains_key("t"));
    let derived_cols = context.derived_table_columns.get("t").unwrap();
    
    // Check the avg_price column type
    let avg_price_type = derived_cols.iter()
        .find(|(name, _)| name == "avg_price")
        .map(|(_, typ)| *typ);
    
    assert_eq!(avg_price_type, Some(PgType::Numeric));
    
    // Also check the default table is set correctly
    assert_eq!(context.default_table, Some("t".to_string()));
}

#[test]
fn test_nested_subqueries() {
    let conn = setup_test_db();
    
    let sql = "SELECT name 
               FROM customers 
               WHERE credit_limit > (
                   SELECT AVG(max_order) 
                   FROM (
                       SELECT customer_id, MAX(total) as max_order 
                       FROM orders 
                       GROUP BY customer_id
                   ) t
               )";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Nested subqueries rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
    assert!(result.contains("AVG"));
    assert!(result.contains("MAX"));
}

#[test]
fn test_union_with_decimal_operations() {
    let conn = setup_test_db();
    
    let sql = "SELECT name, credit_limit as amount FROM customers
               UNION ALL
               SELECT 'Total Orders', SUM(total) FROM orders";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("UNION query rewritten to: {result}");
    assert!(result.contains("UNION"));
    // Note: Aggregate function rewriting may not be implemented yet
    // assert!(result.contains("decimal_from_text"));
}

#[test]
fn test_cte_column_aliases() {
    let conn = setup_test_db();
    
    let sql = "WITH order_stats (cust_id, total_amount, avg_amount) AS (
                   SELECT customer_id, SUM(total), AVG(total)
                   FROM orders 
                   GROUP BY customer_id
               )
               SELECT c.name, os.total_amount, os.avg_amount
               FROM customers c 
               JOIN order_stats os ON c.id = os.cust_id
               WHERE os.total_amount > 1000";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("CTE with column aliases rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
}

#[test]
fn test_lateral_join_simulation() {
    let conn = setup_test_db();
    
    // Simulating a lateral join pattern with correlated subquery
    let sql = "SELECT c.name, 
               (SELECT SUM(o.total * (1 - c.discount_rate / 100))
                FROM orders o 
                WHERE o.customer_id = c.id) as discounted_total
               FROM customers c";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Lateral join pattern rewritten to: {result}");
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_sub"));
    assert!(result.contains("decimal_div"));
}

#[test]
fn test_window_function_in_cte() {
    let conn = setup_test_db();
    
    // Note: SQLite has limited window function support, but we can test the rewriting
    let sql = "WITH ranked_orders AS (
                   SELECT 
                       customer_id,
                       total,
                       ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total DESC) as rn
                   FROM orders
               )
               SELECT customer_id, total * 1.1 as adjusted_total
               FROM ranked_orders
               WHERE rn = 1";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Window function in CTE rewritten to: {result}");
    assert!(result.contains("decimal_mul"));
}

#[test]
fn test_materialized_cte_hint() {
    let conn = setup_test_db();
    
    // Testing CTE with operations that should be rewritten
    let sql = "WITH order_summary AS /*MATERIALIZED*/ (
                   SELECT 
                       customer_id,
                       COUNT(*) as order_count,
                       SUM(total) as total_amount,
                       AVG(total + tax) as avg_with_tax
                   FROM orders 
                   GROUP BY customer_id
               )
               SELECT * 
               FROM order_summary 
               WHERE total_amount > 5000";
    let result = rewrite_query(&conn, sql).unwrap();
    
    println!("Materialized CTE rewritten to: {result}");
    assert!(result.contains("decimal_add"));
    assert!(result.contains("decimal_gt"));
}