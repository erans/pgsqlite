use pgsqlite::rewriter::{DecimalQueryRewriter, ExpressionTypeResolver};
use pgsqlite::types::PgType;
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
    
    // Create test tables with REAL and DOUBLE PRECISION columns
    conn.execute(
        "CREATE TABLE measurements (
            id INTEGER PRIMARY KEY,
            temperature TEXT,
            pressure TEXT,
            humidity TEXT
        )",
        [],
    ).unwrap();
    
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('measurements', 'temperature', 'REAL', 'DECIMAL'),
         ('measurements', 'pressure', 'DOUBLE PRECISION', 'DECIMAL'),
         ('measurements', 'humidity', 'FLOAT4', 'DECIMAL')",
        [],
    ).unwrap();
    
    conn.execute(
        "CREATE TABLE calculations (
            id INTEGER PRIMARY KEY,
            value1 TEXT,
            value2 TEXT,
            result TEXT
        )",
        [],
    ).unwrap();
    
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('calculations', 'value1', 'FLOAT8', 'DECIMAL'),
         ('calculations', 'value2', 'FLOAT4', 'DECIMAL'),
         ('calculations', 'result', 'DOUBLE PRECISION', 'DECIMAL')",
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
fn test_real_type_arithmetic() {
    let conn = setup_test_db();
    
    // Test REAL (float4) columns
    let sql = "SELECT temperature * 1.8 + 32 FROM measurements";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("REAL arithmetic rewritten to: {result}");
    assert!(result.contains("decimal_mul"));
    assert!(result.contains("decimal_add"));
}

#[test]
fn test_double_precision_arithmetic() {
    let conn = setup_test_db();
    
    // Test DOUBLE PRECISION columns
    let sql = "SELECT pressure / 101.325 FROM measurements";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("DOUBLE PRECISION arithmetic rewritten to: {result}");
    assert!(result.contains("decimal_div"));
}

#[test]
fn test_float_type_comparison() {
    let conn = setup_test_db();
    
    // Test comparisons with FLOAT types
    let sql = "SELECT * FROM measurements WHERE temperature > 25.5 AND humidity < 60.0";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Float comparison rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
    assert!(result.contains("decimal_lt"));
}

#[test]
fn test_mixed_float_numeric_operations() {
    let conn = setup_test_db();
    
    // Test operations mixing FLOAT8 and FLOAT4
    let sql = "SELECT value1 + value2 * 2.5 FROM calculations";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Mixed float operations rewritten to: {result}");
    assert!(result.contains("decimal_add"));
    assert!(result.contains("decimal_mul"));
}

#[test]
fn test_float_aggregates() {
    let conn = setup_test_db();
    
    // Test aggregate functions on float columns
    let sql = "SELECT AVG(temperature), MAX(pressure), MIN(humidity) FROM measurements";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Float aggregates rewritten to: {result}");
    assert!(result.contains("AVG"));
    assert!(result.contains("MAX"));
    assert!(result.contains("MIN"));
    // The arguments should be used directly without wrapping since they're already DECIMAL
    assert!(!result.contains("decimal_from_text(temperature)"));
    assert!(!result.contains("decimal_from_text(pressure)"));
    assert!(!result.contains("decimal_from_text(humidity)"));
}

#[test]
fn test_float_in_subquery() {
    let conn = setup_test_db();
    
    // Test float types in subqueries
    let sql = "SELECT * FROM measurements WHERE temperature > (SELECT AVG(temperature) FROM measurements)";
    let result = rewrite_query(&conn, sql).unwrap();
    println!("Float in subquery rewritten to: {result}");
    assert!(result.contains("decimal_gt"));
    assert!(result.contains("AVG"));
}

#[test]
fn test_expression_type_resolver_float_types() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    
    // Test that REAL and DOUBLE PRECISION columns are mapped correctly
    use sqlparser::ast::{Expr, Ident};
    use pgsqlite::rewriter::QueryContext;
    
    let mut context = QueryContext::default();
    context.default_table = Some("measurements".to_string());
    
    // Test temperature column (REAL/FLOAT4)
    let expr = Expr::Identifier(Ident::new("temperature"));
    let col_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(col_type, PgType::Float4);
    
    // Test pressure column (DOUBLE PRECISION/FLOAT8) 
    let expr = Expr::Identifier(Ident::new("pressure"));
    let col_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(col_type, PgType::Float8);
}