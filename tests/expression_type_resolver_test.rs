use pgsqlite::rewriter::{ExpressionTypeResolver, QueryContext};
use pgsqlite::types::PgType;
use pgsqlite::metadata::TypeMetadata;
use rusqlite::Connection;
use sqlparser::parser::Parser;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::ast::{Expr, Query, Statement};

fn setup_test_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata table
    TypeMetadata::init(&conn).unwrap();
    
    // Create test table
    conn.execute(
        "CREATE TABLE test_table (
            id INTEGER,
            name TEXT,
            price TEXT,
            quantity INTEGER,
            active INTEGER,
            created_at TEXT
        )",
        [],
    ).unwrap();
    
    // Insert type metadata
    conn.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('test_table', 'id', 'INTEGER', 'INTEGER'),
         ('test_table', 'name', 'TEXT', 'TEXT'),
         ('test_table', 'price', 'NUMERIC', 'DECIMAL'),
         ('test_table', 'quantity', 'INTEGER', 'INTEGER'),
         ('test_table', 'active', 'BOOLEAN', 'INTEGER'),
         ('test_table', 'created_at', 'TIMESTAMP', 'TEXT')",
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

fn parse_expression(expr_str: &str) -> Expr {
    let sql = format!("SELECT {expr_str} FROM test_table");
    let query = parse_query(&sql);
    
    match &*query.body {
        sqlparser::ast::SetExpr::Select(select) => {
            match &select.projection[0] {
                sqlparser::ast::SelectItem::UnnamedExpr(expr) => expr.clone(),
                _ => panic!("Expected unnamed expression"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_resolve_column_types() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let mut context = QueryContext::default();
    context.default_table = Some("test_table".to_string());
    
    // Test simple column reference
    let expr = parse_expression("price");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    let expr = parse_expression("quantity");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Int4);
    
    let expr = parse_expression("name");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Text);
    
    let expr = parse_expression("active");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Bool);
}

#[test]
fn test_resolve_literal_types() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let context = QueryContext::default();
    
    // Integer literals
    let expr = parse_expression("42");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Int4);
    
    let expr = parse_expression("9999999999");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Int8);
    
    // Decimal literals
    let expr = parse_expression("123.45");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    // Boolean literals
    let expr = parse_expression("true");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Bool);
    
    // String literal
    let expr = parse_expression("'hello'");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Text);
}

#[test]
fn test_resolve_binary_operation_types() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let mut context = QueryContext::default();
    context.default_table = Some("test_table".to_string());
    
    // Numeric + Integer -> Numeric
    let expr = parse_expression("price + quantity");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    // Integer + Integer -> Integer
    let expr = parse_expression("quantity + id");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Int4);
    
    // Comparison -> Boolean
    let expr = parse_expression("price > 100");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Bool);
    
    // String concatenation
    let expr = parse_expression("name || 'suffix'");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Text);
}

#[test]
fn test_resolve_function_types() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let mut context = QueryContext::default();
    context.default_table = Some("test_table".to_string());
    
    // COUNT always returns Int8
    let expr = parse_expression("COUNT(*)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Int8);
    
    // SUM on numeric returns numeric
    let expr = parse_expression("SUM(price)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    // SUM on integer returns float8
    let expr = parse_expression("SUM(quantity)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Float8);
    
    // AVG always returns numeric
    let expr = parse_expression("AVG(quantity)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    // String functions
    let expr = parse_expression("UPPER(name)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Text);
    
    let expr = parse_expression("LENGTH(name)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Int4);
}

#[test]
fn test_query_context_building() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    
    // Test with table alias
    let query = parse_query("SELECT t.price FROM test_table t");
    let context = resolver.build_context(&query);
    
    assert_eq!(context.table_aliases.get("t"), Some(&"test_table".to_string()));
    assert_eq!(context.default_table, None); // No default table when using aliases
    
    // Test without alias (should set default_table)
    let query = parse_query("SELECT price FROM test_table");
    let context = resolver.build_context(&query);
    
    assert_eq!(context.default_table, Some("test_table".to_string()));
    assert!(context.table_aliases.is_empty()); // No aliases when no alias is used
    
    // Test with join
    let query = parse_query("SELECT a.price, b.quantity FROM test_table a JOIN test_table b ON a.id = b.id");
    let context = resolver.build_context(&query);
    
    assert_eq!(context.table_aliases.get("a"), Some(&"test_table".to_string()));
    assert_eq!(context.table_aliases.get("b"), Some(&"test_table".to_string()));
}

#[test]
fn test_resolve_qualified_columns() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    
    // Build context with alias
    let query = parse_query("SELECT t.price FROM test_table t");
    let context = resolver.build_context(&query);
    
    // Test qualified column with alias
    let expr = parse_expression("t.price");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    // Test qualified column with table name
    let expr = parse_expression("test_table.quantity");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Int4);
}

#[test]
fn test_involves_decimal() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let mut context = QueryContext::default();
    context.default_table = Some("test_table".to_string());
    
    // Direct decimal column
    let expr = parse_expression("price");
    assert!(resolver.involves_decimal(&expr, &context));
    
    // Non-decimal column
    let expr = parse_expression("quantity");
    assert!(!resolver.involves_decimal(&expr, &context));
    
    // Expression with decimal
    let expr = parse_expression("price + 10");
    assert!(resolver.involves_decimal(&expr, &context));
    
    // Expression without decimal
    let expr = parse_expression("quantity * 2");
    assert!(!resolver.involves_decimal(&expr, &context));
    
    // Function with decimal argument
    let expr = parse_expression("SUM(price)");
    assert!(resolver.involves_decimal(&expr, &context));
}

#[test]
fn test_cast_expression_types() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let context = QueryContext::default();
    
    // Cast to numeric
    let expr = parse_expression("CAST(quantity AS NUMERIC)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    // Cast to text
    let expr = parse_expression("CAST(price AS TEXT)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Text);
    
    // Cast to integer
    let expr = parse_expression("CAST(price AS INTEGER)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Int4);
}

#[test]
fn test_nested_expressions() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let mut context = QueryContext::default();
    context.default_table = Some("test_table".to_string());
    
    // Nested arithmetic
    let expr = parse_expression("(price + 10) * 2");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    // Nested with different types
    let expr = parse_expression("(quantity * 2) + price");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric); // Numeric takes precedence
}

#[test]
fn test_unknown_columns() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let context = QueryContext::default();
    
    // Unknown column defaults to Text
    let expr = parse_expression("unknown_column");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Text);
    
    // Unknown function defaults to Text
    let expr = parse_expression("UNKNOWN_FUNC(price)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Text);
}

#[test]
fn test_decimal_function_types() {
    let conn = setup_test_db();
    let mut resolver = ExpressionTypeResolver::new(&conn);
    let context = QueryContext::default();
    
    // Decimal arithmetic functions
    let expr = parse_expression("decimal_add(price, 10)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    let expr = parse_expression("decimal_mul(price, quantity)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    // Decimal conversion functions
    let expr = parse_expression("decimal_from_text('123.45')");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Numeric);
    
    let expr = parse_expression("decimal_to_text(price)");
    let pg_type = resolver.resolve_expr_type(&expr, &context);
    assert_eq!(pg_type, PgType::Text);
}