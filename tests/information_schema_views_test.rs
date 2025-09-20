use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_information_schema_views_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_views.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create some test tables and views
    db_handler.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount DECIMAL(10,2))").await.unwrap();

    // Create test views
    db_handler.execute("CREATE VIEW user_summary AS SELECT id, name FROM users WHERE id > 0").await.unwrap();
    db_handler.execute("CREATE VIEW order_totals AS SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id").await.unwrap();

    // Test basic views query - get all views
    let result = db_handler.query_with_session("SELECT table_name FROM information_schema.views", &session_id).await.unwrap();
    assert!(!result.rows.is_empty(), "Should find some views");

    let view_count = result.rows.len();
    assert!(view_count >= 2, "Should have at least 2 views, got {}", view_count);

    // Check if our test views are included
    let view_names: Vec<String> = result.rows.iter().map(|row| {
        let name_bytes = row[0].as_ref().unwrap();
        String::from_utf8(name_bytes.clone()).unwrap()
    }).collect();

    assert!(view_names.contains(&"user_summary".to_string()), "Should contain user_summary view");
    assert!(view_names.contains(&"order_totals".to_string()), "Should contain order_totals view");

    println!("✅ information_schema.views contains {} views", view_count);
}

#[tokio::test]
async fn test_information_schema_views_column_structure() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_views_structure.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create a test view
    db_handler.execute("CREATE TABLE test_table (id INTEGER, data TEXT)").await.unwrap();
    db_handler.execute("CREATE VIEW test_view AS SELECT id FROM test_table").await.unwrap();

    // Test column structure with standard information_schema.views columns
    let result = db_handler.query_with_session(
        "SELECT table_catalog, table_schema, table_name, view_definition, check_option, is_updatable FROM information_schema.views LIMIT 1",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 6, "Should have 6 columns");
    assert_eq!(result.columns, vec![
        "table_catalog", "table_schema", "table_name",
        "view_definition", "check_option", "is_updatable"
    ]);

    if !result.rows.is_empty() {
        let row = &result.rows[0];

        // Check catalog
        let catalog_bytes = row[0].as_ref().unwrap();
        let catalog = String::from_utf8(catalog_bytes.clone()).unwrap();
        assert_eq!(catalog, "main", "Should be in main catalog");

        // Check schema
        let schema_bytes = row[1].as_ref().unwrap();
        let schema = String::from_utf8(schema_bytes.clone()).unwrap();
        assert_eq!(schema, "public", "Should be in public schema");

        // Check check_option
        let check_option_bytes = row[4].as_ref().unwrap();
        let check_option = String::from_utf8(check_option_bytes.clone()).unwrap();
        assert_eq!(check_option, "NONE", "Should have NONE check option");

        // Check is_updatable
        let updatable_bytes = row[5].as_ref().unwrap();
        let updatable = String::from_utf8(updatable_bytes.clone()).unwrap();
        assert_eq!(updatable, "NO", "SQLite views should not be updatable");
    }

    println!("✅ information_schema.views has correct column structure");
}

#[tokio::test]
async fn test_information_schema_views_filtering() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_views_filtering.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test tables and views
    db_handler.execute("CREATE TABLE products (id INTEGER, name TEXT, price REAL)").await.unwrap();
    db_handler.execute("CREATE TABLE categories (id INTEGER, name TEXT)").await.unwrap();

    db_handler.execute("CREATE VIEW product_view AS SELECT name, price FROM products").await.unwrap();
    db_handler.execute("CREATE VIEW category_view AS SELECT name FROM categories").await.unwrap();

    // Test filtering by view name (common ORM pattern)
    let result = db_handler.query_with_session(
        "SELECT table_name, view_definition FROM information_schema.views WHERE table_name = 'product_view'",
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Should find product_view");
    assert_eq!(result.rows.len(), 1, "Should find exactly one view");

    let table_name_bytes = result.rows[0][0].as_ref().unwrap();
    let table_name = String::from_utf8(table_name_bytes.clone()).unwrap();
    assert_eq!(table_name, "product_view", "Should return product_view");

    let view_def_bytes = result.rows[0][1].as_ref().unwrap();
    let view_def = String::from_utf8(view_def_bytes.clone()).unwrap();
    assert!(view_def.contains("SELECT"), "View definition should contain SELECT");
    assert!(view_def.contains("products"), "View definition should reference products table");

    println!("✅ View filtering works correctly");

    // Test filtering by schema (Rails/Django pattern)
    let result = db_handler.query_with_session(
        "SELECT table_name FROM information_schema.views WHERE table_schema = 'public'",
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Should find views in public schema");
    assert!(result.rows.len() >= 2, "Should find at least 2 views in public schema");

    println!("✅ Schema filtering works correctly");
}

#[tokio::test]
async fn test_information_schema_views_definition_parsing() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_views_parsing.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create table and view with complex definition
    db_handler.execute("CREATE TABLE employees (id INTEGER, name TEXT, department TEXT, salary REAL)").await.unwrap();
    db_handler.execute(r#"
        CREATE VIEW high_salary_employees AS
        SELECT name, department, salary
        FROM employees
        WHERE salary > 50000
        ORDER BY salary DESC
    "#).await.unwrap();

    // Test view definition extraction
    let result = db_handler.query_with_session(
        "SELECT table_name, view_definition FROM information_schema.views WHERE table_name = 'high_salary_employees'",
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Should find high_salary_employees view");

    let view_def_bytes = result.rows[0][1].as_ref().unwrap();
    let view_def = String::from_utf8(view_def_bytes.clone()).unwrap();

    // Check that the definition contains key components
    assert!(view_def.to_uppercase().contains("SELECT"), "Should contain SELECT");
    assert!(view_def.contains("employees"), "Should reference employees table");
    assert!(view_def.contains("salary"), "Should contain salary column");
    assert!(view_def.contains("50000"), "Should contain WHERE condition");

    // Ensure it doesn't contain the CREATE VIEW part
    assert!(!view_def.to_uppercase().contains("CREATE VIEW"), "Should not contain CREATE VIEW");

    println!("✅ View definition parsing works correctly");
}

#[tokio::test]
async fn test_information_schema_views_orm_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_views_orm.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create tables and views similar to what ORMs would create
    db_handler.execute("CREATE TABLE auth_user (id INTEGER PRIMARY KEY, username TEXT, email TEXT, is_active BOOLEAN)").await.unwrap();
    db_handler.execute("CREATE TABLE blog_post (id INTEGER PRIMARY KEY, title TEXT, content TEXT, author_id INTEGER)").await.unwrap();

    // Create views that ORMs might use
    db_handler.execute("CREATE VIEW active_users AS SELECT id, username, email FROM auth_user WHERE is_active = 1").await.unwrap();
    db_handler.execute("CREATE VIEW user_posts AS SELECT u.username, p.title FROM auth_user u JOIN blog_post p ON u.id = p.author_id").await.unwrap();

    // Django ORM introspection pattern
    let result = db_handler.query_with_session(
        r#"SELECT v.table_name, v.view_definition, v.is_updatable
        FROM information_schema.views v
        WHERE v.table_schema = 'public'
        ORDER BY v.table_name"#,
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Django pattern should work");
    println!("✅ Django ORM introspection pattern works (found {} views)", result.rows.len());

    // SQLAlchemy reflection pattern
    let result = db_handler.query_with_session(
        r#"SELECT table_catalog, table_schema, table_name, view_definition
        FROM information_schema.views
        WHERE table_name LIKE '%user%'
        ORDER BY table_name"#,
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "SQLAlchemy pattern should work");
    println!("✅ SQLAlchemy reflection pattern works (found {} user-related views)", result.rows.len());

    // Rails ActiveRecord schema introspection pattern
    let result = db_handler.query_with_session(
        r#"SELECT v.table_name, v.view_definition
        FROM information_schema.views v
        WHERE v.table_schema = 'public'
        AND v.table_name NOT LIKE 'pg_%'
        AND v.table_name NOT LIKE 'information_schema%'"#,
        &session_id
    ).await.unwrap();

    assert!(result.rows.len() >= 2, "Rails pattern should find views");
    println!("✅ Rails ActiveRecord schema introspection pattern works");

    // Ecto database introspection pattern
    let result = db_handler.query_with_session(
        r#"SELECT DISTINCT table_schema, COUNT(*) as view_count
        FROM information_schema.views
        WHERE table_schema = 'public'
        GROUP BY table_schema
        ORDER BY table_schema"#,
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Ecto pattern should work");
    println!("✅ Ecto database introspection pattern works");
}

#[tokio::test]
async fn test_information_schema_views_comprehensive_columns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_views_comprehensive.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create a test view
    db_handler.execute("CREATE TABLE test_data (id INTEGER, value TEXT)").await.unwrap();
    db_handler.execute("CREATE VIEW test_comprehensive AS SELECT * FROM test_data WHERE id > 0").await.unwrap();

    // Test all available columns
    let result = db_handler.query_with_session(
        r#"SELECT
            table_catalog, table_schema, table_name, view_definition,
            check_option, is_updatable, is_insertable_into,
            is_trigger_updatable, is_trigger_deletable, is_trigger_insertable_into
        FROM information_schema.views
        WHERE table_name = 'test_comprehensive'"#,
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Should find test_comprehensive view");
    assert_eq!(result.columns.len(), 10, "Should have 10 columns");

    let row = &result.rows[0];

    // Verify catalog and schema
    let catalog = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
    let schema = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
    let name = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();

    assert_eq!(catalog, "main");
    assert_eq!(schema, "public");
    assert_eq!(name, "test_comprehensive");

    // Verify view definition
    let definition = String::from_utf8(row[3].as_ref().unwrap().clone()).unwrap();
    assert!(definition.contains("SELECT"), "Should contain SELECT");

    // Verify PostgreSQL standard defaults for SQLite views
    let check_option = String::from_utf8(row[4].as_ref().unwrap().clone()).unwrap();
    let is_updatable = String::from_utf8(row[5].as_ref().unwrap().clone()).unwrap();
    let is_insertable = String::from_utf8(row[6].as_ref().unwrap().clone()).unwrap();
    let is_trigger_updatable = String::from_utf8(row[7].as_ref().unwrap().clone()).unwrap();
    let is_trigger_deletable = String::from_utf8(row[8].as_ref().unwrap().clone()).unwrap();
    let is_trigger_insertable = String::from_utf8(row[9].as_ref().unwrap().clone()).unwrap();

    assert_eq!(check_option, "NONE");
    assert_eq!(is_updatable, "NO");
    assert_eq!(is_insertable, "NO");
    assert_eq!(is_trigger_updatable, "NO");
    assert_eq!(is_trigger_deletable, "NO");
    assert_eq!(is_trigger_insertable, "NO");

    println!("✅ All information_schema.views columns work correctly");
}

#[tokio::test]
async fn test_information_schema_views_no_views() {
    // Create a temporary database with no views
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_no_views.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create only tables, no views
    db_handler.execute("CREATE TABLE test_table1 (id INTEGER)").await.unwrap();
    db_handler.execute("CREATE TABLE test_table2 (id INTEGER)").await.unwrap();

    // Test query with no views
    let result = db_handler.query_with_session("SELECT table_name FROM information_schema.views", &session_id).await.unwrap();

    // Should return empty result set, not an error
    assert_eq!(result.rows.len(), 0, "Should return no views");
    assert_eq!(result.columns, vec!["table_name"]);

    println!("✅ No views scenario handled correctly");
}

#[tokio::test]
async fn test_information_schema_views_complex_definitions() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_complex_views.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create tables
    db_handler.execute("CREATE TABLE customers (id INTEGER, name TEXT, country TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount DECIMAL(10,2), order_date DATE)").await.unwrap();

    // Create complex view with JOINs, aggregations, and conditions
    db_handler.execute(r#"
        CREATE VIEW customer_order_summary AS
        SELECT
            c.name as customer_name,
            c.country,
            COUNT(o.id) as order_count,
            SUM(o.amount) as total_amount,
            AVG(o.amount) as avg_amount
        FROM customers c
        LEFT JOIN orders o ON c.id = o.customer_id
        WHERE c.country IN ('US', 'UK', 'CA')
        GROUP BY c.id, c.name, c.country
        HAVING COUNT(o.id) > 0
        ORDER BY total_amount DESC
    "#).await.unwrap();

    // Test complex view definition retrieval
    let result = db_handler.query_with_session(
        "SELECT table_name, view_definition FROM information_schema.views WHERE table_name = 'customer_order_summary'",
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Should find customer_order_summary view");

    let view_def_bytes = result.rows[0][1].as_ref().unwrap();
    let view_def = String::from_utf8(view_def_bytes.clone()).unwrap();


    // Verify complex SQL components are preserved
    assert!(view_def.to_uppercase().contains("SELECT"), "Should contain SELECT");
    assert!(view_def.to_uppercase().contains("FROM"), "Should contain FROM");
    assert!(view_def.to_uppercase().contains("JOIN"), "Should contain JOIN");
    assert!(view_def.to_uppercase().contains("WHERE"), "Should contain WHERE");
    assert!(view_def.to_uppercase().contains("GROUP BY"), "Should contain GROUP BY");
    assert!(view_def.to_uppercase().contains("HAVING"), "Should contain HAVING");
    assert!(view_def.to_uppercase().contains("ORDER BY"), "Should contain ORDER BY");

    // Verify table references
    assert!(view_def.contains("customers"), "Should reference customers table");
    assert!(view_def.contains("orders"), "Should reference orders table");

    println!("✅ Complex view definitions handled correctly");
}