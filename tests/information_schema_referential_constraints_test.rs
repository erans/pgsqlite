use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_information_schema_referential_constraints_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ref_constraints.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test tables with foreign key constraints using the same session
    db_handler.execute_with_session("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER REFERENCES departments(id))", &session_id).await.unwrap();

    // Test basic query
    let result = db_handler.query_with_session("SELECT constraint_name, constraint_catalog, constraint_schema FROM information_schema.referential_constraints", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0], "constraint_name");
    assert_eq!(result.columns[1], "constraint_catalog");
    assert_eq!(result.columns[2], "constraint_schema");

    // Should find at least one foreign key constraint
    assert!(result.rows.len() >= 1, "Expected at least one foreign key constraint");

    // Check constraint data
    let constraint_name = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let catalog = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    let schema = String::from_utf8(result.rows[0][2].as_ref().unwrap().clone()).unwrap();

    assert!(constraint_name.contains("employees") && constraint_name.contains("fkey"));
    assert_eq!(catalog, "main");
    assert_eq!(schema, "public");
}

#[tokio::test]
async fn test_information_schema_referential_constraints_all_columns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ref_constraints_all.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test tables using the same session
    db_handler.execute_with_session("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT UNIQUE)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER REFERENCES categories(id))", &session_id).await.unwrap();

    // Test all columns
    let result = db_handler.query_with_session("SELECT * FROM information_schema.referential_constraints", &session_id).await.unwrap();

    // Verify all 9 standard columns are present
    assert_eq!(result.columns.len(), 9);
    let expected_columns = vec![
        "constraint_catalog",
        "constraint_schema",
        "constraint_name",
        "unique_constraint_catalog",
        "unique_constraint_schema",
        "unique_constraint_name",
        "match_option",
        "update_rule",
        "delete_rule"
    ];

    for (i, expected) in expected_columns.iter().enumerate() {
        assert_eq!(result.columns[i], *expected);
    }

    // Should have one foreign key constraint
    assert_eq!(result.rows.len(), 1);

    // Verify data
    let row = &result.rows[0];
    let constraint_catalog = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
    let constraint_schema = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
    let constraint_name = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();
    let unique_constraint_catalog = String::from_utf8(row[3].as_ref().unwrap().clone()).unwrap();
    let unique_constraint_schema = String::from_utf8(row[4].as_ref().unwrap().clone()).unwrap();
    let unique_constraint_name = String::from_utf8(row[5].as_ref().unwrap().clone()).unwrap();
    let match_option = String::from_utf8(row[6].as_ref().unwrap().clone()).unwrap();
    let update_rule = String::from_utf8(row[7].as_ref().unwrap().clone()).unwrap();
    let delete_rule = String::from_utf8(row[8].as_ref().unwrap().clone()).unwrap();

    assert_eq!(constraint_catalog, "main");
    assert_eq!(constraint_schema, "public");
    assert!(constraint_name.contains("products") && constraint_name.contains("fkey"));
    assert_eq!(unique_constraint_catalog, "main");
    assert_eq!(unique_constraint_schema, "public");
    assert!(unique_constraint_name.contains("categories") && unique_constraint_name.contains("pkey"));
    assert_eq!(match_option, "NONE");
    assert_eq!(update_rule, "NO ACTION");
    assert_eq!(delete_rule, "NO ACTION");
}

#[tokio::test]
async fn test_information_schema_referential_constraints_multiple_fks() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ref_constraints_multi.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create multiple tables with foreign keys
    db_handler.execute_with_session("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER REFERENCES users(id), category_id INTEGER REFERENCES categories(id))", &session_id).await.unwrap();

    // Test query
    let result = db_handler.query_with_session("SELECT constraint_name, constraint_catalog FROM information_schema.referential_constraints ORDER BY constraint_name", &session_id).await.unwrap();

    // Should have two foreign key constraints
    assert_eq!(result.rows.len(), 2);

    // Check constraint names
    let constraint1 = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let constraint2 = String::from_utf8(result.rows[1][0].as_ref().unwrap().clone()).unwrap();

    // Both should be from posts table
    assert!(constraint1.contains("posts") && constraint1.contains("fkey"));
    assert!(constraint2.contains("posts") && constraint2.contains("fkey"));

    // One should reference users, one should reference categories
    let constraints_text = format!("{} {}", constraint1, constraint2);
    assert!(constraints_text.contains("user_id") || constraints_text.contains("category_id"));
}

#[tokio::test]
async fn test_information_schema_referential_constraints_where_filtering() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ref_constraints_filter.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test tables
    db_handler.execute_with_session("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount DECIMAL)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE order_items (id INTEGER PRIMARY KEY, order_id INTEGER REFERENCES orders(id), customer_id INTEGER REFERENCES customers(id))", &session_id).await.unwrap();

    // Test filtering by constraint name pattern
    let result = db_handler.query_with_session("SELECT constraint_name FROM information_schema.referential_constraints WHERE constraint_name LIKE '%order_id%'", &session_id).await.unwrap();

    assert_eq!(result.rows.len(), 1);
    let constraint_name = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    assert!(constraint_name.contains("order_id"));

    // Test filtering by catalog
    let result = db_handler.query_with_session("SELECT constraint_name FROM information_schema.referential_constraints WHERE constraint_catalog = 'main'", &session_id).await.unwrap();
    assert_eq!(result.rows.len(), 2); // Should find both foreign keys

    // Test filtering with no matches
    let result = db_handler.query_with_session("SELECT constraint_name FROM information_schema.referential_constraints WHERE constraint_name = 'nonexistent'", &session_id).await.unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[tokio::test]
async fn test_information_schema_referential_constraints_no_foreign_keys() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ref_constraints_none.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create tables without foreign key constraints
    db_handler.execute_with_session("CREATE TABLE simple_table (id INTEGER PRIMARY KEY, name TEXT)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE another_table (id INTEGER PRIMARY KEY, value INTEGER)", &session_id).await.unwrap();

    // Test query - should return no rows
    let result = db_handler.query_with_session("SELECT * FROM information_schema.referential_constraints", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 9); // All columns should be present
    assert_eq!(result.rows.len(), 0); // No foreign key constraints
}

#[tokio::test]
async fn test_information_schema_referential_constraints_table_level_fk() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ref_constraints_table_level.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create tables with table-level foreign key constraint
    db_handler.execute_with_session("CREATE TABLE suppliers (id INTEGER PRIMARY KEY, name TEXT)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, supplier_id INTEGER, FOREIGN KEY (supplier_id) REFERENCES suppliers(id))", &session_id).await.unwrap();

    // Test query
    let result = db_handler.query_with_session("SELECT constraint_name, unique_constraint_name FROM information_schema.referential_constraints", &session_id).await.unwrap();

    assert_eq!(result.rows.len(), 1);

    let constraint_name = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let unique_constraint_name = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();

    assert!(constraint_name.contains("products") && constraint_name.contains("fkey"));
    assert!(unique_constraint_name.contains("suppliers") && unique_constraint_name.contains("pkey"));
}

#[tokio::test]
async fn test_information_schema_referential_constraints_orm_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("ref_constraints_orm.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create Django/Rails-style schema
    db_handler.execute_with_session("CREATE TABLE django_content_type (id INTEGER PRIMARY KEY, app_label TEXT, model TEXT)", &session_id).await.unwrap();
    db_handler.execute_with_session("CREATE TABLE auth_permission (id INTEGER PRIMARY KEY, name TEXT, content_type_id INTEGER REFERENCES django_content_type(id))", &session_id).await.unwrap();

    // Test ORM-style queries

    // Django inspectdb pattern
    let result = db_handler.query_with_session(
        "SELECT constraint_name, unique_constraint_name, update_rule, delete_rule FROM information_schema.referential_constraints WHERE constraint_schema = 'public'",
        &session_id
    ).await.unwrap();

    assert_eq!(result.rows.len(), 1);

    // SQLAlchemy reflection pattern
    let result = db_handler.query_with_session(
        "SELECT constraint_catalog, constraint_schema, constraint_name, unique_constraint_catalog, unique_constraint_schema, unique_constraint_name FROM information_schema.referential_constraints",
        &session_id
    ).await.unwrap();

    assert_eq!(result.rows.len(), 1);

    // Rails schema introspection pattern
    let result = db_handler.query_with_session(
        "SELECT constraint_name, match_option FROM information_schema.referential_constraints WHERE constraint_catalog = 'main'",
        &session_id
    ).await.unwrap();

    assert_eq!(result.rows.len(), 1);
    let match_option = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    assert_eq!(match_option, "NONE");
}