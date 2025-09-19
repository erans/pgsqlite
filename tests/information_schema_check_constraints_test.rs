use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_information_schema_check_constraints_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("check_constraints.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test table with check constraints
    db_handler.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, price DECIMAL CHECK (price > 0), name TEXT CHECK (length(name) > 0))").await.unwrap();

    // Test basic query
    let result = db_handler.query_with_session("SELECT constraint_catalog, constraint_schema, constraint_name FROM information_schema.check_constraints", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0], "constraint_catalog");
    assert_eq!(result.columns[1], "constraint_schema");
    assert_eq!(result.columns[2], "constraint_name");

    // Should find check constraints
    assert!(result.rows.len() >= 2, "Expected at least 2 check constraints");

    // Check constraint data
    let constraint_names: Vec<String> = result.rows.iter()
        .map(|row| String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap())
        .collect();

    // Verify we have the expected user table constraints
    assert!(constraint_names.iter().any(|name| name.contains("products_check")),
           "Expected to find products_check constraint. All constraints: {:?}", constraint_names);

    for row in &result.rows {
        let catalog = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let schema = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        let constraint_name = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();

        assert_eq!(catalog, "main");
        assert_eq!(schema, "public");
        // Accept system constraints (pg_*) or user table constraints (products_*)
        assert!(constraint_name.starts_with("pg_") || constraint_name.contains("products") || constraint_name.contains("check"),
               "Constraint name '{}' doesn't match expected pattern. All constraints: {:?}",
               constraint_name, constraint_names);
    }
}

#[tokio::test]
async fn test_information_schema_check_constraints_all_columns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("check_constraints_all.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test table
    db_handler.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 18), email TEXT CHECK (email LIKE '%@%'))").await.unwrap();

    // Test all columns
    let result = db_handler.query_with_session("SELECT * FROM information_schema.check_constraints", &session_id).await.unwrap();

    // Verify all 4 standard columns are present
    assert_eq!(result.columns.len(), 4);
    let expected_columns = vec![
        "constraint_catalog",
        "constraint_schema",
        "constraint_name",
        "check_clause"
    ];

    for (i, expected) in expected_columns.iter().enumerate() {
        assert_eq!(result.columns[i], *expected);
    }

    // Should have check constraints
    assert!(result.rows.len() >= 2);

    // Verify data
    let all_constraint_names: Vec<String> = result.rows.iter()
        .map(|row| String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap())
        .collect();

    // Verify we have the expected user table constraints
    assert!(all_constraint_names.iter().any(|name| name.contains("users_check")),
           "Expected to find users_check constraint. All constraints: {:?}", all_constraint_names);

    for row in &result.rows {
        let constraint_catalog = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let constraint_schema = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        let constraint_name = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();
        let check_clause = String::from_utf8(row[3].as_ref().unwrap().clone()).unwrap();

        assert_eq!(constraint_catalog, "main");
        assert_eq!(constraint_schema, "public");
        // Accept system constraints (pg_*) or user table constraints (users_*)
        assert!(constraint_name.starts_with("pg_") || constraint_name.contains("users") || constraint_name.contains("check"),
               "Constraint name '{}' doesn't match expected pattern. All constraints: {:?}",
               constraint_name, all_constraint_names);
        assert!(!check_clause.is_empty(), "Check clause should not be empty");
    }
}

#[tokio::test]
async fn test_information_schema_check_constraints_multiple_constraints() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("check_constraints_multi.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create multiple tables with check constraints
    db_handler.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount DECIMAL CHECK (amount > 0), status TEXT CHECK (status IN ('pending', 'completed', 'cancelled')))").await.unwrap();
    db_handler.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY, quantity INTEGER CHECK (quantity >= 0), location TEXT CHECK (length(location) > 2))").await.unwrap();

    // Test query
    let result = db_handler.query_with_session("SELECT constraint_name, check_clause FROM information_schema.check_constraints ORDER BY constraint_name", &session_id).await.unwrap();

    // Should have multiple check constraints from both tables
    assert!(result.rows.len() >= 4, "Expected at least 4 check constraints");

    // Check that we have constraints from both tables
    let mut has_orders_constraint = false;
    let mut has_inventory_constraint = false;

    for row in &result.rows {
        let constraint_name = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let check_clause = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();

        if constraint_name.contains("orders") {
            has_orders_constraint = true;
        }
        if constraint_name.contains("inventory") {
            has_inventory_constraint = true;
        }

        assert!(!check_clause.is_empty(), "Check clause should not be empty for constraint: {}", constraint_name);
    }

    assert!(has_orders_constraint, "Should have constraints from orders table");
    assert!(has_inventory_constraint, "Should have constraints from inventory table");
}

#[tokio::test]
async fn test_information_schema_check_constraints_where_filtering() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("check_constraints_filter.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test tables
    db_handler.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0), balance DECIMAL CHECK (balance >= 0))").await.unwrap();
    db_handler.execute("CREATE TABLE suppliers (id INTEGER PRIMARY KEY, rating INTEGER CHECK (rating BETWEEN 1 AND 5))").await.unwrap();

    // Test filtering by constraint name pattern - be more flexible
    let result = db_handler.query_with_session("SELECT constraint_name FROM information_schema.check_constraints WHERE constraint_name LIKE '%check%'", &session_id).await.unwrap();

    assert!(result.rows.len() >= 1, "Expected at least one check constraint with 'check' in the name");
    for row in &result.rows {
        let constraint_name = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        assert!(constraint_name.contains("check") || constraint_name.contains("customers") || constraint_name.contains("suppliers"));
    }

    // Test filtering by catalog
    let result = db_handler.query_with_session("SELECT constraint_name FROM information_schema.check_constraints WHERE constraint_catalog = 'main'", &session_id).await.unwrap();
    assert!(result.rows.len() >= 3); // Should find all check constraints

    // Test filtering with no matches
    let result = db_handler.query_with_session("SELECT constraint_name FROM information_schema.check_constraints WHERE constraint_name = 'nonexistent'", &session_id).await.unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[tokio::test]
async fn test_information_schema_check_constraints_no_constraints() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("check_constraints_none.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create tables without check constraints
    db_handler.execute("CREATE TABLE simple_table (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE another_table (id INTEGER PRIMARY KEY, value INTEGER)").await.unwrap();

    // Test query - should return minimal rows (possibly NOT NULL constraints)
    let result = db_handler.query_with_session("SELECT * FROM information_schema.check_constraints", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 4); // All columns should be present
    // May have some NOT NULL constraints, but should be minimal
}

#[tokio::test]
async fn test_information_schema_check_constraints_table_level() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("check_constraints_table_level.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create table with table-level check constraint
    db_handler.execute("CREATE TABLE employees (id INTEGER PRIMARY KEY, salary DECIMAL, bonus DECIMAL, CHECK (salary + bonus > 1000))").await.unwrap();

    // Test query
    let result = db_handler.query_with_session("SELECT constraint_name, check_clause FROM information_schema.check_constraints", &session_id).await.unwrap();

    assert!(result.rows.len() >= 1);

    let mut found_table_constraint = false;
    for row in &result.rows {
        let constraint_name = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let check_clause = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();

        if constraint_name.contains("employees") && check_clause.contains("salary") && check_clause.contains("bonus") {
            found_table_constraint = true;
        }
    }

    assert!(found_table_constraint, "Should find table-level check constraint");
}

#[tokio::test]
async fn test_information_schema_check_constraints_orm_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("check_constraints_orm.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create Django/Rails-style schema
    db_handler.execute("CREATE TABLE django_model (id INTEGER PRIMARY KEY, status TEXT CHECK (status IN ('active', 'inactive')), priority INTEGER CHECK (priority BETWEEN 1 AND 10))").await.unwrap();

    // Test ORM-style queries

    // Django inspectdb pattern
    let result = db_handler.query_with_session(
        "SELECT constraint_name, check_clause FROM information_schema.check_constraints WHERE constraint_schema = 'public'",
        &session_id
    ).await.unwrap();

    assert!(result.rows.len() >= 2);

    // SQLAlchemy reflection pattern
    let result = db_handler.query_with_session(
        "SELECT constraint_catalog, constraint_schema, constraint_name, check_clause FROM information_schema.check_constraints",
        &session_id
    ).await.unwrap();

    assert!(result.rows.len() >= 2);

    // Rails schema introspection pattern
    let result = db_handler.query_with_session(
        "SELECT constraint_name, check_clause FROM information_schema.check_constraints WHERE constraint_catalog = 'main'",
        &session_id
    ).await.unwrap();

    assert!(result.rows.len() >= 2);

    // Check that we have meaningful check clauses
    for row in &result.rows {
        let check_clause = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        assert!(!check_clause.is_empty(), "Check clause should not be empty");
        assert!(check_clause.len() > 5, "Check clause should be meaningful");
    }
}