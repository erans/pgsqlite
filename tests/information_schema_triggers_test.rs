use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_information_schema_triggers_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("triggers.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test table and trigger
    db_handler.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)").await.unwrap();
    db_handler.execute("CREATE TRIGGER user_audit BEFORE INSERT ON users BEGIN INSERT INTO audit_log (action, table_name) VALUES ('INSERT', 'users'); END").await.unwrap();

    // Test basic query
    let result = db_handler.query_with_session("SELECT trigger_name, event_manipulation, event_object_table FROM information_schema.triggers", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 3);
    assert_eq!(result.columns[0], "trigger_name");
    assert_eq!(result.columns[1], "event_manipulation");
    assert_eq!(result.columns[2], "event_object_table");

    // Should find at least one trigger
    assert!(result.rows.len() >= 1, "Expected at least one trigger");

    // Check trigger data
    let trigger_names: Vec<String> = result.rows.iter()
        .map(|row| String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap())
        .collect();

    assert!(trigger_names.contains(&"user_audit".to_string()),
           "Expected to find user_audit trigger. All triggers: {:?}", trigger_names);

    for row in &result.rows {
        let trigger_name = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let event_manipulation = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        let event_object_table = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();

        if trigger_name == "user_audit" {
            assert_eq!(event_manipulation, "INSERT");
            assert_eq!(event_object_table, "users");
        }
    }
}

#[tokio::test]
async fn test_information_schema_triggers_all_columns() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("triggers_all.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test table and trigger
    db_handler.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price DECIMAL)").await.unwrap();
    db_handler.execute("CREATE TRIGGER price_update AFTER UPDATE ON products BEGIN UPDATE products SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id; END").await.unwrap();

    // Test all columns
    let result = db_handler.query_with_session("SELECT * FROM information_schema.triggers", &session_id).await.unwrap();

    // Verify all 17 standard columns are present
    assert_eq!(result.columns.len(), 17);
    let expected_columns = vec![
        "trigger_catalog",
        "trigger_schema",
        "trigger_name",
        "event_manipulation",
        "event_object_catalog",
        "event_object_schema",
        "event_object_table",
        "action_order",
        "action_condition",
        "action_statement",
        "action_orientation",
        "action_timing",
        "action_reference_old_table",
        "action_reference_new_table",
        "action_reference_old_row",
        "action_reference_new_row",
        "created"
    ];

    for (i, expected) in expected_columns.iter().enumerate() {
        assert_eq!(result.columns[i], *expected);
    }

    // Should have at least one trigger
    assert!(result.rows.len() >= 1);

    // Verify data structure
    for row in &result.rows {
        let trigger_catalog = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let trigger_schema = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        let trigger_name = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();
        let event_manipulation = String::from_utf8(row[3].as_ref().unwrap().clone()).unwrap();
        let action_orientation = String::from_utf8(row[10].as_ref().unwrap().clone()).unwrap();
        let action_timing = String::from_utf8(row[11].as_ref().unwrap().clone()).unwrap();

        assert_eq!(trigger_catalog, "main");
        assert_eq!(trigger_schema, "public");
        assert!(!trigger_name.is_empty());
        assert!(event_manipulation == "INSERT" || event_manipulation == "UPDATE" || event_manipulation == "DELETE");
        assert_eq!(action_orientation, "ROW"); // SQLite triggers are always ROW-level
        assert!(action_timing == "BEFORE" || action_timing == "AFTER" || action_timing == "INSTEAD OF");
    }
}

#[tokio::test]
async fn test_information_schema_triggers_multiple_triggers() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("triggers_multi.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test tables and multiple triggers
    db_handler.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total DECIMAL, status TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE inventory (id INTEGER PRIMARY KEY, product_id INTEGER, quantity INTEGER)").await.unwrap();

    db_handler.execute("CREATE TRIGGER orders_before_insert BEFORE INSERT ON orders BEGIN UPDATE orders SET created_at = CURRENT_TIMESTAMP; END").await.unwrap();
    db_handler.execute("CREATE TRIGGER orders_after_update AFTER UPDATE ON orders BEGIN INSERT INTO audit (action, table_name) VALUES ('UPDATE', 'orders'); END").await.unwrap();
    db_handler.execute("CREATE TRIGGER inventory_update BEFORE UPDATE ON inventory BEGIN UPDATE inventory SET last_updated = CURRENT_TIMESTAMP; END").await.unwrap();

    // Test query
    let result = db_handler.query_with_session("SELECT trigger_name, event_object_table, action_timing, event_manipulation FROM information_schema.triggers ORDER BY trigger_name", &session_id).await.unwrap();

    // Should have multiple triggers
    assert!(result.rows.len() >= 3, "Expected at least 3 triggers");

    // Check that we have triggers from both tables
    let mut has_orders_trigger = false;
    let mut has_inventory_trigger = false;

    for row in &result.rows {
        let trigger_name = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let event_object_table = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        let action_timing = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();
        let event_manipulation = String::from_utf8(row[3].as_ref().unwrap().clone()).unwrap();

        if trigger_name.contains("orders") {
            has_orders_trigger = true;
            assert_eq!(event_object_table, "orders");
        }
        if trigger_name.contains("inventory") {
            has_inventory_trigger = true;
            assert_eq!(event_object_table, "inventory");
        }

        // Verify timing and event are valid
        assert!(action_timing == "BEFORE" || action_timing == "AFTER");
        assert!(event_manipulation == "INSERT" || event_manipulation == "UPDATE" || event_manipulation == "DELETE");
    }

    assert!(has_orders_trigger, "Should have triggers from orders table");
    assert!(has_inventory_trigger, "Should have triggers from inventory table");
}

#[tokio::test]
async fn test_information_schema_triggers_where_filtering() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("triggers_filter.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test tables and triggers
    db_handler.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE suppliers (id INTEGER PRIMARY KEY, name TEXT, contact TEXT)").await.unwrap();

    db_handler.execute("CREATE TRIGGER customer_insert BEFORE INSERT ON customers BEGIN UPDATE customers SET created_at = CURRENT_TIMESTAMP; END").await.unwrap();
    db_handler.execute("CREATE TRIGGER supplier_update AFTER UPDATE ON suppliers BEGIN INSERT INTO audit VALUES ('supplier updated'); END").await.unwrap();

    // Test filtering by table name
    let result = db_handler.query_with_session("SELECT trigger_name FROM information_schema.triggers WHERE event_object_table = 'customers'", &session_id).await.unwrap();

    assert!(result.rows.len() >= 1, "Expected at least one trigger for customers table");
    for row in &result.rows {
        let trigger_name = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        assert!(trigger_name.contains("customer"));
    }

    // Test filtering by event manipulation
    let result = db_handler.query_with_session("SELECT trigger_name FROM information_schema.triggers WHERE event_manipulation = 'INSERT'", &session_id).await.unwrap();
    assert!(result.rows.len() >= 1); // Should find INSERT triggers

    // Test filtering by trigger schema
    let result = db_handler.query_with_session("SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = 'public'", &session_id).await.unwrap();
    assert!(result.rows.len() >= 2); // Should find all triggers

    // Test filtering with no matches
    let result = db_handler.query_with_session("SELECT trigger_name FROM information_schema.triggers WHERE trigger_name = 'nonexistent'", &session_id).await.unwrap();
    assert_eq!(result.rows.len(), 0);
}

#[tokio::test]
async fn test_information_schema_triggers_no_triggers() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("triggers_none.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create tables without triggers
    db_handler.execute("CREATE TABLE simple_table (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE another_table (id INTEGER PRIMARY KEY, value INTEGER)").await.unwrap();

    // Test query - should return no rows
    let result = db_handler.query_with_session("SELECT * FROM information_schema.triggers", &session_id).await.unwrap();

    assert_eq!(result.columns.len(), 17); // All columns should be present
    assert_eq!(result.rows.len(), 0); // No triggers
}

#[tokio::test]
async fn test_information_schema_triggers_timing_types() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("triggers_timing.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test table and triggers with different timings
    db_handler.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, name TEXT, timestamp DATETIME)").await.unwrap();

    db_handler.execute("CREATE TRIGGER events_before BEFORE INSERT ON events BEGIN UPDATE events SET created = CURRENT_TIMESTAMP; END").await.unwrap();
    db_handler.execute("CREATE TRIGGER events_after AFTER UPDATE ON events BEGIN INSERT INTO audit VALUES ('after update'); END").await.unwrap();

    // Test query
    let result = db_handler.query_with_session("SELECT trigger_name, action_timing, event_manipulation FROM information_schema.triggers", &session_id).await.unwrap();

    assert!(result.rows.len() >= 2);

    let mut found_before = false;
    let mut found_after = false;

    for row in &result.rows {
        let trigger_name = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let action_timing = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        let event_manipulation = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();

        if trigger_name.contains("before") {
            found_before = true;
            assert_eq!(action_timing, "BEFORE");
            assert_eq!(event_manipulation, "INSERT");
        }
        if trigger_name.contains("after") {
            found_after = true;
            assert_eq!(action_timing, "AFTER");
            assert_eq!(event_manipulation, "UPDATE");
        }
    }

    assert!(found_before, "Should find BEFORE trigger");
    assert!(found_after, "Should find AFTER trigger");
}

#[tokio::test]
async fn test_information_schema_triggers_orm_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("triggers_orm.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create Django/Rails-style schema with triggers
    db_handler.execute("CREATE TABLE django_model (id INTEGER PRIMARY KEY, status TEXT, updated_at TIMESTAMP)").await.unwrap();
    db_handler.execute("CREATE TRIGGER django_model_update BEFORE UPDATE ON django_model BEGIN UPDATE django_model SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id; END").await.unwrap();

    // Test ORM-style queries

    // Django inspectdb pattern
    let result = db_handler.query_with_session(
        "SELECT trigger_name, event_object_table, action_timing, event_manipulation FROM information_schema.triggers WHERE trigger_schema = 'public'",
        &session_id
    ).await.unwrap();

    assert!(result.rows.len() >= 1);

    // SQLAlchemy reflection pattern
    let result = db_handler.query_with_session(
        "SELECT trigger_catalog, trigger_schema, trigger_name, event_object_catalog, event_object_schema, event_object_table FROM information_schema.triggers",
        &session_id
    ).await.unwrap();

    assert!(result.rows.len() >= 1);

    // Rails schema introspection pattern
    let result = db_handler.query_with_session(
        "SELECT trigger_name, action_statement FROM information_schema.triggers WHERE trigger_catalog = 'main'",
        &session_id
    ).await.unwrap();

    assert!(result.rows.len() >= 1);

    // Check that we have meaningful action statements
    for row in &result.rows {
        let action_statement = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        assert!(!action_statement.is_empty(), "Action statement should not be empty");
        assert!(action_statement.len() > 10, "Action statement should be meaningful");
        assert!(action_statement.to_uppercase().contains("TRIGGER"), "Action statement should contain CREATE TRIGGER");
    }
}

#[tokio::test]
async fn test_information_schema_triggers_action_statement() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("triggers_action.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create test table and trigger with complex action
    db_handler.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance DECIMAL, account_type TEXT)").await.unwrap();
    db_handler.execute("CREATE TRIGGER account_balance_check BEFORE UPDATE ON accounts WHEN NEW.balance < 0 BEGIN SELECT RAISE(ABORT, 'Balance cannot be negative'); END").await.unwrap();

    // Test query focusing on action statement
    let result = db_handler.query_with_session("SELECT trigger_name, action_statement, action_condition FROM information_schema.triggers WHERE trigger_name LIKE '%balance%'", &session_id).await.unwrap();

    assert!(result.rows.len() >= 1);

    for row in &result.rows {
        let trigger_name = String::from_utf8(row[0].as_ref().unwrap().clone()).unwrap();
        let action_statement = String::from_utf8(row[1].as_ref().unwrap().clone()).unwrap();
        let action_condition = String::from_utf8(row[2].as_ref().unwrap().clone()).unwrap();

        if trigger_name.contains("balance") {
            // Should contain the full CREATE TRIGGER statement
            assert!(action_statement.to_uppercase().contains("CREATE TRIGGER"));
            assert!(action_statement.to_uppercase().contains("BEFORE UPDATE"));
            assert!(action_statement.to_uppercase().contains("ACCOUNTS"));

            // Note: SQLite doesn't expose WHEN conditions separately, so action_condition will be empty
            assert!(action_condition.is_empty());
        }
    }
}