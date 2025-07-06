use rusqlite::Connection;
use pgsqlite::metadata::TypeMetadata;
use pgsqlite::ddl::EnumDdlHandler;
use pgsqlite::translator::CreateTableTranslator;

#[test]
fn test_create_table_with_enum() {
    let mut conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata tables
    TypeMetadata::init(&conn).unwrap();
    
    // Create an ENUM type
    EnumDdlHandler::handle_enum_ddl(&mut conn, "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry')").unwrap();
    
    // Create table with ENUM column
    let create_table_sql = "CREATE TABLE person (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        current_mood mood
    )";
    
    let (translated_sql, type_mappings) = CreateTableTranslator::translate_with_connection(
        create_table_sql,
        Some(&conn)
    ).unwrap();
    
    println!("Translated SQL: {}", translated_sql);
    
    // Verify the translation includes TEXT type for ENUM column
    assert!(translated_sql.contains("TEXT"));
    // With trigger-based validation, CHECK constraints are not added to CREATE TABLE
    assert!(!translated_sql.contains("CHECK"));
    
    // Execute the translated SQL
    conn.execute(&translated_sql, []).unwrap();
    
    // Verify type mapping
    assert_eq!(type_mappings.len(), 3); // id, name, current_mood
    let mood_mapping = type_mappings.get("person.current_mood").unwrap();
    assert_eq!(mood_mapping.pg_type, "mood");
    assert_eq!(mood_mapping.sqlite_type, "TEXT");
}

#[test]
fn test_enum_check_constraint() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Create ENUM and table
    EnumDdlHandler::handle_enum_ddl(&mut conn, "CREATE TYPE status AS ENUM ('pending', 'active', 'done')").unwrap();
    
    let create_table_sql = "CREATE TABLE task (id INTEGER PRIMARY KEY, status status)";
    let (translated_sql, _) = CreateTableTranslator::translate_with_connection(create_table_sql, Some(&conn)).unwrap();
    conn.execute(&translated_sql, []).unwrap();
    
    // Valid insert should succeed
    conn.execute("INSERT INTO task (id, status) VALUES (1, 'active')", []).unwrap();
    
    // With trigger-based validation, we need to create the triggers manually in this test
    // In production, the QueryExecutor would handle this
    use pgsqlite::metadata::EnumTriggers;
    EnumTriggers::init_enum_usage_table(&conn).unwrap();
    EnumTriggers::record_enum_usage(&conn, "task", "status", "status").unwrap();
    EnumTriggers::create_enum_validation_triggers(&conn, "task", "status", "status").unwrap();
    
    // Invalid value should fail due to trigger validation
    let result = conn.execute("INSERT INTO task (id, status) VALUES (2, 'invalid')", []);
    assert!(result.is_err());
    let error_msg = result.unwrap_err().to_string();
    assert!(error_msg.contains("invalid input value for enum status") || error_msg.contains("ABORT"));
}

#[test]
fn test_multiple_enum_columns() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Create multiple ENUM types
    EnumDdlHandler::handle_enum_ddl(&mut conn, "CREATE TYPE color AS ENUM ('red', 'green', 'blue')").unwrap();
    EnumDdlHandler::handle_enum_ddl(&mut conn, "CREATE TYPE size AS ENUM ('small', 'medium', 'large')").unwrap();
    
    // Create table with multiple ENUM columns
    let create_table_sql = "CREATE TABLE product (
        id INTEGER PRIMARY KEY,
        name TEXT,
        color color,
        size size
    )";
    
    let (translated_sql, type_mappings) = CreateTableTranslator::translate_with_connection(
        create_table_sql,
        Some(&conn)
    ).unwrap();
    
    // With trigger-based validation, no CHECK constraints should be in the CREATE TABLE
    let check_count = translated_sql.matches("CHECK").count();
    assert_eq!(check_count, 0);
    
    // Verify type mappings
    assert_eq!(type_mappings.get("product.color").unwrap().pg_type, "color");
    assert_eq!(type_mappings.get("product.size").unwrap().pg_type, "size");
}

#[test]
fn test_enum_with_quotes() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Create ENUM with values containing quotes
    EnumDdlHandler::handle_enum_ddl(&mut conn, "CREATE TYPE quote_test AS ENUM ('it''s', 'quote\"test', 'normal')").unwrap();
    
    let create_table_sql = "CREATE TABLE test_table (id INTEGER PRIMARY KEY, value quote_test)";
    let (translated_sql, _) = CreateTableTranslator::translate_with_connection(create_table_sql, Some(&conn)).unwrap();
    
    // With trigger-based validation, the SQL should not contain CHECK constraints
    // The ENUM values are validated by triggers, not in the CREATE TABLE statement
    assert!(!translated_sql.contains("CHECK"));
    assert!(translated_sql.contains("TEXT")); // ENUM columns are stored as TEXT
}