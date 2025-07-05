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
    
    // Verify the translation includes CHECK constraint
    assert!(translated_sql.contains("TEXT"));
    assert!(translated_sql.contains("CHECK"));
    assert!(translated_sql.contains("'happy'"));
    assert!(translated_sql.contains("'sad'"));
    assert!(translated_sql.contains("'angry'"));
    
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
    
    // Invalid value should fail due to CHECK constraint
    let result = conn.execute("INSERT INTO task (id, status) VALUES (2, 'invalid')", []);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("CHECK constraint failed"));
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
    
    // Should have two CHECK constraints
    let check_count = translated_sql.matches("CHECK").count();
    assert_eq!(check_count, 2);
    
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
    
    // Verify proper escaping in CHECK constraint
    assert!(translated_sql.contains("'it''s'")); // Should be double-quoted for SQL
    assert!(translated_sql.contains("'quote\"test'"));
}