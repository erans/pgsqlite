use rusqlite::Connection;
use pgsqlite::metadata::{EnumMetadata, TypeMetadata};
use pgsqlite::ddl::EnumDdlHandler;

#[test]
fn test_enum_ddl_handler() {
    let mut conn = Connection::open_in_memory().unwrap();
    
    // Initialize metadata tables
    TypeMetadata::init(&conn).unwrap();
    
    // Test CREATE TYPE
    let create_query = "CREATE TYPE mood AS ENUM ('happy', 'sad', 'angry')";
    assert!(EnumDdlHandler::is_enum_ddl(create_query));
    
    let result = EnumDdlHandler::handle_enum_ddl(&mut conn, create_query);
    assert!(result.is_ok(), "Failed to create ENUM: {result:?}");
    
    // Verify the type was created
    let enum_type = EnumMetadata::get_enum_type(&conn, "mood").unwrap();
    assert!(enum_type.is_some());
    
    let values = EnumMetadata::get_enum_values(&conn, enum_type.unwrap().type_oid).unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].label, "happy");
    assert_eq!(values[1].label, "sad");
    assert_eq!(values[2].label, "angry");
}

#[test]
fn test_enum_ddl_alter_type() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Create initial type
    EnumDdlHandler::handle_enum_ddl(&mut conn, "CREATE TYPE status AS ENUM ('pending', 'completed')").unwrap();
    
    // Add a value
    let alter_query = "ALTER TYPE status ADD VALUE 'active' AFTER 'pending'";
    assert!(EnumDdlHandler::is_enum_ddl(alter_query));
    
    let result = EnumDdlHandler::handle_enum_ddl(&mut conn, alter_query);
    assert!(result.is_ok(), "Failed to alter ENUM: {result:?}");
    
    // Verify the new value was added
    let enum_type = EnumMetadata::get_enum_type(&conn, "status").unwrap().unwrap();
    let values = EnumMetadata::get_enum_values(&conn, enum_type.type_oid).unwrap();
    assert_eq!(values.len(), 3);
    
    // Check order
    let labels: Vec<String> = values.iter().map(|v| v.label.clone()).collect();
    assert_eq!(labels, vec!["pending", "active", "completed"]);
}

#[test]
fn test_enum_ddl_drop_type() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Create and drop a type
    EnumDdlHandler::handle_enum_ddl(&mut conn, "CREATE TYPE temp AS ENUM ('a', 'b')").unwrap();
    
    // Verify it exists
    assert!(EnumMetadata::get_enum_type(&conn, "temp").unwrap().is_some());
    
    // Drop it
    let drop_query = "DROP TYPE temp";
    assert!(EnumDdlHandler::is_enum_ddl(drop_query));
    
    let result = EnumDdlHandler::handle_enum_ddl(&mut conn, drop_query);
    assert!(result.is_ok(), "Failed to drop ENUM: {result:?}");
    
    // Verify it's gone
    assert!(EnumMetadata::get_enum_type(&conn, "temp").unwrap().is_none());
}

#[test]
fn test_enum_ddl_drop_if_exists() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Drop non-existent type with IF EXISTS should succeed
    let result = EnumDdlHandler::handle_enum_ddl(&mut conn, "DROP TYPE IF EXISTS nonexistent");
    assert!(result.is_ok(), "DROP IF EXISTS should succeed: {result:?}");
    
    // Drop non-existent type without IF EXISTS should fail
    let result = EnumDdlHandler::handle_enum_ddl(&mut conn, "DROP TYPE nonexistent");
    assert!(result.is_err(), "DROP without IF EXISTS should fail");
}