use rusqlite::Connection;
use pgsqlite::metadata::{EnumMetadata, TypeMetadata};

#[test]
fn test_enum_metadata_initialization() {
    let conn = Connection::open_in_memory().unwrap();
    
    // Initialize TypeMetadata which should also initialize ENUM tables
    TypeMetadata::init(&conn).unwrap();
    
    // Verify __pgsqlite_enum_types table exists
    let types_count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_enum_types'",
        [],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(types_count, 1);
    
    // Verify __pgsqlite_enum_values table exists
    let values_count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__pgsqlite_enum_values'",
        [],
        |row| row.get(0),
    ).unwrap();
    assert_eq!(values_count, 1);
}

#[test]
fn test_create_and_retrieve_enum() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Create an ENUM type
    let type_oid = EnumMetadata::create_enum_type(
        &mut conn,
        "mood",
        &["happy", "sad", "angry", "neutral"],
        None,
    ).unwrap();
    
    // Verify the type was created
    assert!(type_oid >= 10000); // Should be above our offset
    
    // Retrieve by name
    let enum_type = EnumMetadata::get_enum_type(&conn, "mood").unwrap().unwrap();
    assert_eq!(enum_type.type_name, "mood");
    assert_eq!(enum_type.type_oid, type_oid);
    assert_eq!(enum_type.namespace_oid, 2200); // default public schema
    
    // Retrieve by OID
    let enum_type_by_oid = EnumMetadata::get_enum_type_by_oid(&conn, type_oid).unwrap().unwrap();
    assert_eq!(enum_type_by_oid.type_name, "mood");
    
    // Check is_enum_type
    assert!(EnumMetadata::is_enum_type(&conn, "mood").unwrap());
    assert!(!EnumMetadata::is_enum_type(&conn, "nonexistent").unwrap());
}

#[test]
fn test_enum_values() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    let type_oid = EnumMetadata::create_enum_type(
        &mut conn,
        "status",
        &["pending", "active", "completed", "cancelled"],
        None,
    ).unwrap();
    
    // Get all values
    let values = EnumMetadata::get_enum_values(&conn, type_oid).unwrap();
    assert_eq!(values.len(), 4);
    
    // Verify order
    assert_eq!(values[0].label, "pending");
    assert_eq!(values[1].label, "active");
    assert_eq!(values[2].label, "completed");
    assert_eq!(values[3].label, "cancelled");
    
    // Verify sort orders
    assert_eq!(values[0].sort_order, 1.0);
    assert_eq!(values[1].sort_order, 2.0);
    assert_eq!(values[2].sort_order, 3.0);
    assert_eq!(values[3].sort_order, 4.0);
    
    // Get individual value
    let active_value = EnumMetadata::get_enum_value(&conn, type_oid, "active").unwrap().unwrap();
    assert_eq!(active_value.label, "active");
    assert_eq!(active_value.sort_order, 2.0);
    
    // Get by OID
    let value_by_oid = EnumMetadata::get_enum_value_by_oid(&conn, active_value.value_oid).unwrap().unwrap();
    assert_eq!(value_by_oid.label, "active");
    
    // Validate values
    assert!(EnumMetadata::is_valid_enum_value(&conn, type_oid, "active").unwrap());
    assert!(!EnumMetadata::is_valid_enum_value(&conn, type_oid, "invalid").unwrap());
}

#[test]
fn test_add_enum_value() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    let type_oid = EnumMetadata::create_enum_type(
        &mut conn,
        "priority",
        &["low", "high"],
        None,
    ).unwrap();
    
    // Add value at end
    EnumMetadata::add_enum_value(&mut conn, "priority", "urgent", None, None).unwrap();
    
    let values = EnumMetadata::get_enum_values(&conn, type_oid).unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[2].label, "urgent");
    assert_eq!(values[2].sort_order, 3.0);
    
    // Add value before "high"
    EnumMetadata::add_enum_value(&mut conn, "priority", "medium", Some("high"), None).unwrap();
    
    let values = EnumMetadata::get_enum_values(&conn, type_oid).unwrap();
    assert_eq!(values.len(), 4);
    assert_eq!(values[0].label, "low");
    assert_eq!(values[1].label, "medium"); // Should be between low and high
    assert_eq!(values[2].label, "high");
    assert_eq!(values[3].label, "urgent");
    
    // Verify sort orders
    assert!(values[1].sort_order > values[0].sort_order);
    assert!(values[1].sort_order < values[2].sort_order);
    
    // Add value after "low"
    EnumMetadata::add_enum_value(&mut conn, "priority", "very_low", None, Some("low")).unwrap();
    
    let values = EnumMetadata::get_enum_values(&conn, type_oid).unwrap();
    assert_eq!(values.len(), 5);
    // Values should still be in correct order based on sort_order
    let labels: Vec<String> = values.iter().map(|v| v.label.clone()).collect();
    assert!(labels.contains(&"very_low".to_string()));
}

#[test]
fn test_drop_enum_type() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Create and then drop an ENUM
    EnumMetadata::create_enum_type(&mut conn, "temp_enum", &["a", "b", "c"], None).unwrap();
    
    // Verify it exists
    assert!(EnumMetadata::is_enum_type(&conn, "temp_enum").unwrap());
    
    // Drop it
    EnumMetadata::drop_enum_type(&mut conn, "temp_enum").unwrap();
    
    // Verify it's gone
    assert!(!EnumMetadata::is_enum_type(&conn, "temp_enum").unwrap());
    
    // Verify values are also gone
    let value_count: i32 = conn.query_row(
        "SELECT COUNT(*) FROM __pgsqlite_enum_values WHERE type_oid IN (SELECT type_oid FROM __pgsqlite_enum_types WHERE type_name = ?1)",
        ["temp_enum"],
        |row| row.get(0),
    ).unwrap_or(0);
    assert_eq!(value_count, 0);
}

#[test]
fn test_stable_oid_generation() {
    // Test that OIDs are stable across runs
    let oid1 = EnumMetadata::generate_type_oid("test_type");
    let oid2 = EnumMetadata::generate_type_oid("test_type");
    assert_eq!(oid1, oid2);
    
    // Different names should produce different OIDs
    let oid3 = EnumMetadata::generate_type_oid("other_type");
    assert_ne!(oid1, oid3);
    
    // Value OIDs should be stable too
    let value_oid1 = EnumMetadata::generate_value_oid(oid1, "value1");
    let value_oid2 = EnumMetadata::generate_value_oid(oid1, "value1");
    assert_eq!(value_oid1, value_oid2);
    
    // Different values or types should produce different OIDs
    let value_oid3 = EnumMetadata::generate_value_oid(oid1, "value2");
    let value_oid4 = EnumMetadata::generate_value_oid(oid3, "value1");
    assert_ne!(value_oid1, value_oid3);
    assert_ne!(value_oid1, value_oid4);
}

#[test]
fn test_get_all_enum_types() {
    let mut conn = Connection::open_in_memory().unwrap();
    TypeMetadata::init(&conn).unwrap();
    
    // Create multiple ENUM types
    EnumMetadata::create_enum_type(&mut conn, "color", &["red", "green", "blue"], None).unwrap();
    EnumMetadata::create_enum_type(&mut conn, "size", &["small", "medium", "large"], None).unwrap();
    EnumMetadata::create_enum_type(&mut conn, "animal", &["dog", "cat", "bird"], None).unwrap();
    
    // Get all types
    let all_types = EnumMetadata::get_all_enum_types(&conn).unwrap();
    assert_eq!(all_types.len(), 3);
    
    // Verify they're sorted by name
    let names: Vec<String> = all_types.iter().map(|t| t.type_name.clone()).collect();
    assert_eq!(names, vec!["animal", "color", "size"]);
}