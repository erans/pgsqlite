mod common;
use common::*;

#[tokio::test]
async fn test_invalid_enum_value_error() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type using simple_query for DDL
    client.simple_query("CREATE TYPE mood AS ENUM ('happy', 'sad', 'neutral')")
        .await
        .unwrap();
    
    // Create a table with ENUM column using simple_query for DDL
    client.simple_query("CREATE TABLE person (name TEXT, current_mood mood)")
        .await
        .unwrap();
    
    // Try to insert an invalid enum value
    let result = client.execute(
        "INSERT INTO person (name, current_mood) VALUES ('Alice', 'angry')",
        &[]
    ).await;
    
    assert!(result.is_err());
    let error = result.unwrap_err();
    let error_str = error.to_string();
    assert!(error_str.contains("invalid input value for enum mood: \"angry\""),
            "Expected PostgreSQL-compatible error message, got: {error_str}");
}

#[tokio::test]
async fn test_type_does_not_exist_error() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Try to drop a non-existent type using simple_query for DDL
    let result = client.simple_query("DROP TYPE nonexistent_type").await;
    
    assert!(result.is_err());
    let error = result.unwrap_err();
    let error_str = error.to_string();
    assert!(error_str.contains("type \"nonexistent_type\" does not exist") || 
            error_str.contains("Type 'nonexistent_type' does not exist"),
            "Expected PostgreSQL-compatible error message, got: {error_str}");
}

#[tokio::test]
async fn test_cannot_drop_type_with_dependencies() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Create an ENUM type using simple_query for DDL
    client.simple_query("CREATE TYPE status AS ENUM ('active', 'inactive', 'pending')")
        .await
        .unwrap();
    
    // Create a table using the ENUM using simple_query for DDL
    client.simple_query("CREATE TABLE items (id INTEGER, item_status status)")
        .await
        .unwrap();
    
    // Try to drop the type without CASCADE using simple_query for DDL
    let result = client.simple_query("DROP TYPE status").await;
    
    assert!(result.is_err());
    let error = result.unwrap_err();
    let error_str = error.to_string();
    assert!(error_str.contains("cannot drop type status because other objects depend on it") ||
            error_str.contains("Cannot drop type 'status': used by table 'items'"),
            "Expected PostgreSQL-compatible error message, got: {error_str}");
    
    // Verify CASCADE works using simple_query for DDL
    client.simple_query("DROP TYPE status CASCADE")
        .await
        .unwrap();
}

#[tokio::test]
async fn test_alter_type_that_does_not_exist() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Try to alter a non-existent type using simple_query for DDL
    let result = client.simple_query("ALTER TYPE nonexistent ADD VALUE 'new_value'").await;
    
    assert!(result.is_err());
    let error = result.unwrap_err();
    let error_str = error.to_string();
    assert!(error_str.contains("type \"nonexistent\" does not exist") || 
            error_str.contains("Type 'nonexistent' does not exist"),
            "Expected PostgreSQL-compatible error message, got: {error_str}");
}

#[tokio::test] 
async fn test_if_exists_clause() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // DROP TYPE IF EXISTS should not error on non-existent type using simple_query for DDL
    client.simple_query("DROP TYPE IF EXISTS nonexistent_type")
        .await
        .unwrap();
    
    // Create and drop with IF EXISTS using simple_query for DDL
    client.simple_query("CREATE TYPE test_type AS ENUM ('a', 'b')")
        .await
        .unwrap();
    
    client.simple_query("DROP TYPE IF EXISTS test_type")
        .await
        .unwrap();
    
    // Second drop should also succeed with IF EXISTS using simple_query for DDL
    client.simple_query("DROP TYPE IF EXISTS test_type")
        .await
        .unwrap();
}