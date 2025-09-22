mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_create_database_execution() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server().await;
    let client = &server.client;

    // Test CREATE DATABASE command through PostgreSQL protocol
    // Use simple_query() to ensure it goes through the DDL path
    let result = client.simple_query("CREATE DATABASE testdb").await;
    assert!(result.is_ok(), "CREATE DATABASE should succeed: {:?}", result);

    // Test different variations
    let variations = vec![
        "CREATE DATABASE mydb",
        "create database mydb",
        "Create Database mydb",
        "CREATE DATABASE mydb WITH ENCODING 'UTF8'",
        "CREATE DATABASE mydb WITH OWNER 'postgres' ENCODING 'UTF8'"
    ];

    for query in variations {
        let result = client.simple_query(query).await;
        assert!(result.is_ok(), "CREATE DATABASE variation should succeed: {} - {:?}", query, result);
    }
}