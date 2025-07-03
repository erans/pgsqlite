mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_parameter_column_name() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test what column name we get for SELECT $1
    let stmt = client.prepare("SELECT $1").await.unwrap();
    
    assert_eq!(stmt.columns().len(), 1, "Expected 1 column");
    let col = &stmt.columns()[0];
    
    eprintln!("Column name: '{}'", col.name());
    eprintln!("Column type: {:?}", col.type_());
    
    // Also test execution - note: without explicit cast, parameters default to TEXT
    // So we need to pass a string or use prepare_typed
    let row = client.query_one(&stmt, &[&"42"]).await.unwrap();
    eprintln!("Columns in row: {:?}", row.columns());
    let val: String = row.get(0);
    eprintln!("Value: {}", val);
    
    server.abort();
}