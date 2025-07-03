mod common;
use common::setup_test_server;

#[tokio::test]
async fn trace_param_flow() {
    unsafe { std::env::set_var("RUST_LOG", "pgsqlite=info"); }
    let _ = env_logger::try_init();
    
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test with string parameter
    eprintln!("\n=== Testing SELECT $1 with string 'hello' ===");
    match client.query_one("SELECT $1", &[&"hello"]).await {
        Ok(row) => {
            eprintln!("Success! Got row with {} columns", row.columns().len());
            if let Ok(s) = row.try_get::<_, String>(0) {
                eprintln!("Value as string: '{}'", s);
            }
        }
        Err(e) => eprintln!("Error: {:?}", e),
    }
    
    // Test with int parameter
    eprintln!("\n=== Testing SELECT $1 with int 42 ===");
    match client.query_one("SELECT $1", &[&42i32]).await {
        Ok(row) => {
            eprintln!("Success! Got row with {} columns", row.columns().len());
            if let Ok(i) = row.try_get::<_, i32>(0) {
                eprintln!("Value as i32: {}", i);
            } else if let Ok(s) = row.try_get::<_, String>(0) {
                eprintln!("Value as string: '{}'", s);
            }
        }
        Err(e) => eprintln!("Error: {:?}", e),
    }
    
    server.abort();
}