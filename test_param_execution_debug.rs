use tokio_postgres::{NoTls, Client};

#[tokio::main]
async fn main() {
    // Initialize logging
    env_logger::init();
    
    // Start pgsqlite
    let port = 15432;
    let db_path = "/tmp/test_param_exec_debug.db";
    
    // Remove old db
    std::fs::remove_file(db_path).ok();
    
    // Start server in background
    let server = std::process::Command::new("./target/release/pgsqlite")
        .args(&["--database", db_path, "--port", &port.to_string()])
        .spawn()
        .expect("Failed to start pgsqlite");
    
    // Wait for server to start
    std::thread::sleep(std::time::Duration::from_secs(2));
    
    // Connect
    let (client, connection) = tokio_postgres::connect(
        &format!("host=localhost port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Create test table
    client.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, cost REAL, markup REAL)", &[]).await.unwrap();
    client.execute("INSERT INTO items (id, cost, markup) VALUES (1, 50.0, 1.5)", &[]).await.unwrap();
    
    // Test direct query first
    println!("\n=== Testing direct query ===");
    let rows = client.query("SELECT cost * markup AS selling_price FROM items WHERE id = 1", &[]).await.unwrap();
    println!("Direct query returned {} rows", rows.len());
    if !rows.is_empty() {
        let value: f64 = rows[0].get(0);
        println!("Result: {}", value);
    }
    
    // Test parameterized query
    println!("\n=== Testing parameterized query ===");
    let stmt = client.prepare("SELECT cost * markup AS selling_price FROM items WHERE id = $1").await.unwrap();
    
    println!("Statement prepared. Param types: {:?}", stmt.params());
    
    // Try to execute with integer parameter
    println!("Executing query with i32 parameter...");
    match client.query(&stmt, &[&1i32]).await {
        Ok(rows) => {
            println!("Success! Got {} rows", rows.len());
            if !rows.is_empty() {
                let value: f64 = rows[0].get(0);
                println!("Result: {}", value);
            }
        }
        Err(e) => {
            println!("Error: {}", e);
            println!("Error type: {:?}", e);
        }
    }
    
    // Try with explicit text parameter
    println!("\n=== Testing with text parameter ===");
    match client.query(&stmt, &[&"1"]).await {
        Ok(rows) => {
            println!("Success with text! Got {} rows", rows.len());
            if !rows.is_empty() {
                let value: f64 = rows[0].get(0);
                println!("Result: {}", value);
            }
        }
        Err(e) => {
            println!("Error with text: {}", e);
        }
    }
    
    // Kill server
    std::process::Command::new("pkill")
        .args(&["-f", "pgsqlite.*test_param_exec_debug"])
        .output()
        .ok();
}