use tokio_postgres::{NoTls, Error};

#[tokio::test]
async fn test_unnamed_statement_with_cast() -> Result<(), Error> {
    env_logger::init();
    
    // Start pgsqlite
    let pgsqlite_path = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("pgsqlite");
    
    let mut child = std::process::Command::new(pgsqlite_path)
        .arg("--database")
        .arg(":memory:")
        .arg("--port")
        .arg("5434")
        .spawn()
        .expect("Failed to start pgsqlite");
    
    // Give it time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    // Connect
    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5434 user=postgres dbname=main",
        NoTls,
    ).await?;
    
    // Spawn connection handler
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    
    // Create test table
    client.execute("CREATE TABLE test_table (id INTEGER, name TEXT)", &[]).await?;
    client.execute("INSERT INTO test_table VALUES (1, 'test')", &[]).await?;
    
    // This is the problematic query - using unnamed statement with cast
    let stmt = client.prepare("SELECT id FROM test_table WHERE name = $1::VARCHAR").await?;
    let rows = client.query(&stmt, &[&"test"]).await?;
    
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    
    // Kill pgsqlite
    child.kill().expect("Failed to kill pgsqlite");
    
    Ok(())
}