use pgsqlite::session::DbHandler;
use std::time::Instant;
use uuid::Uuid;

#[tokio::test]
async fn test_insert_detailed_timing() {
    println!("\n=== DETAILED INSERT TIMING ANALYSIS ===");
    
    // Use a temporary file instead of in-memory database
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{}.db", test_id);
    
    let db = DbHandler::new(&db_path).expect("Failed to create database");
    
    // Create a session
    let session_id = Uuid::new_v4();
    db.create_session_connection(session_id).await.expect("Failed to create session connection");
    
    // Create test table
    db.execute_with_session("CREATE TABLE test_insert (id INTEGER PRIMARY KEY, name TEXT, value INTEGER)", &session_id)
        .await
        .expect("Failed to create table");
    
    // Warm up
    db.execute_with_session("INSERT INTO test_insert (name, value) VALUES ('warmup', 1)", &session_id)
        .await
        .expect("Failed to warm up");
    
    // Test different INSERT scenarios
    let test_cases = vec![
        ("Simple INSERT", "INSERT INTO test_insert (name, value) VALUES ('test1', 100)"),
        ("INSERT with single quotes", "INSERT INTO test_insert (name, value) VALUES ('test''s', 200)"),
        ("INSERT with numbers", "INSERT INTO test_insert (name, value) VALUES ('test123', 300)"),
        ("INSERT with longer values", "INSERT INTO test_insert (name, value) VALUES ('this is a much longer test string that might affect performance', 400)"),
    ];
    
    println!("\nIndividual INSERT timing:");
    for (desc, query) in &test_cases {
        let mut times = Vec::new();
        
        // Run each query 10 times to get average
        for _ in 0..10 {
            let start = Instant::now();
            db.execute_with_session(query, &session_id).await.expect("Failed to execute INSERT");
            times.push(start.elapsed());
        }
        
        let avg_time = times.iter().sum::<std::time::Duration>() / times.len() as u32;
        println!("{desc}: {avg_time:?} (avg of 10 runs)");
    }
    
    // Test parameterized INSERT through extended protocol
    println!("\nParameterized INSERT timing:");
    let param_query = "INSERT INTO test_insert (name, value) VALUES ($1, $2)";
    let mut param_times = Vec::new();
    
    for i in 0..10 {
        let start = Instant::now();
        db.execute_with_params(
            param_query,
            &[
                Some(format!("param{i}").into_bytes()),
                Some(i.to_string().into_bytes()),
            ],
            &session_id
        )
        .await
        .expect("Failed to execute parameterized INSERT");
        param_times.push(start.elapsed());
    }
    
    let avg_param_time = param_times.iter().sum::<std::time::Duration>() / param_times.len() as u32;
    println!("Parameterized INSERT: {avg_param_time:?} (avg of 10 runs)");
    
    // Test transaction batching
    println!("\nTransaction batching analysis:");
    
    // Without explicit transaction
    let start = Instant::now();
    for i in 0..50 {
        let query = format!("INSERT INTO test_insert (name, value) VALUES ('batch1_{i}', {i})");
        db.execute(&query).await.expect("Failed to execute INSERT");
    }
    let no_txn_time = start.elapsed();
    println!("50 INSERTs without transaction: {:?}, avg: {:?}", no_txn_time, no_txn_time / 50);
    
    // With explicit transaction
    let start = Instant::now();
    db.begin_with_session(&session_id).await.expect("Failed to begin transaction");
    for i in 0..50 {
        let query = format!("INSERT INTO test_insert (name, value) VALUES ('batch2_{i}', {i})");
        db.execute_with_session(&query, &session_id).await.expect("Failed to execute INSERT");
    }
    db.commit_with_session(&session_id).await.expect("Failed to commit transaction");
    let with_txn_time = start.elapsed();
    println!("50 INSERTs with transaction: {:?}, avg: {:?}", with_txn_time, with_txn_time / 50);
    
    // Compare CREATE TABLE with decimal vs without
    println!("\nDecimal column impact:");
    
    // Table without decimal
    let start = Instant::now();
    db.execute_with_session("CREATE TABLE no_decimal (id INTEGER, name TEXT)", &session_id)
        .await
        .expect("Failed to create table");
    let create_no_decimal = start.elapsed();
    
    // Table with decimal
    let start = Instant::now();
    db.execute_with_session("CREATE TABLE with_decimal (id INTEGER, price DECIMAL(10,2))", &session_id)
        .await
        .expect("Failed to create table");
    let create_with_decimal = start.elapsed();
    
    println!("CREATE TABLE without decimal: {create_no_decimal:?}");
    println!("CREATE TABLE with decimal: {create_with_decimal:?}");
    
    // Test INSERT into both tables
    let start = Instant::now();
    for i in 0..20 {
        let query = format!("INSERT INTO no_decimal (id, name) VALUES ({i}, 'test{i}')");
        db.execute(&query).await.expect("Failed to execute INSERT");
    }
    let insert_no_decimal = start.elapsed();
    
    let start = Instant::now();
    for i in 0..20 {
        let query = format!("INSERT INTO with_decimal (id, price) VALUES ({i}, {i}.99)");
        db.execute(&query).await.expect("Failed to execute INSERT");
    }
    let insert_with_decimal = start.elapsed();
    
    println!("20 INSERTs without decimal: {:?}, avg: {:?}", insert_no_decimal, insert_no_decimal / 20);
    println!("20 INSERTs with decimal: {:?}, avg: {:?}", insert_with_decimal, insert_with_decimal / 20);
    
    // Clean up
    db.remove_session_connection(&session_id);
    
    // Clean up database file
    drop(db);
    let _ = std::fs::remove_file(&db_path);
    let _ = std::fs::remove_file(format!("{}-journal", db_path));
    let _ = std::fs::remove_file(format!("{}-wal", db_path));
    let _ = std::fs::remove_file(format!("{}-shm", db_path));
}