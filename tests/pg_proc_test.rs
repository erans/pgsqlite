use pgsqlite::{handle_test_connection_with_pool, session::db_handler::DbHandler};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
async fn test_pg_proc_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_proc.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Start a test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn server
    tokio::spawn(async move {
        while let Ok((stream, client_addr)) = listener.accept().await {
            let db_clone = db_handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_test_connection_with_pool(stream, client_addr, db_clone).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    });

    // Connect client
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", addr.port()),
        NoTls,
    ).await.unwrap();

    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Test basic pg_proc query
    let rows = client.query("SELECT proname, prokind FROM pg_proc WHERE proname = 'length'", &[]).await.unwrap();
    assert!(!rows.is_empty(), "Should find length function");

    let row = &rows[0];
    let proname: String = row.get(0);
    let prokind: String = row.get(1);

    assert_eq!(proname, "length");
    assert_eq!(prokind, "f"); // function
    println!("✅ Found function: {} (kind: {})", proname, prokind);

    // Test wildcard query
    let rows = client.query("SELECT * FROM pg_proc WHERE proname = 'count'", &[]).await.unwrap();
    assert!(!rows.is_empty(), "Should find count function");
    println!("✅ Wildcard query returned {} columns", rows[0].len());

    // Test function count - should have many built-in functions
    let rows = client.query("SELECT COUNT(*) FROM pg_proc", &[]).await.unwrap();
    let count: i64 = rows[0].get(0);
    assert!(count > 20, "Should have many built-in functions, got {}", count);
    println!("✅ Found {} functions total", count);

    println!("🎉 pg_proc basic functionality test passed!");
}

#[tokio::test]
async fn test_pg_proc_contains_pg16_probe_functions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_proc_pg16_probes.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        while let Ok((stream, client_addr)) = listener.accept().await {
            let db_clone = db_handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_test_connection_with_pool(stream, client_addr, db_clone).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    });

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", addr.port()),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    let rows = client
        .query(
            "SELECT proname FROM pg_proc WHERE proname IN ('current_setting', 'current_database', 'current_schema', 'current_schemas') ORDER BY proname",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    for expected in [
        "current_database",
        "current_schema",
        "current_schemas",
        "current_setting",
    ] {
        assert!(
            names.iter().any(|n| n == expected),
            "Expected function {expected} in pg_proc; got {names:?}"
        );
    }
}

#[tokio::test]
async fn test_pg_proc_psql_df_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_proc_df.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Start a test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn server
    tokio::spawn(async move {
        while let Ok((stream, client_addr)) = listener.accept().await {
            let db_clone = db_handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_test_connection_with_pool(stream, client_addr, db_clone).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    });

    // Connect client
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", addr.port()),
        NoTls,
    ).await.unwrap();

    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Test query similar to what \df uses
    let query = r#"
        SELECT p.proname, p.prokind, p.prorettype, p.proargtypes
        FROM pg_proc p
        WHERE p.prokind IN ('f', 'a')
        ORDER BY p.proname
        LIMIT 10
    "#;

    let rows = client.query(query, &[]).await.unwrap();
    assert!(!rows.is_empty(), "Should find functions for \\df");

    for row in &rows {
        let proname: String = row.get(0);
        let prokind: String = row.get(1);
        let prorettype: i32 = row.get(2);  // OID of return type
        println!("Function: {} (kind: {}, return type OID: {})", proname, prokind, prorettype);
    }

    println!("✅ Found {} functions in \\df-style query", rows.len());
    println!("🎉 pg_proc \\df compatibility test passed!");
}
