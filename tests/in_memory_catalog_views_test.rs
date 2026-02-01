use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_postgres::NoTls;

// Regression test: shared in-memory SQLite URI must keep catalog views alive
// across per-session connections.
#[tokio::test]
async fn test_pg_enum_view_available_in_shared_in_memory_uri() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // This matches the integration test runner's DB mode.
    let db_path = "file::memory:?cache=shared&uri=true";
    let db_handler = Arc::new(pgsqlite::session::DbHandler::new(db_path).unwrap());

    tokio::spawn(async move {
        if let Ok((stream, client_addr)) = listener.accept().await {
            let db_clone = db_handler.clone();
            tokio::spawn(async move {
                if let Err(e) = pgsqlite::handle_test_connection_with_pool(stream, client_addr, db_clone).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    });

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname={db_path}", addr.port()),
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Should not error even if no ENUMs exist.
    let _ = client
        .query(
            "SELECT enumtypid, enumsortorder, enumlabel FROM pg_catalog.pg_enum ORDER BY enumtypid, enumsortorder",
            &[],
        )
        .await
        .unwrap();
}
