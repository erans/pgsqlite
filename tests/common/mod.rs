use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};
use std::sync::Arc;

pub struct TestServer {
    pub client: Client,
    #[allow(dead_code)]
    pub port: u16,
    #[allow(dead_code)]
    pub server_handle: tokio::task::JoinHandle<()>,
}

impl TestServer {
    #[allow(dead_code)]
    pub fn abort(self) {
        self.server_handle.abort();
    }
}

/// Setup a test server with an in-memory SQLite database
#[allow(dead_code)]
pub async fn setup_test_server() -> TestServer {
    setup_test_server_with_init(|_| Box::pin(async move { Ok(()) })).await
}

/// Setup a test server with custom initialization
pub async fn setup_test_server_with_init<F, Fut>(init: F) -> TestServer 
where
    F: FnOnce(Arc<pgsqlite::session::DbHandler>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send,
{
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Run custom initialization
        if let Err(e) = init(db_handler.clone()).await {
            eprintln!("Init error: {e}");
            return;
        }
        
        // Force a comprehensive cache refresh after initialization
        // This ensures that tables created during init are visible to catalog queries
        {
            let conn = db_handler.get_mut_connection().unwrap();
            // Force a transaction commit to ensure changes are persisted
            let _ = conn.execute_batch("BEGIN; COMMIT;");
            // Force SQLite to refresh its schema cache by querying sqlite_master
            let _ = conn.execute_batch("SELECT name FROM sqlite_master WHERE type='table'");
            // Explicitly refresh SQLite's internal schema
            let _ = conn.execute_batch("PRAGMA schema_version; PRAGMA table_list;");
            drop(conn);
        }
        
        // Add a small delay to ensure changes propagate
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        
        let (stream, addr) = listener.accept().await.unwrap();
        if let Err(e) = pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await {
            eprintln!("Connection handling error: {e}");
        }
    });
    
    // Give server time to start
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect with tokio-postgres
    let config = format!("host=localhost port={port} dbname=test user=testuser");
    let (client, connection) = tokio_postgres::connect(&config, NoTls).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {e}");
        }
    });
    
    TestServer {
        client,
        port,
        server_handle,
    }
}