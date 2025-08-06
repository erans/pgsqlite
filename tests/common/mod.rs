use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};
use std::sync::Arc;
use uuid::Uuid;

pub struct TestServer {
    pub client: Client,
    #[allow(dead_code)]
    pub port: u16,
    #[allow(dead_code)]
    pub server_handle: tokio::task::JoinHandle<()>,
    #[allow(dead_code)]
    db_path: String,
}

impl TestServer {
    #[allow(dead_code)]
    pub fn abort(self) {
        self.server_handle.abort();
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        // Abort the server handle
        self.server_handle.abort();
        
        // Clean up the database file if it exists
        if !self.db_path.is_empty() && self.db_path != ":memory:" {
            let _ = std::fs::remove_file(&self.db_path);
            // Also try to remove journal and wal files
            let _ = std::fs::remove_file(format!("{}-journal", self.db_path));
            let _ = std::fs::remove_file(format!("{}-wal", self.db_path));
            let _ = std::fs::remove_file(format!("{}-shm", self.db_path));
        }
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
    
    let test_id = Uuid::new_v4().to_string().replace("-", "");
    let db_path = format!("/tmp/pgsqlite_test_{test_id}.db");
    let db_path_clone = db_path.clone();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = Arc::new(
            pgsqlite::session::DbHandler::new(&db_path_clone).unwrap()
        );
        
        // Run custom initialization
        if let Err(e) = init(db_handler.clone()).await {
            eprintln!("Init error: {e}");
            return;
        }
        
        // Force a comprehensive cache refresh after initialization
        // This ensures that tables created during init are visible to catalog queries
        // In connection-per-session mode, we use execute method instead of direct connection access
        let _ = db_handler.execute("PRAGMA schema_version").await;
        let _ = db_handler.execute("PRAGMA table_list").await;
        
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
        db_path,
    }
}