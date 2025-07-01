#[cfg(test)]
mod tests {
    use pgsqlite::protocol::PostgresCodec;
    use pgsqlite::query::QueryExecutorCompat;
    
    #[cfg(feature = "zero-copy-protocol")]
    use pgsqlite::protocol::DirectConnection;
    #[cfg(feature = "zero-copy-protocol")]
    use pgsqlite::query::QueryExecutorV2;
    use pgsqlite::session::DbHandler;
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Framed;
    use std::sync::Arc;
    
    #[tokio::test]
    async fn test_query_executor_v2_with_framed() {
        // Create test database
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)").await.unwrap();
        db_handler.execute("INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob')").await.unwrap();
        
        // Set up test socket pair
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        
        let client = TcpStream::connect(addr).await.unwrap();
        let server = server_task.await.unwrap();
        
        // Create framed connection
        let codec = PostgresCodec::new();
        let mut framed = Framed::new(server, codec);
        
        // Test QueryExecutorCompat (uses QueryExecutorV2 internally)
        QueryExecutorCompat::execute_query(&mut framed, &db_handler, "SELECT * FROM test").await.unwrap();
        
        // Test DML operations
        QueryExecutorCompat::execute_query(&mut framed, &db_handler, "INSERT INTO test (id, name) VALUES (3, 'Charlie')").await.unwrap();
        QueryExecutorCompat::execute_query(&mut framed, &db_handler, "UPDATE test SET name = 'Bob2' WHERE id = 2").await.unwrap();
        QueryExecutorCompat::execute_query(&mut framed, &db_handler, "DELETE FROM test WHERE id = 1").await.unwrap();
        
        drop(client);
    }
    
    #[cfg(feature = "zero-copy-protocol")]
    #[tokio::test]
    async fn test_query_executor_v2_with_direct_connection() {
        use pgsqlite::protocol::WriterType;
        
        // Create test database
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        
        // Create test table
        db_handler.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)").await.unwrap();
        
        // Set up test socket pair
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        
        let client = TcpStream::connect(addr).await.unwrap();
        let server = server_task.await.unwrap();
        
        // Create DirectConnection
        let mut direct_conn = DirectConnection::new(server);
        
        // Test QueryExecutorV2 with DirectConnection's writer
        let writer = direct_conn.writer();
        
        // Test SELECT
        QueryExecutorV2::execute_query(writer, &db_handler, "SELECT * FROM test").await.unwrap();
        
        // Test INSERT with zero-copy optimization
        QueryExecutorV2::execute_query(writer, &db_handler, "INSERT INTO test (id, value) VALUES (1, 100)").await.unwrap();
        
        // Test UPDATE
        QueryExecutorV2::execute_query(writer, &db_handler, "UPDATE test SET value = 200 WHERE id = 1").await.unwrap();
        
        // Test DELETE
        QueryExecutorV2::execute_query(writer, &db_handler, "DELETE FROM test WHERE id = 1").await.unwrap();
        
        // Flush any pending data
        writer.flush().await.unwrap();
        
        drop(client);
    }
    
    #[tokio::test]
    async fn test_query_executor_migration_path() {
        // This test demonstrates the migration path from QueryExecutor to QueryExecutorV2
        
        let db_handler = Arc::new(DbHandler::new(":memory:").unwrap());
        db_handler.execute("CREATE TABLE migration_test (id INTEGER)").await.unwrap();
        
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            stream
        });
        
        let client = TcpStream::connect(addr).await.unwrap();
        let server = server_task.await.unwrap();
        
        let codec = PostgresCodec::new();
        let mut framed = Framed::new(server, codec);
        
        // Old way: QueryExecutor::execute_query(&mut framed, &db_handler, query)
        // New way: QueryExecutorCompat::execute_query(&mut framed, &db_handler, query)
        // Both work the same, but QueryExecutorCompat uses the new ProtocolWriter trait
        
        // Insert test data
        QueryExecutorCompat::execute_query(&mut framed, &db_handler, "INSERT INTO migration_test VALUES (1), (2), (3)").await.unwrap();
        
        // Verify with SELECT
        QueryExecutorCompat::execute_query(&mut framed, &db_handler, "SELECT COUNT(*) FROM migration_test").await.unwrap();
        
        drop(client);
    }
}