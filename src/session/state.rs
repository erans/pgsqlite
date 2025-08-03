use std::collections::HashMap;
use tokio::sync::{RwLock, Mutex};
use crate::protocol::TransactionStatus;
use crate::cache::QueryCache;
use crate::config::CONFIG;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use once_cell::sync::Lazy;
use crate::session::DbHandler;
use parking_lot::Mutex as ParkingMutex;
use tracing::debug;
use rusqlite::Connection;

// Global query cache shared across all sessions
pub static GLOBAL_QUERY_CACHE: Lazy<Arc<QueryCache>> = Lazy::new(|| {
    Arc::new(QueryCache::new(CONFIG.query_cache_size, CONFIG.query_cache_ttl))
});

// Global session counter for WAL mode isolation optimization
static ACTIVE_SESSION_COUNT: AtomicUsize = AtomicUsize::new(0);

pub struct SessionState {
    pub id: uuid::Uuid,
    pub database: String,
    pub user: String,
    pub parameters: RwLock<HashMap<String, String>>,
    pub prepared_statements: RwLock<HashMap<String, PreparedStatement>>,
    pub portals: RwLock<HashMap<String, Portal>>,
    pub transaction_status: RwLock<TransactionStatus>,
    pub portal_manager: Arc<super::PortalManager>,
    pub python_param_mapping: RwLock<HashMap<String, Vec<String>>>, // Maps statement name to Python parameter names
    pub db_handler: Mutex<Option<Arc<DbHandler>>>, // Reference to the database handler for session lifecycle management
    pub cached_connection: ParkingMutex<Option<Arc<ParkingMutex<Connection>>>>, // Cached connection for fast access
}

pub struct PreparedStatement {
    pub query: String,
    pub translated_query: Option<String>, // Cached translation of the query
    pub param_types: Vec<i32>,
    pub client_param_types: Vec<i32>, // Original types sent by client in Parse message
    pub param_formats: Vec<i16>,
    pub field_descriptions: Vec<crate::protocol::FieldDescription>,
    pub translation_metadata: Option<crate::translator::TranslationMetadata>, // Type hints from query translation
}

#[derive(Clone)]
pub struct Portal {
    pub statement_name: String,
    pub query: String,
    pub translated_query: Option<String>, // Cached translation from prepared statement
    pub bound_values: Vec<Option<Vec<u8>>>,
    pub param_formats: Vec<i16>,
    pub result_formats: Vec<i16>,
    pub inferred_param_types: Option<Vec<i32>>, // Types inferred from actual values
    pub client_param_types: Vec<i32>, // Original types sent by client (for binary decoding)
}

impl SessionState {
    pub fn new(database: String, user: String) -> Self {
        let mut parameters = HashMap::new();
        parameters.insert("server_version".to_string(), "14.0 (SQLite wrapper)".to_string());
        parameters.insert("server_encoding".to_string(), "UTF8".to_string());
        parameters.insert("client_encoding".to_string(), "UTF8".to_string());
        parameters.insert("DateStyle".to_string(), "ISO, MDY".to_string());
        parameters.insert("TimeZone".to_string(), "UTC".to_string());
        parameters.insert("IntervalStyle".to_string(), "postgres".to_string());
        parameters.insert("integer_datetimes".to_string(), "on".to_string());
        
        // Increment active session count
        ACTIVE_SESSION_COUNT.fetch_add(1, Ordering::Relaxed);
        
        SessionState {
            id: uuid::Uuid::new_v4(),
            database,
            user,
            parameters: RwLock::new(parameters),
            prepared_statements: RwLock::new(HashMap::new()),
            portals: RwLock::new(HashMap::new()),
            transaction_status: RwLock::new(TransactionStatus::Idle),
            portal_manager: Arc::new(super::PortalManager::new(100)), // Allow up to 100 concurrent portals
            python_param_mapping: RwLock::new(HashMap::new()),
            db_handler: Mutex::new(None), // Will be set after session is created
            cached_connection: ParkingMutex::new(None), // Initialize as None
        }
    }

    /// Create a new session with default database and user (for testing)
    #[cfg(test)]
    pub fn new_test() -> Self {
        Self::new("test".to_string(), "test".to_string())
    }

    /// Check if the session is currently in a transaction
    pub async fn in_transaction(&self) -> bool {
        matches!(
            *self.transaction_status.read().await,
            TransactionStatus::InTransaction | TransactionStatus::InFailedTransaction
        )
    }
    
    /// Set the transaction status
    pub async fn set_transaction_status(&self, status: TransactionStatus) {
        *self.transaction_status.write().await = status;
    }
    
    /// Get the transaction status
    pub async fn get_transaction_status(&self) -> TransactionStatus {
        *self.transaction_status.read().await
    }
    
    /// Get the current number of active sessions
    pub async fn get_session_count(&self) -> usize {
        ACTIVE_SESSION_COUNT.load(Ordering::Relaxed)
    }
    
    /// Set the database handler for this session
    /// This should be called after the session is created and a connection is established
    pub async fn set_db_handler(&self, db_handler: Arc<DbHandler>) {
        *self.db_handler.lock().await = Some(db_handler);
    }
    
    /// Get the database handler for this session
    pub async fn get_db_handler(&self) -> Option<Arc<DbHandler>> {
        self.db_handler.lock().await.clone()
    }
    
    /// Initialize the session connection with the database handler
    /// This ensures the session has its dedicated connection
    pub async fn initialize_connection(&self) -> Result<(), crate::PgSqliteError> {
        if let Some(ref db_handler) = *self.db_handler.lock().await {
            db_handler.create_session_connection(self.id).await?;
        }
        Ok(())
    }
    
    /// Clean up the session connection
    /// This should be called when the session is being terminated
    pub async fn cleanup_connection(&self) {
        // Clear the cached connection first
        self.cached_connection.lock().take();
        
        // Get a clone of the db_handler to avoid holding the lock
        let db_handler = self.db_handler.lock().await.clone();
        
        if let Some(ref handler) = db_handler {
            handler.remove_session_connection(&self.id);
            debug!("Successfully removed session connection for {}", self.id);
        }
    }
    
    /// Cache a connection for fast access
    pub fn cache_connection(&self, connection: Arc<ParkingMutex<Connection>>) {
        *self.cached_connection.lock() = Some(connection);
    }
    
    /// Get the cached connection if available
    pub fn get_cached_connection(&self) -> Option<Arc<ParkingMutex<Connection>>> {
        self.cached_connection.lock().clone()
    }
}

impl Drop for SessionState {
    fn drop(&mut self) {
        // Note: We can't do async operations in Drop, so cleanup is handled
        // explicitly when the session ends or via a background task
        // For now, just decrement the session count
        
        // Decrement active session count when session is destroyed
        ACTIVE_SESSION_COUNT.fetch_sub(1, Ordering::Relaxed);
    }
}