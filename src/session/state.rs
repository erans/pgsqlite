use std::collections::HashMap;
use tokio::sync::RwLock;
use crate::protocol::TransactionStatus;
use crate::cache::QueryCache;
use crate::config::CONFIG;
use std::sync::Arc;
use once_cell::sync::Lazy;

// Global query cache shared across all sessions
pub static GLOBAL_QUERY_CACHE: Lazy<Arc<QueryCache>> = Lazy::new(|| {
    Arc::new(QueryCache::new(CONFIG.query_cache_size, CONFIG.query_cache_ttl))
});

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
}

pub struct PreparedStatement {
    pub query: String,
    pub translated_query: Option<String>, // Cached translation of the query
    pub param_types: Vec<i32>,
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
        }
    }

    /// Create a new session with default database and user (for testing)
    #[cfg(test)]
    pub fn new_test() -> Self {
        Self::new("test".to_string(), "test".to_string())
    }

    /// Check if the session is currently in a transaction
    pub fn in_transaction(&self) -> bool {
        // We need to use try_read() since this might be called from async context
        if let Ok(status) = self.transaction_status.try_read() {
            matches!(*status, TransactionStatus::InTransaction | TransactionStatus::InFailedTransaction)
        } else {
            // If we can't acquire the lock, assume we're in a transaction for safety
            true
        }
    }
}