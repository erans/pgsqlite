//! Process-wide DbHandler registry for reusing database handlers across sessions.
//!
//! This module provides a global registry that ensures DbHandler instances are
//! shared across client connections, which is essential for:
//! - Keeping shared in-memory databases alive across sessions
//! - Avoiding repeated migration runs on each connection
//! - Efficient resource sharing

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use once_cell::sync::Lazy;
use parking_lot::RwLock;
use tracing::debug;

use super::db_handler::DbHandler;
use crate::config::Config;

/// Global registry for DbHandler instances, keyed by database path.
/// Uses Weak references to allow handlers to be dropped when no longer in use.
static REGISTRY: Lazy<RwLock<HashMap<String, Weak<DbHandler>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

/// Get or create a DbHandler for the given database path.
///
/// If a handler already exists for this path and is still alive, it will be reused.
/// Otherwise, a new handler will be created and registered.
///
/// # Arguments
/// * `db_path` - The database path (file path or memory URI)
/// * `config` - The server configuration
///
/// # Returns
/// An Arc-wrapped DbHandler that can be shared across the session
pub fn get_or_create_handler(
    db_path: &str,
    config: &Config,
) -> Result<Arc<DbHandler>, rusqlite::Error> {
    // Fast path: check if we already have a live handler
    {
        let registry = REGISTRY.read();
        if let Some(weak) = registry.get(db_path)
            && let Some(handler) = weak.upgrade() {
                debug!("Reusing existing DbHandler for: {}", db_path);
                return Ok(handler);
            }
    }

    // Slow path: need to create a new handler
    let mut registry = REGISTRY.write();

    // Double-check after acquiring write lock (another thread might have created it)
    if let Some(weak) = registry.get(db_path)
        && let Some(handler) = weak.upgrade() {
            debug!("Reusing existing DbHandler for: {} (after lock)", db_path);
            return Ok(handler);
        }

    // Create new handler
    debug!("Creating new DbHandler for: {}", db_path);
    let handler = Arc::new(DbHandler::new_with_config(db_path, config)?);

    // Store weak reference in registry
    registry.insert(db_path.to_string(), Arc::downgrade(&handler));

    // Clean up any dead entries while we have the write lock
    registry.retain(|_, weak| weak.strong_count() > 0);

    Ok(handler)
}

/// Remove a specific handler from the registry.
/// This is useful for testing or explicit cleanup.
#[allow(dead_code)]
pub fn remove_handler(db_path: &str) {
    let mut registry = REGISTRY.write();
    registry.remove(db_path);
}

/// Clear all handlers from the registry.
/// This is primarily useful for testing.
#[allow(dead_code)]
pub fn clear_registry() {
    let mut registry = REGISTRY.write();
    registry.clear();
}

/// Get the number of registered handlers (including dead weak refs).
#[allow(dead_code)]
pub fn handler_count() -> usize {
    let registry = REGISTRY.read();
    registry.len()
}

/// Get the number of live (upgradeable) handlers.
#[allow(dead_code)]
pub fn live_handler_count() -> usize {
    let registry = REGISTRY.read();
    registry.values().filter(|w| w.strong_count() > 0).count()
}
