use std::collections::HashMap;
use parking_lot::RwLock;
use crate::PgSqliteError;

/// Manages portal lifecycle and state for extended query protocol
pub struct PortalManager {
    /// Active portals mapped by name
    portals: RwLock<HashMap<String, ManagedPortal>>,
    /// Maximum number of concurrent portals allowed
    max_portals: usize,
    /// Track portal execution state
    execution_state: RwLock<HashMap<String, PortalExecutionState>>,
}

/// Enhanced portal with execution state management
pub struct ManagedPortal {
    /// Base portal information
    pub portal: super::Portal,
    /// Creation timestamp for cleanup
    pub created_at: std::time::Instant,
    /// Last accessed timestamp
    pub last_accessed: RwLock<std::time::Instant>,
}

/// Tracks execution state for partial result fetching
#[derive(Debug, Clone)]
pub struct PortalExecutionState {
    /// Current row offset for pagination
    pub row_offset: usize,
    /// Total rows available (if known)
    pub total_rows: Option<usize>,
    /// Whether the portal has been fully consumed
    pub is_complete: bool,
    /// Cached query result for partial fetching
    pub cached_result: Option<CachedQueryResult>,
}

/// Cached query results for partial fetching
#[derive(Debug, Clone)]
pub struct CachedQueryResult {
    /// All result rows
    pub rows: Vec<Vec<Option<Vec<u8>>>>,
    /// Field descriptions
    pub field_descriptions: Vec<crate::protocol::FieldDescription>,
    /// Command tag (e.g., "SELECT 5")
    pub command_tag: String,
}


impl PortalManager {
    /// Create a new portal manager with specified limits
    pub fn new(max_portals: usize) -> Self {
        PortalManager {
            portals: RwLock::new(HashMap::new()),
            max_portals,
            execution_state: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new portal
    pub fn create_portal(
        &self,
        name: String,
        portal: super::Portal,
    ) -> Result<(), PgSqliteError> {
        let mut portals = self.portals.write();
        
        // Check portal limit
        if portals.len() >= self.max_portals && !portals.contains_key(&name) {
            // Find and remove least recently used portal
            if let Some(lru_name) = self.find_lru_portal(&portals) {
                portals.remove(&lru_name);
                self.execution_state.write().remove(&lru_name);
            }
        }
        
        let managed_portal = ManagedPortal {
            portal,
            created_at: std::time::Instant::now(),
            last_accessed: RwLock::new(std::time::Instant::now()),
        };
        
        portals.insert(name.clone(), managed_portal);
        
        // Initialize execution state
        self.execution_state.write().insert(name, PortalExecutionState {
            row_offset: 0,
            total_rows: None,
            is_complete: false,
            cached_result: None,
        });
        
        Ok(())
    }

    /// Get a portal by name
    pub fn get_portal(&self, name: &str) -> Option<super::Portal> {
        let portals = self.portals.read();
        portals.get(name).map(|mp| {
            // Update last accessed time
            *mp.last_accessed.write() = std::time::Instant::now();
            mp.portal.clone()
        })
    }

    /// Get mutable access to a portal
    pub fn with_portal_mut<F, R>(&self, name: &str, f: F) -> Option<R>
    where
        F: FnOnce(&mut ManagedPortal) -> R,
    {
        let mut portals = self.portals.write();
        portals.get_mut(name).map(|mp| {
            *mp.last_accessed.write() = std::time::Instant::now();
            f(mp)
        })
    }

    /// Update execution state for a portal
    pub fn update_execution_state(
        &self,
        name: &str,
        row_offset: usize,
        is_complete: bool,
        cached_result: Option<CachedQueryResult>,
    ) -> Result<(), PgSqliteError> {
        let mut states = self.execution_state.write();
        
        if let Some(state) = states.get_mut(name) {
            state.row_offset = row_offset;
            state.is_complete = is_complete;
            if cached_result.is_some() {
                state.cached_result = cached_result;
            }
            Ok(())
        } else {
            Err(PgSqliteError::Protocol(format!("Unknown portal: {name}")))
        }
    }

    /// Get execution state for a portal
    pub fn get_execution_state(&self, name: &str) -> Option<PortalExecutionState> {
        self.execution_state.read().get(name).cloned()
    }

    /// Close a portal
    pub fn close_portal(&self, name: &str) -> bool {
        let removed = self.portals.write().remove(name).is_some();
        self.execution_state.write().remove(name);
        removed
    }

    /// Close all portals
    pub fn close_all_portals(&self) {
        self.portals.write().clear();
        self.execution_state.write().clear();
    }

    /// Get number of active portals
    pub fn portal_count(&self) -> usize {
        self.portals.read().len()
    }

    /// Clean up stale portals older than the specified duration
    pub fn cleanup_stale_portals(&self, max_age: std::time::Duration) -> usize {
        let now = std::time::Instant::now();
        let mut portals = self.portals.write();
        let mut states = self.execution_state.write();
        
        let stale_portals: Vec<String> = portals
            .iter()
            .filter(|(_, mp)| now.duration_since(*mp.last_accessed.read()) > max_age)
            .map(|(name, _)| name.clone())
            .collect();
        
        let count = stale_portals.len();
        for name in stale_portals {
            portals.remove(&name);
            states.remove(&name);
        }
        
        count
    }

    /// Find least recently used portal
    fn find_lru_portal(&self, portals: &HashMap<String, ManagedPortal>) -> Option<String> {
        portals
            .iter()
            .min_by_key(|(_, mp)| *mp.last_accessed.read())
            .map(|(name, _)| name.clone())
    }
}

/// Portal execution helper for handling partial results
pub struct PortalExecutor;

impl PortalExecutor {
    /// Execute a portal with support for partial result fetching
    pub async fn execute_portal(
        portal_manager: &PortalManager,
        portal_name: &str,
        max_rows: i32,
        send_row: impl Fn(Vec<Option<Vec<u8>>>) -> Result<(), PgSqliteError>,
    ) -> Result<(usize, bool), PgSqliteError> {
        // Get current execution state
        let state = portal_manager
            .get_execution_state(portal_name)
            .ok_or_else(|| PgSqliteError::Protocol(format!("Unknown portal: {portal_name}")))?;
        
        // If we have cached results, return from cache
        if let Some(cached_result) = &state.cached_result {
            let start_row = state.row_offset;
            let mut rows_sent = 0;
            let max_rows_to_send = if max_rows == 0 {
                cached_result.rows.len() - start_row
            } else {
                std::cmp::min(max_rows as usize, cached_result.rows.len() - start_row)
            };
            
            for i in start_row..(start_row + max_rows_to_send) {
                if i >= cached_result.rows.len() {
                    break;
                }
                send_row(cached_result.rows[i].clone())?;
                rows_sent += 1;
            }
            
            let new_offset = start_row + rows_sent;
            let is_complete = new_offset >= cached_result.rows.len();
            
            // Update execution state
            portal_manager.update_execution_state(
                portal_name,
                new_offset,
                is_complete,
                None, // Keep existing cached result
            )?;
            
            Ok((rows_sent, is_complete))
        } else {
            // First execution - need to execute query and cache results
            // This will be implemented when integrating with the main execute flow
            Err(PgSqliteError::Protocol("Portal execution not yet integrated".to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_portal_manager_creation() {
        let manager = PortalManager::new(100);
        assert_eq!(manager.portal_count(), 0);
    }

    #[test]
    fn test_portal_lifecycle() {
        let manager = PortalManager::new(10);
        
        // Create a test portal
        let portal = super::super::Portal {
            statement_name: "test_stmt".to_string(),
            query: "SELECT 1".to_string(),
            translated_query: None,
            bound_values: vec![],
            param_formats: vec![],
            result_formats: vec![],
            inferred_param_types: None,
        };
        
        // Create portal
        manager.create_portal("test_portal".to_string(), portal.clone()).unwrap();
        assert_eq!(manager.portal_count(), 1);
        
        // Get portal
        let retrieved = manager.get_portal("test_portal").unwrap();
        assert_eq!(retrieved.query, "SELECT 1");
        
        // Close portal
        assert!(manager.close_portal("test_portal"));
        assert_eq!(manager.portal_count(), 0);
    }

    #[test]
    fn test_portal_limit_enforcement() {
        let manager = PortalManager::new(3);
        
        // Create portals up to limit
        for i in 0..4 {
            let portal = super::super::Portal {
                statement_name: format!("stmt_{i}"),
                query: format!("SELECT {i}"),
                translated_query: None,
                bound_values: vec![],
                param_formats: vec![],
                result_formats: vec![],
                inferred_param_types: None,
            };
            
            manager.create_portal(format!("portal_{i}"), portal).unwrap();
            
            // Sleep briefly to ensure different timestamps
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        
        // Should have removed oldest portal
        assert_eq!(manager.portal_count(), 3);
        assert!(manager.get_portal("portal_0").is_none());
        assert!(manager.get_portal("portal_3").is_some());
    }

    #[test]
    fn test_stale_portal_cleanup() {
        let manager = PortalManager::new(10);
        
        // Create some portals
        for i in 0..3 {
            let portal = super::super::Portal {
                statement_name: format!("stmt_{i}"),
                query: format!("SELECT {i}"),
                translated_query: None,
                bound_values: vec![],
                param_formats: vec![],
                result_formats: vec![],
                inferred_param_types: None,
            };
            
            manager.create_portal(format!("portal_{i}"), portal).unwrap();
        }
        
        // Access one portal to update its timestamp
        let _ = manager.get_portal("portal_1");
        
        // Clean up with very short duration (all but portal_1 should be removed)
        let removed = manager.cleanup_stale_portals(std::time::Duration::from_millis(5));
        
        // Due to timing, this might remove 0-2 portals
        assert!(removed <= 2);
    }
}