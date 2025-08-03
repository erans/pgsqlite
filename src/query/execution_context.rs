use std::sync::Arc;
use parking_lot::Mutex;

/// Context for tracking state during query execution
/// This is passed through all execution paths to maintain state
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Tracks whether RowDescription has been sent for this execution
    row_description_sent: Arc<Mutex<bool>>,
    /// The portal name being executed
    pub portal_name: String,
}

impl ExecutionContext {
    /// Create a new execution context
    pub fn new(portal_name: String) -> Self {
        Self {
            row_description_sent: Arc::new(Mutex::new(false)),
            portal_name,
        }
    }
    
    /// Check if RowDescription has been sent
    pub fn is_row_description_sent(&self) -> bool {
        *self.row_description_sent.lock()
    }
    
    /// Mark that RowDescription has been sent
    pub fn mark_row_description_sent(&self) {
        *self.row_description_sent.lock() = true;
    }
    
    /// Check if we should send RowDescription
    /// Returns true only if it hasn't been sent yet
    pub fn should_send_row_description(&self) -> bool {
        let mut sent = self.row_description_sent.lock();
        if !*sent {
            *sent = true;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_execution_context() {
        let ctx = ExecutionContext::new("test_portal".to_string());
        
        // Initially not sent
        assert!(!ctx.is_row_description_sent());
        
        // First check should return true and mark as sent
        assert!(ctx.should_send_row_description());
        
        // Now it should be marked as sent
        assert!(ctx.is_row_description_sent());
        
        // Subsequent checks should return false
        assert!(!ctx.should_send_row_description());
    }
}