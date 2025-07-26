use pgsqlite::session::{DbHandler, SessionState};
use std::sync::Arc;

#[tokio::test]
async fn test_portal_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let db_handler = Arc::new(DbHandler::new(":memory:")?);
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));
    
    // Create test table with data
    db_handler.execute("CREATE TABLE test_portal (id INTEGER PRIMARY KEY, value TEXT)").await?;
    for i in 1..=10 {
        db_handler.execute(&format!("INSERT INTO test_portal VALUES ({i}, 'value{i}')")).await?;
    }
    
    // Create a portal manually
    let portal = pgsqlite::session::Portal {
        statement_name: "test_stmt".to_string(),
        query: "SELECT * FROM test_portal".to_string(),
        translated_query: None,
        bound_values: vec![],
        param_formats: vec![],
        result_formats: vec![],
        inferred_param_types: None,
    };
    
    // Test portal creation
    session.portal_manager.create_portal("test_portal".to_string(), portal.clone())?;
    assert_eq!(session.portal_manager.portal_count(), 1);
    
    // Test portal retrieval
    let retrieved = session.portal_manager.get_portal("test_portal");
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().query, "SELECT * FROM test_portal");
    
    // Test portal execution state
    let cached_result = pgsqlite::session::CachedQueryResult {
        rows: vec![vec![Some(b"1".to_vec()), Some(b"value1".to_vec())]],
        field_descriptions: vec![],
        command_tag: "SELECT 1".to_string(),
    };
    
    session.portal_manager.update_execution_state(
        "test_portal",
        1,
        false,
        Some(cached_result),
    )?;
    
    let state = session.portal_manager.get_execution_state("test_portal");
    assert!(state.is_some());
    assert_eq!(state.unwrap().row_offset, 1);
    
    // Test portal close
    assert!(session.portal_manager.close_portal("test_portal"));
    assert_eq!(session.portal_manager.portal_count(), 0);
    assert!(session.portal_manager.get_portal("test_portal").is_none());
    
    println!("✅ Portal lifecycle test passed");
    Ok(())
}

#[tokio::test]
async fn test_multiple_concurrent_portals() -> Result<(), Box<dyn std::error::Error>> {
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));
    
    // Create multiple portals
    for i in 1..=5 {
        let portal = pgsqlite::session::Portal {
            statement_name: format!("stmt{i}"),
            query: format!("SELECT * FROM table{i}"),
            translated_query: None,
            bound_values: vec![],
            param_formats: vec![],
            result_formats: vec![],
            inferred_param_types: None,
        };
        
        session.portal_manager.create_portal(format!("portal{i}"), portal)?;
    }
    
    // Check all portals exist
    assert_eq!(session.portal_manager.portal_count(), 5);
    
    // Update execution state for some portals
    session.portal_manager.update_execution_state(
        "portal1",
        10,
        false,
        Some(pgsqlite::session::CachedQueryResult {
            rows: vec![],
            field_descriptions: vec![],
            command_tag: "SELECT 10".to_string(),
        }),
    )?;
    
    session.portal_manager.update_execution_state(
        "portal2",
        20,
        true,
        Some(pgsqlite::session::CachedQueryResult {
            rows: vec![],
            field_descriptions: vec![],
            command_tag: "SELECT 20".to_string(),
        }),
    )?;
    
    // Check states are independent
    let state1 = session.portal_manager.get_execution_state("portal1").unwrap();
    let state2 = session.portal_manager.get_execution_state("portal2").unwrap();
    
    assert_eq!(state1.row_offset, 10);
    assert!(!state1.is_complete);
    assert_eq!(state2.row_offset, 20);
    assert!(state2.is_complete);
    
    println!("✅ Multiple concurrent portals test passed");
    Ok(())
}

#[tokio::test]
async fn test_portal_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));
    
    // Create a portal
    let portal = pgsqlite::session::Portal {
        statement_name: "stmt".to_string(),
        query: "SELECT 1".to_string(),
        translated_query: None,
        bound_values: vec![],
        param_formats: vec![],
        result_formats: vec![],
        inferred_param_types: None,
    };
    
    session.portal_manager.create_portal("test_portal".to_string(), portal)?;
    session.portal_manager.update_execution_state(
        "test_portal",
        0,
        false,
        None,
    )?;
    
    // Verify portal exists
    assert_eq!(session.portal_manager.portal_count(), 1);
    assert!(session.portal_manager.get_execution_state("test_portal").is_some());
    
    // Close the portal
    let closed = session.portal_manager.close_portal("test_portal");
    assert!(closed);
    
    // Verify cleanup
    assert_eq!(session.portal_manager.portal_count(), 0);
    assert!(session.portal_manager.get_execution_state("test_portal").is_none());
    
    // Closing again should return false
    assert!(!session.portal_manager.close_portal("test_portal"));
    
    println!("✅ Portal cleanup test passed");
    Ok(())
}

#[tokio::test]
async fn test_portal_limit_enforcement() -> Result<(), Box<dyn std::error::Error>> {
    // Create session with small portal limit
    let mut session = SessionState::new("test".to_string(), "test".to_string());
    session.portal_manager = Arc::new(pgsqlite::session::PortalManager::new(3));
    let session = Arc::new(session);
    
    // Create portals beyond the limit
    for i in 1..=5 {
        let portal = pgsqlite::session::Portal {
            statement_name: format!("stmt{i}"),
            query: format!("SELECT {i}"),
            translated_query: None,
            bound_values: vec![],
            param_formats: vec![],
            result_formats: vec![],
            inferred_param_types: None,
        };
        
        session.portal_manager.create_portal(format!("portal{i}"), portal)?;
        
        // Small delay to ensure different timestamps
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    }
    
    // Should only have 3 portals (limit enforced)
    assert_eq!(session.portal_manager.portal_count(), 3);
    
    // Oldest portals should be evicted
    assert!(session.portal_manager.get_portal("portal1").is_none());
    assert!(session.portal_manager.get_portal("portal2").is_none());
    
    // Newest portals should exist
    assert!(session.portal_manager.get_portal("portal3").is_some());
    assert!(session.portal_manager.get_portal("portal4").is_some());
    assert!(session.portal_manager.get_portal("portal5").is_some());
    
    println!("✅ Portal limit enforcement test passed");
    Ok(())
}

#[tokio::test]
async fn test_stale_portal_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));
    
    // Create several portals with delays
    for i in 1..=3 {
        let portal = pgsqlite::session::Portal {
            statement_name: format!("stmt{i}"),
            query: format!("SELECT {i}"),
            translated_query: None,
            bound_values: vec![],
            param_formats: vec![],
            result_formats: vec![],
            inferred_param_types: None,
        };
        
        session.portal_manager.create_portal(format!("portal{i}"), portal)?;
        
        if i < 3 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
    }
    
    assert_eq!(session.portal_manager.portal_count(), 3);
    
    // Access portal2 to update its last_accessed time
    let _ = session.portal_manager.get_portal("portal2");
    
    // Clean up portals older than 40ms
    let removed = session.portal_manager.cleanup_stale_portals(
        std::time::Duration::from_millis(40)
    );
    
    // At least portal1 should be removed (oldest and not accessed)
    assert!(removed >= 1);
    assert!(session.portal_manager.get_portal("portal1").is_none());
    
    // portal2 (accessed) and portal3 (newest) might remain
    // depending on exact timing
    
    println!("✅ Stale portal cleanup test passed");
    Ok(())
}

#[tokio::test]
async fn test_portal_with_values() -> Result<(), Box<dyn std::error::Error>> {
    let session = Arc::new(SessionState::new("test".to_string(), "test".to_string()));
    
    // Create portal with bound values
    let portal = pgsqlite::session::Portal {
        statement_name: "param_stmt".to_string(),
        query: "SELECT * FROM test WHERE id = $1".to_string(),
        translated_query: Some("SELECT * FROM test WHERE id = ?".to_string()),
        bound_values: vec![Some(b"42".to_vec())],
        param_formats: vec![0], // Text format
        result_formats: vec![0], // Text result
        inferred_param_types: Some(vec![23]), // Int4
    };
    
    session.portal_manager.create_portal("param_portal".to_string(), portal)?;
    
    // Simulate execution with cached results
    let cached_result = pgsqlite::session::CachedQueryResult {
        rows: vec![
            vec![Some(b"42".to_vec()), Some(b"Test Value".to_vec())],
            vec![Some(b"43".to_vec()), Some(b"Another Value".to_vec())],
        ],
        field_descriptions: vec![],
        command_tag: "SELECT 2".to_string(),
    };
    
    session.portal_manager.update_execution_state(
        "param_portal",
        0,
        false,
        Some(cached_result),
    )?;
    
    // Verify state
    let state = session.portal_manager.get_execution_state("param_portal").unwrap();
    assert_eq!(state.row_offset, 0);
    assert_eq!(state.cached_result.as_ref().unwrap().rows.len(), 2);
    
    // Simulate partial fetch
    session.portal_manager.update_execution_state(
        "param_portal",
        1,
        false,
        None,
    )?;
    
    let state = session.portal_manager.get_execution_state("param_portal").unwrap();
    assert_eq!(state.row_offset, 1);
    assert!(!state.is_complete);
    
    println!("✅ Portal with values test passed");
    Ok(())
}