use std::net::IpAddr;
use pgsqlite::security::{
    SecurityAuditLogger, SecurityEvent, SecurityEventType, SecuritySeverity,
    AuditConfig, events,
};

#[test]
fn test_audit_logger_basic_functionality() {
    let config = AuditConfig {
        enabled: true,
        json_format: false,
        log_queries: true,
        min_severity: SecuritySeverity::Info,
        enable_alerting: false, // Disable to avoid alert noise in tests
        max_metadata_size: 1024,
        buffer_size: 10,
    };

    let logger = SecurityAuditLogger::with_config(config);

    // Test logging a basic event
    let event = SecurityEvent::new(
        SecurityEventType::AuthenticationSuccess,
        SecuritySeverity::Info,
        "Test authentication event".to_string(),
    )
    .with_client_ip("192.168.1.100".parse().unwrap())
    .with_username("testuser".to_string())
    .with_database("testdb".to_string())
    .with_metadata("test_key".to_string(), "test_value".to_string());

    logger.log_event(event);

    let stats = logger.get_stats();
    assert_eq!(stats.total_events, 1);
    assert_eq!(stats.events_by_severity.get("INFO"), Some(&1));
    assert_eq!(stats.events_by_type.get("AUTH_SUCCESS"), Some(&1));
}

#[test]
fn test_severity_filtering() {
    let config = AuditConfig {
        enabled: true,
        min_severity: SecuritySeverity::Warning,
        enable_alerting: false,
        ..Default::default()
    };

    let logger = SecurityAuditLogger::with_config(config);

    // Should log warning and above
    let warning_event = SecurityEvent::new(
        SecurityEventType::RateLimitExceeded,
        SecuritySeverity::Warning,
        "Test warning".to_string(),
    );

    let info_event = SecurityEvent::new(
        SecurityEventType::QueryExecuted,
        SecuritySeverity::Info,
        "Test info".to_string(),
    );

    logger.log_event(warning_event);
    logger.log_event(info_event);

    let stats = logger.get_stats();
    assert_eq!(stats.total_events, 1); // Only warning should be logged
    assert_eq!(stats.events_by_severity.get("WARNING"), Some(&1));
    assert_eq!(stats.events_by_severity.get("INFO"), None);
}

#[test]
fn test_alert_detection() {
    let config = AuditConfig {
        enabled: true,
        enable_alerting: true,
        ..Default::default()
    };

    let logger = SecurityAuditLogger::with_config(config);

    // Test events that should trigger alerts
    let sql_injection_event = SecurityEvent::new(
        SecurityEventType::SqlInjectionAttempt,
        SecuritySeverity::Warning,
        "SQL injection detected".to_string(),
    );

    let high_severity_event = SecurityEvent::new(
        SecurityEventType::ProtocolViolation,
        SecuritySeverity::High,
        "Protocol violation".to_string(),
    );

    let normal_event = SecurityEvent::new(
        SecurityEventType::QueryExecuted,
        SecuritySeverity::Info,
        "Normal query".to_string(),
    );

    assert!(sql_injection_event.requires_alert());
    assert!(high_severity_event.requires_alert());
    assert!(!normal_event.requires_alert());

    logger.log_event(sql_injection_event);
    logger.log_event(high_severity_event);
    logger.log_event(normal_event);

    let stats = logger.get_stats();
    // Note: May be more than 2 if other tests use global logger
    assert!(stats.alerts_triggered >= 2); // At least two events should trigger alerts
}

#[test]
fn test_query_truncation() {
    let long_query = "SELECT * FROM ".repeat(200);
    let event = SecurityEvent::new(
        SecurityEventType::QueryExecuted,
        SecuritySeverity::Info,
        "Long query test".to_string(),
    )
    .with_query(long_query);

    assert!(event.query.as_ref().unwrap().contains("[TRUNCATED]"));
    assert!(event.query.as_ref().unwrap().len() <= 1000 + 15); // 15 for "[TRUNCATED]"
}

#[test]
fn test_convenience_functions() {
    // Test that convenience functions work without panicking
    let ip: IpAddr = "192.168.1.200".parse().unwrap();

    events::connection_accepted(ip, true);
    events::connection_rejected(ip, "rate limit exceeded");
    events::authentication_success(ip, "testuser", "testdb");
    events::sql_injection_attempt(
        Some(ip),
        Some("session_123".to_string()),
        "SELECT * FROM users WHERE id = 1 OR 1=1",
        "OR 1=1",
    );
    events::rate_limit_exceeded(Some(ip), "per-ip", 100);
    events::circuit_breaker_opened(5, 3);
    events::protocol_violation(Some(ip), "invalid message format");
    events::query_executed(
        Some(ip),
        Some("session_123".to_string()),
        Some("testuser".to_string()),
        Some("testdb".to_string()),
        "SELECT COUNT(*) FROM users",
        50,
    );

    // If we get here without panicking, the convenience functions work
    assert!(true);
}

#[test]
fn test_timestamp_formatting() {
    let event = SecurityEvent::new(
        SecurityEventType::AuthenticationSuccess,
        SecuritySeverity::Info,
        "Timestamp test".to_string(),
    );

    let formatted = event.formatted_timestamp();

    // Should be in ISO 8601 format
    assert!(formatted.contains("T"));
    assert!(formatted.ends_with("Z"));

    // Should be a valid format (basic check)
    assert!(formatted.len() >= 20); // Minimum length for ISO 8601
}

#[test]
fn test_metadata_handling() {
    let event = SecurityEvent::new(
        SecurityEventType::ConfigurationChange,
        SecuritySeverity::Info,
        "Config change test".to_string(),
    )
    .with_metadata("changed_setting".to_string(), "rate_limit".to_string())
    .with_metadata("old_value".to_string(), "100".to_string())
    .with_metadata("new_value".to_string(), "200".to_string());

    assert_eq!(event.metadata.len(), 3);
    assert_eq!(event.metadata.get("changed_setting"), Some(&"rate_limit".to_string()));
    assert_eq!(event.metadata.get("old_value"), Some(&"100".to_string()));
    assert_eq!(event.metadata.get("new_value"), Some(&"200".to_string()));
}

#[test]
fn test_audit_config_from_env() {
    // Set environment variables
    unsafe {
        std::env::set_var("PGSQLITE_AUDIT_ENABLED", "true");
        std::env::set_var("PGSQLITE_AUDIT_JSON_FORMAT", "true");
        std::env::set_var("PGSQLITE_AUDIT_LOG_QUERIES", "false");
        std::env::set_var("PGSQLITE_AUDIT_MIN_SEVERITY", "HIGH");
        std::env::set_var("PGSQLITE_AUDIT_ENABLE_ALERTING", "false");
        std::env::set_var("PGSQLITE_AUDIT_BUFFER_SIZE", "50");
    }

    let config = AuditConfig::from_env();

    assert!(config.enabled);
    assert!(config.json_format);
    assert!(!config.log_queries);
    assert_eq!(config.min_severity, SecuritySeverity::High);
    assert!(!config.enable_alerting);
    assert_eq!(config.buffer_size, 50);

    // Clean up environment variables
    unsafe {
        std::env::remove_var("PGSQLITE_AUDIT_ENABLED");
        std::env::remove_var("PGSQLITE_AUDIT_JSON_FORMAT");
        std::env::remove_var("PGSQLITE_AUDIT_LOG_QUERIES");
        std::env::remove_var("PGSQLITE_AUDIT_MIN_SEVERITY");
        std::env::remove_var("PGSQLITE_AUDIT_ENABLE_ALERTING");
        std::env::remove_var("PGSQLITE_AUDIT_BUFFER_SIZE");
    }
}

#[test]
fn test_buffer_management() {
    let config = AuditConfig {
        enabled: true,
        buffer_size: 3, // Small buffer for testing
        enable_alerting: false,
        ..Default::default()
    };

    let logger = SecurityAuditLogger::with_config(config);

    // Add events to trigger buffer flush
    for i in 0..5 {
        let event = SecurityEvent::new(
            SecurityEventType::QueryExecuted,
            SecuritySeverity::Info,
            format!("Query {}", i),
        );
        logger.log_event(event);
    }

    let stats = logger.get_stats();
    assert_eq!(stats.total_events, 5);
    assert!(stats.buffer_flushes > 0); // Buffer should have been flushed
}

#[test]
fn test_statistics_reset() {
    let logger = SecurityAuditLogger::new();

    // Log some events
    let event1 = SecurityEvent::new(
        SecurityEventType::AuthenticationSuccess,
        SecuritySeverity::Info,
        "Test 1".to_string(),
    );

    let event2 = SecurityEvent::new(
        SecurityEventType::SqlInjectionAttempt,
        SecuritySeverity::High,
        "Test 2".to_string(),
    );

    logger.log_event(event1);
    logger.log_event(event2);

    let stats_before = logger.get_stats();
    assert!(stats_before.total_events > 0);

    // Reset statistics
    logger.reset_stats();

    let stats_after = logger.get_stats();
    assert_eq!(stats_after.total_events, 0);
    assert!(stats_after.events_by_severity.is_empty());
    assert!(stats_after.events_by_type.is_empty());
}

#[test]
fn test_disabled_audit_logging() {
    let config = AuditConfig {
        enabled: false, // Disabled
        ..Default::default()
    };

    let logger = SecurityAuditLogger::with_config(config);

    let event = SecurityEvent::new(
        SecurityEventType::AuthenticationSuccess,
        SecuritySeverity::Info,
        "Test event".to_string(),
    );

    logger.log_event(event);

    let stats = logger.get_stats();
    assert_eq!(stats.total_events, 0); // Should not log when disabled
}

#[test]
fn test_event_serialization() {
    let event = SecurityEvent::new(
        SecurityEventType::AuthenticationSuccess,
        SecuritySeverity::Info,
        "Serialization test".to_string(),
    )
    .with_client_ip("192.168.1.50".parse().unwrap())
    .with_username("testuser".to_string())
    .with_database("testdb".to_string())
    .with_query("SELECT * FROM users".to_string())
    .with_metadata("session_id".to_string(), "abc123".to_string());

    // Test JSON serialization
    let json_result = serde_json::to_string(&event);
    assert!(json_result.is_ok());

    let json_str = json_result.unwrap();
    assert!(json_str.contains("AuthenticationSuccess")); // Serde uses variant name, not Display
    assert!(json_str.contains("testuser"));
    assert!(json_str.contains("192.168.1.50"));

    // Test deserialization
    let deserialized: Result<SecurityEvent, _> = serde_json::from_str(&json_str);
    assert!(deserialized.is_ok());

    let deserialized_event = deserialized.unwrap();
    assert_eq!(deserialized_event.event_type, SecurityEventType::AuthenticationSuccess);
    assert_eq!(deserialized_event.severity, SecuritySeverity::Info);
    assert_eq!(deserialized_event.username, Some("testuser".to_string()));
}