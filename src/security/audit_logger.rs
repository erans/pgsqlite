use std::collections::HashMap;
use std::fmt;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{error, warn, info, debug};

#[derive(Error, Debug)]
pub enum AuditError {
    #[error("Serialization error: {0}")]
    SerializationError(String),
    #[error("I/O error: {0}")]
    IoError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Security event severity levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecuritySeverity {
    /// Informational events (successful logins, queries)
    Info,
    /// Warning events (rate limiting, suspicious patterns)
    Warning,
    /// High priority events (failed authentication, injection attempts)
    High,
    /// Critical events (circuit breaker triggers, system failures)
    Critical,
}

impl fmt::Display for SecuritySeverity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecuritySeverity::Info => write!(f, "INFO"),
            SecuritySeverity::Warning => write!(f, "WARNING"),
            SecuritySeverity::High => write!(f, "HIGH"),
            SecuritySeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Types of security events
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecurityEventType {
    /// Connection events
    ConnectionAccepted,
    ConnectionRejected,
    ConnectionClosed,
    SslNegotiation,

    /// Authentication events
    AuthenticationAttempt,
    AuthenticationSuccess,
    AuthenticationFailure,

    /// Query events
    QueryExecuted,
    QueryFailed,
    SqlInjectionAttempt,
    SuspiciousQuery,

    /// Rate limiting events
    RateLimitExceeded,
    CircuitBreakerOpen,
    CircuitBreakerClosed,

    /// Protocol events
    ProtocolViolation,
    MalformedMessage,
    UnauthorizedOperation,

    /// System events
    ConfigurationChange,
    ErrorCondition,
    SecurityPolicyViolation,
}

impl fmt::Display for SecurityEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecurityEventType::ConnectionAccepted => write!(f, "CONNECTION_ACCEPTED"),
            SecurityEventType::ConnectionRejected => write!(f, "CONNECTION_REJECTED"),
            SecurityEventType::ConnectionClosed => write!(f, "CONNECTION_CLOSED"),
            SecurityEventType::SslNegotiation => write!(f, "SSL_NEGOTIATION"),
            SecurityEventType::AuthenticationAttempt => write!(f, "AUTH_ATTEMPT"),
            SecurityEventType::AuthenticationSuccess => write!(f, "AUTH_SUCCESS"),
            SecurityEventType::AuthenticationFailure => write!(f, "AUTH_FAILURE"),
            SecurityEventType::QueryExecuted => write!(f, "QUERY_EXECUTED"),
            SecurityEventType::QueryFailed => write!(f, "QUERY_FAILED"),
            SecurityEventType::SqlInjectionAttempt => write!(f, "SQL_INJECTION_ATTEMPT"),
            SecurityEventType::SuspiciousQuery => write!(f, "SUSPICIOUS_QUERY"),
            SecurityEventType::RateLimitExceeded => write!(f, "RATE_LIMIT_EXCEEDED"),
            SecurityEventType::CircuitBreakerOpen => write!(f, "CIRCUIT_BREAKER_OPEN"),
            SecurityEventType::CircuitBreakerClosed => write!(f, "CIRCUIT_BREAKER_CLOSED"),
            SecurityEventType::ProtocolViolation => write!(f, "PROTOCOL_VIOLATION"),
            SecurityEventType::MalformedMessage => write!(f, "MALFORMED_MESSAGE"),
            SecurityEventType::UnauthorizedOperation => write!(f, "UNAUTHORIZED_OPERATION"),
            SecurityEventType::ConfigurationChange => write!(f, "CONFIG_CHANGE"),
            SecurityEventType::ErrorCondition => write!(f, "ERROR_CONDITION"),
            SecurityEventType::SecurityPolicyViolation => write!(f, "SECURITY_POLICY_VIOLATION"),
        }
    }
}

/// Structured security audit event
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityEvent {
    /// Event timestamp (Unix timestamp with microseconds)
    pub timestamp: u64,
    /// Event type
    pub event_type: SecurityEventType,
    /// Severity level
    pub severity: SecuritySeverity,
    /// Client IP address
    pub client_ip: Option<IpAddr>,
    /// Session ID
    pub session_id: Option<String>,
    /// Database name
    pub database: Option<String>,
    /// Username
    pub username: Option<String>,
    /// Query or command (truncated for security)
    pub query: Option<String>,
    /// Error message or description
    pub message: String,
    /// Additional structured data
    pub metadata: HashMap<String, String>,
    /// Process ID
    pub process_id: u32,
    /// Thread ID (for debugging)
    pub thread_id: Option<String>,
}

impl SecurityEvent {
    /// Create a new security event
    pub fn new(
        event_type: SecurityEventType,
        severity: SecuritySeverity,
        message: String,
    ) -> Self {
        Self {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64,
            event_type,
            severity,
            client_ip: None,
            session_id: None,
            database: None,
            username: None,
            query: None,
            message,
            metadata: HashMap::new(),
            process_id: std::process::id(),
            thread_id: None,
        }
    }

    /// Set client IP address
    pub fn with_client_ip(mut self, ip: IpAddr) -> Self {
        self.client_ip = Some(ip);
        self
    }

    /// Set session ID
    pub fn with_session_id(mut self, session_id: String) -> Self {
        self.session_id = Some(session_id);
        self
    }

    /// Set database name
    pub fn with_database(mut self, database: String) -> Self {
        self.database = Some(database);
        self
    }

    /// Set username
    pub fn with_username(mut self, username: String) -> Self {
        self.username = Some(username);
        self
    }

    /// Set query (will be truncated for security)
    pub fn with_query(mut self, query: String) -> Self {
        const MAX_QUERY_LOG_LENGTH: usize = 1000;
        self.query = Some(if query.len() > MAX_QUERY_LOG_LENGTH {
            format!("{}... [TRUNCATED]", &query[..MAX_QUERY_LOG_LENGTH])
        } else {
            query
        });
        self
    }

    /// Add metadata field
    pub fn with_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Set thread ID for debugging
    pub fn with_thread_id(mut self, thread_id: String) -> Self {
        self.thread_id = Some(thread_id);
        self
    }

    /// Get formatted timestamp as ISO 8601 string
    pub fn formatted_timestamp(&self) -> String {
        let secs = self.timestamp / 1_000_000;
        let micros = self.timestamp % 1_000_000;

        if let Some(datetime) = chrono::DateTime::from_timestamp(secs as i64, (micros * 1000) as u32) {
            datetime.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
        } else {
            format!("invalid-timestamp-{}", self.timestamp)
        }
    }

    /// Check if this event should trigger alerting
    pub fn requires_alert(&self) -> bool {
        matches!(self.severity, SecuritySeverity::High | SecuritySeverity::Critical)
            || matches!(
                self.event_type,
                SecurityEventType::SqlInjectionAttempt
                    | SecurityEventType::AuthenticationFailure
                    | SecurityEventType::CircuitBreakerOpen
                    | SecurityEventType::SecurityPolicyViolation
            )
    }
}

/// Configuration for audit logging
#[derive(Debug, Clone)]
pub struct AuditConfig {
    /// Enable audit logging
    pub enabled: bool,
    /// Log to structured JSON format
    pub json_format: bool,
    /// Include query content in logs
    pub log_queries: bool,
    /// Minimum severity level to log
    pub min_severity: SecuritySeverity,
    /// Enable real-time alerting for high-severity events
    pub enable_alerting: bool,
    /// Maximum size of metadata fields
    pub max_metadata_size: usize,
    /// Buffer size for batching events
    pub buffer_size: usize,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            json_format: true,
            log_queries: true,
            min_severity: SecuritySeverity::Info,
            enable_alerting: true,
            max_metadata_size: 1024,
            buffer_size: 100,
        }
    }
}

impl AuditConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("PGSQLITE_AUDIT_ENABLED") {
            config.enabled = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_AUDIT_JSON_FORMAT") {
            config.json_format = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_AUDIT_LOG_QUERIES") {
            config.log_queries = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_AUDIT_MIN_SEVERITY") {
            config.min_severity = match val.to_uppercase().as_str() {
                "INFO" => SecuritySeverity::Info,
                "WARNING" => SecuritySeverity::Warning,
                "HIGH" => SecuritySeverity::High,
                "CRITICAL" => SecuritySeverity::Critical,
                _ => SecuritySeverity::Info,
            };
        }

        if let Ok(val) = std::env::var("PGSQLITE_AUDIT_ENABLE_ALERTING") {
            config.enable_alerting = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_AUDIT_BUFFER_SIZE")
            && let Ok(size) = val.parse::<usize>() {
                config.buffer_size = size;
            }

        config
    }
}

/// Security audit logger
pub struct SecurityAuditLogger {
    config: AuditConfig,
    event_buffer: Arc<RwLock<Vec<SecurityEvent>>>,
    stats: Arc<RwLock<AuditStats>>,
}

/// Audit logging statistics
#[derive(Debug, Default, Clone)]
pub struct AuditStats {
    pub total_events: u64,
    pub events_by_severity: HashMap<String, u64>,
    pub events_by_type: HashMap<String, u64>,
    pub alerts_triggered: u64,
    pub buffer_flushes: u64,
    pub errors: u64,
}

impl SecurityAuditLogger {
    /// Create a new audit logger with default configuration
    pub fn new() -> Self {
        Self::with_config(AuditConfig::default())
    }

    /// Create a new audit logger with custom configuration
    pub fn with_config(config: AuditConfig) -> Self {
        Self {
            config,
            event_buffer: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(AuditStats::default())),
        }
    }

    /// Log a security event
    pub fn log_event(&self, event: SecurityEvent) {
        if !self.config.enabled {
            return;
        }

        // Check severity filtering
        if !self.should_log_severity(event.severity) {
            return;
        }

        // Update statistics
        self.update_stats(&event);

        // Log the event
        self.write_event(&event);

        // Handle alerting for high-severity events
        if self.config.enable_alerting && event.requires_alert() {
            self.trigger_alert(&event);
        }

        // Buffer management
        self.maybe_flush_buffer(event);
    }

    /// Check if severity level should be logged
    fn should_log_severity(&self, severity: SecuritySeverity) -> bool {
        use SecuritySeverity::*;
        match (self.config.min_severity, severity) {
            (Info, _) => true,
            (Warning, Warning | High | Critical) => true,
            (High, High | Critical) => true,
            (Critical, Critical) => true,
            _ => false,
        }
    }

    /// Update internal statistics
    fn update_stats(&self, event: &SecurityEvent) {
        let mut stats = self.stats.write();
        stats.total_events += 1;

        // Count by severity
        let severity_key = event.severity.to_string();
        *stats.events_by_severity.entry(severity_key).or_insert(0) += 1;

        // Count by type
        let type_key = event.event_type.to_string();
        *stats.events_by_type.entry(type_key).or_insert(0) += 1;

        // Count alerts
        if event.requires_alert() {
            stats.alerts_triggered += 1;
        }
    }

    /// Write event to log
    fn write_event(&self, event: &SecurityEvent) {
        if self.config.json_format {
            self.write_json_event(event);
        } else {
            self.write_text_event(event);
        }
    }

    /// Write event in JSON format
    fn write_json_event(&self, event: &SecurityEvent) {
        match serde_json::to_string(event) {
            Ok(json) => {
                info!(target: "security_audit", "{}", json);
            }
            Err(e) => {
                let mut stats = self.stats.write();
                stats.errors += 1;
                error!("Failed to serialize security event to JSON: {}", e);
            }
        }
    }

    /// Write event in human-readable text format
    fn write_text_event(&self, event: &SecurityEvent) {
        let client_info = event.client_ip
            .map(|ip| format!("client={}", ip))
            .unwrap_or_else(|| "client=unknown".to_string());

        let session_info = event.session_id
            .as_ref()
            .map(|s| format!("session={}", s))
            .unwrap_or_else(|| "session=none".to_string());

        let user_info = event.username
            .as_ref()
            .map(|u| format!("user={}", u))
            .unwrap_or_else(|| "user=unknown".to_string());

        let db_info = event.database
            .as_ref()
            .map(|d| format!("db={}", d))
            .unwrap_or_else(|| "db=unknown".to_string());

        let query_info = if self.config.log_queries {
            event.query
                .as_ref()
                .map(|q| format!("query=\"{}\"", q))
                .unwrap_or_default()
        } else {
            String::new()
        };

        let metadata_info = if !event.metadata.is_empty() {
            let metadata_str: String = event.metadata
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect::<Vec<_>>()
                .join(" ");
            format!("metadata=[{}]", metadata_str)
        } else {
            String::new()
        };

        let log_line = format!(
            "SECURITY_AUDIT timestamp={} severity={} type={} {} {} {} {} {} {} message=\"{}\"",
            event.formatted_timestamp(),
            event.severity,
            event.event_type,
            client_info,
            session_info,
            user_info,
            db_info,
            query_info,
            metadata_info,
            event.message
        );

        match event.severity {
            SecuritySeverity::Info => info!(target: "security_audit", "{}", log_line),
            SecuritySeverity::Warning => warn!(target: "security_audit", "{}", log_line),
            SecuritySeverity::High => warn!(target: "security_audit", "{}", log_line),
            SecuritySeverity::Critical => error!(target: "security_audit", "{}", log_line),
        }
    }

    /// Trigger alerting for high-severity events
    fn trigger_alert(&self, event: &SecurityEvent) {
        // In a real implementation, this would integrate with alerting systems
        // like PagerDuty, Slack, email, etc.
        error!(
            target: "security_alert",
            "SECURITY ALERT: {} - {} from {} - {}",
            event.severity,
            event.event_type,
            event.client_ip.map(|ip| ip.to_string()).unwrap_or_else(|| "unknown".to_string()),
            event.message
        );

        // Update alerting stats
        let mut stats = self.stats.write();
        stats.alerts_triggered += 1;
    }

    /// Buffer management for batching
    fn maybe_flush_buffer(&self, event: SecurityEvent) {
        let mut buffer = self.event_buffer.write();
        buffer.push(event);

        if buffer.len() >= self.config.buffer_size {
            // In a real implementation, this would batch-write to external systems
            debug!("Flushing audit event buffer with {} events", buffer.len());
            buffer.clear();

            let mut stats = self.stats.write();
            stats.buffer_flushes += 1;
        }
    }

    /// Get current statistics
    pub fn get_stats(&self) -> AuditStats {
        self.stats.read().clone()
    }

    /// Reset all statistics
    pub fn reset_stats(&self) {
        let mut stats = self.stats.write();
        *stats = AuditStats::default();
    }

    /// Force flush the event buffer
    pub fn flush_buffer(&self) {
        let mut buffer = self.event_buffer.write();
        if !buffer.is_empty() {
            debug!("Force flushing audit event buffer with {} events", buffer.len());
            buffer.clear();

            let mut stats = self.stats.write();
            stats.buffer_flushes += 1;
        }
    }
}

impl Default for SecurityAuditLogger {
    fn default() -> Self {
        Self::new()
    }
}

/// Global audit logger instance
static GLOBAL_AUDIT_LOGGER: std::sync::OnceLock<SecurityAuditLogger> = std::sync::OnceLock::new();

/// Get the global audit logger instance
pub fn global_audit_logger() -> &'static SecurityAuditLogger {
    GLOBAL_AUDIT_LOGGER.get_or_init(|| {
        let config = AuditConfig::from_env();
        SecurityAuditLogger::with_config(config)
    })
}

/// Convenience function to log a security event using the global logger
pub fn log_security_event(event: SecurityEvent) {
    global_audit_logger().log_event(event);
}

/// Convenience functions for common security events
pub mod events {
    use super::*;

    pub fn connection_accepted(client_ip: IpAddr, ssl: bool) {
        let event = SecurityEvent::new(
            SecurityEventType::ConnectionAccepted,
            SecuritySeverity::Info,
            format!("Connection accepted (SSL: {})", ssl),
        )
        .with_client_ip(client_ip)
        .with_metadata("ssl_enabled".to_string(), ssl.to_string());

        log_security_event(event);
    }

    pub fn connection_rejected(client_ip: IpAddr, reason: &str) {
        let event = SecurityEvent::new(
            SecurityEventType::ConnectionRejected,
            SecuritySeverity::Warning,
            format!("Connection rejected: {}", reason),
        )
        .with_client_ip(client_ip)
        .with_metadata("rejection_reason".to_string(), reason.to_string());

        log_security_event(event);
    }

    pub fn authentication_success(client_ip: IpAddr, username: &str, database: &str) {
        let event = SecurityEvent::new(
            SecurityEventType::AuthenticationSuccess,
            SecuritySeverity::Info,
            format!("User '{}' authenticated successfully", username),
        )
        .with_client_ip(client_ip)
        .with_username(username.to_string())
        .with_database(database.to_string());

        log_security_event(event);
    }

    pub fn sql_injection_attempt(
        client_ip: Option<IpAddr>,
        session_id: Option<String>,
        query: &str,
        pattern: &str,
    ) {
        let mut event = SecurityEvent::new(
            SecurityEventType::SqlInjectionAttempt,
            SecuritySeverity::High,
            format!("SQL injection attempt detected: pattern '{}'", pattern),
        )
        .with_query(query.to_string())
        .with_metadata("detected_pattern".to_string(), pattern.to_string());

        if let Some(ip) = client_ip {
            event = event.with_client_ip(ip);
        }

        if let Some(sid) = session_id {
            event = event.with_session_id(sid);
        }

        log_security_event(event);
    }

    pub fn rate_limit_exceeded(client_ip: Option<IpAddr>, limit_type: &str, current_rate: u32) {
        let mut event = SecurityEvent::new(
            SecurityEventType::RateLimitExceeded,
            SecuritySeverity::Warning,
            format!("Rate limit exceeded: {} (current: {})", limit_type, current_rate),
        )
        .with_metadata("limit_type".to_string(), limit_type.to_string())
        .with_metadata("current_rate".to_string(), current_rate.to_string());

        if let Some(ip) = client_ip {
            event = event.with_client_ip(ip);
        }

        log_security_event(event);
    }

    pub fn circuit_breaker_opened(failure_count: u32, threshold: u32) {
        let event = SecurityEvent::new(
            SecurityEventType::CircuitBreakerOpen,
            SecuritySeverity::Critical,
            format!(
                "Circuit breaker opened: {} failures (threshold: {})",
                failure_count, threshold
            ),
        )
        .with_metadata("failure_count".to_string(), failure_count.to_string())
        .with_metadata("threshold".to_string(), threshold.to_string());

        log_security_event(event);
    }

    pub fn protocol_violation(client_ip: Option<IpAddr>, violation: &str) {
        let mut event = SecurityEvent::new(
            SecurityEventType::ProtocolViolation,
            SecuritySeverity::High,
            format!("Protocol violation: {}", violation),
        )
        .with_metadata("violation_type".to_string(), violation.to_string());

        if let Some(ip) = client_ip {
            event = event.with_client_ip(ip);
        }

        log_security_event(event);
    }

    pub fn query_executed(
        client_ip: Option<IpAddr>,
        session_id: Option<String>,
        username: Option<String>,
        database: Option<String>,
        query: &str,
        duration_ms: u64,
    ) {
        let mut event = SecurityEvent::new(
            SecurityEventType::QueryExecuted,
            SecuritySeverity::Info,
            format!("Query executed in {}ms", duration_ms),
        )
        .with_query(query.to_string())
        .with_metadata("duration_ms".to_string(), duration_ms.to_string());

        if let Some(ip) = client_ip {
            event = event.with_client_ip(ip);
        }

        if let Some(sid) = session_id {
            event = event.with_session_id(sid);
        }

        if let Some(user) = username {
            event = event.with_username(user);
        }

        if let Some(db) = database {
            event = event.with_database(db);
        }

        log_security_event(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_security_event_creation() {
        let event = SecurityEvent::new(
            SecurityEventType::AuthenticationSuccess,
            SecuritySeverity::Info,
            "Test authentication".to_string(),
        )
        .with_client_ip("192.168.1.1".parse().unwrap())
        .with_username("testuser".to_string())
        .with_database("testdb".to_string())
        .with_metadata("test_key".to_string(), "test_value".to_string());

        assert_eq!(event.event_type, SecurityEventType::AuthenticationSuccess);
        assert_eq!(event.severity, SecuritySeverity::Info);
        assert_eq!(event.message, "Test authentication");
        assert!(event.client_ip.is_some());
        assert_eq!(event.username, Some("testuser".to_string()));
        assert_eq!(event.database, Some("testdb".to_string()));
        assert_eq!(event.metadata.get("test_key"), Some(&"test_value".to_string()));
    }

    #[test]
    fn test_query_truncation() {
        let long_query = "SELECT * FROM ".repeat(200);
        let event = SecurityEvent::new(
            SecurityEventType::QueryExecuted,
            SecuritySeverity::Info,
            "Test query".to_string(),
        )
        .with_query(long_query);

        assert!(event.query.unwrap().contains("[TRUNCATED]"));
    }

    #[test]
    fn test_alert_requirements() {
        let high_event = SecurityEvent::new(
            SecurityEventType::AuthenticationFailure,
            SecuritySeverity::High,
            "Test".to_string(),
        );
        assert!(high_event.requires_alert());

        let info_event = SecurityEvent::new(
            SecurityEventType::QueryExecuted,
            SecuritySeverity::Info,
            "Test".to_string(),
        );
        assert!(!info_event.requires_alert());

        let injection_event = SecurityEvent::new(
            SecurityEventType::SqlInjectionAttempt,
            SecuritySeverity::Warning,
            "Test".to_string(),
        );
        assert!(injection_event.requires_alert());
    }

    #[test]
    fn test_audit_logger_configuration() {
        let config = AuditConfig {
            enabled: true,
            min_severity: SecuritySeverity::Warning,
            ..Default::default()
        };

        let logger = SecurityAuditLogger::with_config(config);

        // Should log warning event
        assert!(logger.should_log_severity(SecuritySeverity::Warning));
        assert!(logger.should_log_severity(SecuritySeverity::High));
        assert!(logger.should_log_severity(SecuritySeverity::Critical));

        // Should not log info event
        assert!(!logger.should_log_severity(SecuritySeverity::Info));
    }

    #[test]
    fn test_audit_statistics() {
        let logger = SecurityAuditLogger::new();

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

        let stats = logger.get_stats();
        assert_eq!(stats.total_events, 2);
        assert_eq!(stats.events_by_severity.get("INFO"), Some(&1));
        assert_eq!(stats.events_by_severity.get("HIGH"), Some(&1));
        assert_eq!(stats.alerts_triggered, 1); // Only the high severity event
    }

    #[test]
    fn test_config_from_env() {
        unsafe {
            std::env::set_var("PGSQLITE_AUDIT_ENABLED", "true");
            std::env::set_var("PGSQLITE_AUDIT_JSON_FORMAT", "false");
            std::env::set_var("PGSQLITE_AUDIT_MIN_SEVERITY", "HIGH");
        }

        let config = AuditConfig::from_env();
        assert!(config.enabled);
        assert!(!config.json_format);
        assert_eq!(config.min_severity, SecuritySeverity::High);

        unsafe {
            std::env::remove_var("PGSQLITE_AUDIT_ENABLED");
            std::env::remove_var("PGSQLITE_AUDIT_JSON_FORMAT");
            std::env::remove_var("PGSQLITE_AUDIT_MIN_SEVERITY");
        }
    }

    #[test]
    fn test_convenience_functions() {
        // Test that convenience functions don't panic
        events::connection_accepted("192.168.1.1".parse().unwrap(), true);
        events::connection_rejected("192.168.1.2".parse().unwrap(), "rate limit");
        events::authentication_success("192.168.1.3".parse().unwrap(), "testuser", "testdb");
        events::sql_injection_attempt(
            Some("192.168.1.4".parse().unwrap()),
            Some("session123".to_string()),
            "SELECT * FROM users WHERE id = 1 OR 1=1",
            "OR 1=1",
        );
        events::rate_limit_exceeded(Some("192.168.1.5".parse().unwrap()), "per-ip", 100);
        events::circuit_breaker_opened(5, 3);
        events::protocol_violation(Some("192.168.1.6".parse().unwrap()), "invalid message");
    }

    #[test]
    fn test_timestamp_formatting() {
        let event = SecurityEvent::new(
            SecurityEventType::AuthenticationSuccess,
            SecuritySeverity::Info,
            "Test".to_string(),
        );

        let formatted = event.formatted_timestamp();
        // Should be in ISO 8601 format
        assert!(formatted.contains("T"));
        assert!(formatted.ends_with("Z"));
    }
}