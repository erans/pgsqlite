// Module for security functionality
pub mod audit_logger;
pub mod sql_injection_detector;

pub use audit_logger::{
    SecurityAuditLogger, SecurityEvent, SecurityEventType, SecuritySeverity,
    AuditConfig, AuditStats, AuditError,
    global_audit_logger, log_security_event, events,
};

pub use sql_injection_detector::{
    SqlInjectionDetector, SqlAnalysisResult,
};