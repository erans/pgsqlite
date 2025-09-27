# pgsqlite Security Architecture

## Overview

pgsqlite implements defense-in-depth security with multiple layers of protection against common attack vectors. This document details the security features, configuration options, and best practices for deploying pgsqlite in production environments.

## Table of Contents

1. [SQL Injection Protection](#sql-injection-protection)
2. [Security Audit Logging](#security-audit-logging)
3. [Rate Limiting & DoS Protection](#rate-limiting--dos-protection)
4. [Memory Safety](#memory-safety)
5. [Input Validation](#input-validation)
6. [Network Security](#network-security)
7. [Configuration Best Practices](#configuration-best-practices)
8. [Security Monitoring](#security-monitoring)

## SQL Injection Protection

### Architecture

pgsqlite employs a sophisticated two-tier SQL injection detection system:

1. **AST-based Analysis (Primary)**
   - Parses SQL using `sqlparser` with PostgreSQL dialect
   - Builds Abstract Syntax Tree for structural analysis
   - Detects injection patterns at the semantic level

2. **Pattern Matching (Fallback)**
   - Activates when SQL parsing fails
   - High-confidence pattern detection
   - Zero false positives for legitimate queries

### Detection Capabilities

#### Tautology Detection
Identifies always-true conditions commonly used in SQL injection:
- Numeric tautologies: `1=1`, `2=2`, `1<>0`
- String tautologies: `'a'='a'`, `"x"="x"`
- Complex tautologies: `1=1 AND 2=2`

#### Dangerous Function Detection
Blocks execution of high-risk functions:
- System commands: `exec`, `execute`, `system`, `shell`
- Microsoft SQL Server: `xp_cmdshell`, `sp_executesql`
- Generic: `eval`, `cmd`

#### Union-based Attack Prevention
- Limits UNION operations (default: 5)
- Detects suspicious UNION with sensitive tables
- Blocks `UNION SELECT password FROM admin` patterns

#### Multi-statement Attack Prevention
- Limits statement count per query (default: 3)
- Prevents statement stacking attacks
- Blocks `; DROP TABLE users; --` patterns

### Configuration

The SQL injection detector is always active but can be tuned:

```rust
// In code - for embedded use
let detector = SqlInjectionDetector::new()
    .with_max_depth(10)          // Maximum query nesting depth
    .with_max_statements(3)       // Maximum statements per query
    .with_max_unions(5);          // Maximum UNION operations
```

### Implementation Details

Location: `/src/security/sql_injection_detector.rs`

Key components:
- `SqlInjectionDetector`: Main detection engine
- `SqlAnalysisResult`: Analysis output with detailed findings
- Integration with `DbHandler` for query validation
- Automatic security event logging

## Security Audit Logging

### Features

Comprehensive logging of security-relevant events:

- **Authentication Events**: Login attempts, successes, failures
- **SQL Injection Attempts**: Detailed analysis of blocked queries
- **Permission Violations**: Unauthorized access attempts
- **Rate Limit Violations**: DoS attempt detection
- **System Anomalies**: Unexpected errors, resource exhaustion

### Configuration

Environment variables for audit configuration:

```bash
# Enable/disable audit logging
PGSQLITE_AUDIT_ENABLED=true

# Minimum severity level (debug, info, warning, error, critical)
PGSQLITE_AUDIT_SEVERITY=info

# Specific event types
PGSQLITE_AUDIT_LOG_AUTH=true       # Authentication events
PGSQLITE_AUDIT_LOG_QUERIES=true    # Query execution
PGSQLITE_AUDIT_LOG_ERRORS=true     # System errors
PGSQLITE_AUDIT_LOG_ADMIN=true      # Administrative actions

# Output configuration
PGSQLITE_AUDIT_BUFFER_SIZE=1000    # Event buffer size
PGSQLITE_AUDIT_MAX_QUERY_LENGTH=1000  # Query truncation
```

### Event Format

Audit events are logged as structured JSON:

```json
{
  "timestamp": 1758931844607131,
  "event_type": "SqlInjectionAttempt",
  "severity": "High",
  "client_ip": "192.168.1.100",
  "session_id": "abc123",
  "database": "production",
  "username": "webapp",
  "query": "SELECT * FROM users WHERE id = 1 OR 1=1",
  "message": "SQL injection attempt detected: tautology",
  "metadata": {
    "detected_pattern": "tautology",
    "detection_method": "ast_analysis"
  },
  "process_id": 1234,
  "thread_id": 5678
}
```

### Alert System

High-severity events trigger immediate alerts:
- SQL injection attempts
- Authentication failures (repeated)
- Rate limit violations
- System resource exhaustion

## Rate Limiting & DoS Protection

### Architecture

Multi-layered protection against denial-of-service attacks:

1. **Per-Client Rate Limiting**
   - Token bucket algorithm
   - Configurable limits per IP
   - Sliding window tracking

2. **Circuit Breaker Pattern**
   - Automatic client isolation
   - Failure threshold detection
   - Graduated recovery

3. **Resource Protection**
   - Query size limits
   - Nesting depth limits
   - Statement count limits

### Configuration

```bash
# Rate limiting
PGSQLITE_RATE_LIMIT_ENABLED=true
PGSQLITE_RATE_LIMIT_REQUESTS=1000    # Requests per window
PGSQLITE_RATE_LIMIT_WINDOW=1         # Window in seconds
PGSQLITE_RATE_LIMIT_BURST=100        # Burst capacity

# Circuit breaker
PGSQLITE_CIRCUIT_BREAKER_ENABLED=true
PGSQLITE_CIRCUIT_BREAKER_THRESHOLD=0.5  # Failure rate threshold
PGSQLITE_CIRCUIT_BREAKER_WINDOW=60      # Evaluation window (seconds)
PGSQLITE_CIRCUIT_BREAKER_COOLDOWN=300   # Recovery time (seconds)

# Resource limits
PGSQLITE_MAX_QUERY_SIZE=1048576         # 1MB max query
PGSQLITE_MAX_QUERY_DEPTH=100            # Max nesting
PGSQLITE_MAX_STATEMENTS=10              # Max statements per query
```

### Implementation

Location: `/src/security/rate_limiter.rs`

Features:
- Lock-free atomic operations for performance
- Memory-efficient sliding window
- Automatic cleanup of old entries
- Statistics and metrics collection

## Memory Safety

### Rust Safety Guarantees

pgsqlite leverages Rust's ownership system for memory safety:

- **No buffer overflows**: Bounds checking at compile time
- **No use-after-free**: Ownership tracking prevents dangling pointers
- **No data races**: Send/Sync traits ensure thread safety
- **No null pointer dereferences**: Option types for nullable values

### Memory Optimization

Advanced memory management for performance:

1. **Copy-on-Write Strings** (`Cow<str>`)
   - Avoids unnecessary allocations
   - Reduces memory fragmentation
   - Improves cache locality

2. **Arena Allocators**
   - Bulk allocation for related objects
   - Reduced allocation overhead
   - Improved cleanup performance

3. **TTL-based Caching**
   - Automatic eviction of stale entries
   - Memory pressure handling
   - Configurable size limits

### Memory Leak Prevention

- Smart pointers (`Arc`, `Rc`) for reference counting
- RAII pattern for resource management
- Automatic cleanup on scope exit
- No manual memory management

## Input Validation

### Protocol-Level Validation

All PostgreSQL wire protocol messages are validated:

- Message size limits
- Type checking
- Format verification
- Sequence validation

### Query Parameter Validation

- Type safety for prepared statements
- Length validation for strings
- Range checking for numerics
- Format validation for specialized types

### Connection Validation

- Authentication verification
- SSL/TLS certificate validation
- Client IP allowlisting (optional)
- Connection limit enforcement

## Network Security

### SSL/TLS Support

Full TLS 1.2+ support for encrypted connections:

```bash
# Generate certificates
pgsqlite --generate-certs --cert-dir ./certs

# Run with TLS
pgsqlite --ssl --cert ./certs/server.crt --key ./certs/server.key
```

Features:
- Certificate-based authentication
- Perfect forward secrecy
- Modern cipher suites only
- Optional client certificate verification

### Unix Socket Support

For local connections with enhanced security:

```bash
# Use Unix socket (more secure for local connections)
pgsqlite --unix-socket /var/run/pgsqlite.sock
```

Benefits:
- No network exposure
- File system permissions
- Lower latency
- Reduced attack surface

## Configuration Best Practices

### Production Deployment

```bash
#!/bin/bash
# Production configuration example

# Core settings
export PGSQLITE_DATABASE="/secure/path/database.db"
export PGSQLITE_BIND_ADDRESS="127.0.0.1"  # Local only
export PGSQLITE_PORT=5432

# Security
export PGSQLITE_SSL=true
export PGSQLITE_SSL_CERT="/secure/certs/server.crt"
export PGSQLITE_SSL_KEY="/secure/certs/server.key"
export PGSQLITE_REQUIRE_SSL=true  # Force SSL connections

# Audit logging
export PGSQLITE_AUDIT_ENABLED=true
export PGSQLITE_AUDIT_SEVERITY=info
export PGSQLITE_AUDIT_LOG_AUTH=true
export PGSQLITE_AUDIT_LOG_QUERIES=false  # Only for debugging

# Rate limiting
export PGSQLITE_RATE_LIMIT_ENABLED=true
export PGSQLITE_RATE_LIMIT_REQUESTS=100
export PGSQLITE_RATE_LIMIT_WINDOW=1

# Resource limits
export PGSQLITE_MAX_CONNECTIONS=100
export PGSQLITE_CONNECTION_TIMEOUT=300

# Start with restricted permissions
umask 077
pgsqlite
```

### Security Checklist

- [ ] Enable SSL/TLS for network connections
- [ ] Configure audit logging
- [ ] Set appropriate rate limits
- [ ] Use Unix sockets for local connections
- [ ] Restrict file permissions on database files
- [ ] Enable connection limits
- [ ] Configure firewall rules
- [ ] Monitor audit logs
- [ ] Regular security updates
- [ ] Backup strategy in place

## Security Monitoring

### Key Metrics to Monitor

1. **Authentication Metrics**
   - Failed login attempts
   - Successful authentications
   - Authentication latency

2. **SQL Injection Metrics**
   - Blocked queries count
   - Detection method distribution
   - Attack pattern trends

3. **Rate Limiting Metrics**
   - Rate limit violations
   - Circuit breaker trips
   - Client distribution

4. **Resource Metrics**
   - Memory usage
   - Connection count
   - Query execution time

### Integration with Monitoring Systems

Export metrics to monitoring systems:

```bash
# Prometheus metrics endpoint (planned)
PGSQLITE_METRICS_ENABLED=true
PGSQLITE_METRICS_PORT=9090

# StatsD integration (planned)
PGSQLITE_STATSD_HOST=localhost
PGSQLITE_STATSD_PORT=8125
```

### Alert Configuration

Critical alerts to configure:

1. **High SQL injection attempt rate** (> 10/min)
2. **Authentication failure spike** (> 50/min)
3. **Circuit breaker activation**
4. **Memory usage > 90%**
5. **Connection pool exhaustion**

## Security Updates

Stay informed about security updates:

- Watch the [GitHub repository](https://github.com/erans/pgsqlite) for security advisories
- Enable GitHub security alerts
- Subscribe to release notifications
- Review the changelog for security fixes

## Reporting Security Issues

If you discover a security vulnerability:

1. **Do not** create a public GitHub issue
2. Email security details to the maintainers
3. Include:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
   - Suggested fix (if available)

## Compliance Considerations

pgsqlite's security features support compliance with:

- **PCI DSS**: SQL injection protection, audit logging
- **HIPAA**: Encryption in transit, audit trails
- **GDPR**: Data protection, audit logging
- **SOC 2**: Security controls, monitoring

Note: pgsqlite itself is not certified for these standards. Compliance depends on your overall implementation and controls.

## Future Security Enhancements

Planned security improvements:

- [ ] Row-level security (RLS) support
- [ ] Column-level encryption
- [ ] Advanced threat detection with ML
- [ ] Security scanning integration
- [ ] Automated security testing
- [ ] Certificate rotation support
- [ ] OAuth/SAML authentication
- [ ] Audit log shipping to SIEM

## Conclusion

pgsqlite provides comprehensive security features suitable for production deployments. By following the configuration guidelines and best practices in this document, you can deploy pgsqlite with confidence in security-sensitive environments.

Remember: Security is a shared responsibility. While pgsqlite provides the tools, proper configuration, monitoring, and operational practices are essential for maintaining a secure deployment.