# SSL/TLS Implementation Plan for pgsqlite

## Overview
This document outlines the plan for adding SSL/TLS support to pgsqlite, enabling secure connections between PostgreSQL clients and the pgsqlite server.

## Requirements

### SSL Configuration Priority
1. **Command line arguments / Environment variables** - Highest priority
2. **File system** - Check for certificates next to the database file
3. **In-memory generation** - For `:memory:` databases or when ephemeral keys are requested

### Configuration Options
- `--ssl` flag (or `PGSQLITE_SSL=true` env var) - Enable SSL support
- `--ssl-cert` / `PGSQLITE_SSL_CERT` - Path to SSL certificate
- `--ssl-key` / `PGSQLITE_SSL_KEY` - Path to SSL private key
- `--ssl-ca` / `PGSQLITE_SSL_CA` - Path to CA certificate (optional)
- `--ssl-ephemeral` / `PGSQLITE_SSL_EPHEMERAL` - Generate ephemeral keys on startup

### Behavior Rules
1. SSL is only available for TCP connections (not Unix sockets)
2. If SSL flag is not set, respond to SSL requests with 'N' (no SSL)
3. If SSL is enabled:
   - Check for provided certificate paths first
   - If not provided, look for certificates next to the database file
   - For `:memory:` databases, always generate in-memory certificates
   - For file-based databases with `--ssl-ephemeral`, generate temporary certificates
   - For file-based databases without ephemeral flag, generate and save certificates if missing

### Certificate File Naming Convention
When storing certificates next to the database file:
- Certificate: `<database_name>.crt`
- Private key: `<database_name>.key`
- CA certificate: `<database_name>-ca.crt` (if needed)

Example: For `mydb.sqlite`, certificates would be `mydb.crt` and `mydb.key`

## Implementation Steps

### 1. Add SSL Configuration Options
- Extend the existing configuration system to include SSL-related options
- Add validation to ensure SSL is not enabled for Unix socket connections

### 2. PostgreSQL SSL Negotiation
- Implement SSL negotiation phase in the connection handler
- When client sends SSL request packet (8 bytes: length + SSL request code)
  - Respond with 'S' if SSL is available and configured
  - Respond with 'N' if SSL is not available
- After 'S' response, upgrade connection to TLS

### 3. Certificate Management
Implement certificate discovery and generation logic:

```rust
enum CertificateSource {
    Provided { cert_path: String, key_path: String },
    FileSystem { cert_path: String, key_path: String },
    Generated { cert: Vec<u8>, key: Vec<u8> },
}
```

Discovery flow:
1. Check command line args / env vars
2. If not found and not `:memory:`, check file system
3. If not found or `:memory:` or ephemeral, generate

### 4. TLS Integration
- Use `tokio-rustls` or similar async TLS library
- Wrap the TCP stream with TLS after successful negotiation
- Ensure all subsequent PostgreSQL protocol communication happens over TLS

### 5. Certificate Generation
For self-signed certificates:
- Use `rcgen` crate for certificate generation
- Generate RSA 2048-bit or ECDSA P-256 keys
- Set appropriate certificate fields (CN, validity period, etc.)
- For ephemeral certificates, use short validity (e.g., 90 days)
- For persistent certificates, use longer validity (e.g., 10 years)

### 6. Logging
Log SSL status on server startup:
- "SSL enabled with existing certificates from <path>"
- "SSL enabled with newly generated certificates stored at <path>"
- "SSL enabled with ephemeral in-memory certificates"
- "SSL disabled - using unencrypted connections"

### 7. Error Handling
- Clear error messages for certificate read/write failures
- Graceful fallback when SSL setup fails
- Proper error propagation to clients

## Security Considerations
1. Default to secure defaults (e.g., TLS 1.2 minimum)
2. Allow configuration of TLS versions and cipher suites
3. Validate certificate permissions (warn if world-readable private keys)
4. Consider adding support for client certificate authentication in the future

## Testing Strategy
1. Unit tests for certificate discovery logic
2. Integration tests for SSL negotiation
3. End-to-end tests with actual PostgreSQL clients using SSL
4. Performance benchmarks comparing SSL vs non-SSL connections

## Future Enhancements
- Client certificate authentication
- Certificate rotation without restart
- ACME/Let's Encrypt integration for public deployments
- SSL compression (if beneficial)
- SCRAM authentication over SSL