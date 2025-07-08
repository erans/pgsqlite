# SSL/TLS Setup Guide

pgsqlite supports SSL/TLS encryption for secure connections over TCP. This guide covers all SSL configuration options.

## Quick Start

### Enable SSL with Auto-Generated Certificates

```bash
# Simplest way - auto-generates self-signed certificates
pgsqlite --ssl

# With in-memory database (ephemeral certificates)
pgsqlite --in-memory --ssl
```

### Use Custom Certificates

```bash
# Provide your own certificates
pgsqlite --ssl \
  --ssl-cert /path/to/server.crt \
  --ssl-key /path/to/server.key
```

## Certificate Management

### Certificate Discovery Order

pgsqlite looks for certificates in this order:

1. **Command line paths**: `--ssl-cert` and `--ssl-key`
2. **Environment variables**: `PGSQLITE_SSL_CERT` and `PGSQLITE_SSL_KEY`
3. **Automatic discovery**: Looks next to database file
   - For `mydb.sqlite` → looks for `mydb.crt` and `mydb.key`
4. **Auto-generation**: Creates self-signed certificates if not found

### Certificate Storage Behavior

| Database Type | Flag | Behavior |
|--------------|------|----------|
| In-memory | Any | Always uses ephemeral certificates (not saved) |
| File-based | `--ssl-ephemeral` | Generates temporary certificates (not saved) |
| File-based | No ephemeral flag | Generates and saves certificates next to database |

## Connection Examples

### PostgreSQL Clients

```bash
# Connect with psql requiring SSL
psql "postgresql://localhost:5432/mydb?sslmode=require"

# Connect with psql (prefer SSL but allow non-SSL)
psql "postgresql://localhost:5432/mydb?sslmode=prefer"
```

### Python (psycopg2)

```python
import psycopg2

# Require SSL
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="mydb",
    sslmode="require"
)

# With custom CA certificate
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="mydb",
    sslmode="verify-ca",
    sslrootcert="/path/to/ca.crt"
)
```

### Node.js (pg)

```javascript
const { Client } = require('pg')

// Require SSL
const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'mydb',
  ssl: {
    rejectUnauthorized: false  // For self-signed certificates
  }
})

// With CA verification
const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'mydb',
  ssl: {
    rejectUnauthorized: true,
    ca: fs.readFileSync('/path/to/ca.crt').toString()
  }
})
```

## Advanced Configuration

### Generate Certificates Manually

```bash
# Generate a self-signed certificate (for development)
openssl req -new -x509 -days 365 -nodes \
  -out server.crt \
  -keyout server.key \
  -subj "/CN=localhost"

# Use with pgsqlite
pgsqlite --ssl \
  --ssl-cert server.crt \
  --ssl-key server.key
```

### Production Setup

For production, use certificates from a trusted CA:

```bash
# Example with Let's Encrypt certificates
pgsqlite --ssl \
  --ssl-cert /etc/letsencrypt/live/yourdomain/fullchain.pem \
  --ssl-key /etc/letsencrypt/live/yourdomain/privkey.pem
```

### Docker Configuration

```dockerfile
FROM rust:latest

# Copy certificates
COPY certs/server.crt /app/certs/
COPY certs/server.key /app/certs/

# Run with SSL
CMD ["pgsqlite", "--ssl", \
     "--ssl-cert", "/app/certs/server.crt", \
     "--ssl-key", "/app/certs/server.key"]
```

## SSL Modes

PostgreSQL clients support various SSL modes:

| Mode | Description | Security Level |
|------|-------------|----------------|
| `disable` | No SSL | None |
| `allow` | Try non-SSL first, then SSL | Low |
| `prefer` | Try SSL first, fall back to non-SSL | Medium |
| `require` | Only SSL connections | High |
| `verify-ca` | SSL + verify server certificate | Higher |
| `verify-full` | SSL + verify certificate + hostname | Highest |

## Troubleshooting

### Common Issues

1. **"SSL not supported" error**
   - Ensure pgsqlite was started with `--ssl` flag
   - Check that you're connecting via TCP (not Unix socket)

2. **Certificate verification failures**
   - For self-signed certificates, use `sslmode=require` (not `verify-ca`)
   - Or configure client to trust the certificate

3. **Permission errors**
   - Ensure certificate files are readable by pgsqlite process
   - Check file permissions: `chmod 600 server.key`

### Debug SSL Connections

```bash
# Enable debug logging
RUST_LOG=debug pgsqlite --ssl

# Test SSL connection
openssl s_client -connect localhost:5432 -servername localhost
```

## Security Best Practices

1. **Use trusted certificates** in production (not self-signed)
2. **Protect private keys** with appropriate file permissions
3. **Rotate certificates** regularly
4. **Use strong SSL modes** (`require` or higher) for sensitive data
5. **Monitor certificate expiration** dates

## Environment Variables

All SSL options can be set via environment:

```bash
export PGSQLITE_SSL=true
export PGSQLITE_SSL_CERT=/secure/path/server.crt
export PGSQLITE_SSL_KEY=/secure/path/server.key
export PGSQLITE_SSL_EPHEMERAL=true

pgsqlite
```