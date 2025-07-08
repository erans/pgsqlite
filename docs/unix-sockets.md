# Unix Socket Configuration

pgsqlite supports Unix domain sockets for local connections, providing lower latency and better security than TCP connections for local clients.

## Quick Start

```bash
# Enable Unix socket (default: /tmp)
pgsqlite --socket-dir /tmp

# Connect with psql
psql -h /tmp -p 5432 -d your_database

# Unix socket only (disable TCP)
pgsqlite --socket-dir /var/run/pgsqlite --no-tcp
```

## How It Works

When Unix socket support is enabled, pgsqlite creates a socket file named `.s.PGSQL.{port}` in the specified directory. This follows PostgreSQL's naming convention, ensuring compatibility with all PostgreSQL clients.

## Configuration Options

| Option | CLI Flag | Environment Variable | Default | Description |
|--------|----------|---------------------|---------|-------------|
| Socket Directory | `--socket-dir` | `PGSQLITE_SOCKET_DIR` | `/tmp` | Directory for Unix socket file |
| Disable TCP | `--no-tcp` | `PGSQLITE_NO_TCP` | `false` | Use only Unix socket, no TCP |

## Connection Examples

### psql

```bash
# Standard connection
psql -h /tmp -p 5432 -d mydatabase

# Using connection string
psql "postgresql:///mydatabase?host=/tmp&port=5432"
```

### Python (psycopg2)

```python
import psycopg2

# Connect via Unix socket
conn = psycopg2.connect(
    host='/tmp',              # Socket directory
    port=5432,
    database='mydatabase'
)

# Alternative using DSN
conn = psycopg2.connect("host=/tmp port=5432 dbname=mydatabase")
```

### Node.js (pg)

```javascript
const { Client } = require('pg')

const client = new Client({
  host: '/tmp',
  port: 5432,
  database: 'mydatabase'
})
```

### Go (pq)

```go
import (
    "database/sql"
    _ "github.com/lib/pq"
)

db, err := sql.Open("postgres", "host=/tmp port=5432 dbname=mydatabase sslmode=disable")
```

## Security Benefits

Unix sockets provide several security advantages:

1. **Local-only access**: No network exposure
2. **File system permissions**: Control access via directory/file permissions
3. **No SSL overhead**: Secure by default for local connections
4. **Process isolation**: Can restrict to specific users/groups

### Securing Socket Access

```bash
# Create dedicated directory with restricted permissions
sudo mkdir -p /var/run/pgsqlite
sudo chown myuser:mygroup /var/run/pgsqlite
sudo chmod 750 /var/run/pgsqlite

# Run pgsqlite
pgsqlite --socket-dir /var/run/pgsqlite
```

## Performance Benefits

Unix sockets typically provide:
- **Lower latency**: No TCP/IP stack overhead
- **Higher throughput**: Direct kernel communication
- **Less CPU usage**: No packet processing

Benchmarks show 10-20% performance improvement over TCP for local connections.

## Common Configurations

### Development Setup

```bash
# Standard development configuration
pgsqlite --socket-dir /tmp --database dev.db
```

### Production Setup

```bash
# Secure production configuration
pgsqlite \
  --socket-dir /var/run/pgsqlite \
  --no-tcp \
  --database /data/production.db
```

### Docker Setup

```dockerfile
FROM rust:latest

# Create socket directory
RUN mkdir -p /var/run/pgsqlite

# Volume for socket access
VOLUME ["/var/run/pgsqlite"]

CMD ["pgsqlite", "--socket-dir", "/var/run/pgsqlite"]
```

```yaml
# docker-compose.yml
services:
  pgsqlite:
    image: pgsqlite
    volumes:
      - pgsqlite-socket:/var/run/pgsqlite
      - ./data:/data
    command: ["--socket-dir", "/var/run/pgsqlite", "--database", "/data/app.db"]

  app:
    image: myapp
    volumes:
      - pgsqlite-socket:/var/run/pgsqlite
    environment:
      DATABASE_HOST: /var/run/pgsqlite
      DATABASE_PORT: 5432

volumes:
  pgsqlite-socket:
```

## Troubleshooting

### Common Issues

1. **"No such file or directory" error**
   - Ensure socket directory exists
   - Check client is using correct directory path

2. **Permission denied**
   - Verify user has access to socket directory
   - Check socket file permissions

3. **Socket file not created**
   - Confirm pgsqlite started successfully
   - Check logs for binding errors

### Debugging

```bash
# Check if socket exists
ls -la /tmp/.s.PGSQL.5432

# Verify permissions
stat /tmp/.s.PGSQL.5432

# Test connection
psql -h /tmp -p 5432 -c "SELECT 1"
```

## Platform Notes

### Linux
- Default socket directory: `/tmp`
- Alternative: `/var/run/postgresql`

### macOS
- Default socket directory: `/tmp`
- Note: `/var` directories may require sudo

### Windows
- Unix sockets not supported
- Use TCP connections on Windows

## Best Practices

1. **Use dedicated directory** for production (`/var/run/pgsqlite`)
2. **Set appropriate permissions** on socket directory
3. **Monitor socket file** existence in health checks
4. **Clean up stale sockets** on startup
5. **Document socket path** for client applications