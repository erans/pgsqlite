# Security

This document describes the security-related behavior that exists in the current pgsqlite codebase (not aspirational features).

## Network Surface

### TCP Listener

- By default, pgsqlite listens on `0.0.0.0:<port>` (see `--port` / `PGSQLITE_PORT`).
- There is currently no bind-address configuration option. If you need to restrict exposure, use firewall rules, container/network policy, or disable TCP entirely.

### Unix Domain Socket (Unix Only)

- On Unix platforms, pgsqlite creates a PostgreSQL-compatible socket file named `.s.PGSQL.<port>` in `--socket-dir` (default: `/tmp`).
- `--no-tcp` disables the TCP listener and leaves Unix sockets as the only listener.

See `docs/unix-sockets.md` for connection examples.

### TLS/SSL (TCP Only)

- `--ssl` enables PostgreSQL-style SSL negotiation (clients that send an `SSLRequest` will be upgraded to TLS).
- TLS cannot be enabled when TCP is disabled (`--no-tcp`), because Unix sockets do not use TLS.
- TLS support does not currently force encryption. Clients that do not request SSL can continue in plaintext even when `--ssl` is enabled. If you want to enforce encryption, configure clients with `sslmode=require` and/or block plaintext at the network layer.

Certificate behavior is implemented in `src/ssl/cert_manager.rs`:

- If `--ssl-cert` and `--ssl-key` (or `PGSQLITE_SSL_CERT`/`PGSQLITE_SSL_KEY`) are provided, those files are used.
- Otherwise, pgsqlite looks for `<cert-stem>.crt` and `<cert-stem>.key` in the database directory (file mode: next to the database file; directory mode: inside the data directory using the default database name as the stem).
- If no cert/key are found, pgsqlite generates self-signed certificates.
  - In-memory databases (or `--ssl-ephemeral`) use ephemeral certs (not persisted).
  - Otherwise, generated certs are written to `<cert-stem>.crt` / `<cert-stem>.key` in the database directory.

See `docs/ssl-setup.md` for usage.

## SQL Injection Detection

Queries are validated by `SqlInjectionDetector` before execution (see `src/security/sql_injection_detector.rs` and usage in `src/session/db_handler.rs`). There is currently no CLI/env configuration surface for tuning the detector; tuning is a code-level concern for embedded use.

## Security Audit Logging

Security-relevant events are logged via the global audit logger (see `src/security/audit_logger.rs`).

Defaults:

- Audit logging is enabled by default.
- JSON output is enabled by default.
- Query text is included by default.
- Logs are emitted via `tracing` targets `security_audit` and `security_alert`.

Environment variables:

| Variable | Default | Notes |
|----------|---------|------|
| `PGSQLITE_AUDIT_ENABLED` | `true` | `true/1` enables, `false/0` disables |
| `PGSQLITE_AUDIT_JSON_FORMAT` | `true` | `true/1` JSON, `false/0` text |
| `PGSQLITE_AUDIT_LOG_QUERIES` | `true` | Include query text in audit logs |
| `PGSQLITE_AUDIT_MIN_SEVERITY` | `INFO` | One of `INFO`, `WARNING`, `HIGH`, `CRITICAL` |
| `PGSQLITE_AUDIT_ENABLE_ALERTING` | `true` | Emits high-severity alerts to `security_alert` target |
| `PGSQLITE_AUDIT_BUFFER_SIZE` | `100` | Buffer size for batching internal audit events |

Example:

```bash
PGSQLITE_AUDIT_ENABLED=true \
PGSQLITE_AUDIT_MIN_SEVERITY=WARNING \
PGSQLITE_AUDIT_LOG_QUERIES=false \
pgsqlite --database ./data
```

## Rate Limiting and Circuit Breaker

Rate limiting and a circuit breaker are enforced through `src/protocol/rate_limiter.rs` and are used in the TCP connection path and query path (see `src/main.rs`).

Notes:

- There is no single "disable rate limiting" switch. If you want it effectively disabled, increase the limits.
- The circuit breaker can be disabled via env var.

Environment variables:

| Variable | Default | Notes |
|----------|---------|------|
| `PGSQLITE_RATE_LIMIT_MAX_REQUESTS` | `1000` | Max requests per window |
| `PGSQLITE_RATE_LIMIT_WINDOW_SECS` | `60` | Window size in seconds |
| `PGSQLITE_RATE_LIMIT_PER_IP` | `true` | If `true`, applies limits per-client-IP (when IP is known) |
| `PGSQLITE_RATE_LIMIT_MAX_IPS` | `10000` | Max tracked IPs for per-IP limiting |
| `PGSQLITE_CIRCUIT_BREAKER_ENABLED` | `true` | `true/1` enables, `false/0` disables |
| `PGSQLITE_CIRCUIT_BREAKER_FAILURE_THRESHOLD` | `50` | Failures before opening circuit |
| `PGSQLITE_CIRCUIT_BREAKER_TIMEOUT_SECS` | `60` | Time to keep circuit open |
| `PGSQLITE_CIRCUIT_BREAKER_SUCCESS_THRESHOLD` | `10` | Successes required to close circuit |

Example:

```bash
PGSQLITE_RATE_LIMIT_MAX_REQUESTS=10000 \
PGSQLITE_RATE_LIMIT_WINDOW_SECS=60 \
PGSQLITE_CIRCUIT_BREAKER_ENABLED=false \
pgsqlite --database ./data
```

## Operational Recommendations

- Prefer Unix sockets + `--no-tcp` for local-only deployments.
- If TCP is required, use `--ssl` and configure clients with `sslmode=require` for encryption-in-transit.
- Use `--max-connections` / `PGSQLITE_MAX_CONNECTIONS` to cap concurrent sessions.
