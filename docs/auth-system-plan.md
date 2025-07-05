# Authentication System Implementation Plan for pgsqlite

## Overview

This document outlines the implementation plan for adding a PostgreSQL-compatible authentication system to pgsqlite. The system will use a dedicated SQLite database to store user credentials and permissions, enabling user authentication with username/password and per-database access control.

**Prerequisites**: This feature requires the multi-database support to be implemented first (see `multi-database-support-plan.md`).

## Goals

1. Implement PostgreSQL-compatible authentication with username/password
2. Store user credentials securely in a dedicated SQLite database
3. Support per-database access control
4. Maintain backward compatibility with no-auth mode
5. Support PostgreSQL authentication methods (md5, scram-sha-256)
6. Enable role-based access control (RBAC) foundation

## Feature Flag

Authentication will be controlled by:
- Command-line flag: `--auth` or `--enable-auth`
- Environment variable: `PGSQLITE_AUTH=true`
- Requires: `--multi-db` to be enabled

When disabled (default):
- No authentication required (current behavior)
- All connections have full access
- Backward compatible with existing deployments

## Architecture Design

### 1. Auth Database Schema

Create a dedicated `_pgsqlite_auth.db` database in the data directory:

```sql
-- Users table
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    auth_method TEXT NOT NULL, -- 'md5' or 'scram-sha-256'
    salt TEXT,
    iterations INTEGER, -- for scram
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT 1,
    is_superuser BOOLEAN DEFAULT 0
);

-- Database permissions
CREATE TABLE database_permissions (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    database_name TEXT NOT NULL,
    can_connect BOOLEAN DEFAULT 1,
    can_create_table BOOLEAN DEFAULT 1,
    can_drop_table BOOLEAN DEFAULT 1,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    granted_by TEXT,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    UNIQUE(user_id, database_name)
);

-- Roles (future enhancement)
CREATE TABLE roles (
    id INTEGER PRIMARY KEY,
    role_name TEXT UNIQUE NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User roles mapping
CREATE TABLE user_roles (
    user_id INTEGER NOT NULL,
    role_id INTEGER NOT NULL,
    granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
    FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE,
    PRIMARY KEY (user_id, role_id)
);

-- Audit log
CREATE TABLE auth_audit_log (
    id INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL, -- 'login', 'logout', 'failed_login', 'permission_change'
    username TEXT,
    database_name TEXT,
    client_address TEXT,
    success BOOLEAN,
    details TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Default superuser
INSERT INTO users (username, password_hash, auth_method, is_superuser) 
VALUES ('postgres', '', 'trust', 1);
```

### 2. Authentication Manager Component

```rust
// src/auth/manager.rs
pub struct AuthManager {
    auth_db: Arc<Mutex<Connection>>,
    config: Arc<AuthConfig>,
    cache: Arc<RwLock<HashMap<String, CachedUser>>>,
}

#[derive(Clone)]
struct CachedUser {
    id: i64,
    username: String,
    password_hash: String,
    auth_method: AuthMethod,
    salt: Option<Vec<u8>>,
    iterations: Option<u32>,
    is_superuser: bool,
    permissions: HashMap<String, DatabasePermissions>,
    cached_at: Instant,
}

#[derive(Clone)]
struct DatabasePermissions {
    can_connect: bool,
    can_create_table: bool,
    can_drop_table: bool,
}

impl AuthManager {
    pub fn new(data_dir: &Path) -> Result<Self, Error> {
        let auth_db_path = data_dir.join("_pgsqlite_auth.db");
        let conn = Connection::open(&auth_db_path)?;
        
        // Initialize schema if not exists
        Self::initialize_schema(&conn)?;
        
        Ok(Self {
            auth_db: Arc::new(Mutex::new(conn)),
            config: Arc::new(AuthConfig::default()),
            cache: Arc::new(RwLock::new(HashMap::new())),
        })
    }
    
    pub async fn authenticate(&self, username: &str, password: &str, method: AuthMethod) 
        -> Result<AuthResult, Error> {
        // Check cache first
        if let Some(cached) = self.get_cached_user(username).await {
            return self.verify_password(&cached, password, method);
        }
        
        // Load from database
        let user = self.load_user(username).await?;
        self.cache_user(user.clone()).await;
        
        self.verify_password(&user, password, method)
    }
    
    pub async fn check_database_access(&self, username: &str, database: &str) 
        -> Result<bool, Error> {
        let user = self.get_user(username).await?;
        
        // Superusers have access to all databases
        if user.is_superuser {
            return Ok(true);
        }
        
        // Check specific permissions
        Ok(user.permissions.get(database)
            .map(|p| p.can_connect)
            .unwrap_or(false))
    }
}
```

### 3. PostgreSQL Authentication Protocol Integration

Enhance the connection handler to support PostgreSQL authentication flow:

```rust
// src/protocol/auth.rs
pub enum AuthMethod {
    Trust,      // No password required
    Password,   // Clear text (not recommended)
    Md5,        // MD5 hash
    ScramSha256, // SCRAM-SHA-256
}

pub struct AuthenticationHandler {
    auth_manager: Arc<AuthManager>,
}

impl AuthenticationHandler {
    pub async fn handle_startup(&self, params: StartupParams, writer: &mut FramedWrite) 
        -> Result<SessionState, Error> {
        let username = params.get("user")
            .ok_or("User parameter required")?;
        let database = params.get("database")
            .unwrap_or("postgres");
        
        // Send authentication request based on configured method
        let auth_method = self.auth_manager.get_user_auth_method(username).await?;
        
        match auth_method {
            AuthMethod::Trust => {
                // No authentication needed
                writer.send(BackendMessage::AuthenticationOk).await?;
            }
            AuthMethod::Md5 => {
                let salt = generate_random_salt();
                writer.send(BackendMessage::AuthenticationMD5Password { salt }).await?;
                
                // Wait for password response
                let password_msg = read_password_message().await?;
                self.verify_md5_password(username, password_msg, salt).await?;
            }
            AuthMethod::ScramSha256 => {
                // Implement SCRAM-SHA-256 flow
                self.handle_scram_auth(username, writer).await?;
            }
            _ => return Err("Unsupported auth method"),
        }
        
        // Check database access
        if !self.auth_manager.check_database_access(username, database).await? {
            return Err(ErrorResponse {
                severity: "FATAL",
                code: "42501",
                message: format!("User {} does not have CONNECT privilege for database {}", 
                    username, database),
            });
        }
        
        // Create session state
        Ok(SessionState {
            username: username.to_string(),
            current_database: database.to_string(),
            is_authenticated: true,
            // ... other fields
        })
    }
}
```

### 4. SQL Command Support

Implement PostgreSQL-compatible user management commands:

```rust
// src/auth/sql_commands.rs
pub struct AuthCommandHandler {
    auth_manager: Arc<AuthManager>,
}

impl AuthCommandHandler {
    pub async fn handle_command(&self, query: &str, session: &SessionState) 
        -> Option<Result<DbResponse, Error>> {
        if let Some(command) = parse_auth_command(query) {
            match command {
                AuthCommand::CreateUser { username, password, options } => {
                    self.handle_create_user(username, password, options, session).await
                }
                AuthCommand::AlterUser { username, changes } => {
                    self.handle_alter_user(username, changes, session).await
                }
                AuthCommand::DropUser { username } => {
                    self.handle_drop_user(username, session).await
                }
                AuthCommand::Grant { privilege, database, username } => {
                    self.handle_grant(privilege, database, username, session).await
                }
                AuthCommand::Revoke { privilege, database, username } => {
                    self.handle_revoke(privilege, database, username, session).await
                }
            }
        } else {
            None
        }
    }
}
```

### 5. Password Hashing Implementation

```rust
// src/auth/password.rs
use sha2::{Sha256, Digest};
use md5::Md5;

pub fn hash_password_md5(username: &str, password: &str, salt: &[u8]) -> String {
    // PostgreSQL MD5 format: "md5" + md5(password + username)
    let mut hasher = Md5::new();
    hasher.update(password.as_bytes());
    hasher.update(username.as_bytes());
    let result = hasher.finalize();
    
    // Now hash with salt for wire protocol
    let mut hasher2 = Md5::new();
    hasher2.update(&format!("{:x}", result));
    hasher2.update(salt);
    format!("md5{:x}", hasher2.finalize())
}

pub fn hash_password_scram(password: &str, salt: &[u8], iterations: u32) 
    -> Result<ScramCredentials, Error> {
    // Implement SCRAM-SHA-256 according to RFC 5802
    let salted_password = pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations);
    let client_key = hmac_sha256(&salted_password, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted_password, b"Server Key");
    
    Ok(ScramCredentials {
        stored_key,
        server_key,
        salt: salt.to_vec(),
        iterations,
    })
}
```

## SQL Commands Support

### User Management
```sql
-- Create user
CREATE USER username WITH PASSWORD 'password';
CREATE USER username WITH ENCRYPTED PASSWORD 'md5hash';

-- Alter user
ALTER USER username WITH PASSWORD 'newpassword';
ALTER USER username WITH SUPERUSER;
ALTER USER username WITH NOSUPERUSER;

-- Drop user
DROP USER username;
DROP USER IF EXISTS username;

-- List users
\du
SELECT * FROM pg_user;
SELECT * FROM pg_roles;
```

### Permission Management
```sql
-- Grant database access
GRANT CONNECT ON DATABASE dbname TO username;
GRANT ALL ON DATABASE dbname TO username;

-- Revoke database access  
REVOKE CONNECT ON DATABASE dbname FROM username;
REVOKE ALL ON DATABASE dbname FROM username;

-- Check permissions
\l  -- List databases with access info
SELECT has_database_privilege('username', 'dbname', 'CONNECT');
```

## Configuration

### New Configuration Options

```toml
# pgsqlite.toml
[auth]
# Enable authentication
enabled = false

# Default authentication method
default_method = "scram-sha-256"  # or "md5", "trust"

# Password encryption iterations (for SCRAM)
scram_iterations = 4096

# Session timeout (minutes)
session_timeout = 60

# Maximum login attempts before lockout
max_login_attempts = 5

# Lockout duration (minutes)
lockout_duration = 15

# Cache user credentials (minutes)
user_cache_ttl = 5

# Audit logging
enable_audit_log = true
audit_log_retention_days = 30
```

### Command-Line Arguments

```bash
# Enable auth with multi-db
pgsqlite --multi-db --auth

# With custom auth database location
pgsqlite --multi-db --auth --auth-db ./auth/users.db

# With specific auth method
pgsqlite --multi-db --auth --auth-method scram-sha-256

# Environment variables
PGSQLITE_AUTH=true PGSQLITE_AUTH_METHOD=md5 pgsqlite --multi-db
```

## Implementation Phases

### Phase 1: Foundation (Week 1)
1. Create auth database schema
2. Implement AuthManager component
3. Add configuration options
4. Basic user CRUD operations

### Phase 2: Authentication Protocol (Week 2)
1. Integrate with PostgreSQL protocol handler
2. Implement MD5 authentication
3. Add password verification
4. Session state management

### Phase 3: SCRAM-SHA-256 (Week 3)
1. Implement SCRAM-SHA-256 protocol
2. Add password hashing functions
3. Handle authentication flow
4. Test with PostgreSQL clients

### Phase 4: Permission System (Week 4)
1. Database-level permissions
2. GRANT/REVOKE commands
3. Permission checking middleware
4. Superuser privileges

### Phase 5: SQL Commands & Compatibility (Week 5)
1. CREATE/ALTER/DROP USER commands
2. System catalogs (pg_user, pg_roles)
3. psql meta-commands (\du, \l)
4. Audit logging

### Phase 6: Testing & Security (Week 6)
1. Security audit
2. Performance testing
3. Client compatibility testing
4. Documentation

## Security Considerations

### Password Storage
- Never store plain text passwords
- Use strong hashing (SCRAM-SHA-256 preferred)
- Salt all password hashes
- Implement password complexity requirements

### Connection Security
- Require SSL/TLS for auth mode
- Implement rate limiting for login attempts
- Account lockout after failed attempts
- Audit all authentication events

### Permission Model
- Principle of least privilege
- Default deny for new users
- Superuser access limited
- Regular permission audits

## Migration Path

### From No-Auth to Auth Mode

1. Start pgsqlite with `--multi-db --auth --init-superuser`
2. Set initial superuser password
3. Create additional users as needed
4. Grant database permissions
5. Update client connection strings

### Backward Compatibility

- Without `--auth`, no authentication required
- Existing deployments unaffected
- Can enable auth without data migration

## Testing Strategy

### Unit Tests
- Password hashing algorithms
- Permission checking logic
- User CRUD operations
- Cache behavior

### Integration Tests
- Full authentication flow
- Multiple auth methods
- Permission enforcement
- Session management

### Security Tests
- Password brute force protection
- SQL injection in auth queries
- Permission escalation attempts
- Session hijacking prevention

### Client Compatibility Tests
- psql authentication
- Various PostgreSQL drivers
- Connection poolers
- ORMs and frameworks

## Performance Considerations

### Caching Strategy
- Cache authenticated users
- Cache permission checks
- TTL-based expiration
- LRU eviction policy

### Auth Database Optimization
- Indexes on username
- Prepared statements
- Connection pooling
- Async operations

## Future Enhancements

1. **LDAP/AD Integration** - External authentication providers
2. **OAuth2/SAML** - Modern authentication protocols  
3. **Row-Level Security** - Fine-grained permissions
4. **Column-Level Permissions** - Restrict column access
5. **Connection Limits** - Per-user connection quotas
6. **Password Policies** - Expiration, history, complexity
7. **Two-Factor Authentication** - TOTP/hardware tokens
8. **Certificate Authentication** - Client certificates

## Success Criteria

1. PostgreSQL client compatibility
2. Secure password storage
3. Fast authentication (<10ms)
4. Zero impact when disabled
5. Clear audit trail
6. Easy user management