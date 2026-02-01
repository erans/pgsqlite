use clap::Parser;
use std::env;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug, Clone)]
#[command(name = "pgsqlite")]
#[command(about = concat!("pgsqlite v", env!("CARGO_PKG_VERSION"), " - 🐘 PostgreSQL + 🪶 SQLite = ♥\nPostgreSQL wire protocol server on top of SQLite"), long_about = None)]
#[command(version)]
pub struct Config {
    // Basic configuration
    #[arg(short, long, default_value = "5432", env = "PGSQLITE_PORT")]
    pub port: u16,

    #[arg(
        short,
        long,
        default_value = "./data",
        env = "PGSQLITE_DATABASE",
        help = "Database data directory (recommended) or a single .db file path (legacy)"
    )]
    pub database: String,

    #[arg(
        long,
        default_value = "main",
        env = "PGSQLITE_DEFAULT_DATABASE",
        help = "Default database name used when client doesn't specify one"
    )]
    pub default_database: String,

    #[arg(long, default_value = "info", env = "PGSQLITE_LOG_LEVEL")]
    pub log_level: String,

    #[arg(
        long,
        env = "PGSQLITE_IN_MEMORY",
        help = "Use in-memory SQLite database (for testing/benchmarking only)"
    )]
    pub in_memory: bool,

    #[arg(
        long,
        default_value = "/tmp",
        env = "PGSQLITE_SOCKET_DIR",
        help = "Directory for Unix domain socket"
    )]
    pub socket_dir: String,

    #[arg(
        long,
        env = "PGSQLITE_NO_TCP",
        help = "Disable TCP listener and use only Unix socket"
    )]
    pub no_tcp: bool,

    // Connection pool configuration
    #[arg(
        long,
        env = "PGSQLITE_USE_POOLING",
        help = "Enable connection pooling with read/write separation"
    )]
    pub use_pooling: bool,

    #[arg(
        long,
        default_value = "100",
        env = "PGSQLITE_MAX_CONNECTIONS",
        help = "Maximum number of concurrent connections allowed"
    )]
    pub max_connections: usize,

    #[arg(
        long,
        default_value = "8",
        env = "PGSQLITE_POOL_SIZE",
        help = "Number of connections in the read-only connection pool"
    )]
    pub pool_size: usize,

    #[arg(
        long,
        default_value = "30",
        env = "PGSQLITE_POOL_CONNECTION_TIMEOUT_SECONDS",
        help = "Timeout for getting a connection from the pool"
    )]
    pub pool_connection_timeout_seconds: u64,

    #[arg(
        long,
        default_value = "300",
        env = "PGSQLITE_POOL_IDLE_TIMEOUT_SECONDS",
        help = "Timeout for idle connections in the pool"
    )]
    pub pool_idle_timeout_seconds: u64,

    #[arg(
        long,
        default_value = "60",
        env = "PGSQLITE_POOL_HEALTH_CHECK_INTERVAL_SECONDS",
        help = "Interval for connection health checks"
    )]
    pub pool_health_check_interval_seconds: u64,

    #[arg(
        long,
        default_value = "3",
        env = "PGSQLITE_POOL_MAX_RETRIES",
        help = "Maximum number of retries for failed connections"
    )]
    pub pool_max_retries: usize,

    // Cache configuration
    #[arg(
        long,
        default_value = "1000",
        env = "PGSQLITE_ROW_DESC_CACHE_SIZE",
        help = "Maximum number of RowDescription entries to cache"
    )]
    pub row_desc_cache_size: usize,

    #[arg(
        long,
        default_value = "10",
        env = "PGSQLITE_ROW_DESC_CACHE_TTL_MINUTES",
        help = "TTL for RowDescription cache entries in minutes"
    )]
    pub row_desc_cache_ttl: u64,

    #[arg(
        long,
        default_value = "500",
        env = "PGSQLITE_PARAM_CACHE_SIZE",
        help = "Maximum number of parameter type entries to cache"
    )]
    pub param_cache_size: usize,

    #[arg(
        long,
        default_value = "30",
        env = "PGSQLITE_PARAM_CACHE_TTL_MINUTES",
        help = "TTL for parameter cache entries in minutes"
    )]
    pub param_cache_ttl: u64,

    #[arg(
        long,
        default_value = "1000",
        env = "PGSQLITE_QUERY_CACHE_SIZE",
        help = "Maximum number of query plan entries to cache"
    )]
    pub query_cache_size: usize,

    #[arg(
        long,
        default_value = "600",
        env = "PGSQLITE_QUERY_CACHE_TTL",
        help = "TTL for query cache entries in seconds"
    )]
    pub query_cache_ttl: u64,

    #[arg(
        long,
        default_value = "300",
        env = "PGSQLITE_EXECUTION_CACHE_TTL",
        help = "TTL for execution metadata cache in seconds"
    )]
    pub execution_cache_ttl: u64,

    #[arg(
        long,
        default_value = "100",
        env = "PGSQLITE_RESULT_CACHE_SIZE",
        help = "Maximum number of result set entries to cache"
    )]
    pub result_cache_size: usize,

    #[arg(
        long,
        default_value = "60",
        env = "PGSQLITE_RESULT_CACHE_TTL",
        help = "TTL for result cache entries in seconds"
    )]
    pub result_cache_ttl: u64,

    #[arg(
        long,
        default_value = "100",
        env = "PGSQLITE_STATEMENT_POOL_SIZE",
        help = "Maximum number of prepared statements to cache"
    )]
    pub statement_pool_size: usize,

    #[arg(
        long,
        default_value = "300",
        env = "PGSQLITE_CACHE_METRICS_INTERVAL",
        help = "Interval for logging cache metrics in seconds"
    )]
    pub cache_metrics_interval: u64,

    #[arg(
        long,
        default_value = "300",
        env = "PGSQLITE_SCHEMA_CACHE_TTL",
        help = "TTL for schema cache entries in seconds"
    )]
    pub schema_cache_ttl: u64,

    // Buffer pool configuration
    #[arg(
        long,
        env = "PGSQLITE_BUFFER_MONITORING",
        help = "Enable buffer pool monitoring and statistics"
    )]
    pub buffer_monitoring: bool,

    #[arg(
        long,
        default_value = "50",
        env = "PGSQLITE_BUFFER_POOL_SIZE",
        help = "Maximum number of buffers to keep in the pool"
    )]
    pub buffer_pool_size: usize,

    #[arg(
        long,
        default_value = "4096",
        env = "PGSQLITE_BUFFER_INITIAL_CAPACITY",
        help = "Initial capacity for new buffers in bytes"
    )]
    pub buffer_initial_capacity: usize,

    #[arg(
        long,
        default_value = "65536",
        env = "PGSQLITE_BUFFER_MAX_CAPACITY",
        help = "Maximum capacity a buffer can grow to before being discarded"
    )]
    pub buffer_max_capacity: usize,

    // Memory monitor configuration
    #[arg(
        long,
        env = "PGSQLITE_AUTO_CLEANUP",
        help = "Enable automatic memory pressure response"
    )]
    pub auto_cleanup: bool,

    #[arg(
        long,
        env = "PGSQLITE_MEMORY_MONITORING",
        help = "Enable detailed memory monitoring"
    )]
    pub memory_monitoring: bool,

    #[arg(
        long,
        default_value = "67108864",
        env = "PGSQLITE_MEMORY_THRESHOLD",
        help = "Memory threshold in bytes before triggering cleanup (default: 64MB)"
    )]
    pub memory_threshold: usize,

    #[arg(
        long,
        default_value = "134217728",
        env = "PGSQLITE_HIGH_MEMORY_THRESHOLD",
        help = "High memory threshold for aggressive cleanup (default: 128MB)"
    )]
    pub high_memory_threshold: usize,

    #[arg(
        long,
        default_value = "10",
        env = "PGSQLITE_MEMORY_CHECK_INTERVAL",
        help = "Interval for memory usage checks in seconds"
    )]
    pub memory_check_interval: u64,

    // Memory mapping configuration
    #[arg(
        long,
        env = "PGSQLITE_ENABLE_MMAP",
        help = "Enable memory mapping optimization for large values"
    )]
    pub enable_mmap: bool,

    #[arg(
        long,
        default_value = "65536",
        env = "PGSQLITE_MMAP_MIN_SIZE",
        help = "Minimum size in bytes to use memory mapping (default: 64KB)"
    )]
    pub mmap_min_size: usize,

    #[arg(
        long,
        default_value = "1048576",
        env = "PGSQLITE_MMAP_MAX_MEMORY",
        help = "Maximum size for in-memory values before using temp files (default: 1MB)"
    )]
    pub mmap_max_memory: usize,

    #[arg(
        long,
        env = "PGSQLITE_TEMP_DIR",
        help = "Directory for temporary files used by memory mapping"
    )]
    pub temp_dir: Option<String>,

    // SQLite PRAGMA settings
    #[arg(
        long,
        default_value = "WAL",
        env = "PGSQLITE_JOURNAL_MODE",
        help = "SQLite journal mode (WAL, DELETE, TRUNCATE, etc.)"
    )]
    pub pragma_journal_mode: String,

    #[arg(
        long,
        default_value = "NORMAL",
        env = "PGSQLITE_SYNCHRONOUS",
        help = "SQLite synchronous mode (NORMAL, FULL, OFF)"
    )]
    pub pragma_synchronous: String,

    #[arg(
        long,
        default_value = "-64000",
        env = "PGSQLITE_CACHE_SIZE",
        help = "SQLite page cache size in KB (negative for KB, positive for pages)"
    )]
    pub pragma_cache_size: i32,

    #[arg(
        long,
        default_value = "268435456",
        env = "PGSQLITE_MMAP_SIZE",
        help = "SQLite memory-mapped I/O size in bytes"
    )]
    pub pragma_mmap_size: u64,

    // SSL/TLS configuration
    #[arg(long, env = "PGSQLITE_SSL", help = "Enable SSL/TLS support")]
    pub ssl: bool,

    #[arg(long, env = "PGSQLITE_SSL_CERT", help = "Path to SSL certificate file")]
    pub ssl_cert: Option<String>,

    #[arg(long, env = "PGSQLITE_SSL_KEY", help = "Path to SSL private key file")]
    pub ssl_key: Option<String>,

    #[arg(long, env = "PGSQLITE_SSL_CA", help = "Path to CA certificate file")]
    pub ssl_ca: Option<String>,

    #[arg(
        long,
        env = "PGSQLITE_SSL_EPHEMERAL",
        help = "Generate ephemeral SSL certificates on startup"
    )]
    pub ssl_ephemeral: bool,

    // Migration configuration
    #[arg(long, help = "Run pending database migrations and exit")]
    pub migrate: bool,
}

impl Config {
    /// Get a configuration instance with all values resolved from CLI args and environment variables
    pub fn load() -> Self {
        let config = Config::parse();

        // Validate database path
        if !config.in_memory && config.database != ":memory:" {
            let p = Path::new(&config.database);
            if p.exists() && !(p.is_dir() || p.is_file()) {
                eprintln!(
                    "Error: PGSQLITE_DATABASE/--database must be a directory or a file path (got: {})",
                    config.database
                );
                std::process::exit(1);
            }
        }

        // Validate SSL configuration
        if config.ssl && config.no_tcp {
            eprintln!("Error: SSL cannot be enabled when TCP is disabled (Unix sockets don't support SSL)");
            std::process::exit(1);
        }

        config
    }

    pub fn database_layout(&self) -> DatabaseLayout {
        if self.in_memory || self.database == ":memory:" {
            return DatabaseLayout::InMemory;
        }

        let p = PathBuf::from(&self.database);
        if p.exists() {
            if p.is_dir() {
                return DatabaseLayout::Directory { dir: p };
            }
            if p.is_file() {
                return DatabaseLayout::File { path: p };
            }
        }

        // Non-existent path: infer based on extension.
        if looks_like_db_file_path(&p) {
            DatabaseLayout::File { path: p }
        } else {
            DatabaseLayout::Directory { dir: p }
        }
    }

    pub fn resolve_db_file_path(&self, database_name: &str) -> Option<PathBuf> {
        match self.database_layout() {
            DatabaseLayout::InMemory => None,
            DatabaseLayout::File { path } => Some(path),
            DatabaseLayout::Directory { dir } => {
                if !is_valid_db_identifier(database_name) {
                    return None;
                }
                Some(dir.join(format!("{database_name}.db")))
            }
        }
    }

    pub fn data_dir(&self) -> Option<PathBuf> {
        match self.database_layout() {
            DatabaseLayout::InMemory => None,
            DatabaseLayout::Directory { dir } => Some(dir),
            DatabaseLayout::File { path } => Some(
                path.parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| PathBuf::from(".")),
            ),
        }
    }

    pub fn cert_stem(&self) -> String {
        match self.database_layout() {
            DatabaseLayout::InMemory => self.default_database.clone(),
            DatabaseLayout::Directory { .. } => self.default_database.clone(),
            DatabaseLayout::File { path } => path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or(self.default_database.as_str())
                .to_string(),
        }
    }

    /// Get the cache metrics interval as Duration
    pub fn cache_metrics_interval_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.cache_metrics_interval)
    }

    /// Get the memory check interval as Duration
    pub fn memory_check_interval_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.memory_check_interval)
    }

    /// Get the row description cache TTL as Duration
    pub fn row_desc_cache_ttl_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.row_desc_cache_ttl * 60)
    }

    /// Get the parameter cache TTL as Duration
    pub fn param_cache_ttl_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.param_cache_ttl * 60)
    }

    /// Get the query cache TTL as Duration
    pub fn query_cache_ttl_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.query_cache_ttl)
    }

    /// Get the result cache TTL as Duration
    pub fn result_cache_ttl_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.result_cache_ttl)
    }

    /// Get the schema cache TTL as Duration
    pub fn schema_cache_ttl_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.schema_cache_ttl)
    }

    /// Get the temp directory, defaulting to system temp if not specified
    pub fn get_temp_dir(&self) -> String {
        self.temp_dir
            .clone()
            .unwrap_or_else(|| env::temp_dir().to_string_lossy().to_string())
    }
}

#[derive(Debug, Clone)]
pub enum DatabaseLayout {
    InMemory,
    File { path: PathBuf },
    Directory { dir: PathBuf },
}

pub fn is_valid_db_identifier(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

fn looks_like_db_file_path(p: &Path) -> bool {
    let ext = p.extension().and_then(|s| s.to_str()).unwrap_or("");
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "db" | "sqlite" | "sqlite3"
    )
}

// Global configuration instance
lazy_static::lazy_static! {
    pub static ref CONFIG: Config = Config::load();
}
