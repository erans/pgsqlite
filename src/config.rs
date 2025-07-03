use clap::Parser;
use std::env;

#[derive(Parser, Debug, Clone)]
#[command(name = "pgsqlite")]
#[command(about = "pgsqlite - 🐘 PostgreSQL + 🪶 SQLite = ♥\nPostgreSQL wire protocol server on top of SQLite", long_about = None)]
pub struct Config {
    // Basic configuration
    #[arg(short, long, default_value = "5432", env = "PGSQLITE_PORT")]
    pub port: u16,

    #[arg(short, long, default_value = "sqlite.db", env = "PGSQLITE_DATABASE")]
    pub database: String,

    #[arg(long, default_value = "info", env = "PGSQLITE_LOG_LEVEL")]
    pub log_level: String,

    #[arg(long, env = "PGSQLITE_IN_MEMORY", help = "Use in-memory SQLite database (for testing/benchmarking only)")]
    pub in_memory: bool,

    #[arg(long, default_value = "/tmp", env = "PGSQLITE_SOCKET_DIR", help = "Directory for Unix domain socket")]
    pub socket_dir: String,

    #[arg(long, env = "PGSQLITE_NO_TCP", help = "Disable TCP listener and use only Unix socket")]
    pub no_tcp: bool,

    // Cache configuration
    #[arg(long, default_value = "1000", env = "PGSQLITE_ROW_DESC_CACHE_SIZE", help = "Maximum number of RowDescription entries to cache")]
    pub row_desc_cache_size: usize,

    #[arg(long, default_value = "10", env = "PGSQLITE_ROW_DESC_CACHE_TTL_MINUTES", help = "TTL for RowDescription cache entries in minutes")]
    pub row_desc_cache_ttl: u64,

    #[arg(long, default_value = "500", env = "PGSQLITE_PARAM_CACHE_SIZE", help = "Maximum number of parameter type entries to cache")]
    pub param_cache_size: usize,

    #[arg(long, default_value = "30", env = "PGSQLITE_PARAM_CACHE_TTL_MINUTES", help = "TTL for parameter cache entries in minutes")]
    pub param_cache_ttl: u64,

    #[arg(long, default_value = "1000", env = "PGSQLITE_QUERY_CACHE_SIZE", help = "Maximum number of query plan entries to cache")]
    pub query_cache_size: usize,

    #[arg(long, default_value = "600", env = "PGSQLITE_QUERY_CACHE_TTL", help = "TTL for query cache entries in seconds")]
    pub query_cache_ttl: u64,

    #[arg(long, default_value = "300", env = "PGSQLITE_EXECUTION_CACHE_TTL", help = "TTL for execution metadata cache in seconds")]
    pub execution_cache_ttl: u64,

    #[arg(long, default_value = "100", env = "PGSQLITE_RESULT_CACHE_SIZE", help = "Maximum number of result set entries to cache")]
    pub result_cache_size: usize,

    #[arg(long, default_value = "60", env = "PGSQLITE_RESULT_CACHE_TTL", help = "TTL for result cache entries in seconds")]
    pub result_cache_ttl: u64,

    #[arg(long, default_value = "100", env = "PGSQLITE_STATEMENT_POOL_SIZE", help = "Maximum number of prepared statements to cache")]
    pub statement_pool_size: usize,

    #[arg(long, default_value = "300", env = "PGSQLITE_CACHE_METRICS_INTERVAL", help = "Interval for logging cache metrics in seconds")]
    pub cache_metrics_interval: u64,

    #[arg(long, default_value = "300", env = "PGSQLITE_SCHEMA_CACHE_TTL", help = "TTL for schema cache entries in seconds")]
    pub schema_cache_ttl: u64,

    // Buffer pool configuration
    #[arg(long, env = "PGSQLITE_BUFFER_MONITORING", help = "Enable buffer pool monitoring and statistics")]
    pub buffer_monitoring: bool,

    #[arg(long, default_value = "50", env = "PGSQLITE_BUFFER_POOL_SIZE", help = "Maximum number of buffers to keep in the pool")]
    pub buffer_pool_size: usize,

    #[arg(long, default_value = "4096", env = "PGSQLITE_BUFFER_INITIAL_CAPACITY", help = "Initial capacity for new buffers in bytes")]
    pub buffer_initial_capacity: usize,

    #[arg(long, default_value = "65536", env = "PGSQLITE_BUFFER_MAX_CAPACITY", help = "Maximum capacity a buffer can grow to before being discarded")]
    pub buffer_max_capacity: usize,

    // Memory monitor configuration
    #[arg(long, env = "PGSQLITE_AUTO_CLEANUP", help = "Enable automatic memory pressure response")]
    pub auto_cleanup: bool,

    #[arg(long, env = "PGSQLITE_MEMORY_MONITORING", help = "Enable detailed memory monitoring")]
    pub memory_monitoring: bool,

    #[arg(long, default_value = "67108864", env = "PGSQLITE_MEMORY_THRESHOLD", help = "Memory threshold in bytes before triggering cleanup (default: 64MB)")]
    pub memory_threshold: usize,

    #[arg(long, default_value = "134217728", env = "PGSQLITE_HIGH_MEMORY_THRESHOLD", help = "High memory threshold for aggressive cleanup (default: 128MB)")]
    pub high_memory_threshold: usize,

    #[arg(long, default_value = "10", env = "PGSQLITE_MEMORY_CHECK_INTERVAL", help = "Interval for memory usage checks in seconds")]
    pub memory_check_interval: u64,

    // Memory mapping configuration
    #[arg(long, env = "PGSQLITE_ENABLE_MMAP", help = "Enable memory mapping optimization for large values")]
    pub enable_mmap: bool,

    #[arg(long, default_value = "65536", env = "PGSQLITE_MMAP_MIN_SIZE", help = "Minimum size in bytes to use memory mapping (default: 64KB)")]
    pub mmap_min_size: usize,

    #[arg(long, default_value = "1048576", env = "PGSQLITE_MMAP_MAX_MEMORY", help = "Maximum size for in-memory values before using temp files (default: 1MB)")]
    pub mmap_max_memory: usize,

    #[arg(long, env = "PGSQLITE_TEMP_DIR", help = "Directory for temporary files used by memory mapping")]
    pub temp_dir: Option<String>,

    // SQLite PRAGMA settings
    #[arg(long, default_value = "WAL", env = "PGSQLITE_JOURNAL_MODE", help = "SQLite journal mode (WAL, DELETE, TRUNCATE, etc.)")]
    pub pragma_journal_mode: String,

    #[arg(long, default_value = "NORMAL", env = "PGSQLITE_SYNCHRONOUS", help = "SQLite synchronous mode (NORMAL, FULL, OFF)")]
    pub pragma_synchronous: String,

    #[arg(long, default_value = "-64000", env = "PGSQLITE_CACHE_SIZE", help = "SQLite page cache size in KB (negative for KB, positive for pages)")]
    pub pragma_cache_size: i32,

    #[arg(long, default_value = "268435456", env = "PGSQLITE_MMAP_SIZE", help = "SQLite memory-mapped I/O size in bytes")]
    pub pragma_mmap_size: u64,
}

impl Config {
    /// Get a configuration instance with all values resolved from CLI args and environment variables
    pub fn load() -> Self {
        Config::parse()
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
        self.temp_dir.clone().unwrap_or_else(|| {
            env::temp_dir().to_string_lossy().to_string()
        })
    }
}

// Global configuration instance
lazy_static::lazy_static! {
    pub static ref CONFIG: Config = Config::load();
}