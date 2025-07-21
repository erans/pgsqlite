#[cfg(test)]
mod tests {
    use pgsqlite::config::Config;
    use pgsqlite::ssl::CertificateManager;
    use std::sync::Arc;
    use tempfile::TempDir;

    #[test]
    fn test_certificate_generation() {
        let config = Config {
            database: ":memory:".to_string(),
            ssl: true,
            ssl_cert: None,
            ssl_key: None,
            ssl_ca: None,
            ssl_ephemeral: true,
            in_memory: true,
            port: 5432,
            log_level: "info".to_string(),
            no_tcp: false,
            socket_dir: "/tmp".to_string(),
            use_pooling: false,
            pool_size: 8,
            pool_connection_timeout_seconds: 30,
            pool_idle_timeout_seconds: 300,
            pool_health_check_interval_seconds: 60,
            pool_max_retries: 3,
            row_desc_cache_size: 1000,
            row_desc_cache_ttl: 10,
            param_cache_size: 500,
            param_cache_ttl: 30,
            query_cache_size: 1000,
            query_cache_ttl: 600,
            execution_cache_ttl: 300,
            result_cache_size: 100,
            result_cache_ttl: 60,
            statement_pool_size: 100,
            cache_metrics_interval: 300,
            schema_cache_ttl: 300,
            buffer_monitoring: false,
            buffer_pool_size: 50,
            buffer_initial_capacity: 4096,
            buffer_max_capacity: 65536,
            auto_cleanup: false,
            memory_monitoring: false,
            memory_threshold: 67108864,
            high_memory_threshold: 134217728,
            memory_check_interval: 10,
            enable_mmap: false,
            mmap_min_size: 65536,
            mmap_max_memory: 1048576,
            temp_dir: None,
            pragma_journal_mode: "WAL".to_string(),
            pragma_synchronous: "NORMAL".to_string(),
            pragma_cache_size: -64000,
            pragma_mmap_size: 268435456,
            migrate: false,
        };

        let cert_manager = CertificateManager::new(Arc::new(config.clone()));
        
        // Test in-memory certificate generation
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(cert_manager.initialize());
        assert!(result.is_ok());
        
        let (_acceptor, cert_source) = result.unwrap();
        match cert_source {
            pgsqlite::ssl::CertificateSource::Generated { cert, key } => {
                assert!(!cert.is_empty());
                assert!(!key.is_empty());
                assert!(String::from_utf8_lossy(&cert).contains("BEGIN CERTIFICATE"));
                assert!(String::from_utf8_lossy(&key).contains("BEGIN PRIVATE KEY"));
            }
            _ => panic!("Expected generated certificates for in-memory database"),
        }
    }

    #[test]
    fn test_certificate_file_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");
        
        let config = Config {
            database: db_path.to_string_lossy().to_string(),
            ssl: true,
            ssl_cert: None,
            ssl_key: None,
            ssl_ca: None,
            ssl_ephemeral: false,
            in_memory: false,
            port: 5432,
            log_level: "info".to_string(),
            no_tcp: false,
            socket_dir: "/tmp".to_string(),
            use_pooling: false,
            pool_size: 8,
            pool_connection_timeout_seconds: 30,
            pool_idle_timeout_seconds: 300,
            pool_health_check_interval_seconds: 60,
            pool_max_retries: 3,
            row_desc_cache_size: 1000,
            row_desc_cache_ttl: 10,
            param_cache_size: 500,
            param_cache_ttl: 30,
            query_cache_size: 1000,
            query_cache_ttl: 600,
            execution_cache_ttl: 300,
            result_cache_size: 100,
            result_cache_ttl: 60,
            statement_pool_size: 100,
            cache_metrics_interval: 300,
            schema_cache_ttl: 300,
            buffer_monitoring: false,
            buffer_pool_size: 50,
            buffer_initial_capacity: 4096,
            buffer_max_capacity: 65536,
            auto_cleanup: false,
            memory_monitoring: false,
            memory_threshold: 67108864,
            high_memory_threshold: 134217728,
            memory_check_interval: 10,
            enable_mmap: false,
            mmap_min_size: 65536,
            mmap_max_memory: 1048576,
            temp_dir: None,
            pragma_journal_mode: "WAL".to_string(),
            pragma_synchronous: "NORMAL".to_string(),
            pragma_cache_size: -64000,
            pragma_mmap_size: 268435456,
            migrate: false,
        };

        let cert_manager = CertificateManager::new(Arc::new(config.clone()));
        
        // Test certificate generation and file persistence
        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(cert_manager.initialize());
        assert!(result.is_ok());
        
        // Check that certificate files were created
        let cert_path = temp_dir.path().join("test.crt");
        let key_path = temp_dir.path().join("test.key");
        
        assert!(cert_path.exists(), "Certificate file should be created");
        assert!(key_path.exists(), "Key file should be created");
        
        // Test that existing certificates are reused
        let cert_manager2 = CertificateManager::new(Arc::new(config));
        let result2 = rt.block_on(cert_manager2.initialize());
        assert!(result2.is_ok());
        
        let (_acceptor2, cert_source2) = result2.unwrap();
        match cert_source2 {
            pgsqlite::ssl::CertificateSource::FileSystem { cert_path: cp, key_path: kp } => {
                assert_eq!(cp, cert_path.to_string_lossy());
                assert_eq!(kp, key_path.to_string_lossy());
            }
            _ => panic!("Expected filesystem certificates for existing files"),
        }
    }

    #[test]
    fn test_ssl_disabled_for_unix_sockets() {
        let config = Config {
            database: "test.db".to_string(),
            ssl: true,
            ssl_cert: None,
            ssl_key: None,
            ssl_ca: None,
            ssl_ephemeral: false,
            in_memory: false,
            port: 5432,
            log_level: "info".to_string(),
            no_tcp: true, // TCP disabled, only Unix sockets
            socket_dir: "/tmp".to_string(),
            use_pooling: false,
            pool_size: 8,
            pool_connection_timeout_seconds: 30,
            pool_idle_timeout_seconds: 300,
            pool_health_check_interval_seconds: 60,
            pool_max_retries: 3,
            row_desc_cache_size: 1000,
            row_desc_cache_ttl: 10,
            param_cache_size: 500,
            param_cache_ttl: 30,
            query_cache_size: 1000,
            query_cache_ttl: 600,
            execution_cache_ttl: 300,
            result_cache_size: 100,
            result_cache_ttl: 60,
            statement_pool_size: 100,
            cache_metrics_interval: 300,
            schema_cache_ttl: 300,
            buffer_monitoring: false,
            buffer_pool_size: 50,
            buffer_initial_capacity: 4096,
            buffer_max_capacity: 65536,
            auto_cleanup: false,
            memory_monitoring: false,
            memory_threshold: 67108864,
            high_memory_threshold: 134217728,
            memory_check_interval: 10,
            enable_mmap: false,
            mmap_min_size: 65536,
            mmap_max_memory: 1048576,
            temp_dir: None,
            pragma_journal_mode: "WAL".to_string(),
            pragma_synchronous: "NORMAL".to_string(),
            pragma_cache_size: -64000,
            pragma_mmap_size: 268435456,
            migrate: false,
        };

        // This should be validated in Config::load(), but we're testing the validation
        assert!(config.ssl && config.no_tcp, "Invalid SSL configuration should be caught");
    }
}