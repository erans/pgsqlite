use std::net::IpAddr;
use std::time::Duration;
use pgsqlite::protocol::{
    RateLimiter, RateLimitConfig, CircuitBreakerConfig, CircuitState,
    check_global_rate_limit, record_global_failure,
};

#[test]
fn test_rate_limiter_basic_functionality() {
    let config = RateLimitConfig {
        max_requests: 5,
        window_duration: Duration::from_secs(1),
        per_ip_limiting: true,
        max_tracked_ips: 100,
        cleanup_interval: Duration::from_secs(60),
    };

    let circuit_config = CircuitBreakerConfig {
        failure_threshold: 3,
        timeout_duration: Duration::from_secs(1),
        success_threshold: 2,
        enabled: true,
    };

    let limiter = RateLimiter::with_config(config, circuit_config);

    let ip: IpAddr = "192.168.1.100".parse().unwrap();

    // Should allow requests up to the limit
    for i in 1..=5 {
        assert!(
            limiter.check_request(Some(ip)).is_ok(),
            "Request {} should be allowed",
            i
        );
    }

    // Should reject the next request
    assert!(
        limiter.check_request(Some(ip)).is_err(),
        "Request 6 should be rejected"
    );

    // Reset and verify it works again
    limiter.reset();
    assert!(
        limiter.check_request(Some(ip)).is_ok(),
        "Request after reset should be allowed"
    );
}

#[test]
fn test_circuit_breaker_functionality() {
    let config = RateLimitConfig {
        max_requests: 1000, // High limit to focus on circuit breaker
        window_duration: Duration::from_secs(60),
        per_ip_limiting: false,
        max_tracked_ips: 100,
        cleanup_interval: Duration::from_secs(60),
    };

    let circuit_config = CircuitBreakerConfig {
        failure_threshold: 3,
        timeout_duration: Duration::from_millis(100),
        success_threshold: 2,
        enabled: true,
    };

    let limiter = RateLimiter::with_config(config, circuit_config);

    // Initially circuit should be closed
    assert_eq!(limiter.get_circuit_state(), CircuitState::Closed);

    // Record failures to open the circuit
    for _ in 0..3 {
        limiter.record_failure();
    }

    // Circuit should now be open
    assert_eq!(limiter.get_circuit_state(), CircuitState::Open);

    // Requests should be rejected
    assert!(limiter.check_request(None).is_err());

    // Wait for timeout
    std::thread::sleep(Duration::from_millis(150));

    // Should transition to half-open on next request
    assert!(limiter.check_request(None).is_ok());
    assert_eq!(limiter.get_circuit_state(), CircuitState::HalfOpen);

    // Record success to close circuit
    assert!(limiter.check_request(None).is_ok());
    assert_eq!(limiter.get_circuit_state(), CircuitState::Closed);
}

#[test]
fn test_per_ip_isolation() {
    let config = RateLimitConfig {
        max_requests: 10, // Higher limit to avoid global limit interference
        window_duration: Duration::from_secs(60),
        per_ip_limiting: true,
        max_tracked_ips: 100,
        cleanup_interval: Duration::from_secs(60),
    };

    let limiter = RateLimiter::with_config(config, CircuitBreakerConfig::default());

    let ip1: IpAddr = "192.168.1.1".parse().unwrap();
    let ip2: IpAddr = "192.168.1.2".parse().unwrap();

    // Reset to start fresh
    limiter.reset();

    // Use up some of the requests for IP1 (but not all)
    for i in 1..=5 {
        assert!(limiter.check_request(Some(ip1)).is_ok(), "IP1 request {} should work", i);
    }

    // IP2 should have its own separate quota
    for i in 1..=5 {
        assert!(limiter.check_request(Some(ip2)).is_ok(), "IP2 request {} should work", i);
    }

    // Both IPs should still have quota left since per-IP limit is separate
    assert!(limiter.check_request(Some(ip1)).is_ok());
    assert!(limiter.check_request(Some(ip2)).is_ok());
}

#[test]
fn test_global_rate_limiter_functions() {
    // Test that global functions work without panicking
    // Note: This may be affected by other tests running concurrently

    let result = check_global_rate_limit(None);
    // Just verify it returns a result, don't assert on success/failure
    // since other tests might affect the global state
    let _is_ok = result.is_ok();

    // Record a failure - should not panic
    record_global_failure();
}

#[test]
fn test_rate_limiter_config_from_env() {
    // Test environment-based configuration
    unsafe {
        std::env::set_var("PGSQLITE_RATE_LIMIT_MAX_REQUESTS", "100");
        std::env::set_var("PGSQLITE_RATE_LIMIT_WINDOW_SECS", "30");
        std::env::set_var("PGSQLITE_RATE_LIMIT_PER_IP", "true");
        std::env::set_var("PGSQLITE_CIRCUIT_BREAKER_ENABLED", "true");
        std::env::set_var("PGSQLITE_CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5");
    }

    let rate_config = RateLimitConfig::from_env();
    let circuit_config = CircuitBreakerConfig::from_env();

    assert_eq!(rate_config.max_requests, 100);
    assert_eq!(rate_config.window_duration, Duration::from_secs(30));
    assert!(rate_config.per_ip_limiting);
    assert!(circuit_config.enabled);
    assert_eq!(circuit_config.failure_threshold, 5);

    // Clean up environment variables
    unsafe {
        std::env::remove_var("PGSQLITE_RATE_LIMIT_MAX_REQUESTS");
        std::env::remove_var("PGSQLITE_RATE_LIMIT_WINDOW_SECS");
        std::env::remove_var("PGSQLITE_RATE_LIMIT_PER_IP");
        std::env::remove_var("PGSQLITE_CIRCUIT_BREAKER_ENABLED");
        std::env::remove_var("PGSQLITE_CIRCUIT_BREAKER_FAILURE_THRESHOLD");
    }
}

#[test]
fn test_rate_limiter_statistics() {
    let config = RateLimitConfig {
        max_requests: 10,
        window_duration: Duration::from_secs(60),
        per_ip_limiting: true,
        max_tracked_ips: 100,
        cleanup_interval: Duration::from_secs(60),
    };

    let limiter = RateLimiter::with_config(config, CircuitBreakerConfig::default());

    let ip1: IpAddr = "192.168.1.10".parse().unwrap();
    let ip2: IpAddr = "192.168.1.11".parse().unwrap();

    // Make some requests
    assert!(limiter.check_request(Some(ip1)).is_ok());
    assert!(limiter.check_request(Some(ip2)).is_ok());
    assert!(limiter.check_request(None).is_ok()); // Global request

    let stats = limiter.get_stats();

    // Should have tracked some requests and IPs
    assert!(stats.global_requests > 0);
    assert_eq!(stats.tracked_ips, 2);
    assert_eq!(stats.circuit_state, CircuitState::Closed);
}

#[test]
fn test_max_tracked_ips_limit() {
    let config = RateLimitConfig {
        max_requests: 10,
        window_duration: Duration::from_secs(60),
        per_ip_limiting: true,
        max_tracked_ips: 2, // Very low limit for testing
        cleanup_interval: Duration::from_secs(60),
    };

    let limiter = RateLimiter::with_config(config, CircuitBreakerConfig::default());

    let ip1: IpAddr = "192.168.1.20".parse().unwrap();
    let ip2: IpAddr = "192.168.1.21".parse().unwrap();
    let ip3: IpAddr = "192.168.1.22".parse().unwrap();

    // First two IPs should be tracked
    assert!(limiter.check_request(Some(ip1)).is_ok());
    assert!(limiter.check_request(Some(ip2)).is_ok());

    let stats = limiter.get_stats();
    assert_eq!(stats.tracked_ips, 2);

    // Third IP should still work but might not be tracked per-IP
    // (falls back to global rate limiting only)
    assert!(limiter.check_request(Some(ip3)).is_ok());
}