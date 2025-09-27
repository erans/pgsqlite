use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use parking_lot::RwLock;
use thiserror::Error;
use tracing::{debug, warn, info};
use crate::security::events;

#[derive(Error, Debug)]
pub enum RateLimitError {
    #[error("Rate limit exceeded: {0}")]
    RateLimitExceeded(String),
    #[error("Circuit breaker open: {0}")]
    CircuitBreakerOpen(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
}

/// Configuration for rate limiting
#[derive(Debug, Clone)]
pub struct RateLimitConfig {
    /// Maximum requests per window
    pub max_requests: u32,
    /// Time window duration
    pub window_duration: Duration,
    /// Enable per-IP rate limiting
    pub per_ip_limiting: bool,
    /// Maximum number of IP addresses to track
    pub max_tracked_ips: usize,
    /// Cleanup interval for expired entries
    pub cleanup_interval: Duration,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: 1000,
            window_duration: Duration::from_secs(60),
            per_ip_limiting: true,
            max_tracked_ips: 10000,
            cleanup_interval: Duration::from_secs(300), // 5 minutes
        }
    }
}

impl RateLimitConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("PGSQLITE_RATE_LIMIT_MAX_REQUESTS")
            && let Ok(max_requests) = val.parse::<u32>() {
                config.max_requests = max_requests;
            }

        if let Ok(val) = std::env::var("PGSQLITE_RATE_LIMIT_WINDOW_SECS")
            && let Ok(window_secs) = val.parse::<u64>() {
                config.window_duration = Duration::from_secs(window_secs);
            }

        if let Ok(val) = std::env::var("PGSQLITE_RATE_LIMIT_PER_IP") {
            config.per_ip_limiting = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_RATE_LIMIT_MAX_IPS")
            && let Ok(max_ips) = val.parse::<usize>() {
                config.max_tracked_ips = max_ips;
            }

        config
    }
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: u32,
    /// Duration to keep circuit open
    pub timeout_duration: Duration,
    /// Number of successful calls to close circuit
    pub success_threshold: u32,
    /// Enable circuit breaker
    pub enabled: bool,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 50,
            timeout_duration: Duration::from_secs(60),
            success_threshold: 10,
            enabled: true,
        }
    }
}

impl CircuitBreakerConfig {
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("PGSQLITE_CIRCUIT_BREAKER_ENABLED") {
            config.enabled = val == "1" || val.to_lowercase() == "true";
        }

        if let Ok(val) = std::env::var("PGSQLITE_CIRCUIT_BREAKER_FAILURE_THRESHOLD")
            && let Ok(threshold) = val.parse::<u32>() {
                config.failure_threshold = threshold;
            }

        if let Ok(val) = std::env::var("PGSQLITE_CIRCUIT_BREAKER_TIMEOUT_SECS")
            && let Ok(timeout_secs) = val.parse::<u64>() {
                config.timeout_duration = Duration::from_secs(timeout_secs);
            }

        if let Ok(val) = std::env::var("PGSQLITE_CIRCUIT_BREAKER_SUCCESS_THRESHOLD")
            && let Ok(threshold) = val.parse::<u32>() {
                config.success_threshold = threshold;
            }

        config
    }
}

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,    // Normal operation
    Open,      // Circuit is open, rejecting requests
    HalfOpen,  // Testing if service is recovered
}

/// Atomic rate limiting window tracking
#[derive(Debug)]
struct RateLimitWindow {
    requests: AtomicU32,
    window_start_nanos: AtomicU64, // Store as nanoseconds since UNIX epoch for atomicity
}

impl RateLimitWindow {
    fn new() -> Self {
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            requests: AtomicU32::new(0),
            window_start_nanos: AtomicU64::new(now_nanos),
        }
    }

    fn is_expired(&self, window_duration: Duration) -> bool {
        let window_start = self.window_start_nanos.load(Ordering::Acquire);
        let current_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        current_nanos.saturating_sub(window_start) >= window_duration.as_nanos() as u64
    }

    fn is_expired_nanos(&self, window_duration_nanos: u64, current_nanos: u64) -> bool {
        let window_start = self.window_start_nanos.load(Ordering::Acquire);
        current_nanos.saturating_sub(window_start) >= window_duration_nanos
    }

    /// Atomically check if expired and reset if necessary, then increment
    fn check_and_increment(&self, window_duration: Duration) -> (u32, bool) {
        let current_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        loop {
            let window_start = self.window_start_nanos.load(Ordering::Acquire);
            let is_expired = current_nanos.saturating_sub(window_start) >= window_duration.as_nanos() as u64;

            if is_expired {
                // Try to reset the window atomically
                if self.window_start_nanos.compare_exchange_weak(
                    window_start,
                    current_nanos,
                    Ordering::Release,
                    Ordering::Relaxed
                ).is_ok() {
                    // Successfully reset window, now reset counter and increment
                    self.requests.store(1, Ordering::Release);
                    return (1, true);
                }
                // If CAS failed, another thread reset it, retry
                continue;
            } else {
                // Window not expired, just increment
                let count = self.requests.fetch_add(1, Ordering::AcqRel) + 1;
                return (count, false);
            }
        }
    }

    fn get_request_count(&self) -> u32 {
        self.requests.load(Ordering::Acquire)
    }

    fn reset(&self) {
        let current_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.requests.store(0, Ordering::Release);
        self.window_start_nanos.store(current_nanos, Ordering::Release);
    }
}

/// Atomic circuit breaker state tracking
#[derive(Debug)]
struct CircuitBreakerState {
    // Pack state into a single atomic for atomic state transitions
    // Format: [state: 8 bits][failure_count: 12 bits][success_count: 12 bits]
    packed_state: AtomicU32,
    last_failure_time_nanos: AtomicU64,
    next_attempt_time_nanos: AtomicU64,
}

impl CircuitBreakerState {
    const STATE_SHIFT: u32 = 24;
    const FAILURE_COUNT_SHIFT: u32 = 12;
    const COUNT_MASK: u32 = 0x0FFF; // 12 bits = 4095 max
    const STATE_MASK: u32 = 0xFF; // 8 bits

    fn new() -> Self {
        Self {
            packed_state: AtomicU32::new(Self::pack_state(CircuitState::Closed, 0, 0)),
            last_failure_time_nanos: AtomicU64::new(0),
            next_attempt_time_nanos: AtomicU64::new(0),
        }
    }

    fn pack_state(state: CircuitState, failure_count: u32, success_count: u32) -> u32 {
        let state_val = match state {
            CircuitState::Closed => 0u32,
            CircuitState::Open => 1u32,
            CircuitState::HalfOpen => 2u32,
        };

        (state_val << Self::STATE_SHIFT) |
        ((failure_count & Self::COUNT_MASK) << Self::FAILURE_COUNT_SHIFT) |
        (success_count & Self::COUNT_MASK)
    }

    fn unpack_state(packed: u32) -> (CircuitState, u32, u32) {
        let state_val = (packed >> Self::STATE_SHIFT) & Self::STATE_MASK;
        let failure_count = (packed >> Self::FAILURE_COUNT_SHIFT) & Self::COUNT_MASK;
        let success_count = packed & Self::COUNT_MASK;

        let state = match state_val {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed, // Fallback
        };

        (state, failure_count, success_count)
    }

    fn get_state(&self) -> (CircuitState, u32, u32) {
        let packed = self.packed_state.load(Ordering::Acquire);
        Self::unpack_state(packed)
    }

    fn instant_to_nanos(instant: Instant) -> u64 {
        // Convert Instant to nanoseconds since UNIX epoch (approximate)
        // This is a best-effort conversion for atomic storage
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }

    fn nanos_to_duration_since_now(nanos: u64) -> Option<Duration> {
        if nanos == 0 {
            return None;
        }

        let current_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        if nanos > current_nanos {
            Some(Duration::from_nanos(nanos - current_nanos))
        } else {
            Some(Duration::from_nanos(0))
        }
    }
}

/// Rate limiter with circuit breaker functionality
pub struct RateLimiter {
    rate_config: RateLimitConfig,
    circuit_config: CircuitBreakerConfig,

    // Per-IP rate limiting
    ip_windows: Arc<RwLock<HashMap<IpAddr, RateLimitWindow>>>,

    // Global rate limiting
    global_window: Arc<RwLock<RateLimitWindow>>,

    // Circuit breaker state (now atomic)
    circuit_state: Arc<CircuitBreakerState>,

    // Last cleanup time (atomic)
    last_cleanup_nanos: Arc<AtomicU64>,
}

impl RateLimiter {
    /// Create a new rate limiter with default configuration
    pub fn new() -> Self {
        Self::with_config(
            RateLimitConfig::default(),
            CircuitBreakerConfig::default(),
        )
    }

    /// Create a new rate limiter with custom configuration
    pub fn with_config(
        rate_config: RateLimitConfig,
        circuit_config: CircuitBreakerConfig,
    ) -> Self {
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            rate_config,
            circuit_config,
            ip_windows: Arc::new(RwLock::new(HashMap::new())),
            global_window: Arc::new(RwLock::new(RateLimitWindow::new())),
            circuit_state: Arc::new(CircuitBreakerState::new()),
            last_cleanup_nanos: Arc::new(AtomicU64::new(now_nanos)),
        }
    }

    /// Check if a request should be allowed
    pub fn check_request(&self, client_ip: Option<IpAddr>) -> Result<(), RateLimitError> {
        // First check circuit breaker
        if self.circuit_config.enabled {
            self.check_circuit_breaker()?;
        }

        // Then check rate limits
        self.check_rate_limits(client_ip)?;

        // Record successful request for circuit breaker
        if self.circuit_config.enabled {
            self.record_success();
        }

        // Cleanup old entries periodically
        self.maybe_cleanup();

        Ok(())
    }

    /// Record a request failure for circuit breaker
    pub fn record_failure(&self) {
        if !self.circuit_config.enabled {
            return;
        }

        let current_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.circuit_state.last_failure_time_nanos.store(current_nanos, Ordering::Release);

        loop {
            let packed = self.circuit_state.packed_state.load(Ordering::Acquire);
            let (state, failure_count, success_count) = CircuitBreakerState::unpack_state(packed);

            let new_failure_count = failure_count.saturating_add(1);

            // Check if we should open the circuit
            let (new_state, new_success_count) = if new_failure_count >= self.circuit_config.failure_threshold && state == CircuitState::Closed {
                // Set next attempt time
                let next_attempt_nanos = current_nanos + self.circuit_config.timeout_duration.as_nanos() as u64;
                self.circuit_state.next_attempt_time_nanos.store(next_attempt_nanos, Ordering::Release);

                warn!(
                    "Circuit breaker opened due to {} failures (threshold: {})",
                    new_failure_count,
                    self.circuit_config.failure_threshold
                );

                // Log security event for circuit breaker opening
                events::circuit_breaker_opened(new_failure_count, self.circuit_config.failure_threshold);

                (CircuitState::Open, 0)
            } else {
                (state, success_count)
            };

            let new_packed = CircuitBreakerState::pack_state(new_state, new_failure_count, new_success_count);

            // Atomic compare-exchange to update state
            if self.circuit_state.packed_state.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::Release,
                Ordering::Relaxed
            ).is_ok() {
                break;
            }
            // If CAS failed, retry
        }
    }

    /// Record a successful request for circuit breaker
    fn record_success(&self) {
        loop {
            let packed = self.circuit_state.packed_state.load(Ordering::Acquire);
            let (state, failure_count, success_count) = CircuitBreakerState::unpack_state(packed);

            let (new_state, new_failure_count, new_success_count) = match state {
                CircuitState::Closed => {
                    // Reset failure count on success
                    if failure_count > 0 {
                        debug!("Circuit breaker failure count reset after successful request");
                        (CircuitState::Closed, 0, success_count)
                    } else {
                        // No change needed
                        return;
                    }
                }
                CircuitState::HalfOpen => {
                    let new_success_count = success_count.saturating_add(1);
                    if new_success_count >= self.circuit_config.success_threshold {
                        info!("Circuit breaker closed after {} successful requests", new_success_count);
                        (CircuitState::Closed, 0, 0)
                    } else {
                        (CircuitState::HalfOpen, failure_count, new_success_count)
                    }
                }
                CircuitState::Open => {
                    // Should not happen if check_circuit_breaker is called first
                    return;
                }
            };

            let new_packed = CircuitBreakerState::pack_state(new_state, new_failure_count, new_success_count);

            // Atomic compare-exchange to update state
            if self.circuit_state.packed_state.compare_exchange_weak(
                packed,
                new_packed,
                Ordering::Release,
                Ordering::Relaxed
            ).is_ok() {
                break;
            }
            // If CAS failed, retry
        }
    }

    /// Check circuit breaker state
    fn check_circuit_breaker(&self) -> Result<(), RateLimitError> {
        let current_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        loop {
            let packed = self.circuit_state.packed_state.load(Ordering::Acquire);
            let (state, failure_count, success_count) = CircuitBreakerState::unpack_state(packed);

            match state {
                CircuitState::Closed => return Ok(()),
                CircuitState::Open => {
                    // Check if timeout has passed
                    let next_attempt_nanos = self.circuit_state.next_attempt_time_nanos.load(Ordering::Acquire);
                    if next_attempt_nanos > 0 && current_nanos >= next_attempt_nanos {
                        // Try to transition to half-open atomically
                        let new_packed = CircuitBreakerState::pack_state(CircuitState::HalfOpen, failure_count, 0);
                        if self.circuit_state.packed_state.compare_exchange_weak(
                            packed,
                            new_packed,
                            Ordering::Release,
                            Ordering::Relaxed
                        ).is_ok() {
                            debug!("Circuit breaker transitioned to half-open state");
                            return Ok(());
                        }
                        // If CAS failed, retry
                        continue;
                    } else {
                        return Err(RateLimitError::CircuitBreakerOpen(
                            "Service temporarily unavailable".to_string()
                        ));
                    }
                }
                CircuitState::HalfOpen => {
                    // Allow request but will monitor for success/failure
                    return Ok(());
                }
            }
        }
    }

    /// Check rate limits
    fn check_rate_limits(&self, client_ip: Option<IpAddr>) -> Result<(), RateLimitError> {
        // If per-IP limiting is enabled and we have an IP, check per-IP limits
        if self.rate_config.per_ip_limiting {
            if let Some(ip) = client_ip {
                let mut ip_windows = self.ip_windows.write();

                // Check if we're tracking too many IPs
                if ip_windows.len() >= self.rate_config.max_tracked_ips && !ip_windows.contains_key(&ip) {
                    warn!("Too many IP addresses being tracked, falling back to global rate limiting for {}", ip);
                    // Fall through to global rate limiting
                } else {
                    let window = ip_windows.entry(ip).or_insert_with(RateLimitWindow::new);

                    let (requests, _was_reset) = window.check_and_increment(self.rate_config.window_duration);
                    if requests > self.rate_config.max_requests {
                        // Log rate limit exceeded event
                        events::rate_limit_exceeded(Some(ip), "per-ip", requests);

                        return Err(RateLimitError::RateLimitExceeded(
                            format!("Per-IP rate limit exceeded for {}: {} requests per {} seconds",
                                ip,
                                self.rate_config.max_requests,
                                self.rate_config.window_duration.as_secs())
                        ));
                    }

                    // Per-IP limit passed, no need to check global limit
                    return Ok(());
                }
            }
        }

        // Check global rate limit (only if per-IP limiting is disabled or IP tracking is full)
        {
            let global_window = self.global_window.read();
            let (requests, _was_reset) = global_window.check_and_increment(self.rate_config.window_duration);
            if requests > self.rate_config.max_requests {
                // Log rate limit exceeded event
                events::rate_limit_exceeded(client_ip, "global", requests);

                return Err(RateLimitError::RateLimitExceeded(
                    format!("Global rate limit exceeded: {} requests per {} seconds",
                        self.rate_config.max_requests,
                        self.rate_config.window_duration.as_secs())
                ));
            }
        }

        Ok(())
    }

    /// Clean up expired rate limit windows
    fn maybe_cleanup(&self) {
        let current_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let last_cleanup_nanos = self.last_cleanup_nanos.load(Ordering::Acquire);
        let cleanup_interval_nanos = self.rate_config.cleanup_interval.as_nanos() as u64;

        if current_nanos.saturating_sub(last_cleanup_nanos) >= cleanup_interval_nanos {
            // Try to atomically update cleanup time
            if self.last_cleanup_nanos.compare_exchange_weak(
                last_cleanup_nanos,
                current_nanos,
                Ordering::Release,
                Ordering::Relaxed
            ).is_ok() {
                // Successfully claimed cleanup duty
                let window_expiry_nanos = (self.rate_config.window_duration * 2).as_nanos() as u64;

                // Clean up expired IP windows
                let mut ip_windows = self.ip_windows.write();
                ip_windows.retain(|_ip, window| {
                    !window.is_expired_nanos(window_expiry_nanos, current_nanos)
                });

                let remaining_ips = ip_windows.len();
                if remaining_ips > 0 {
                    debug!("Rate limiter cleanup: {} IP windows remaining", remaining_ips);
                }
            }
        }
    }

    /// Get current circuit breaker state
    pub fn get_circuit_state(&self) -> CircuitState {
        let (state, _, _) = self.circuit_state.get_state();
        state
    }

    /// Get rate limiting statistics
    pub fn get_stats(&self) -> RateLimitStats {
        let global_window = self.global_window.read();
        let ip_windows = self.ip_windows.read();
        let (circuit_state, circuit_failures, circuit_successes) = self.circuit_state.get_state();

        RateLimitStats {
            global_requests: global_window.get_request_count(),
            tracked_ips: ip_windows.len(),
            circuit_state,
            circuit_failures,
            circuit_successes,
        }
    }

    /// Reset all rate limiting state
    pub fn reset(&self) {
        // Reset global window atomically
        {
            let global_window = self.global_window.read();
            global_window.reset();
        }

        // Clear IP windows
        {
            let mut ip_windows = self.ip_windows.write();
            ip_windows.clear();
        }

        // Reset circuit breaker state atomically
        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        self.circuit_state.packed_state.store(
            CircuitBreakerState::pack_state(CircuitState::Closed, 0, 0),
            Ordering::Release
        );
        self.circuit_state.last_failure_time_nanos.store(0, Ordering::Release);
        self.circuit_state.next_attempt_time_nanos.store(0, Ordering::Release);

        // Reset cleanup time
        self.last_cleanup_nanos.store(now_nanos, Ordering::Release);

        info!("Rate limiter state reset");
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Rate limiting statistics
#[derive(Debug, Clone)]
pub struct RateLimitStats {
    pub global_requests: u32,
    pub tracked_ips: usize,
    pub circuit_state: CircuitState,
    pub circuit_failures: u32,
    pub circuit_successes: u32,
}

/// Global rate limiter instance
static GLOBAL_RATE_LIMITER: std::sync::OnceLock<RateLimiter> = std::sync::OnceLock::new();

/// Get the global rate limiter instance
pub fn global_rate_limiter() -> &'static RateLimiter {
    GLOBAL_RATE_LIMITER.get_or_init(|| {
        let rate_config = RateLimitConfig::from_env();
        let circuit_config = CircuitBreakerConfig::from_env();
        RateLimiter::with_config(rate_config, circuit_config)
    })
}

/// Check if a request should be allowed using the global rate limiter
pub fn check_global_rate_limit(client_ip: Option<IpAddr>) -> Result<(), RateLimitError> {
    global_rate_limiter().check_request(client_ip)
}

/// Record a failure using the global rate limiter
pub fn record_global_failure() {
    global_rate_limiter().record_failure();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_rate_limiter_creation() {
        let limiter = RateLimiter::new();
        let stats = limiter.get_stats();

        assert_eq!(stats.global_requests, 0);
        assert_eq!(stats.tracked_ips, 0);
        assert_eq!(stats.circuit_state, CircuitState::Closed);
    }

    #[test]
    fn test_global_rate_limiting() {
        let config = RateLimitConfig {
            max_requests: 5,
            window_duration: Duration::from_secs(1),
            per_ip_limiting: false,
            ..Default::default()
        };
        let limiter = RateLimiter::with_config(config, CircuitBreakerConfig::default());

        // Should allow up to max_requests
        for i in 1..=5 {
            assert!(limiter.check_request(None).is_ok(), "Request {} should be allowed", i);
        }

        // Should reject the next request
        assert!(limiter.check_request(None).is_err(), "Request 6 should be rejected");
    }

    #[test]
    fn test_per_ip_rate_limiting() {
        let config = RateLimitConfig {
            max_requests: 3,
            window_duration: Duration::from_secs(1),
            per_ip_limiting: true,
            ..Default::default()
        };
        let limiter = RateLimiter::with_config(config, CircuitBreakerConfig::default());

        let ip1: IpAddr = "192.168.1.1".parse().unwrap();
        let ip2: IpAddr = "192.168.1.2".parse().unwrap();

        // Should allow up to max_requests per IP
        for i in 1..=3 {
            assert!(limiter.check_request(Some(ip1)).is_ok(), "IP1 request {} should be allowed", i);
            assert!(limiter.check_request(Some(ip2)).is_ok(), "IP2 request {} should be allowed", i);
        }

        // Should reject the next request from each IP
        assert!(limiter.check_request(Some(ip1)).is_err(), "IP1 request 4 should be rejected");
        assert!(limiter.check_request(Some(ip2)).is_err(), "IP2 request 4 should be rejected");
    }

    #[test]
    fn test_window_expiration() {
        let config = RateLimitConfig {
            max_requests: 2,
            window_duration: Duration::from_millis(100),
            per_ip_limiting: false,
            ..Default::default()
        };
        let limiter = RateLimiter::with_config(config, CircuitBreakerConfig::default());

        // Use up the rate limit
        assert!(limiter.check_request(None).is_ok());
        assert!(limiter.check_request(None).is_ok());
        assert!(limiter.check_request(None).is_err());

        // Wait for window to expire
        thread::sleep(Duration::from_millis(150));

        // Should be allowed again
        assert!(limiter.check_request(None).is_ok());
    }

    #[test]
    fn test_circuit_breaker_open() {
        let circuit_config = CircuitBreakerConfig {
            failure_threshold: 3,
            timeout_duration: Duration::from_millis(100),
            success_threshold: 2,
            enabled: true,
        };
        let limiter = RateLimiter::with_config(RateLimitConfig::default(), circuit_config);

        // Record failures to open circuit
        for _ in 0..3 {
            limiter.record_failure();
        }

        assert_eq!(limiter.get_circuit_state(), CircuitState::Open);

        // Requests should be rejected
        assert!(limiter.check_request(None).is_err());
    }

    #[test]
    fn test_circuit_breaker_half_open() {
        let circuit_config = CircuitBreakerConfig {
            failure_threshold: 2,
            timeout_duration: Duration::from_millis(50),
            success_threshold: 2,
            enabled: true,
        };
        let limiter = RateLimiter::with_config(RateLimitConfig::default(), circuit_config);

        // Open circuit
        limiter.record_failure();
        limiter.record_failure();
        assert_eq!(limiter.get_circuit_state(), CircuitState::Open);

        // Wait for timeout
        thread::sleep(Duration::from_millis(100));

        // Next request should put it in half-open
        assert!(limiter.check_request(None).is_ok());
        assert_eq!(limiter.get_circuit_state(), CircuitState::HalfOpen);
    }

    #[test]
    fn test_circuit_breaker_close() {
        let circuit_config = CircuitBreakerConfig {
            failure_threshold: 2,
            timeout_duration: Duration::from_millis(50),
            success_threshold: 2,
            enabled: true,
        };
        let limiter = RateLimiter::with_config(RateLimitConfig::default(), circuit_config);

        // Open circuit
        limiter.record_failure();
        limiter.record_failure();

        // Wait for timeout and transition to half-open
        thread::sleep(Duration::from_millis(100));
        assert!(limiter.check_request(None).is_ok());

        // Make enough successful requests to close circuit
        assert!(limiter.check_request(None).is_ok());
        assert_eq!(limiter.get_circuit_state(), CircuitState::Closed);
    }

    #[test]
    fn test_rate_limiter_reset() {
        let config = RateLimitConfig {
            max_requests: 1,
            window_duration: Duration::from_secs(1),
            per_ip_limiting: true,
            ..Default::default()
        };
        let limiter = RateLimiter::with_config(config, CircuitBreakerConfig::default());

        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // Use up rate limit
        assert!(limiter.check_request(Some(ip)).is_ok());
        assert!(limiter.check_request(Some(ip)).is_err());

        // Reset and try again
        limiter.reset();
        assert!(limiter.check_request(Some(ip)).is_ok());
    }

    #[test]
    fn test_global_rate_limiter() {
        // Test that global functions work
        let result = check_global_rate_limit(None);
        assert!(result.is_ok());

        record_global_failure();
        // Should still work since circuit breaker threshold is high
        assert!(check_global_rate_limit(None).is_ok());
    }
}