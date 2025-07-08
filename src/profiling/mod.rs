use std::time::Instant;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use once_cell::sync::Lazy;

/// Performance metrics for different stages of query processing
#[derive(Debug, Default)]
pub struct QueryMetrics {
    // Protocol parsing
    pub protocol_parse_ns: AtomicU64,
    pub protocol_parse_count: AtomicUsize,
    
    // Translation phases
    pub cast_translation_ns: AtomicU64,
    pub cast_translation_count: AtomicUsize,
    pub datetime_translation_ns: AtomicU64,
    pub datetime_translation_count: AtomicUsize,
    pub decimal_rewriting_ns: AtomicU64,
    pub decimal_rewriting_count: AtomicUsize,
    
    // Cache lookups
    pub cache_lookup_ns: AtomicU64,
    pub cache_lookup_count: AtomicUsize,
    pub cache_hit_count: AtomicUsize,
    
    // SQLite execution
    pub sqlite_prepare_ns: AtomicU64,
    pub sqlite_prepare_count: AtomicUsize,
    pub sqlite_execute_ns: AtomicU64,
    pub sqlite_execute_count: AtomicUsize,
    
    // Result formatting
    pub result_format_ns: AtomicU64,
    pub result_format_count: AtomicUsize,
    
    // Protocol serialization
    pub protocol_serialize_ns: AtomicU64,
    pub protocol_serialize_count: AtomicUsize,
    
    // Type resolution
    pub type_resolution_ns: AtomicU64,
    pub type_resolution_count: AtomicUsize,
    
    // Fast path
    pub fast_path_attempts: AtomicUsize,
    pub fast_path_success: AtomicUsize,
}

impl QueryMetrics {
    pub fn report(&self) -> String {
        let mut report = String::new();
        report.push_str("\n=== Query Performance Metrics ===\n");
        
        // Helper to format timing
        let format_timing = |ns: u64, count: usize| -> (f64, f64) {
            if count == 0 {
                (0.0, 0.0)
            } else {
                let total_ms = ns as f64 / 1_000_000.0;
                let avg_us = (ns as f64 / count as f64) / 1_000.0;
                (total_ms, avg_us)
            }
        };
        
        // Protocol parsing
        let (parse_total, parse_avg) = format_timing(
            self.protocol_parse_ns.load(Ordering::Relaxed),
            self.protocol_parse_count.load(Ordering::Relaxed)
        );
        report.push_str(&format!("Protocol Parsing: {:.2}ms total, {:.2}µs avg ({}x)\n", 
            parse_total, parse_avg, self.protocol_parse_count.load(Ordering::Relaxed)));
        
        // Translation
        let (cast_total, cast_avg) = format_timing(
            self.cast_translation_ns.load(Ordering::Relaxed),
            self.cast_translation_count.load(Ordering::Relaxed)
        );
        if self.cast_translation_count.load(Ordering::Relaxed) > 0 {
            report.push_str(&format!("Cast Translation: {:.2}ms total, {:.2}µs avg ({}x)\n", 
                cast_total, cast_avg, self.cast_translation_count.load(Ordering::Relaxed)));
        }
        
        let (dt_total, dt_avg) = format_timing(
            self.datetime_translation_ns.load(Ordering::Relaxed),
            self.datetime_translation_count.load(Ordering::Relaxed)
        );
        if self.datetime_translation_count.load(Ordering::Relaxed) > 0 {
            report.push_str(&format!("DateTime Translation: {:.2}ms total, {:.2}µs avg ({}x)\n", 
                dt_total, dt_avg, self.datetime_translation_count.load(Ordering::Relaxed)));
        }
        
        let (dec_total, dec_avg) = format_timing(
            self.decimal_rewriting_ns.load(Ordering::Relaxed),
            self.decimal_rewriting_count.load(Ordering::Relaxed)
        );
        if self.decimal_rewriting_count.load(Ordering::Relaxed) > 0 {
            report.push_str(&format!("Decimal Rewriting: {:.2}ms total, {:.2}µs avg ({}x)\n", 
                dec_total, dec_avg, self.decimal_rewriting_count.load(Ordering::Relaxed)));
        }
        
        // Cache
        let cache_hits = self.cache_hit_count.load(Ordering::Relaxed);
        let cache_lookups = self.cache_lookup_count.load(Ordering::Relaxed);
        let cache_hit_rate = if cache_lookups > 0 {
            (cache_hits as f64 / cache_lookups as f64) * 100.0
        } else {
            0.0
        };
        let (cache_total, cache_avg) = format_timing(
            self.cache_lookup_ns.load(Ordering::Relaxed),
            cache_lookups
        );
        report.push_str(&format!("Cache Lookups: {:.2}ms total, {:.2}µs avg ({}x, {:.1}% hit rate)\n", 
            cache_total, cache_avg, cache_lookups, cache_hit_rate));
        
        // SQLite
        let (prepare_total, prepare_avg) = format_timing(
            self.sqlite_prepare_ns.load(Ordering::Relaxed),
            self.sqlite_prepare_count.load(Ordering::Relaxed)
        );
        report.push_str(&format!("SQLite Prepare: {:.2}ms total, {:.2}µs avg ({}x)\n", 
            prepare_total, prepare_avg, self.sqlite_prepare_count.load(Ordering::Relaxed)));
        
        let (exec_total, exec_avg) = format_timing(
            self.sqlite_execute_ns.load(Ordering::Relaxed),
            self.sqlite_execute_count.load(Ordering::Relaxed)
        );
        report.push_str(&format!("SQLite Execute: {:.2}ms total, {:.2}µs avg ({}x)\n", 
            exec_total, exec_avg, self.sqlite_execute_count.load(Ordering::Relaxed)));
        
        // Type resolution
        let (type_total, type_avg) = format_timing(
            self.type_resolution_ns.load(Ordering::Relaxed),
            self.type_resolution_count.load(Ordering::Relaxed)
        );
        if self.type_resolution_count.load(Ordering::Relaxed) > 0 {
            report.push_str(&format!("Type Resolution: {:.2}ms total, {:.2}µs avg ({}x)\n", 
                type_total, type_avg, self.type_resolution_count.load(Ordering::Relaxed)));
        }
        
        // Result formatting
        let (fmt_total, fmt_avg) = format_timing(
            self.result_format_ns.load(Ordering::Relaxed),
            self.result_format_count.load(Ordering::Relaxed)
        );
        report.push_str(&format!("Result Formatting: {:.2}ms total, {:.2}µs avg ({}x)\n", 
            fmt_total, fmt_avg, self.result_format_count.load(Ordering::Relaxed)));
        
        // Protocol serialization
        let (ser_total, ser_avg) = format_timing(
            self.protocol_serialize_ns.load(Ordering::Relaxed),
            self.protocol_serialize_count.load(Ordering::Relaxed)
        );
        report.push_str(&format!("Protocol Serialization: {:.2}ms total, {:.2}µs avg ({}x)\n", 
            ser_total, ser_avg, self.protocol_serialize_count.load(Ordering::Relaxed)));
        
        // Fast path
        let fast_attempts = self.fast_path_attempts.load(Ordering::Relaxed);
        let fast_success = self.fast_path_success.load(Ordering::Relaxed);
        let fast_rate = if fast_attempts > 0 {
            (fast_success as f64 / fast_attempts as f64) * 100.0
        } else {
            0.0
        };
        report.push_str(&format!("Fast Path: {} attempts, {} success ({:.1}% rate)\n", 
            fast_attempts, fast_success, fast_rate));
        
        report
    }
    
    pub fn reset(&self) {
        self.protocol_parse_ns.store(0, Ordering::Relaxed);
        self.protocol_parse_count.store(0, Ordering::Relaxed);
        self.cast_translation_ns.store(0, Ordering::Relaxed);
        self.cast_translation_count.store(0, Ordering::Relaxed);
        self.datetime_translation_ns.store(0, Ordering::Relaxed);
        self.datetime_translation_count.store(0, Ordering::Relaxed);
        self.decimal_rewriting_ns.store(0, Ordering::Relaxed);
        self.decimal_rewriting_count.store(0, Ordering::Relaxed);
        self.cache_lookup_ns.store(0, Ordering::Relaxed);
        self.cache_lookup_count.store(0, Ordering::Relaxed);
        self.cache_hit_count.store(0, Ordering::Relaxed);
        self.sqlite_prepare_ns.store(0, Ordering::Relaxed);
        self.sqlite_prepare_count.store(0, Ordering::Relaxed);
        self.sqlite_execute_ns.store(0, Ordering::Relaxed);
        self.sqlite_execute_count.store(0, Ordering::Relaxed);
        self.result_format_ns.store(0, Ordering::Relaxed);
        self.result_format_count.store(0, Ordering::Relaxed);
        self.protocol_serialize_ns.store(0, Ordering::Relaxed);
        self.protocol_serialize_count.store(0, Ordering::Relaxed);
        self.type_resolution_ns.store(0, Ordering::Relaxed);
        self.type_resolution_count.store(0, Ordering::Relaxed);
        self.fast_path_attempts.store(0, Ordering::Relaxed);
        self.fast_path_success.store(0, Ordering::Relaxed);
    }
}

/// Global metrics instance
pub static METRICS: Lazy<QueryMetrics> = Lazy::new(|| QueryMetrics::default());

/// Timer for measuring specific operations
pub struct Timer {
    start: Instant,
    metric_ns: &'static AtomicU64,
    metric_count: &'static AtomicUsize,
}

impl Timer {
    pub fn new(metric_ns: &'static AtomicU64, metric_count: &'static AtomicUsize) -> Self {
        metric_count.fetch_add(1, Ordering::Relaxed);
        Self {
            start: Instant::now(),
            metric_ns,
            metric_count,
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_nanos() as u64;
        self.metric_ns.fetch_add(elapsed, Ordering::Relaxed);
    }
}

/// Convenience macros for timing operations
#[macro_export]
macro_rules! time_operation {
    ($metric_ns:expr, $metric_count:expr, $op:expr) => {{
        let _timer = $crate::profiling::Timer::new($metric_ns, $metric_count);
        $op
    }};
}

#[macro_export]
macro_rules! time_protocol_parse {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.protocol_parse_ns, &$crate::profiling::METRICS.protocol_parse_count, $op)
    };
}

#[macro_export]
macro_rules! time_cast_translation {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.cast_translation_ns, &$crate::profiling::METRICS.cast_translation_count, $op)
    };
}

#[macro_export]
macro_rules! time_datetime_translation {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.datetime_translation_ns, &$crate::profiling::METRICS.datetime_translation_count, $op)
    };
}

#[macro_export]
macro_rules! time_cache_lookup {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.cache_lookup_ns, &$crate::profiling::METRICS.cache_lookup_count, $op)
    };
}

#[macro_export]
macro_rules! time_sqlite_prepare {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.sqlite_prepare_ns, &$crate::profiling::METRICS.sqlite_prepare_count, $op)
    };
}

#[macro_export]
macro_rules! time_sqlite_execute {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.sqlite_execute_ns, &$crate::profiling::METRICS.sqlite_execute_count, $op)
    };
}

#[macro_export]
macro_rules! time_result_format {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.result_format_ns, &$crate::profiling::METRICS.result_format_count, $op)
    };
}

#[macro_export]
macro_rules! time_protocol_serialize {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.protocol_serialize_ns, &$crate::profiling::METRICS.protocol_serialize_count, $op)
    };
}

#[macro_export]
macro_rules! time_type_resolution {
    ($op:expr) => {
        $crate::time_operation!(&$crate::profiling::METRICS.type_resolution_ns, &$crate::profiling::METRICS.type_resolution_count, $op)
    };
}

/// Enable or disable profiling
static PROFILING_ENABLED: AtomicUsize = AtomicUsize::new(0);

pub fn enable_profiling() {
    PROFILING_ENABLED.store(1, Ordering::Relaxed);
}

pub fn disable_profiling() {
    PROFILING_ENABLED.store(0, Ordering::Relaxed);
}

pub fn is_profiling_enabled() -> bool {
    PROFILING_ENABLED.load(Ordering::Relaxed) > 0
}