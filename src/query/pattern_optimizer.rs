use std::collections::HashMap;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::debug;

/// Query patterns that can be optimized
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QueryPattern {
    SimpleSelect,
    SimpleInsert,
    SimpleUpdate,
    SimpleDelete,
    BatchInsert,
    CountQuery,
    ExistsQuery,
    MaxMinQuery,
    GroupByAggregation,
    OrderByLimit,
    JoinWithWhere,
    UnionQuery,
    SubqueryExists,
    NestedSubquery,
    ComplexQuery,
}

/// Optimization hints for different query patterns
#[derive(Debug, Clone)]
pub struct OptimizationHints {
    /// Whether to use ultra-fast path
    pub use_fast_path: bool,
    /// Whether to cache the result
    pub cache_result: bool,
    /// Whether to use batch processing
    pub use_batch_processing: bool,
    /// Whether to skip translation
    pub skip_translation: bool,
    /// Whether to use prepared statements
    pub use_prepared_statement: bool,
    /// Expected result set size
    pub expected_result_size: ResultSize,
    /// Query complexity level
    pub complexity: QueryComplexity,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum ResultSize {
    Empty,
    Single,
    Small,    // < 100 rows
    Medium,   // 100-1000 rows
    Large,    // > 1000 rows
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum QueryComplexity {
    Simple,
    Medium,
    Complex,
}

/// Pattern recognition system for query optimization
pub struct QueryPatternOptimizer {
    pattern_cache: HashMap<String, (QueryPattern, OptimizationHints)>,
    recognition_stats: HashMap<QueryPattern, u64>,
}

// Pre-compiled regex patterns for different query types
static COUNT_QUERY_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)^\s*SELECT\s+COUNT\s*\(\s*[*]?\s*\)\s+FROM\s+\w+").unwrap()
});

static EXISTS_QUERY_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)SELECT\s+EXISTS\s*\(").unwrap()
});

static MAX_MIN_QUERY_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)SELECT\s+(MAX|MIN)\s*\(\s*\w+\s*\)\s+FROM").unwrap()
});

static GROUP_BY_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)GROUP\s+BY").unwrap()
});

static ORDER_BY_LIMIT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)ORDER\s+BY\s+.*\s+LIMIT\s+\d+").unwrap()
});

static JOIN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\s+(INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+").unwrap()
});

static UNION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\s+UNION(\s+ALL)?\s+").unwrap()
});

static SUBQUERY_EXISTS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)WHERE\s+EXISTS\s*\(").unwrap()
});

static NESTED_SUBQUERY_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)SELECT.*\(.*SELECT.*\(.*SELECT").unwrap()
});

static BATCH_INSERT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)INSERT\s+INTO\s+\w+.*VALUES\s*\([^)]+\)(?:\s*,\s*\([^)]+\))+").unwrap()
});

impl QueryPatternOptimizer {
    pub fn new() -> Self {
        Self {
            pattern_cache: HashMap::new(),
            recognition_stats: HashMap::new(),
        }
    }

    /// Analyze a query and return optimization hints
    pub fn analyze_query(&mut self, query: &str) -> (QueryPattern, OptimizationHints) {
        // Check cache first
        if let Some((pattern, hints)) = self.pattern_cache.get(query) {
            return (pattern.clone(), hints.clone());
        }

        let (pattern, hints) = self.recognize_pattern(query);
        
        // Update statistics
        *self.recognition_stats.entry(pattern.clone()).or_insert(0) += 1;
        
        // Cache the result
        self.pattern_cache.insert(query.to_string(), (pattern.clone(), hints.clone()));
        
        debug!("Query pattern recognized: {:?} for query: {}", pattern, query);
        
        (pattern, hints)
    }

    fn recognize_pattern(&self, query: &str) -> (QueryPattern, OptimizationHints) {
        
        // Check for batch insert first (more specific)
        if BATCH_INSERT_PATTERN.is_match(query) {
            return (QueryPattern::BatchInsert, OptimizationHints {
                use_fast_path: true,
                cache_result: false,
                use_batch_processing: true,
                skip_translation: false, // May need datetime/array conversion
                use_prepared_statement: true,
                expected_result_size: ResultSize::Empty,
                complexity: QueryComplexity::Simple,
            });
        }

        // Check for simple operations first
        if self.is_simple_select(query) {
            return (QueryPattern::SimpleSelect, OptimizationHints {
                use_fast_path: true,
                cache_result: true,
                use_batch_processing: false,
                skip_translation: true,
                use_prepared_statement: false,
                expected_result_size: ResultSize::Small,
                complexity: QueryComplexity::Simple,
            });
        }

        if self.is_simple_insert(query) {
            return (QueryPattern::SimpleInsert, OptimizationHints {
                use_fast_path: true,
                cache_result: false,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: true,
                expected_result_size: ResultSize::Empty,
                complexity: QueryComplexity::Simple,
            });
        }

        if self.is_simple_update(query) {
            return (QueryPattern::SimpleUpdate, OptimizationHints {
                use_fast_path: true,
                cache_result: false,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: true,
                expected_result_size: ResultSize::Empty,
                complexity: QueryComplexity::Simple,
            });
        }

        if self.is_simple_delete(query) {
            return (QueryPattern::SimpleDelete, OptimizationHints {
                use_fast_path: true,
                cache_result: false,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: true,
                expected_result_size: ResultSize::Empty,
                complexity: QueryComplexity::Simple,
            });
        }

        // Check for specific query patterns
        if COUNT_QUERY_PATTERN.is_match(query) {
            return (QueryPattern::CountQuery, OptimizationHints {
                use_fast_path: false,
                cache_result: true,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: true,
                expected_result_size: ResultSize::Single,
                complexity: QueryComplexity::Simple,
            });
        }

        if EXISTS_QUERY_PATTERN.is_match(query) {
            return (QueryPattern::ExistsQuery, OptimizationHints {
                use_fast_path: false,
                cache_result: true,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: true,
                expected_result_size: ResultSize::Single,
                complexity: QueryComplexity::Medium,
            });
        }

        if MAX_MIN_QUERY_PATTERN.is_match(query) {
            return (QueryPattern::MaxMinQuery, OptimizationHints {
                use_fast_path: false,
                cache_result: true,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: true,
                expected_result_size: ResultSize::Single,
                complexity: QueryComplexity::Simple,
            });
        }

        // Check for complex patterns
        if NESTED_SUBQUERY_PATTERN.is_match(query) {
            return (QueryPattern::NestedSubquery, OptimizationHints {
                use_fast_path: false,
                cache_result: false,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: false,
                expected_result_size: ResultSize::Unknown,
                complexity: QueryComplexity::Complex,
            });
        }

        if SUBQUERY_EXISTS_PATTERN.is_match(query) {
            return (QueryPattern::SubqueryExists, OptimizationHints {
                use_fast_path: false,
                cache_result: true,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: true,
                expected_result_size: ResultSize::Single,
                complexity: QueryComplexity::Medium,
            });
        }

        if UNION_PATTERN.is_match(query) {
            return (QueryPattern::UnionQuery, OptimizationHints {
                use_fast_path: false,
                cache_result: false,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: false,
                expected_result_size: ResultSize::Medium,
                complexity: QueryComplexity::Medium,
            });
        }

        if JOIN_PATTERN.is_match(query) {
            return (QueryPattern::JoinWithWhere, OptimizationHints {
                use_fast_path: false,
                cache_result: false,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: false,
                expected_result_size: ResultSize::Medium,
                complexity: QueryComplexity::Medium,
            });
        }

        if GROUP_BY_PATTERN.is_match(query) {
            return (QueryPattern::GroupByAggregation, OptimizationHints {
                use_fast_path: false,
                cache_result: false,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: false,
                expected_result_size: ResultSize::Medium,
                complexity: QueryComplexity::Medium,
            });
        }

        if ORDER_BY_LIMIT_PATTERN.is_match(query) {
            return (QueryPattern::OrderByLimit, OptimizationHints {
                use_fast_path: false,
                cache_result: true,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: true,
                expected_result_size: ResultSize::Small,
                complexity: QueryComplexity::Simple,
            });
        }

        // Default to complex query
        (QueryPattern::ComplexQuery, OptimizationHints {
            use_fast_path: false,
            cache_result: false,
            use_batch_processing: false,
            skip_translation: false,
            use_prepared_statement: false,
            expected_result_size: ResultSize::Unknown,
            complexity: QueryComplexity::Complex,
        })
    }

    fn is_simple_select(&self, query: &str) -> bool {
        crate::query::simple_query_detector::is_ultra_simple_query(query) && 
        query.trim().to_uppercase().starts_with("SELECT")
    }

    fn is_simple_insert(&self, query: &str) -> bool {
        crate::query::simple_query_detector::is_ultra_simple_query(query) && 
        query.trim().to_uppercase().starts_with("INSERT")
    }

    fn is_simple_update(&self, query: &str) -> bool {
        crate::query::simple_query_detector::is_ultra_simple_query(query) && 
        query.trim().to_uppercase().starts_with("UPDATE")
    }

    fn is_simple_delete(&self, query: &str) -> bool {
        crate::query::simple_query_detector::is_ultra_simple_query(query) && 
        query.trim().to_uppercase().starts_with("DELETE")
    }

    /// Get statistics about pattern recognition
    pub fn get_pattern_stats(&self) -> HashMap<QueryPattern, u64> {
        self.recognition_stats.clone()
    }

    /// Clear pattern cache (useful for testing or memory management)
    pub fn clear_cache(&mut self) {
        self.pattern_cache.clear();
    }

    /// Get cache hit rate
    pub fn get_cache_hit_rate(&self) -> f64 {
        if self.pattern_cache.is_empty() {
            return 0.0;
        }
        
        let total_recognitions: u64 = self.recognition_stats.values().sum();
        let cache_size = self.pattern_cache.len() as u64;
        
        if total_recognitions > 0 {
            1.0 - (cache_size as f64 / total_recognitions as f64)
        } else {
            0.0
        }
    }
}

impl Default for QueryPatternOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select_recognition() {
        let mut optimizer = QueryPatternOptimizer::new();
        let (pattern, hints) = optimizer.analyze_query("SELECT * FROM users WHERE id = 1");
        
        assert_eq!(pattern, QueryPattern::SimpleSelect);
        assert!(hints.use_fast_path);
        assert!(hints.cache_result);
        assert!(hints.skip_translation);
    }

    #[test]
    fn test_count_query_recognition() {
        let mut optimizer = QueryPatternOptimizer::new();
        let (pattern, hints) = optimizer.analyze_query("SELECT COUNT(*) FROM users");
        
        assert_eq!(pattern, QueryPattern::CountQuery);
        assert!(!hints.use_fast_path);
        assert!(hints.cache_result);
        assert_eq!(hints.expected_result_size, ResultSize::Single);
    }

    #[test]
    fn test_batch_insert_recognition() {
        let mut optimizer = QueryPatternOptimizer::new();
        let (pattern, hints) = optimizer.analyze_query("INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')");
        
        assert_eq!(pattern, QueryPattern::BatchInsert);
        assert!(hints.use_fast_path);
        assert!(hints.use_batch_processing);
        assert!(hints.use_prepared_statement);
    }

    #[test]
    fn test_complex_query_recognition() {
        let mut optimizer = QueryPatternOptimizer::new();
        let (pattern, hints) = optimizer.analyze_query("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE EXISTS (SELECT 1 FROM products p WHERE p.id = o.product_id)");
        
        // This query will be recognized as SubqueryExists first because that pattern matches
        assert_eq!(pattern, QueryPattern::SubqueryExists);
        assert!(!hints.use_fast_path);
        assert!(hints.cache_result);
        assert_eq!(hints.complexity, QueryComplexity::Medium);
    }

    #[test]
    fn test_caching_behavior() {
        let mut optimizer = QueryPatternOptimizer::new();
        let query = "SELECT * FROM users";
        
        // First call should recognize and cache
        let (pattern1, _) = optimizer.analyze_query(query);
        
        // Second call should hit cache
        let (pattern2, _) = optimizer.analyze_query(query);
        
        assert_eq!(pattern1, pattern2);
        assert_eq!(optimizer.pattern_cache.len(), 1);
    }
}