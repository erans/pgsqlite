use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use rusqlite::{Connection, Statement};
use crate::query::{QueryPatternOptimizer, QueryPattern, OptimizationHints};
use crate::cache::query_fingerprint::QueryFingerprint;
use tracing::{debug, info};

/// Enhanced statement pool with smart caching based on query patterns and optimization hints
pub struct EnhancedStatementPool {
    /// Cached statement metadata by normalized query fingerprint
    statements: Arc<RwLock<HashMap<String, CachedStatementEntry>>>,
    /// Maximum number of cached statements
    max_size: usize,
    /// Pool statistics for monitoring
    stats: Arc<RwLock<PoolStats>>,
    /// Query pattern optimizer for intelligent caching decisions
    pattern_optimizer: Arc<RwLock<QueryPatternOptimizer>>,
}

/// Enhanced cached statement entry with access patterns and optimization data
#[derive(Debug, Clone)]
struct CachedStatementEntry {
    /// Statement metadata (column info, types, etc.)
    metadata: StatementMetadata,
    /// Query pattern classification
    #[allow(dead_code)]
    pattern: QueryPattern,
    /// Optimization hints for this query
    #[allow(dead_code)]
    hints: OptimizationHints,
    /// Access statistics
    access_count: u64,
    /// Last access time for LRU eviction
    last_used: Instant,
    /// Creation time
    created_at: Instant,
    /// Average execution time (if available)
    #[allow(dead_code)]
    avg_execution_time: Option<Duration>,
    /// Cache priority score (higher = more important to keep)
    priority_score: f64,
}

/// Enhanced statement metadata with optimization information
#[derive(Debug, Clone)]
pub struct StatementMetadata {
    pub column_names: Vec<String>,
    pub column_types: Vec<Option<String>>,
    pub parameter_count: usize,
    pub is_select: bool,
    /// Query complexity classification
    pub complexity: crate::query::QueryComplexity,
    /// Expected result size
    pub expected_result_size: crate::query::ResultSize,
    /// Whether this query should use fast path
    pub use_fast_path: bool,
    /// Whether results should be cached
    pub cache_results: bool,
}

/// Pool performance statistics
#[derive(Debug, Default, Clone)]
pub struct PoolStats {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub evictions: u64,
    pub total_queries: u64,
    pub total_preparation_time_ms: u64,
    pub average_hit_rate: f64,
    pub most_accessed_patterns: HashMap<QueryPattern, u64>,
}

impl EnhancedStatementPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            statements: Arc::new(RwLock::new(HashMap::new())),
            max_size,
            stats: Arc::new(RwLock::new(PoolStats::default())),
            pattern_optimizer: Arc::new(RwLock::new(QueryPatternOptimizer::new())),
        }
    }

    /// Prepare a statement with enhanced caching based on query patterns
    pub fn prepare_and_cache_enhanced<'conn>(
        &self,
        conn: &'conn Connection,
        query: &str,
    ) -> Result<(Statement<'conn>, StatementMetadata), rusqlite::Error> {
        info!("Enhanced statement pool preparing query: {}", query);
        let start_time = Instant::now();
        
        // Generate normalized fingerprint for cache key
        let cache_key = self.generate_cache_key(query);
        
        // Update total queries stat
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
        }

        // Check cache first
        if let Some(metadata) = self.get_cached_metadata(&cache_key) {
            // Cache hit - prepare statement with cached metadata
            let stmt = conn.prepare(query)?;
            self.record_cache_hit(&cache_key);
            debug!("Statement cache hit for query: {}", cache_key);
            return Ok((stmt, metadata));
        }

        // Cache miss - analyze query pattern and prepare statement
        let (pattern, hints) = {
            let mut optimizer = self.pattern_optimizer.write().unwrap();
            optimizer.analyze_query(query)
        };

        // Decide whether to cache based on optimization hints
        let should_cache = self.should_cache_query(&pattern, &hints);
        
        if should_cache {
            debug!("Preparing and caching statement for pattern: {:?}", pattern);
            
            // Prepare statement and extract metadata
            let stmt = conn.prepare(query)?;
            let metadata = self.extract_enhanced_metadata(&stmt, query, &pattern, &hints)?;
            
            // Cache the statement metadata
            self.cache_statement_metadata(cache_key.clone(), metadata.clone(), pattern.clone(), hints);
            
            // Record preparation time
            let preparation_time = start_time.elapsed();
            {
                let mut stats = self.stats.write().unwrap();
                stats.cache_misses += 1;
                stats.total_preparation_time_ms += preparation_time.as_millis() as u64;
            }
            
            info!("Cached new statement for pattern {:?} in {}ms", pattern, preparation_time.as_millis());
            Ok((stmt, metadata))
        } else {
            // Don't cache this query - just prepare it
            debug!("Not caching query due to optimization hints: {:?}", hints);
            let stmt = conn.prepare(query)?;
            let metadata = self.extract_basic_metadata(&stmt, query)?;
            
            {
                let mut stats = self.stats.write().unwrap();
                stats.cache_misses += 1;
            }
            
            Ok((stmt, metadata))
        }
    }

    /// Generate a normalized cache key for the query
    fn generate_cache_key(&self, query: &str) -> String {
        // First check for batch INSERT pattern (existing logic)
        if let Some(fingerprint) = self.batch_insert_fingerprint(query) {
            return fingerprint;
        }

        // Use enhanced query fingerprinting for better normalization
        QueryFingerprint::generate(query).to_string()
    }

    /// Determine if a query should be cached based on its pattern and hints
    fn should_cache_query(&self, pattern: &QueryPattern, hints: &OptimizationHints) -> bool {
        use crate::query::{QueryPattern, QueryComplexity};
        
        match pattern {
            // Always cache these patterns - high reuse potential
            QueryPattern::SimpleSelect | 
            QueryPattern::SimpleInsert | 
            QueryPattern::SimpleUpdate | 
            QueryPattern::SimpleDelete |
            QueryPattern::BatchInsert |
            QueryPattern::CountQuery |
            QueryPattern::ExistsQuery |
            QueryPattern::MaxMinQuery => true,

            // Cache medium complexity queries if they use prepared statements
            QueryPattern::GroupByAggregation |
            QueryPattern::OrderByLimit |
            QueryPattern::JoinWithWhere |
            QueryPattern::SubqueryExists => {
                hints.use_prepared_statement
            },

            // Only cache complex queries if explicitly recommended
            QueryPattern::NestedSubquery |
            QueryPattern::UnionQuery |
            QueryPattern::ComplexQuery => {
                hints.cache_result && hints.complexity != QueryComplexity::Complex
            },
        }
    }

    /// Extract enhanced metadata including optimization information
    fn extract_enhanced_metadata(
        &self,
        stmt: &Statement,
        query: &str,
        _pattern: &QueryPattern,
        hints: &OptimizationHints,
    ) -> Result<StatementMetadata, rusqlite::Error> {
        let columns = stmt.columns();
        let mut column_names = Vec::with_capacity(columns.len());
        let mut column_types = Vec::with_capacity(columns.len());

        for column in columns {
            column_names.push(column.name().to_string());
            
            // Extract column type information from the column
            let column_type = column.decl_type().map(|s| s.to_string());
            column_types.push(column_type);
        }

        let parameter_count = stmt.parameter_count();
        let is_select = query.trim().to_uppercase().starts_with("SELECT") || 
                       query.trim().to_uppercase().starts_with("WITH");

        Ok(StatementMetadata {
            column_names,
            column_types,
            parameter_count,
            is_select,
            complexity: hints.complexity,
            expected_result_size: hints.expected_result_size,
            use_fast_path: hints.use_fast_path,
            cache_results: hints.cache_result,
        })
    }

    /// Extract basic metadata for non-cached queries
    fn extract_basic_metadata(&self, stmt: &Statement, query: &str) -> Result<StatementMetadata, rusqlite::Error> {
        let columns = stmt.columns();
        let mut column_names = Vec::with_capacity(columns.len());
        let mut column_types = Vec::with_capacity(columns.len());

        for column in columns {
            column_names.push(column.name().to_string());
            
            // Extract column type information from the column
            let column_type = column.decl_type().map(|s| s.to_string());
            column_types.push(column_type);
        }

        let parameter_count = stmt.parameter_count();
        let is_select = query.trim().to_uppercase().starts_with("SELECT") || 
                       query.trim().to_uppercase().starts_with("WITH");

        Ok(StatementMetadata {
            column_names,
            column_types,
            parameter_count,
            is_select,
            complexity: crate::query::QueryComplexity::Medium,
            expected_result_size: crate::query::ResultSize::Unknown,
            use_fast_path: false,
            cache_results: false,
        })
    }

    /// Get cached metadata if available
    fn get_cached_metadata(&self, cache_key: &str) -> Option<StatementMetadata> {
        let statements = self.statements.read().unwrap();
        statements.get(cache_key).map(|entry| entry.metadata.clone())
    }

    /// Cache statement metadata with optimization data
    fn cache_statement_metadata(
        &self,
        cache_key: String,
        metadata: StatementMetadata,
        pattern: QueryPattern,
        hints: OptimizationHints,
    ) {
        let mut statements = self.statements.write().unwrap();
        
        // Check if we need to evict entries
        if statements.len() >= self.max_size {
            self.evict_least_valuable(&mut statements);
        }

        // Calculate priority score for this entry
        let priority_score = self.calculate_priority_score(&pattern, &hints);

        let entry = CachedStatementEntry {
            metadata,
            pattern: pattern.clone(),
            hints,
            access_count: 0,
            last_used: Instant::now(),
            created_at: Instant::now(),
            avg_execution_time: None,
            priority_score,
        };

        statements.insert(cache_key, entry);

        // Update pattern statistics
        {
            let mut stats = self.stats.write().unwrap();
            *stats.most_accessed_patterns.entry(pattern).or_insert(0) += 1;
        }
    }

    /// Calculate priority score for caching decisions
    fn calculate_priority_score(&self, pattern: &QueryPattern, hints: &OptimizationHints) -> f64 {
        use crate::query::{QueryPattern, QueryComplexity};
        
        let mut score = 1.0;

        // Pattern-based scoring
        score *= match pattern {
            QueryPattern::SimpleSelect => 3.0,
            QueryPattern::SimpleInsert => 2.5,
            QueryPattern::BatchInsert => 4.0,
            QueryPattern::CountQuery => 2.0,
            QueryPattern::SimpleUpdate | QueryPattern::SimpleDelete => 2.0,
            QueryPattern::JoinWithWhere => 1.8,
            QueryPattern::GroupByAggregation => 1.5,
            _ => 1.0,
        };

        // Complexity-based scoring (simpler queries are more valuable to cache)
        score *= match hints.complexity {
            QueryComplexity::Simple => 2.0,
            QueryComplexity::Medium => 1.5,
            QueryComplexity::Complex => 0.8,
        };

        // Fast path queries get higher priority
        if hints.use_fast_path {
            score *= 1.5;
        }

        // Prepared statement friendly queries get higher priority
        if hints.use_prepared_statement {
            score *= 1.3;
        }

        score
    }

    /// Record a cache hit and update access statistics
    fn record_cache_hit(&self, cache_key: &str) {
        let mut statements = self.statements.write().unwrap();
        if let Some(entry) = statements.get_mut(cache_key) {
            entry.access_count += 1;
            entry.last_used = Instant::now();
        }

        let mut stats = self.stats.write().unwrap();
        stats.cache_hits += 1;
        stats.average_hit_rate = stats.cache_hits as f64 / stats.total_queries as f64;
    }

    /// Evict the least valuable entry from the cache
    fn evict_least_valuable(&self, statements: &mut HashMap<String, CachedStatementEntry>) {
        if statements.is_empty() {
            return;
        }

        // Find entry with lowest value score
        let mut lowest_score = f64::MAX;
        let mut evict_key = String::new();

        for (key, entry) in statements.iter() {
            // Calculate current value: priority_score * access_frequency / age_penalty
            let age_penalty = entry.created_at.elapsed().as_secs() as f64 / 3600.0; // Hours
            let access_frequency = entry.access_count as f64;
            let value_score = entry.priority_score * (1.0 + access_frequency) / (1.0 + age_penalty);

            if value_score < lowest_score {
                lowest_score = value_score;
                evict_key = key.clone();
            }
        }

        if !evict_key.is_empty() {
            statements.remove(&evict_key);
            let mut stats = self.stats.write().unwrap();
            stats.evictions += 1;
            debug!("Evicted statement with key: {} (score: {:.2})", evict_key, lowest_score);
        }
    }

    /// Batch INSERT fingerprint generation (from existing implementation)
    fn batch_insert_fingerprint(&self, query: &str) -> Option<String> {
        let upper_query = query.to_uppercase();
        
        if !upper_query.contains("INSERT") || (!query.contains("),(") && !query.contains("), (")) {
            return None;
        }
        
        if let Some(_values_pos) = upper_query.find("VALUES") {
            let original_values_pos = query.to_uppercase().find("VALUES").unwrap();
            let prefix = &query[..original_values_pos + 6].trim();
            Some(format!("{} (?)", prefix))
        } else {
            None
        }
    }

    /// Get current pool statistics
    pub fn get_stats(&self) -> PoolStats {
        let stats = self.stats.read().unwrap();
        stats.clone()
    }

    /// Clear all cached statements (useful for DDL operations)
    pub fn clear(&self) {
        let mut statements = self.statements.write().unwrap();
        statements.clear();
        debug!("Cleared all cached statements");
    }

    /// Get cache utilization information
    pub fn get_cache_info(&self) -> (usize, usize, f64) {
        let statements = self.statements.read().unwrap();
        let current_size = statements.len();
        let hit_rate = self.get_stats().average_hit_rate;
        (current_size, self.max_size, hit_rate)
    }
}

impl Default for EnhancedStatementPool {
    fn default() -> Self {
        Self::new(100)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_enhanced_statement_pool_creation() {
        let pool = EnhancedStatementPool::new(50);
        let (current, max, hit_rate) = pool.get_cache_info();
        assert_eq!(current, 0);
        assert_eq!(max, 50);
        assert_eq!(hit_rate, 0.0);
    }

    #[test]
    fn test_cache_key_generation() {
        let pool = EnhancedStatementPool::new(100);
        
        // Test simple query
        let _key1 = pool.generate_cache_key("SELECT * FROM users WHERE id = 1");
        let _key2 = pool.generate_cache_key("SELECT * FROM users WHERE id = 2");
        // Should be similar (normalized) but not identical due to different literals
        
        // Test batch INSERT
        let batch_key = pool.generate_cache_key("INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')");
        assert!(batch_key.contains("INSERT INTO users (name, email) VALUES (?)"));
    }

    #[test]
    fn test_priority_scoring() {
        let pool = EnhancedStatementPool::new(100);
        
        // Simple SELECT should have high priority
        let hints = OptimizationHints {
            use_fast_path: true,
            cache_result: true,
            use_batch_processing: false,
            skip_translation: false,
            use_prepared_statement: true,
            expected_result_size: crate::query::ResultSize::Small,
            complexity: crate::query::QueryComplexity::Simple,
        };
        
        let score = pool.calculate_priority_score(&QueryPattern::SimpleSelect, &hints);
        assert!(score > 5.0); // Should be high priority
    }

    #[test]
    fn test_should_cache_decisions() {
        let pool = EnhancedStatementPool::new(100);
        
        // Simple queries should always be cached
        let simple_hints = OptimizationHints {
            use_fast_path: true,
            cache_result: true,
            use_batch_processing: false,
            skip_translation: false,
            use_prepared_statement: true,
            expected_result_size: crate::query::ResultSize::Small,
            complexity: crate::query::QueryComplexity::Simple,
        };
        
        assert!(pool.should_cache_query(&QueryPattern::SimpleSelect, &simple_hints));
        assert!(pool.should_cache_query(&QueryPattern::BatchInsert, &simple_hints));
        
        // Complex queries should be more selective
        let complex_hints = OptimizationHints {
            use_fast_path: false,
            cache_result: false,
            use_batch_processing: false,
            skip_translation: false,
            use_prepared_statement: false,
            expected_result_size: crate::query::ResultSize::Large,
            complexity: crate::query::QueryComplexity::Complex,
        };
        
        assert!(!pool.should_cache_query(&QueryPattern::ComplexQuery, &complex_hints));
    }
}