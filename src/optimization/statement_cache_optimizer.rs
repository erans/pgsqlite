use std::sync::Arc;
use rusqlite::Connection;
use crate::cache::EnhancedStatementPool;
use crate::optimization::OptimizationManager;
use crate::query::{QueryPattern, OptimizationHints};
use tracing::{debug, info};

/// Statement cache optimizer that integrates enhanced statement pooling with query optimization
pub struct StatementCacheOptimizer {
    /// Enhanced statement pool for intelligent caching
    statement_pool: Arc<EnhancedStatementPool>,
    /// Optimization manager for query analysis
    optimization_manager: Arc<OptimizationManager>,
    /// Whether statement caching is enabled
    enabled: bool,
}

impl StatementCacheOptimizer {
    pub fn new(pool_size: usize, optimization_manager: Arc<OptimizationManager>) -> Self {
        Self {
            statement_pool: Arc::new(EnhancedStatementPool::new(pool_size)),
            optimization_manager,
            enabled: true,
        }
    }

    /// Execute a query with optimized statement caching
    pub fn execute_with_optimization<P: rusqlite::Params>(
        &self,
        conn: &Connection,
        query: &str,
        params: P,
    ) -> Result<usize, rusqlite::Error> {
        if !self.enabled {
            return conn.execute(query, params);
        }

        // Analyze query for optimization opportunities
        let optimization_result = self.optimization_manager
            .analyze_query(query)
            .map_err(|e| rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some(format!("Query optimization failed: {}", e))
            ))?;

        // Use enhanced statement pool for intelligent caching
        if self.should_use_statement_cache(&optimization_result.pattern, &optimization_result.hints) &&
           self.supports_binary_protocol(query) {
            debug!("Using enhanced statement cache for query pattern: {:?}", optimization_result.pattern);
            let (mut stmt, _metadata) = self.statement_pool.prepare_and_cache_enhanced(conn, query)?;
            let result = stmt.execute(params)?;
            
            // Log performance information if this is a significant query
            if matches!(optimization_result.pattern, 
                QueryPattern::BatchInsert | 
                QueryPattern::JoinWithWhere | 
                QueryPattern::GroupByAggregation
            ) {
                let cache_info = self.statement_pool.get_cache_info();
                debug!("Statement cache stats - Size: {}/{}, Hit rate: {:.2}%", 
                       cache_info.0, cache_info.1, cache_info.2 * 100.0);
            }
            
            Ok(result)
        } else {
            // Execute without caching for queries that don't benefit
            debug!("Executing without statement cache: {:?}", optimization_result.pattern);
            conn.execute(query, params)
        }
    }

    /// Query with optimized statement caching
    pub fn query_with_optimization<P: rusqlite::Params>(
        &self,
        conn: &Connection,
        query: &str,
        params: P,
    ) -> Result<(Vec<String>, Vec<Vec<Option<Vec<u8>>>>), rusqlite::Error> {
        if !self.enabled {
            return self.execute_basic_query(conn, query, params);
        }

        // Analyze query for optimization opportunities
        let optimization_result = self.optimization_manager
            .analyze_query(query)
            .map_err(|e| rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_MISUSE),
                Some(format!("Query optimization failed: {}", e))
            ))?;

        // Use enhanced statement pool for SELECT queries that benefit from caching
        // and don't require binary protocol support
        info!("Query analysis result for '{}': pattern={:?}, cache_result={}, supports_binary={}", 
               query, optimization_result.pattern, optimization_result.hints.cache_result, 
               self.supports_binary_protocol(query));
        
        if optimization_result.hints.cache_result && 
           self.supports_binary_protocol(query) &&
           matches!(optimization_result.pattern, 
               QueryPattern::SimpleSelect | 
               QueryPattern::CountQuery | 
               QueryPattern::MaxMinQuery |
               QueryPattern::OrderByLimit
           ) {
            debug!("Using enhanced statement cache for SELECT query pattern: {:?}", optimization_result.pattern);
            let (mut stmt, metadata) = self.statement_pool.prepare_and_cache_enhanced(conn, query)?;
            
            // Execute query and collect results
            let mut results = Vec::new();
            let column_names = metadata.column_names.clone();
            info!("Statement metadata - column_names: {:?}, column_types: {:?}", column_names, metadata.column_types);
            
            let rows = stmt.query_map(params, |row| {
                let mut row_data = Vec::new();
                for i in 0..column_names.len() {
                    match row.get_ref(i)? {
                        rusqlite::types::ValueRef::Null => row_data.push(None),
                        rusqlite::types::ValueRef::Integer(val) => {
                            // Check if this column is a boolean type
                            let is_boolean = metadata.column_types.get(i)
                                .and_then(|opt| opt.as_ref())
                                .map(|pg_type| {
                                    let type_lower = pg_type.to_lowercase();
                                    type_lower == "boolean" || type_lower == "bool"
                                })
                                .unwrap_or(false);
                            
                            if is_boolean {
                                // Convert integer 0/1 to PostgreSQL f/t format
                                let bool_str = if val == 0 { "f" } else { "t" };
                                debug!("Converting boolean value {} to '{}'", val, bool_str);
                                row_data.push(Some(bool_str.as_bytes().to_vec()));
                            } else {
                                row_data.push(Some(val.to_string().into_bytes()));
                            }
                        },
                        rusqlite::types::ValueRef::Real(val) => {
                            row_data.push(Some(val.to_string().into_bytes()));
                        },
                        rusqlite::types::ValueRef::Text(val) => {
                            row_data.push(Some(val.to_vec()));
                        },
                        rusqlite::types::ValueRef::Blob(val) => {
                            row_data.push(Some(val.to_vec()));
                        },
                    }
                }
                Ok(row_data)
            })?;

            for row_result in rows {
                results.push(row_result?);
            }

            Ok((column_names, results))
        } else {
            // Execute without caching for queries that don't benefit
            debug!("Executing SELECT without statement cache: {:?}", optimization_result.pattern);
            self.execute_basic_query(conn, query, params)
        }
    }

    /// Execute basic query without statement caching
    fn execute_basic_query<P: rusqlite::Params>(
        &self,
        conn: &Connection,
        query: &str,
        params: P,
    ) -> Result<(Vec<String>, Vec<Vec<Option<Vec<u8>>>>), rusqlite::Error> {
        let mut stmt = conn.prepare(query)?;
        let column_names: Vec<String> = stmt.column_names().iter().map(|&s| s.to_string()).collect();
        
        let mut results = Vec::new();
        let rows = stmt.query_map(params, |row| {
            let mut row_data = Vec::new();
            for i in 0..column_names.len() {
                match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => row_data.push(None),
                    rusqlite::types::ValueRef::Integer(val) => {
                        row_data.push(Some(val.to_string().into_bytes()));
                    },
                    rusqlite::types::ValueRef::Real(val) => {
                        row_data.push(Some(val.to_string().into_bytes()));
                    },
                    rusqlite::types::ValueRef::Text(val) => {
                        row_data.push(Some(val.to_vec()));
                    },
                    rusqlite::types::ValueRef::Blob(val) => {
                        row_data.push(Some(val.to_vec()));
                    },
                }
            }
            Ok(row_data)
        })?;

        for row_result in rows {
            results.push(row_result?);
        }

        Ok((column_names, results))
    }

    /// Determine if a query should use statement caching based on pattern and hints
    fn should_use_statement_cache(&self, pattern: &QueryPattern, hints: &OptimizationHints) -> bool {
        use crate::query::{QueryPattern, QueryComplexity};

        match pattern {
            // Always cache these high-value patterns
            QueryPattern::SimpleSelect |
            QueryPattern::SimpleInsert |
            QueryPattern::SimpleUpdate |
            QueryPattern::SimpleDelete |
            QueryPattern::BatchInsert |
            QueryPattern::CountQuery |
            QueryPattern::MaxMinQuery => true,

            // Cache if optimization hints suggest it's beneficial
            QueryPattern::GroupByAggregation |
            QueryPattern::OrderByLimit |
            QueryPattern::JoinWithWhere |
            QueryPattern::ExistsQuery |
            QueryPattern::SubqueryExists => {
                hints.use_prepared_statement && hints.complexity != QueryComplexity::Complex
            },

            // Only cache complex queries if explicitly recommended
            QueryPattern::NestedSubquery |
            QueryPattern::UnionQuery |
            QueryPattern::ComplexQuery => {
                hints.cache_result && 
                hints.use_prepared_statement && 
                hints.complexity == QueryComplexity::Medium
            },
        }
    }
    
    /// Check if query might need binary protocol support (return false to use fallback)
    fn supports_binary_protocol(&self, query: &str) -> bool {
        // For now, don't use enhanced caching for queries that might have datetime types
        // that require binary protocol support, as our caching doesn't yet handle
        // the binary format requirements properly
        let query_upper = query.to_uppercase();
        
        // Skip enhanced caching for queries involving datetime types that might
        // be accessed via extended protocol with binary format
        if query_upper.contains("DATE") || 
           query_upper.contains("TIME") || 
           query_upper.contains("TIMESTAMP") {
            return false;
        }
        
        true
    }

    /// Get comprehensive statistics including both statement pool and optimization metrics
    pub fn get_comprehensive_stats(&self) -> StatementCacheStats {
        let pool_stats = self.statement_pool.get_stats();
        let optimization_stats = self.optimization_manager.get_stats();
        let cache_info = self.statement_pool.get_cache_info();

        StatementCacheStats {
            pool_stats,
            optimization_stats,
            cache_size: cache_info.0,
            max_cache_size: cache_info.1,
            cache_utilization: cache_info.0 as f64 / cache_info.1 as f64,
            overall_hit_rate: cache_info.2,
        }
    }

    /// Clear statement cache (useful for DDL operations or testing)
    pub fn clear_cache(&self) {
        self.statement_pool.clear();
        info!("Enhanced statement cache cleared");
    }

    /// Enable or disable statement caching
    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
        info!("Enhanced statement caching {}", if enabled { "enabled" } else { "disabled" });
    }

    /// Get the underlying statement pool for advanced operations
    pub fn get_statement_pool(&self) -> &Arc<EnhancedStatementPool> {
        &self.statement_pool
    }

    /// Perform periodic maintenance
    pub fn maintenance(&self) {
        if !self.enabled {
            return;
        }

        let stats = self.get_comprehensive_stats();
        
        // Log statistics if cache is being actively used
        if stats.pool_stats.total_queries > 0 {
            info!("Statement cache maintenance - Hit rate: {:.1}%, Utilization: {:.1}%, Total queries: {}", 
                  stats.overall_hit_rate * 100.0,
                  stats.cache_utilization * 100.0,
                  stats.pool_stats.total_queries);
        }

        // Trigger optimization manager maintenance as well
        self.optimization_manager.maintenance();
    }
}

/// Combined statistics from statement caching and query optimization
#[derive(Debug, Clone)]
pub struct StatementCacheStats {
    pub pool_stats: crate::cache::PoolStats,
    pub optimization_stats: crate::optimization::OptimizationStats,
    pub cache_size: usize,
    pub max_cache_size: usize,
    pub cache_utilization: f64,
    pub overall_hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::optimization::OptimizationManager;

    #[test]
    fn test_statement_cache_optimizer_creation() {
        let optimization_manager = Arc::new(OptimizationManager::new(true));
        let optimizer = StatementCacheOptimizer::new(100, optimization_manager);
        
        let stats = optimizer.get_comprehensive_stats();
        assert_eq!(stats.cache_size, 0);
        assert_eq!(stats.max_cache_size, 100);
    }

    #[test]
    fn test_should_use_cache_decisions() {
        let optimization_manager = Arc::new(OptimizationManager::new(true));
        let optimizer = StatementCacheOptimizer::new(100, optimization_manager);

        // Simple queries should use cache
        let simple_hints = OptimizationHints {
            use_fast_path: true,
            cache_result: true,
            use_batch_processing: false,
            skip_translation: false,
            use_prepared_statement: true,
            expected_result_size: crate::query::ResultSize::Small,
            complexity: crate::query::QueryComplexity::Simple,
        };

        assert!(optimizer.should_use_statement_cache(&QueryPattern::SimpleSelect, &simple_hints));
        assert!(optimizer.should_use_statement_cache(&QueryPattern::BatchInsert, &simple_hints));

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

        assert!(!optimizer.should_use_statement_cache(&QueryPattern::ComplexQuery, &complex_hints));
    }

    #[test]
    fn test_enable_disable_caching() {
        let optimization_manager = Arc::new(OptimizationManager::new(true));
        let mut optimizer = StatementCacheOptimizer::new(100, optimization_manager);

        // Test enabling/disabling
        optimizer.set_enabled(false);
        optimizer.set_enabled(true);
        
        // Should be able to clear cache
        optimizer.clear_cache();
    }
}