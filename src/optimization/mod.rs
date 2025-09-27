use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::time::Instant;
use rusqlite::Connection;
use tracing::{debug, info};

use crate::query::{QueryPatternOptimizer, QueryPattern, OptimizationHints, QueryComplexity, ResultSize};
use crate::rewriter::{ContextOptimizer};
use crate::cache::LazySchemaLoader;
use crate::PgSqliteError;

pub mod statement_cache_optimizer;
pub mod read_only_optimizer;
pub mod string_utils;

use read_only_optimizer::ReadOnlyOptimizer;

/// Centralized optimization manager that coordinates all query optimization features
pub struct OptimizationManager {
    pattern_optimizer: Arc<RwLock<QueryPatternOptimizer>>,
    context_optimizer: Arc<RwLock<ContextOptimizer>>,
    lazy_schema_loader: Arc<LazySchemaLoader>,
    read_only_optimizer: Arc<ReadOnlyOptimizer>,
    optimization_stats: Arc<RwLock<OptimizationStats>>,
    enabled: bool,
}

#[derive(Debug, Default, Clone)]
pub struct OptimizationStats {
    pub total_queries: u64,
    pub fast_path_hits: u64,
    pub context_cache_hits: u64,
    pub schema_cache_hits: u64,
    pub pattern_recognition_hits: u64,
    pub total_optimization_time_ms: u64,
}

impl OptimizationManager {
    pub fn new(enabled: bool) -> Self {
        Self {
            pattern_optimizer: Arc::new(RwLock::new(QueryPatternOptimizer::new())),
            context_optimizer: Arc::new(RwLock::new(ContextOptimizer::new(300))), // 5 min TTL
            lazy_schema_loader: Arc::new(LazySchemaLoader::new(600)), // 10 min TTL
            read_only_optimizer: Arc::new(ReadOnlyOptimizer::new(200)), // Cache up to 200 query plans
            optimization_stats: Arc::new(RwLock::new(OptimizationStats::default())),
            enabled,
        }
    }

    /// Analyze a query and return optimization recommendations
    pub fn analyze_query(&self, query: &str) -> Result<QueryOptimizationResult, PgSqliteError> {
        if !self.enabled {
            return Ok(QueryOptimizationResult::no_optimization());
        }

        let start_time = Instant::now();
        
        // Update statistics
        {
            let mut stats = self.optimization_stats.write().unwrap();
            stats.total_queries += 1;
        }

        // Pattern recognition
        let (pattern, hints) = {
            let mut pattern_optimizer = self.pattern_optimizer.write().unwrap();
            let result = pattern_optimizer.analyze_query(query);
            
            // Update stats
            {
                let mut stats = self.optimization_stats.write().unwrap();
                stats.pattern_recognition_hits += 1;
            }
            
            result
        };

        // Generate optimization result
        let result = QueryOptimizationResult {
            pattern,
            should_use_fast_path: hints.use_fast_path,
            should_cache_result: hints.cache_result,
            should_use_batch_processing: hints.use_batch_processing,
            should_skip_translation: hints.skip_translation,
            should_use_prepared_statement: hints.use_prepared_statement,
            estimated_complexity: hints.complexity,
            recommended_execution_strategy: self.recommend_execution_strategy(&hints),
            hints,
        };

        // Update timing statistics
        {
            let mut stats = self.optimization_stats.write().unwrap();
            stats.total_optimization_time_ms += start_time.elapsed().as_millis() as u64;
        }

        debug!("Query optimization analysis completed in {}ms: {:?}", 
               start_time.elapsed().as_millis(), result);

        Ok(result)
    }

    /// Get schema for a table using lazy loading
    pub fn get_table_schema(&self, conn: &Connection, table_name: &str) -> Result<Option<crate::cache::schema::TableSchema>, rusqlite::Error> {
        let result = self.lazy_schema_loader.get_schema(conn, table_name)?;
        
        if result.is_some() {
            let mut stats = self.optimization_stats.write().unwrap();
            stats.schema_cache_hits += 1;
        }
        
        Ok(result)
    }

    /// Preload schemas for multiple tables (useful for JOIN queries)
    pub fn preload_schemas(&self, conn: &Connection, table_names: &[String]) -> Result<(), rusqlite::Error> {
        if !self.enabled {
            return Ok(());
        }

        self.lazy_schema_loader.preload_schemas(conn, table_names)
    }

    /// Optimize context for nested subqueries
    pub fn optimize_context(&self, outer_context: &crate::rewriter::QueryContext, inner_contexts: Vec<crate::rewriter::QueryContext>) -> crate::rewriter::QueryContext {
        if !self.enabled {
            return outer_context.clone();
        }

        let mut context_optimizer = self.context_optimizer.write().unwrap();
        let result = context_optimizer.optimize_nested_context(outer_context, inner_contexts);
        
        {
            let mut stats = self.optimization_stats.write().unwrap();
            stats.context_cache_hits += 1;
        }
        
        result
    }

    /// Get optimization statistics
    pub fn get_stats(&self) -> OptimizationStats {
        let stats = self.optimization_stats.read().unwrap();
        stats.clone()
    }

    /// Reset optimization statistics
    pub fn reset_stats(&self) {
        *self.optimization_stats.write().unwrap() = OptimizationStats::default();
    }

    /// Execute a read-only query using optimizations
    pub fn execute_read_only_query(
        &self, 
        conn: &Connection, 
        query: &str, 
        schema_cache: &crate::cache::SchemaCache
    ) -> Result<Option<crate::session::db_handler::DbResponse>, rusqlite::Error> {
        if !self.enabled {
            return Ok(None);
        }

        self.read_only_optimizer.execute_read_only_query(conn, query, schema_cache)
    }

    /// Get read-only optimizer statistics
    pub fn get_read_only_stats(&self) -> read_only_optimizer::ReadOnlyStats {
        self.read_only_optimizer.get_stats()
    }

    /// Perform periodic maintenance (cleanup caches, etc.)
    pub fn maintenance(&self) {
        if !self.enabled {
            return;
        }

        info!("Performing optimization manager maintenance");

        // Cleanup context cache
        {
            let mut context_optimizer = self.context_optimizer.write().unwrap();
            context_optimizer.cleanup_cache();
        }

        // Cleanup schema cache
        self.lazy_schema_loader.cleanup_cache();

        // Clear pattern cache if it gets too large
        {
            let mut pattern_optimizer = self.pattern_optimizer.write().unwrap();
            let cache_hit_rate = pattern_optimizer.get_cache_hit_rate();
            if cache_hit_rate < 0.5 {
                pattern_optimizer.clear_cache();
                info!("Cleared pattern cache due to low hit rate: {:.2}", cache_hit_rate);
            }
        }

        // Clear read-only cache if hit rate is low
        let read_only_hit_rate = self.read_only_optimizer.get_cache_hit_rate();
        if read_only_hit_rate < 0.3 {
            self.read_only_optimizer.clear_cache();
            info!("Cleared read-only cache due to low hit rate: {:.2}", read_only_hit_rate);
        }
    }

    /// Get overall optimization effectiveness
    pub fn get_effectiveness_metrics(&self) -> OptimizationEffectiveness {
        let stats = self.get_stats();
        let total_queries = stats.total_queries;
        
        if total_queries == 0 {
            return OptimizationEffectiveness::default();
        }

        let fast_path_rate = stats.fast_path_hits as f64 / total_queries as f64;
        let cache_hit_rate = (stats.context_cache_hits + stats.schema_cache_hits) as f64 / (total_queries * 2) as f64;
        let avg_optimization_time = stats.total_optimization_time_ms as f64 / total_queries as f64;

        let pattern_stats = self.pattern_optimizer.read().unwrap().get_pattern_stats();
        let schema_hit_rate = self.lazy_schema_loader.get_cache_hit_rate();
        let avg_schema_load_time = self.lazy_schema_loader.get_average_load_time_ms();

        OptimizationEffectiveness {
            fast_path_rate,
            cache_hit_rate,
            avg_optimization_time_ms: avg_optimization_time,
            schema_cache_hit_rate: schema_hit_rate,
            avg_schema_load_time_ms: avg_schema_load_time,
            pattern_distribution: pattern_stats,
        }
    }

    fn recommend_execution_strategy(&self, hints: &OptimizationHints) -> ExecutionStrategy {
        match hints.complexity {
            QueryComplexity::Simple => {
                if hints.use_fast_path {
                    ExecutionStrategy::UltraFastPath
                } else {
                    ExecutionStrategy::FastPath
                }
            }
            QueryComplexity::Medium => {
                if hints.cache_result {
                    ExecutionStrategy::CachedExecution
                } else {
                    ExecutionStrategy::StandardExecution
                }
            }
            QueryComplexity::Complex => {
                ExecutionStrategy::OptimizedComplex
            }
        }
    }
}

/// Result of query optimization analysis
#[derive(Debug, Clone)]
pub struct QueryOptimizationResult {
    pub pattern: QueryPattern,
    pub hints: OptimizationHints,
    pub should_use_fast_path: bool,
    pub should_cache_result: bool,
    pub should_use_batch_processing: bool,
    pub should_skip_translation: bool,
    pub should_use_prepared_statement: bool,
    pub estimated_complexity: QueryComplexity,
    pub recommended_execution_strategy: ExecutionStrategy,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExecutionStrategy {
    UltraFastPath,
    FastPath,
    CachedExecution,
    StandardExecution,
    OptimizedComplex,
}

#[derive(Debug, Clone)]
pub struct OptimizationEffectiveness {
    pub fast_path_rate: f64,
    pub cache_hit_rate: f64,
    pub avg_optimization_time_ms: f64,
    pub schema_cache_hit_rate: f64,
    pub avg_schema_load_time_ms: f64,
    pub pattern_distribution: HashMap<QueryPattern, u64>,
}

impl QueryOptimizationResult {
    pub fn no_optimization() -> Self {
        Self {
            pattern: QueryPattern::ComplexQuery,
            hints: OptimizationHints {
                use_fast_path: false,
                cache_result: false,
                use_batch_processing: false,
                skip_translation: false,
                use_prepared_statement: false,
                expected_result_size: ResultSize::Unknown,
                complexity: QueryComplexity::Complex,
            },
            should_use_fast_path: false,
            should_cache_result: false,
            should_use_batch_processing: false,
            should_skip_translation: false,
            should_use_prepared_statement: false,
            estimated_complexity: QueryComplexity::Complex,
            recommended_execution_strategy: ExecutionStrategy::StandardExecution,
        }
    }
}

impl Default for OptimizationEffectiveness {
    fn default() -> Self {
        Self {
            fast_path_rate: 0.0,
            cache_hit_rate: 0.0,
            avg_optimization_time_ms: 0.0,
            schema_cache_hit_rate: 0.0,
            avg_schema_load_time_ms: 0.0,
            pattern_distribution: HashMap::new(),
        }
    }
}

impl OptimizationStats {
    pub fn total_queries(&self) -> u64 { self.total_queries }
    pub fn fast_path_hits(&self) -> u64 { self.fast_path_hits }
    pub fn context_cache_hits(&self) -> u64 { self.context_cache_hits }
    pub fn schema_cache_hits(&self) -> u64 { self.schema_cache_hits }
    pub fn pattern_recognition_hits(&self) -> u64 { self.pattern_recognition_hits }
    pub fn total_optimization_time_ms(&self) -> u64 { self.total_optimization_time_ms }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    #[test]
    fn test_optimization_manager_creation() {
        let manager = OptimizationManager::new(true);
        let stats = manager.get_stats();
        assert_eq!(stats.total_queries, 0);
    }
    
    #[test]
    fn test_query_analysis() {
        let manager = OptimizationManager::new(true);
        let result = manager.analyze_query("SELECT * FROM users WHERE id = 1").unwrap();
        
        assert_eq!(result.pattern, QueryPattern::SimpleSelect);
        assert!(result.should_use_fast_path);
        assert!(result.should_cache_result);
        assert_eq!(result.recommended_execution_strategy, ExecutionStrategy::UltraFastPath);
        
        let stats = manager.get_stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.pattern_recognition_hits, 1);
    }
    
    #[test]
    fn test_disabled_optimization() {
        let manager = OptimizationManager::new(false);
        let result = manager.analyze_query("SELECT * FROM users WHERE id = 1").unwrap();
        
        assert_eq!(result.pattern, QueryPattern::ComplexQuery);
        assert!(!result.should_use_fast_path);
        assert_eq!(result.recommended_execution_strategy, ExecutionStrategy::StandardExecution);
    }
    
    #[test]
    fn test_schema_loading() {
        let manager = OptimizationManager::new(true);
        let conn = Connection::open_in_memory().unwrap();
        
        // Create a test table
        conn.execute("CREATE TABLE test_table (id INTEGER, name TEXT)", []).unwrap();
        
        // Load schema
        let schema = manager.get_table_schema(&conn, "test_table").unwrap().unwrap();
        assert_eq!(schema.columns.len(), 2);
        
        // Check that cache hit is recorded
        let stats = manager.get_stats();
        assert_eq!(stats.schema_cache_hits, 1);
    }
    
    #[test]
    fn test_effectiveness_metrics() {
        let manager = OptimizationManager::new(true);
        
        // Analyze a few queries to get stats
        let result1 = manager.analyze_query("SELECT * FROM users WHERE id = 1").unwrap();
        let result2 = manager.analyze_query("INSERT INTO users (name) VALUES ('test')").unwrap();
        let result3 = manager.analyze_query("SELECT COUNT(*) FROM users").unwrap();
        
        // Update fast path hits manually since we're not actually executing queries
        {
            let mut stats = manager.optimization_stats.write().unwrap();
            if result1.should_use_fast_path {
                stats.fast_path_hits += 1;
            }
            if result2.should_use_fast_path {
                stats.fast_path_hits += 1;
            }
            if result3.should_use_fast_path {
                stats.fast_path_hits += 1;
            }
        }
        
        let effectiveness = manager.get_effectiveness_metrics();
        assert!(effectiveness.fast_path_rate >= 0.0);
        assert!(effectiveness.avg_optimization_time_ms >= 0.0);
        assert!(!effectiveness.pattern_distribution.is_empty());
    }
}