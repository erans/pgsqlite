use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use rusqlite::Connection;
use crate::cache::SchemaCache;
use crate::session::db_handler::DbResponse;
use crate::query::QueryComplexity;
use tracing::{debug, info};

/// Direct read-only access optimizer for SELECT queries
/// Bypasses many overhead layers for simple SELECT operations
pub struct ReadOnlyOptimizer {
    /// Cache for pre-compiled query plans
    query_plans: Arc<RwLock<HashMap<String, CachedQueryPlan>>>,
    /// Maximum number of cached plans
    max_cache_size: usize,
    /// Statistics for monitoring
    stats: Arc<RwLock<ReadOnlyStats>>,
}

/// A cached query execution plan for fast repeated execution
#[derive(Debug, Clone)]
struct CachedQueryPlan {
    /// Original query text
    query: String,
    /// Table being queried
    #[allow(dead_code)]
    table_name: String,
    /// Column names in result order
    columns: Vec<String>,
    /// Column types for proper conversion
    column_types: Vec<Option<String>>,
    /// Whether this query has WHERE clauses
    #[allow(dead_code)]
    has_where_clause: bool,
    /// Query complexity classification
    complexity: QueryComplexity,
    /// Last access time for LRU eviction
    last_used: Instant,
    /// Access count for priority calculation
    access_count: u64,
    /// Average execution time
    #[allow(dead_code)]
    avg_execution_time: Duration,
    /// Success rate (for reliability scoring)
    success_rate: f64,
}


/// Statistics for read-only optimizer
#[derive(Debug, Default, Clone)]
pub struct ReadOnlyStats {
    pub total_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub fast_path_executions: u64,
    pub avg_execution_time_ms: f64,
    pub connection_pool_hits: u64,
    pub successful_optimizations: u64,
    pub failed_optimizations: u64,
}

impl ReadOnlyOptimizer {
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            query_plans: Arc::new(RwLock::new(HashMap::new())),
            max_cache_size,
            stats: Arc::new(RwLock::new(ReadOnlyStats::default())),
        }
    }

    /// Execute a SELECT query using read-only optimizations
    pub fn execute_read_only_query(
        &self,
        primary_conn: &Connection,
        query: &str,
        schema_cache: &SchemaCache,
    ) -> Result<Option<DbResponse>, rusqlite::Error> {
        let start_time = Instant::now();

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.total_queries += 1;
        }

        // Check if this query can use read-only optimization
        if !self.can_use_read_only_optimization(query) {
            return Ok(None);
        }

        // Generate cache key
        let cache_key = self.generate_cache_key(query);

        // Check cache for query plan
        if let Some(mut plan) = self.get_cached_plan(&cache_key) {
            // Cache hit - execute with cached plan
            debug!("Read-only cache hit for query: {}", query);
            
            // Update access stats
            plan.access_count += 1;
            plan.last_used = Instant::now();
            self.update_cached_plan(cache_key, plan.clone());

            // Update stats
            {
                let mut stats = self.stats.write().unwrap();
                stats.cache_hits += 1;
            }

            // Execute using cached plan
            return self.execute_with_cached_plan(primary_conn, &plan, query);
        }

        // Cache miss - analyze and create new plan
        debug!("Read-only cache miss, analyzing query: {}", query);
        
        if let Some(plan) = self.analyze_and_create_plan(query, schema_cache, primary_conn)? {
            // Cache the new plan
            self.cache_query_plan(cache_key, plan.clone());
            
            // Update stats
            {
                let mut stats = self.stats.write().unwrap();
                stats.cache_misses += 1;
            }

            // Execute with new plan
            let result = self.execute_with_cached_plan(primary_conn, &plan, query);
            
            // Update execution time statistics
            let execution_time = start_time.elapsed();
            self.update_execution_stats(&plan.query, execution_time, result.is_ok());
            
            return result;
        }

        // Could not optimize
        {
            let mut stats = self.stats.write().unwrap();
            stats.failed_optimizations += 1;
        }

        Ok(None)
    }

    /// Check if a query can use read-only optimization
    fn can_use_read_only_optimization(&self, query: &str) -> bool {
        let query_upper = query.to_uppercase();
        
        // Must be a SELECT query
        if !query_upper.trim().starts_with("SELECT") {
            return false;
        }

        // Exclude complex queries that need full processing
        if query_upper.contains("JOIN") ||
           query_upper.contains("UNION") ||
           query_upper.contains("SUBQUERY") ||
           query_upper.contains("WITH") ||
           query_upper.contains("CASE") ||
           query_upper.contains("WINDOW") ||
           query_upper.contains("RECURSIVE") {
            return false;
        }

        // Exclude queries with functions that might need translation
        if query_upper.contains("NOW()") ||
           query_upper.contains("CURRENT_TIMESTAMP") ||
           query_upper.contains("CURRENT_DATE") ||
           query_upper.contains("CURRENT_TIME") ||
           query_upper.contains("::") ||  // Type casts
           query_upper.contains("EXTRACT") ||
           query_upper.contains("DATE_TRUNC") {
            return false;
        }

        true
    }

    /// Generate a normalized cache key for the query
    fn generate_cache_key(&self, query: &str) -> String {
        // Simple normalization - remove extra whitespace and convert to lowercase
        query
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    }

    /// Get cached query plan if available
    fn get_cached_plan(&self, cache_key: &str) -> Option<CachedQueryPlan> {
        let plans = self.query_plans.read().unwrap();
        plans.get(cache_key).cloned()
    }

    /// Update cached query plan
    fn update_cached_plan(&self, cache_key: String, plan: CachedQueryPlan) {
        let mut plans = self.query_plans.write().unwrap();
        plans.insert(cache_key, plan);
    }

    /// Cache a new query plan
    fn cache_query_plan(&self, cache_key: String, plan: CachedQueryPlan) {
        let mut plans = self.query_plans.write().unwrap();
        
        // Check if we need to evict entries
        if plans.len() >= self.max_cache_size {
            self.evict_least_valuable_plan(&mut plans);
        }
        
        plans.insert(cache_key, plan);
    }

    /// Evict the least valuable cached plan
    fn evict_least_valuable_plan(&self, plans: &mut HashMap<String, CachedQueryPlan>) {
        if plans.is_empty() {
            return;
        }

        let mut lowest_score = f64::MAX;
        let mut evict_key = String::new();

        for (key, plan) in plans.iter() {
            // Calculate value score: (access_count / age) * success_rate
            let age_hours = plan.last_used.elapsed().as_secs() as f64 / 3600.0;
            let access_frequency = plan.access_count as f64;
            let value_score = (access_frequency / (1.0 + age_hours)) * plan.success_rate;

            if value_score < lowest_score {
                lowest_score = value_score;
                evict_key = key.clone();
            }
        }

        if !evict_key.is_empty() {
            plans.remove(&evict_key);
            debug!("Evicted read-only query plan: {} (score: {:.2})", evict_key, lowest_score);
        }
    }

    /// Analyze query and create execution plan
    fn analyze_and_create_plan(
        &self,
        query: &str,
        schema_cache: &SchemaCache,
        conn: &Connection,
    ) -> Result<Option<CachedQueryPlan>, rusqlite::Error> {
        // Extract table name from query
        let table_name = match self.extract_table_name(query) {
            Some(name) => name,
            None => return Ok(None),
        };

        // Prepare statement to get column information
        let stmt = conn.prepare(query)?;
        let column_count = stmt.column_count();
        
        // Get column names
        let mut columns = Vec::new();
        for i in 0..column_count {
            columns.push(stmt.column_name(i)?.to_string());
        }

        // Get column types from schema cache
        let mut column_types = Vec::new();
        if let Ok(table_schema) = schema_cache.get_or_load(conn, &table_name) {
            for col_name in &columns {
                if let Some(col_info) = table_schema.column_map.get(&col_name.to_lowercase()) {
                    column_types.push(Some(col_info.pg_type.clone()));
                } else {
                    column_types.push(None);
                }
            }
        } else {
            column_types.resize(columns.len(), None);
        }

        // Determine query complexity
        let complexity = self.classify_query_complexity(query);

        // Check for WHERE clause
        let has_where_clause = query.to_uppercase().contains("WHERE");

        Ok(Some(CachedQueryPlan {
            query: query.to_string(),
            table_name,
            columns,
            column_types,
            has_where_clause,
            complexity,
            last_used: Instant::now(),
            access_count: 0,
            avg_execution_time: Duration::from_millis(0),
            success_rate: 1.0,
        }))
    }

    /// Execute query using cached plan
    fn execute_with_cached_plan(
        &self,
        conn: &Connection,
        plan: &CachedQueryPlan,
        query: &str,
    ) -> Result<Option<DbResponse>, rusqlite::Error> {
        // For simple queries, use direct execution
        if matches!(plan.complexity, QueryComplexity::Simple) {
            return self.execute_simple_query_direct(conn, query, plan);
        }

        // For more complex queries, use optimized execution
        self.execute_optimized_query(conn, query, plan)
    }

    /// Execute simple query directly with minimal overhead
    fn execute_simple_query_direct(
        &self,
        conn: &Connection,
        query: &str,
        plan: &CachedQueryPlan,
    ) -> Result<Option<DbResponse>, rusqlite::Error> {
        let mut stmt = conn.prepare(query)?;
        let mut rows = Vec::new();

        // Execute query with pre-cached column information
        let result_rows = stmt.query_map([], |row| {
            let mut values = Vec::new();
            for (i, col_type) in plan.column_types.iter().enumerate() {
                match row.get_ref(i)? {
                    rusqlite::types::ValueRef::Null => values.push(None),
                    rusqlite::types::ValueRef::Integer(int_val) => {
                        // Use cached type information for conversion
                        if let Some(pg_type) = col_type {
                            match pg_type.to_lowercase().as_str() {
                                "boolean" | "bool" => {
                                    let bool_str = if int_val == 0 { "f" } else { "t" };
                                    values.push(Some(bool_str.as_bytes().to_vec()));
                                }
                                "date" => {
                                    use crate::types::datetime_utils::format_days_to_date_buf;
                                    let mut buf = vec![0u8; 32];
                                    let len = format_days_to_date_buf(int_val as i32, &mut buf);
                                    buf.truncate(len);
                                    values.push(Some(buf));
                                }
                                "time" | "timetz" => {
                                    use crate::types::datetime_utils::format_microseconds_to_time_buf;
                                    let mut buf = vec![0u8; 32];
                                    let len = format_microseconds_to_time_buf(int_val, &mut buf);
                                    buf.truncate(len);
                                    values.push(Some(buf));
                                }
                                "timestamp" | "timestamptz" => {
                                    use crate::types::datetime_utils::format_microseconds_to_timestamp_buf;
                                    let mut buf = vec![0u8; 64];
                                    let len = format_microseconds_to_timestamp_buf(int_val, &mut buf);
                                    buf.truncate(len);
                                    values.push(Some(buf));
                                }
                                _ => {
                                    values.push(Some(int_val.to_string().into_bytes()));
                                }
                            }
                        } else {
                            values.push(Some(int_val.to_string().into_bytes()));
                        }
                    }
                    rusqlite::types::ValueRef::Real(f) => {
                        values.push(Some(f.to_string().into_bytes()));
                    }
                    rusqlite::types::ValueRef::Text(s) => {
                        values.push(Some(s.to_vec()));
                    }
                    rusqlite::types::ValueRef::Blob(b) => {
                        values.push(Some(b.to_vec()));
                    }
                }
            }
            Ok(values)
        })?;

        for row in result_rows {
            rows.push(row?);
        }

        // Update stats
        {
            let mut stats = self.stats.write().unwrap();
            stats.fast_path_executions += 1;
        }

        Ok(Some(DbResponse {
            columns: plan.columns.clone(),
            rows,
            rows_affected: 0,
        }))
    }

    /// Execute optimized query for medium complexity
    fn execute_optimized_query(
        &self,
        conn: &Connection,
        query: &str,
        plan: &CachedQueryPlan,
    ) -> Result<Option<DbResponse>, rusqlite::Error> {
        // For now, use the same direct execution
        // In the future, this could implement query-specific optimizations
        self.execute_simple_query_direct(conn, query, plan)
    }

    /// Extract table name from SELECT query
    fn extract_table_name(&self, query: &str) -> Option<String> {
        let query_upper = query.to_uppercase();
        
        // Find FROM clause
        if let Some(from_pos) = query_upper.find(" FROM ") {
            let after_from = &query[from_pos + 6..].trim();
            
            // Find end of table name
            let end = after_from.find(' ')
                .or_else(|| after_from.find(';'))
                .or_else(|| after_from.find('\n'))
                .unwrap_or(after_from.len());
            
            let table_name = after_from[..end].trim();
            
            // Remove quotes if present
            let table_name = table_name.trim_matches('"').trim_matches('\'');
            
            if !table_name.is_empty() {
                return Some(table_name.to_string());
            }
        }
        
        None
    }

    /// Classify query complexity for optimization decisions
    fn classify_query_complexity(&self, query: &str) -> QueryComplexity {
        let query_upper = query.to_uppercase();
        
        // Count complexity indicators
        let mut complexity_score = 0;
        
        if query_upper.contains("WHERE") { complexity_score += 1; }
        if query_upper.contains("ORDER BY") { complexity_score += 1; }
        if query_upper.contains("GROUP BY") { complexity_score += 2; }
        if query_upper.contains("HAVING") { complexity_score += 2; }
        if query_upper.contains("LIMIT") { complexity_score += 1; }
        if query_upper.contains("OFFSET") { complexity_score += 1; }
        
        // Check for aggregate functions
        if query_upper.contains("COUNT(") || 
           query_upper.contains("SUM(") || 
           query_upper.contains("AVG(") || 
           query_upper.contains("MAX(") || 
           query_upper.contains("MIN(") {
            complexity_score += 1;
        }
        
        match complexity_score {
            0..=1 => QueryComplexity::Simple,
            2..=4 => QueryComplexity::Medium,
            _ => QueryComplexity::Complex,
        }
    }

    /// Update execution statistics
    fn update_execution_stats(&self, _query: &str, execution_time: Duration, success: bool) {
        let mut stats = self.stats.write().unwrap();
        
        // Update average execution time
        let total_time = stats.avg_execution_time_ms * stats.total_queries as f64;
        let new_time = execution_time.as_millis() as f64;
        stats.avg_execution_time_ms = (total_time + new_time) / (stats.total_queries + 1) as f64;
        
        if success {
            stats.successful_optimizations += 1;
        } else {
            stats.failed_optimizations += 1;
        }
    }

    /// Get current statistics
    pub fn get_stats(&self) -> ReadOnlyStats {
        let stats = self.stats.read().unwrap();
        stats.clone()
    }

    /// Clear all cached plans
    pub fn clear_cache(&self) {
        let mut plans = self.query_plans.write().unwrap();
        plans.clear();
        
        info!("Cleared read-only optimizer cache");
    }

    /// Get cache hit rate
    pub fn get_cache_hit_rate(&self) -> f64 {
        let stats = self.stats.read().unwrap();
        if stats.total_queries > 0 {
            stats.cache_hits as f64 / stats.total_queries as f64
        } else {
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::SchemaCache;

    #[test]
    fn test_read_only_optimizer_creation() {
        let optimizer = ReadOnlyOptimizer::new(100);
        let stats = optimizer.get_stats();
        assert_eq!(stats.total_queries, 0);
        assert_eq!(optimizer.get_cache_hit_rate(), 0.0);
    }

    #[test]
    fn test_can_use_read_only_optimization() {
        let optimizer = ReadOnlyOptimizer::new(100);
        
        // Simple SELECT should be optimizable
        assert!(optimizer.can_use_read_only_optimization("SELECT * FROM users"));
        assert!(optimizer.can_use_read_only_optimization("SELECT id, name FROM products WHERE active = 1"));
        
        // Complex queries should not be optimizable
        assert!(!optimizer.can_use_read_only_optimization("SELECT * FROM users JOIN orders ON users.id = orders.user_id"));
        assert!(!optimizer.can_use_read_only_optimization("SELECT * FROM users WHERE created_at > NOW()"));
        assert!(!optimizer.can_use_read_only_optimization("UPDATE users SET name = 'test'"));
        assert!(!optimizer.can_use_read_only_optimization("SELECT COUNT(*) FROM users UNION SELECT COUNT(*) FROM orders"));
    }

    #[test]
    fn test_cache_key_generation() {
        let optimizer = ReadOnlyOptimizer::new(100);
        
        // Test normalization
        let key1 = optimizer.generate_cache_key("SELECT * FROM users");
        let key2 = optimizer.generate_cache_key("  SELECT   *   FROM   users  ");
        let key3 = optimizer.generate_cache_key("select * from users");
        
        assert_eq!(key1, key2);
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_table_name_extraction() {
        let optimizer = ReadOnlyOptimizer::new(100);
        
        assert_eq!(optimizer.extract_table_name("SELECT * FROM users"), Some("users".to_string()));
        assert_eq!(optimizer.extract_table_name("SELECT id, name FROM products WHERE active = 1"), Some("products".to_string()));
        assert_eq!(optimizer.extract_table_name("SELECT * FROM \"quoted_table\""), Some("quoted_table".to_string()));
        assert_eq!(optimizer.extract_table_name("SELECT * FROM users;"), Some("users".to_string()));
        
        // Should not extract from complex queries
        assert_eq!(optimizer.extract_table_name("SELECT * FROM users JOIN orders"), Some("users".to_string()));
    }

    #[test]
    fn test_query_complexity_classification() {
        let optimizer = ReadOnlyOptimizer::new(100);
        
        // Simple queries
        assert!(matches!(optimizer.classify_query_complexity("SELECT * FROM users"), QueryComplexity::Simple));
        assert!(matches!(optimizer.classify_query_complexity("SELECT * FROM users WHERE id = 1"), QueryComplexity::Simple));
        
        // Medium complexity queries
        assert!(matches!(optimizer.classify_query_complexity("SELECT * FROM users WHERE id = 1 ORDER BY name"), QueryComplexity::Medium));
        assert!(matches!(optimizer.classify_query_complexity("SELECT COUNT(*) FROM users GROUP BY status"), QueryComplexity::Medium));
        
        // Complex queries
        assert!(matches!(optimizer.classify_query_complexity("SELECT * FROM users WHERE id = 1 ORDER BY name GROUP BY status HAVING COUNT(*) > 5 LIMIT 10 OFFSET 20"), QueryComplexity::Complex));
    }

    #[test]
    fn test_query_execution() {
        let optimizer = ReadOnlyOptimizer::new(100);
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        let schema_cache = SchemaCache::new(3600);
        
        // Create a test table
        conn.execute("CREATE TABLE users (id INTEGER, name TEXT, active BOOLEAN)", []).unwrap();
        conn.execute("INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 0)", []).unwrap();
        
        // Test simple query execution
        let result = optimizer.execute_read_only_query(&conn, "SELECT * FROM users", &schema_cache).unwrap();
        assert!(result.is_some());
        
        let response = result.unwrap();
        assert_eq!(response.columns, vec!["id", "name", "active"]);
        assert_eq!(response.rows.len(), 2);
        
        // Test that stats are updated
        let stats = optimizer.get_stats();
        assert_eq!(stats.total_queries, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.successful_optimizations, 1);
        
        // Test cache hit on second execution
        let result2 = optimizer.execute_read_only_query(&conn, "SELECT * FROM users", &schema_cache).unwrap();
        assert!(result2.is_some());
        
        let stats2 = optimizer.get_stats();
        assert_eq!(stats2.total_queries, 2);
        assert_eq!(stats2.cache_hits, 1);
        assert_eq!(stats2.cache_misses, 1);
        assert!(optimizer.get_cache_hit_rate() > 0.0);
    }
}