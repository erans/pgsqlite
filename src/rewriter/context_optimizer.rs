use std::collections::HashMap;
use std::time::Instant;
use super::expression_type_resolver::QueryContext;

/// A cached context entry with timestamp for TTL
#[derive(Debug, Clone)]
struct CachedContext {
    context: QueryContext,
    created_at: Instant,
}

/// Optimized context manager for deeply nested queries
pub struct ContextOptimizer {
    /// Cache of computed contexts to avoid recomputation
    context_cache: HashMap<String, CachedContext>,
    /// Cache TTL in seconds
    cache_ttl: u64,
    /// Statistics for monitoring
    cache_hits: u64,
    cache_misses: u64,
}

impl ContextOptimizer {
    pub fn new(cache_ttl: u64) -> Self {
        Self {
            context_cache: HashMap::new(),
            cache_ttl,
            cache_hits: 0,
            cache_misses: 0,
        }
    }

    /// Get or create a context for a query, using caching for performance
    pub fn get_or_create_context<F>(&mut self, query_hash: &str, builder: F) -> QueryContext
    where
        F: FnOnce() -> QueryContext,
    {
        // Check cache first
        if let Some(cached) = self.context_cache.get(query_hash)
            && cached.created_at.elapsed().as_secs() < self.cache_ttl {
                self.cache_hits += 1;
                return cached.context.clone();
            }

        // Cache miss - compute new context
        self.cache_misses += 1;
        let context = builder();
        
        // Cache the result
        self.context_cache.insert(query_hash.to_string(), CachedContext {
            context: context.clone(),
            created_at: Instant::now(),
        });
        
        context
    }

    /// Merge multiple contexts efficiently using a builder pattern
    pub fn merge_contexts(&self, contexts: Vec<QueryContext>) -> QueryContext {
        let mut merged = QueryContext::default();
        
        // Pre-allocate capacity for better performance
        let total_aliases = contexts.iter().map(|c| c.table_aliases.len()).sum();
        let total_ctes = contexts.iter().map(|c| c.cte_columns.len()).sum();
        let total_derived = contexts.iter().map(|c| c.derived_table_columns.len()).sum();
        
        merged.table_aliases.reserve(total_aliases);
        merged.cte_columns.reserve(total_ctes);
        merged.derived_table_columns.reserve(total_derived);
        
        // Merge contexts in order
        for context in contexts {
            merged.table_aliases.extend(context.table_aliases);
            merged.cte_columns.extend(context.cte_columns);
            merged.derived_table_columns.extend(context.derived_table_columns);
            
            // Keep the first non-None default table
            if merged.default_table.is_none() {
                merged.default_table = context.default_table;
            }
        }
        
        merged
    }

    /// Optimize context for nested subqueries by creating a hierarchical structure
    pub fn optimize_nested_context(&mut self, outer_context: &QueryContext, inner_contexts: Vec<QueryContext>) -> QueryContext {
        // Create a combined context that prioritizes inner scope but falls back to outer scope
        let mut optimized = QueryContext::default();
        
        // Start with outer context as base
        optimized.table_aliases.extend(outer_context.table_aliases.clone());
        optimized.cte_columns.extend(outer_context.cte_columns.clone());
        optimized.derived_table_columns.extend(outer_context.derived_table_columns.clone());
        optimized.default_table = outer_context.default_table.clone();
        
        // Layer inner contexts on top (later contexts override earlier ones)
        for inner_context in inner_contexts {
            // Inner context overrides outer context
            optimized.table_aliases.extend(inner_context.table_aliases);
            optimized.cte_columns.extend(inner_context.cte_columns);
            optimized.derived_table_columns.extend(inner_context.derived_table_columns);
            
            // Inner context's default table takes precedence
            if inner_context.default_table.is_some() {
                optimized.default_table = inner_context.default_table;
            }
        }
        
        optimized
    }

    /// Clear expired cache entries to prevent memory leaks
    pub fn cleanup_cache(&mut self) {
        let now = Instant::now();
        self.context_cache.retain(|_, cached| {
            now.duration_since(cached.created_at).as_secs() < self.cache_ttl
        });
    }

    /// Get cache statistics for monitoring
    pub fn get_stats(&self) -> (u64, u64, f64) {
        let total_requests = self.cache_hits + self.cache_misses;
        let hit_rate = if total_requests > 0 {
            self.cache_hits as f64 / total_requests as f64
        } else {
            0.0
        };
        (self.cache_hits, self.cache_misses, hit_rate)
    }

    /// Reset cache statistics
    pub fn reset_stats(&mut self) {
        self.cache_hits = 0;
        self.cache_misses = 0;
    }
}

/// Enhanced QueryContext with optimization features
pub trait QueryContextExt {
    /// Find table for column with fallback chain
    fn find_table_for_column_optimized(&self, column: &str, fallback_tables: &[String]) -> Option<String>;
    
    /// Check if context contains a specific table
    fn contains_table(&self, table: &str) -> bool;
    
    /// Get all available tables in this context
    fn get_all_tables(&self) -> Vec<String>;
}

impl QueryContextExt for QueryContext {
    fn find_table_for_column_optimized(&self, column: &str, fallback_tables: &[String]) -> Option<String> {
        // First check if we have a default table
        if let Some(default) = &self.default_table {
            return Some(default.clone());
        }
        
        // Check CTE columns
        for (cte_name, columns) in &self.cte_columns {
            if columns.iter().any(|(col_name, _)| col_name == column) {
                return Some(cte_name.clone());
            }
        }
        
        // Check derived table columns
        for (table_name, columns) in &self.derived_table_columns {
            if columns.iter().any(|(col_name, _)| col_name == column) {
                return Some(table_name.clone());
            }
        }
        
        // Check table aliases
        for table in fallback_tables {
            if self.table_aliases.contains_key(table) {
                return Some(table.clone());
            }
        }
        
        None
    }
    
    fn contains_table(&self, table: &str) -> bool {
        self.table_aliases.contains_key(table) || 
        self.cte_columns.contains_key(table) ||
        self.derived_table_columns.contains_key(table) ||
        self.default_table.as_ref().is_some_and(|t| t == table)
    }
    
    fn get_all_tables(&self) -> Vec<String> {
        let mut tables = Vec::new();
        
        if let Some(default) = &self.default_table {
            tables.push(default.clone());
        }
        
        tables.extend(self.table_aliases.keys().cloned());
        tables.extend(self.cte_columns.keys().cloned());
        tables.extend(self.derived_table_columns.keys().cloned());
        
        tables.sort();
        tables.dedup();
        tables
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_context_caching() {
        let mut optimizer = ContextOptimizer::new(300); // 5 minutes TTL
        
        let context1 = optimizer.get_or_create_context("query1", || {
            QueryContext {
                default_table: Some("table1".to_string()),
                ..Default::default()
            }
        });
        
        let context2 = optimizer.get_or_create_context("query1", || {
            panic!("Should not be called - should hit cache");
        });
        
        assert_eq!(context1.default_table, context2.default_table);
        
        let (hits, misses, hit_rate) = optimizer.get_stats();
        assert_eq!(hits, 1);
        assert_eq!(misses, 1);
        assert_eq!(hit_rate, 0.5);
    }
    
    #[test]
    fn test_context_merging() {
        let optimizer = ContextOptimizer::new(300);
        
        let mut ctx1 = QueryContext::default();
        ctx1.table_aliases.insert("t1".to_string(), "table1".to_string());
        
        let mut ctx2 = QueryContext::default();
        ctx2.table_aliases.insert("t2".to_string(), "table2".to_string());
        ctx2.default_table = Some("table2".to_string());
        
        let merged = optimizer.merge_contexts(vec![ctx1, ctx2]);
        
        assert_eq!(merged.table_aliases.len(), 2);
        assert_eq!(merged.default_table, Some("table2".to_string()));
    }
    
    #[test]
    fn test_nested_context_optimization() {
        let mut optimizer = ContextOptimizer::new(300);
        
        let mut outer = QueryContext {
            default_table: Some("outer_table".to_string()),
            ..Default::default()
        };
        outer.table_aliases.insert("o".to_string(), "outer_table".to_string());
        
        let mut inner = QueryContext {
            default_table: Some("inner_table".to_string()),
            ..Default::default()
        };
        inner.table_aliases.insert("i".to_string(), "inner_table".to_string());
        
        let optimized = optimizer.optimize_nested_context(&outer, vec![inner]);
        
        // Inner context should override outer
        assert_eq!(optimized.default_table, Some("inner_table".to_string()));
        assert_eq!(optimized.table_aliases.len(), 2);
        assert!(optimized.table_aliases.contains_key("o"));
        assert!(optimized.table_aliases.contains_key("i"));
    }
}