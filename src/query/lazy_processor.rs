use rusqlite::Connection;
use crate::cache::SchemaCache;
use crate::query::{QueryTypeDetector, QueryType};
use crate::translator::{BatchDeleteTranslator, BatchUpdateTranslator};
use std::borrow::Cow;

/// Lazy query processor that combines cast translation, decimal rewriting, regex translation,
/// and schema prefix removal to minimize unnecessary processing
pub struct LazyQueryProcessor<'a> {
    original_query: &'a str,
    translated_query: Option<Cow<'a, str>>,
    needs_cast_translation: bool,
    needs_decimal_rewrite: Option<bool>,
    needs_regex_translation: bool,
    needs_schema_translation: bool,
    needs_numeric_cast_translation: bool,
    needs_array_translation: bool,
    needs_delete_using_translation: bool,
    needs_batch_update_translation: bool,
    needs_datetime_translation: bool,
}

impl<'a> LazyQueryProcessor<'a> {
    /// Create a new lazy query processor
    pub fn new(query: &'a str) -> Self {
        Self {
            original_query: query,
            translated_query: None,
            needs_cast_translation: crate::translator::CastTranslator::needs_translation(query),
            needs_decimal_rewrite: None,
            needs_regex_translation: query.contains(" ~ ") || query.contains(" !~ ") || 
                                     query.contains(" ~* ") || query.contains(" !~* "),
            needs_schema_translation: query.contains("pg_catalog.") || query.contains("PG_CATALOG."),
            needs_numeric_cast_translation: crate::translator::NumericCastTranslator::needs_translation(query),
            needs_array_translation: query.contains("[") || query.contains("ANY(") || query.contains("ALL(") ||
                                    query.contains("@>") || query.contains("<@") || query.contains("&&"),
            needs_delete_using_translation: BatchDeleteTranslator::contains_batch_delete(query),
            needs_batch_update_translation: BatchUpdateTranslator::contains_batch_update(query),
            needs_datetime_translation: crate::translator::DateTimeTranslator::needs_translation(query),
        }
    }
    
    /// Get the query for cache lookup (original query if no translation needed)
    pub fn cache_key(&self) -> &str {
        self.original_query
    }
    
    /// Check if the query needs any processing
    pub fn needs_processing(&self, schema_cache: &SchemaCache) -> bool {
        if self.needs_cast_translation {
            return true;
        }
        
        if self.needs_regex_translation {
            return true;
        }
        
        if self.needs_schema_translation {
            return true;
        }
        
        if self.needs_numeric_cast_translation {
            return true;
        }
        
        if self.needs_array_translation {
            return true;
        }
        
        if self.needs_delete_using_translation {
            return true;
        }
        
        if self.needs_batch_update_translation {
            return true;
        }
        
        if self.needs_datetime_translation {
            return true;
        }
        
        // Check decimal rewrite need if not already determined
        if let Some(needs_decimal) = self.needs_decimal_rewrite {
            return needs_decimal;
        }
        
        // For INSERT queries, check if table has decimal columns
        if matches!(QueryTypeDetector::detect_query_type(self.original_query), QueryType::Insert) {
            if let Some(table_name) = extract_insert_table_name(self.original_query) {
                return schema_cache.has_decimal_columns(&table_name);
            }
        }
        
        // For SELECT queries, always assume decimal rewrite might be needed
        // This is conservative but safe
        matches!(
            QueryTypeDetector::detect_query_type(self.original_query),
            QueryType::Select
        )
    }
    
    /// Process the query lazily - only do the work when needed
    pub fn process(&mut self, conn: &Connection, _schema_cache: &SchemaCache) -> Result<&str, rusqlite::Error> {
        // Re-enable translations now that wire protocol cache is disabled
        // tracing::info!("TESTING: All query translations disabled - returning original query: {}", self.original_query);
        // return Ok(self.original_query);
        
        // If already processed, return the result
        if let Some(ref translated) = self.translated_query {
            return Ok(translated.as_ref());
        }
        
        let mut current_query = Cow::Borrowed(self.original_query);
        
        // Step 1: Schema prefix removal if needed
        if self.needs_schema_translation {
            tracing::debug!("Before schema translation: {}", current_query);
            let translated = crate::translator::SchemaPrefixTranslator::translate_query(&current_query);
            tracing::debug!("After schema translation: {}", translated);
            current_query = Cow::Owned(translated);
        }
        
        // Step 2: Numeric cast translation MUST come before general cast translation
        // to ensure CAST(x AS NUMERIC(p,s)) is handled properly
        if self.needs_numeric_cast_translation {
            tracing::debug!("Before numeric cast translation: {}", current_query);
            let translated = crate::translator::NumericCastTranslator::translate_query(&current_query, conn);
            tracing::debug!("After numeric cast translation: {}", translated);
            current_query = Cow::Owned(translated);
        }
        
        // Step 3: Cast translation if needed (after numeric cast translation)
        if self.needs_cast_translation {
            // Debug enum cast issue
            if current_query.contains("casted_status") {
                eprintln!("DEBUG LazyQueryProcessor: Processing cast translation");
                eprintln!("  Current query: {current_query}");
            }
            
            // Check translation cache first
            if let Some(cached) = crate::cache::global_translation_cache().get(self.original_query) {
                current_query = Cow::Owned(cached);
            } else {
                tracing::debug!("Before cast translation: {}", current_query);
                let translated = crate::translator::CastTranslator::translate_query(
                    &current_query,
                    Some(conn)
                );
                tracing::debug!("After cast translation: {}", translated);
                // Cache the translation if it's the original query
                if current_query.as_ref() == self.original_query {
                    crate::cache::global_translation_cache().insert(
                        self.original_query.to_string(),
                        translated.clone()
                    );
                }
                current_query = Cow::Owned(translated);
            }
            
            // Debug enum cast issue
            if self.original_query.contains("casted_status") {
                eprintln!("  After cast translation: {current_query}");
            }
        }
        
        // Step 4: Regex translation if needed
        if self.needs_regex_translation {
            tracing::debug!("Before regex translation: {}", current_query);
            match crate::translator::RegexTranslator::translate_query(&current_query) {
                Ok(translated) => {
                    tracing::debug!("After regex translation: {}", translated);
                    current_query = Cow::Owned(translated);
                }
                Err(e) => {
                    tracing::warn!("Failed to translate regex operators: {}", e);
                    // Continue with original query
                }
            }
        }
        
        // Step 5: Array translation if needed
        if self.needs_array_translation {
            tracing::debug!("Before array translation: {}", current_query);
            match crate::translator::ArrayTranslator::translate_array_operators(&current_query) {
                Ok(translated) => {
                    if translated != current_query {
                        tracing::debug!("After array translation: {}", translated);
                        current_query = Cow::Owned(translated);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to translate array operators: {}", e);
                    // Continue with original query
                }
            }
        }
        
        // Step 6: DELETE USING translation if needed
        if self.needs_delete_using_translation {
            tracing::debug!("Before DELETE USING translation: {}", current_query);
            use std::collections::HashMap;
            use parking_lot::Mutex;
            use std::sync::Arc;
            
            let cache = Arc::new(Mutex::new(HashMap::new()));
            let translator = BatchDeleteTranslator::new(cache);
            let translated = translator.translate(&current_query, &[]);
            tracing::debug!("After DELETE USING translation: {}", translated);
            current_query = Cow::Owned(translated);
        }
        
        // Step 7: Batch UPDATE translation if needed
        if self.needs_batch_update_translation {
            tracing::debug!("Before batch UPDATE translation: {}", current_query);
            use std::collections::HashMap;
            use parking_lot::Mutex;
            use std::sync::Arc;
            
            let cache = Arc::new(Mutex::new(HashMap::new()));
            let translator = BatchUpdateTranslator::new(cache);
            let translated = translator.translate(&current_query, &[]);
            tracing::debug!("After batch UPDATE translation: {}", translated);
            current_query = Cow::Owned(translated);
        }
        
        // Step 8: DateTime translation if needed
        if self.needs_datetime_translation {
            tracing::debug!("Before datetime translation: {}", current_query);
            let translated = crate::translator::DateTimeTranslator::translate_query(&current_query);
            tracing::debug!("After datetime translation: {}", translated);
            current_query = Cow::Owned(translated);
        }
        
        // Step 9: Decimal rewriting if needed  
        let query_type = QueryTypeDetector::detect_query_type(&current_query);
        
        // For performance, only rewrite when necessary
        if matches!(query_type, QueryType::Insert | QueryType::Select) {
            if let Some(table_name) = extract_insert_table_name(&current_query) {
                if _schema_cache.has_decimal_columns(&table_name) {
                    tracing::debug!("Before decimal rewriting: {}", current_query);
                    match rewrite_query_for_decimal(&current_query, conn) {
                        Ok(rewritten) => {
                            if rewritten != current_query {
                                tracing::debug!("After decimal rewriting: {}", rewritten);
                                current_query = Cow::Owned(rewritten);
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Failed to rewrite query for decimal: {}", e);
                            // Continue with original query
                        }
                    }
                }
            } else if matches!(query_type, QueryType::Select) {
                // For SELECT queries, be conservative and always try decimal rewriting
                tracing::debug!("Before decimal rewriting (SELECT): {}", current_query);
                match rewrite_query_for_decimal(&current_query, conn) {
                    Ok(rewritten) => {
                        if rewritten != current_query {
                            tracing::debug!("After decimal rewriting (SELECT): {}", rewritten);
                            current_query = Cow::Owned(rewritten);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to rewrite SELECT query for decimal: {}", e);
                        // Continue with original query
                    }
                }
            }
            
            self.needs_decimal_rewrite = Some(true);
        } else {
            self.needs_decimal_rewrite = Some(false);
        }
        
        // Store the processed query
        self.translated_query = Some(current_query);
        
        Ok(self.translated_query.as_ref().unwrap().as_ref())
    }
    
    /// Get the final query without processing (for fast path scenarios)
    pub fn get_unprocessed(&self) -> &str {
        self.original_query
    }
}

// Helper functions (these should be imported from the appropriate modules)
fn extract_insert_table_name(query: &str) -> Option<String> {
    crate::session::db_handler::extract_insert_table_name(query)
}

fn rewrite_query_for_decimal(query: &str, conn: &Connection) -> Result<String, rusqlite::Error> {
    crate::session::db_handler::rewrite_query_for_decimal(query, conn)
}

