use std::borrow::Cow;
use std::cell::RefCell;
use std::collections::HashMap;
use rusqlite::Connection;
use lru::LruCache;
use std::num::NonZeroUsize;
use bitflags::bitflags;
use crate::cache::SchemaCache;
use crate::query::{QueryTypeDetector, QueryType};

bitflags! {
    struct TranslationFlags: u32 {
        const CAST = 0x1;
        const REGEX = 0x2;
        const SCHEMA = 0x4;
        const NUMERIC = 0x8;
        const ARRAY = 0x10;
        const DATETIME = 0x20;
        const DECIMAL = 0x40;
        const BATCH_DELETE = 0x80;
        const BATCH_UPDATE = 0x100;
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[allow(dead_code)]
enum ComplexityLevel {
    Simple,      // No translation needed at all
    SimpleDML,   // Simple DML, possibly with RETURNING (just pass through)
    Moderate,    // Needs one or two translations
    Complex,     // Needs multiple translations
}

thread_local! {
    static COMPLEXITY_CACHE: RefCell<LruCache<u64, ComplexityLevel>> = 
        RefCell::new(LruCache::new(NonZeroUsize::new(1024).unwrap()));
}

/// Ultra-fast unified query processor
pub struct UnifiedProcessor<'a> {
    _query: &'a str,
    _query_bytes: &'a [u8],
    _complexity: ComplexityLevel,
    translations_needed: TranslationFlags,
}

impl<'a> UnifiedProcessor<'a> {
    /// Analyze a query and determine its complexity level
    #[inline(always)]
    fn analyze(query: &'a str) -> Self {
        let query_bytes = query.as_bytes();
        let mut translations = TranslationFlags::empty();
        let mut complexity = ComplexityLevel::Simple;
        
        // Quick length check
        if query.len() < 10 || query.len() > 10000 {
            return Self {
                _query: query,
                _query_bytes: query_bytes,
                _complexity: ComplexityLevel::Complex,
                translations_needed: TranslationFlags::all(),
            };
        }
        
        // Ultra-fast checks using memchr
        if memchr::memmem::find(query_bytes, b"::").is_some() {
            translations.insert(TranslationFlags::CAST);
            complexity = ComplexityLevel::Moderate;
        }
        
        if memchr::memmem::find(query_bytes, b" ~ ").is_some() ||
           memchr::memmem::find(query_bytes, b" !~ ").is_some() ||
           memchr::memmem::find(query_bytes, b" ~* ").is_some() ||
           memchr::memmem::find(query_bytes, b" !~* ").is_some() {
            translations.insert(TranslationFlags::REGEX);
            complexity = ComplexityLevel::Moderate;
        }
        
        if memchr::memmem::find(query_bytes, b"pg_catalog").is_some() ||
           memchr::memmem::find(query_bytes, b"PG_CATALOG").is_some() {
            translations.insert(TranslationFlags::SCHEMA);
            complexity = ComplexityLevel::Moderate;
        }
        
        if memchr::memmem::find(query_bytes, b"::NUMERIC").is_some() ||
           memchr::memmem::find(query_bytes, b"::numeric").is_some() ||
           memchr::memmem::find(query_bytes, b"CAST(").is_some() ||
           memchr::memmem::find(query_bytes, b"cast(").is_some() {
            if query.contains("NUMERIC") || query.contains("DECIMAL") {
                translations.insert(TranslationFlags::NUMERIC);
                complexity = ComplexityLevel::Moderate;
            }
        }
        
        if memchr::memchr(b'[', query_bytes).is_some() ||
           memchr::memmem::find(query_bytes, b"ANY(").is_some() ||
           memchr::memmem::find(query_bytes, b"ALL(").is_some() ||
           memchr::memmem::find(query_bytes, b" @> ").is_some() ||
           memchr::memmem::find(query_bytes, b" <@ ").is_some() ||
           memchr::memmem::find(query_bytes, b" && ").is_some() {
            translations.insert(TranslationFlags::ARRAY);
            complexity = ComplexityLevel::Complex;
        }
        
        if memchr::memmem::find(query_bytes, b"NOW()").is_some() ||
           memchr::memmem::find(query_bytes, b"CURRENT_").is_some() ||
           memchr::memmem::find(query_bytes, b"AT TIME ZONE").is_some() {
            translations.insert(TranslationFlags::DATETIME);
            complexity = ComplexityLevel::Complex;
        }
        
        // Check for datetime patterns in INSERT/UPDATE statements
        if query_bytes.starts_with(b"INSERT") || query_bytes.starts_with(b"UPDATE") {
            if memchr::memchr(b'\'', query_bytes).is_some() {
                // Check for date pattern YYYY-MM-DD or time pattern HH:MM:SS
                if memchr::memchr(b'-', query_bytes).is_some() ||
                   memchr::memchr(b':', query_bytes).is_some() {
                    translations.insert(TranslationFlags::DATETIME);
                    complexity = ComplexityLevel::Moderate;
                }
            }
        }
        
        // Check for batch operations
        if let Some(delete_pos) = memchr::memmem::find(query_bytes, b"DELETE") {
            if memchr::memmem::find(&query_bytes[delete_pos..], b"USING").is_some() {
                translations.insert(TranslationFlags::BATCH_DELETE);
                complexity = ComplexityLevel::Complex;
            }
        }
        
        if let Some(update_pos) = memchr::memmem::find(query_bytes, b"UPDATE") {
            if let Some(set_pos) = memchr::memmem::find(&query_bytes[update_pos..], b" SET ") {
                if memchr::memmem::find(&query_bytes[update_pos + set_pos..], b" FROM ").is_some() {
                    translations.insert(TranslationFlags::BATCH_UPDATE);
                    complexity = ComplexityLevel::Complex;
                }
            }
        }
        
        // If multiple translations needed, it's complex
        if translations.bits().count_ones() > 2 {
            complexity = ComplexityLevel::Complex;
        }
        
        Self {
            _query: query,
            _query_bytes: query_bytes,
            _complexity: complexity,
            translations_needed: translations,
        }
    }
    
    #[inline(always)]
    fn needs_translation(&self, flag: TranslationFlags) -> bool {
        self.translations_needed.contains(flag)
    }
}

/// Main entry point - ultra-optimized for simple queries
#[inline(always)]
pub fn process_query<'a>(
    query: &'a str,
    conn: &Connection,
    schema_cache: &SchemaCache,
) -> Result<Cow<'a, str>, rusqlite::Error> {
    // Quick length check
    let len = query.len();
    if len < 10 || len > 10000 {
        return process_complex_query(query, conn, schema_cache);
    }
    
    let bytes = query.as_bytes();
    
    // Ultra-fast first byte check for query type
    let first_byte = bytes[0].to_ascii_uppercase();
    
    match first_byte {
        b'S' if len >= 7 && bytes[..7].eq_ignore_ascii_case(b"SELECT ") => {
            // SELECT queries - fast path for simple ones
            if !has_any_special_pattern_fast(bytes) {
                return Ok(Cow::Borrowed(query)); // Zero allocation!
            }
        }
        b'I' if len >= 12 && bytes[..12].eq_ignore_ascii_case(b"INSERT INTO ") => {
            // INSERT queries - check for patterns that need translation
            if !has_insert_special_patterns(bytes) {
                // Even with RETURNING, if it's simple, pass through
                if let Some(ret_pos) = find_returning_fast(bytes) {
                    if is_simple_returning(&bytes[ret_pos..]) {
                        tracing::debug!("UNIFIED: Simple INSERT with RETURNING using fast path: {}", query);
                        return Ok(Cow::Borrowed(query));
                    }
                    tracing::debug!("UNIFIED: Complex RETURNING, needs processing: {}", query);
                    // Complex RETURNING, needs processing
                } else {
                    // No RETURNING, simple INSERT
                    tracing::debug!("UNIFIED: Simple INSERT without RETURNING using fast path: {}", query);
                    return Ok(Cow::Borrowed(query));
                }
            } else {
                tracing::debug!("UNIFIED: INSERT has special patterns, needs processing: {}", query);
            }
        }
        b'U' if len >= 7 && bytes[..7].eq_ignore_ascii_case(b"UPDATE ") => {
            // UPDATE queries
            if !has_update_special_patterns(bytes) {
                // Check for RETURNING
                if let Some(ret_pos) = find_returning_fast(bytes) {
                    if is_simple_returning(&bytes[ret_pos..]) {
                        return Ok(Cow::Borrowed(query));
                    }
                } else {
                    return Ok(Cow::Borrowed(query));
                }
            }
        }
        b'D' if len >= 12 && bytes[..12].eq_ignore_ascii_case(b"DELETE FROM ") => {
            // DELETE queries
            if !has_delete_special_patterns(bytes) {
                // Check for RETURNING
                if let Some(ret_pos) = find_returning_fast(bytes) {
                    if is_simple_returning(&bytes[ret_pos..]) {
                        return Ok(Cow::Borrowed(query));
                    }
                } else {
                    return Ok(Cow::Borrowed(query));
                }
            }
        }
        _ => {} // Fall through to complex processing
    }
    
    // Complex query processing
    process_complex_query(query, conn, schema_cache)
}

/// Check if query has any special patterns that need translation
#[inline(always)]
fn has_any_special_pattern_fast(bytes: &[u8]) -> bool {
    // Most common patterns that need translation
    memchr::memmem::find(bytes, b"::").is_some() ||
    memchr::memmem::find(bytes, b"NOW()").is_some() ||
    memchr::memmem::find(bytes, b"CURRENT_").is_some() ||
    memchr::memmem::find(bytes, b" ~ ").is_some() ||
    memchr::memmem::find(bytes, b"pg_catalog").is_some() ||
    memchr::memmem::find(bytes, b"PG_CATALOG").is_some() ||
    memchr::memmem::find(bytes, b"CAST(").is_some() ||
    memchr::memmem::find(bytes, b"cast(").is_some() ||
    memchr::memchr(b'[', bytes).is_some() ||
    memchr::memmem::find(bytes, b"ANY(").is_some() ||
    memchr::memmem::find(bytes, b"ALL(").is_some() ||
    memchr::memmem::find(bytes, b" @> ").is_some() ||
    memchr::memmem::find(bytes, b" <@ ").is_some() ||
    memchr::memmem::find(bytes, b" && ").is_some() ||
    memchr::memmem::find(bytes, b"JOIN").is_some() ||
    memchr::memmem::find(bytes, b"UNION").is_some() ||
    memchr::memmem::find(bytes, b"(SELECT").is_some() ||
    memchr::memmem::find(bytes, b"GROUP BY").is_some() ||
    memchr::memmem::find(bytes, b"HAVING").is_some() ||
    memchr::memmem::find(bytes, b"unnest").is_some() ||
    memchr::memmem::find(bytes, b"UNNEST").is_some()
}

/// Check INSERT-specific patterns
#[inline(always)]
fn has_insert_special_patterns(bytes: &[u8]) -> bool {
    // Check for datetime patterns (dates with - or times with :)
    if memchr::memchr(b'\'', bytes).is_some() {
        if memchr::memchr(b'-', bytes).is_some() || memchr::memchr(b':', bytes).is_some() {
            return true;
        }
    }
    
    // Check for array literals
    if memchr::memchr(b'{', bytes).is_some() || memchr::memmem::find(bytes, b"ARRAY[").is_some() {
        return true;
    }
    
    // Check common special patterns
    has_any_special_pattern_fast(bytes)
}

/// Check UPDATE-specific patterns
#[inline(always)]
fn has_update_special_patterns(bytes: &[u8]) -> bool {
    // Check for UPDATE ... FROM
    if let Some(set_pos) = memchr::memmem::find(bytes, b" SET ") {
        if memchr::memmem::find(&bytes[set_pos..], b" FROM ").is_some() {
            return true;
        }
    }
    
    has_any_special_pattern_fast(bytes)
}

/// Check DELETE-specific patterns
#[inline(always)]
fn has_delete_special_patterns(bytes: &[u8]) -> bool {
    // Check for DELETE ... USING
    if memchr::memmem::find(bytes, b"USING").is_some() {
        return true;
    }
    
    has_any_special_pattern_fast(bytes)
}

/// Find RETURNING clause position
#[inline(always)]
fn find_returning_fast(bytes: &[u8]) -> Option<usize> {
    // Check both cases with SIMD-optimized search
    memchr::memmem::find(bytes, b"RETURNING")
        .or_else(|| memchr::memmem::find(bytes, b"returning"))
}

/// Check if RETURNING clause is simple (just column names)
#[inline(always)]
fn is_simple_returning(bytes: &[u8]) -> bool {
    // Skip "RETURNING" (9 chars) and whitespace
    let after_returning = if bytes.len() > 9 {
        &bytes[9..]
    } else {
        return false;
    };
    
    // Skip whitespace
    let mut i = 0;
    while i < after_returning.len() && after_returning[i].is_ascii_whitespace() {
        i += 1;
    }
    
    if i >= after_returning.len() {
        return false; // Empty RETURNING
    }
    
    let content = &after_returning[i..];
    
    // Special case: RETURNING * is simple
    if content.starts_with(b"*") {
        let after_star = &content[1..];
        for &b in after_star {
            if !b.is_ascii_whitespace() && b != b';' {
                return false; // Something after *, it's complex
            }
        }
        return true; // Just RETURNING *
    }
    
    // Check if it's just column names (alphanumeric, underscore, comma, whitespace)
    for &b in content {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_' | b',' | b' ' | b'\t' | b'\n' | b'\r' | b';' => {},
            _ => return false, // Complex expression
        }
    }
    
    true // Simple column list
}

/// Process complex queries that need translation
fn process_complex_query<'a>(
    query: &'a str,
    conn: &Connection,
    schema_cache: &SchemaCache,
) -> Result<Cow<'a, str>, rusqlite::Error> {
    let processor = UnifiedProcessor::analyze(query);
    
    // If no translations needed, return original
    if processor.translations_needed.is_empty() {
        return Ok(Cow::Borrowed(query));
    }
    
    let mut result = Cow::Borrowed(query);
    
    // Apply translations in optimal order (destructive ones first)
    
    // 1. Schema translation (changes table references)
    if processor.needs_translation(TranslationFlags::SCHEMA) {
        let translated = crate::translator::SchemaPrefixTranslator::translate_query(&result);
        result = Cow::Owned(translated);
    }
    
    // 2. Numeric cast translation (must come before general cast)
    if processor.needs_translation(TranslationFlags::NUMERIC) {
        let translated = crate::translator::NumericCastTranslator::translate_query(&result, conn);
        result = Cow::Owned(translated);
    }
    
    // 3. Cast translation
    if processor.needs_translation(TranslationFlags::CAST) {
        // Check translation cache first
        if let Some(cached) = crate::cache::global_translation_cache().get(query) {
            result = Cow::Owned(cached);
        } else {
            let translated = crate::translator::CastTranslator::translate_query(&result, Some(conn));
            
            // Cache if it's the original query
            if result.as_ref() == query {
                crate::cache::global_translation_cache().insert(
                    query.to_string(),
                    translated.clone()
                );
            }
            result = Cow::Owned(translated);
        }
    }
    
    // 4. Regex translation
    if processor.needs_translation(TranslationFlags::REGEX) {
        match crate::translator::RegexTranslator::translate_query(&result) {
            Ok(translated) => {
                result = Cow::Owned(translated);
            }
            Err(e) => {
                tracing::warn!("Failed to translate regex operators: {}", e);
            }
        }
    }
    
    // 5. Array translation
    if processor.needs_translation(TranslationFlags::ARRAY) {
        match crate::translator::ArrayTranslator::translate_array_operators(&result) {
            Ok(translated) => {
                if translated != result.as_ref() {
                    result = Cow::Owned(translated);
                }
            }
            Err(e) => {
                tracing::warn!("Failed to translate array operators: {}", e);
            }
        }
    }
    
    // 6. DELETE USING translation
    if processor.needs_translation(TranslationFlags::BATCH_DELETE) {
        use crate::translator::BatchDeleteTranslator;
        use std::sync::Arc;
        use parking_lot::Mutex;
        
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let translator = BatchDeleteTranslator::new(cache);
        let translated = translator.translate(&result, &[]);
        result = Cow::Owned(translated);
    }
    
    // 7. Batch UPDATE translation
    if processor.needs_translation(TranslationFlags::BATCH_UPDATE) {
        use crate::translator::BatchUpdateTranslator;
        use std::sync::Arc;
        use parking_lot::Mutex;
        
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let translator = BatchUpdateTranslator::new(cache);
        let translated = translator.translate(&result, &[]);
        result = Cow::Owned(translated);
    }
    
    // 8. DateTime translation
    if processor.needs_translation(TranslationFlags::DATETIME) {
        let translated = crate::translator::DateTimeTranslator::translate_query(&result);
        result = Cow::Owned(translated);
    }
    
    // 9. Decimal rewriting (always check for INSERT/SELECT)
    let query_type = QueryTypeDetector::detect_query_type(&result);
    if matches!(query_type, QueryType::Insert | QueryType::Select) {
        if let Some(table_name) = extract_table_name(&result) {
            if schema_cache.has_decimal_columns(&table_name) {
                match rewrite_query_for_decimal(&result, conn) {
                    Ok(rewritten) => {
                        if rewritten != result.as_ref() {
                            result = Cow::Owned(rewritten);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to rewrite query for decimal: {}", e);
                    }
                }
            }
        } else if matches!(query_type, QueryType::Select) {
            // Conservative: always try decimal rewriting for SELECT
            match rewrite_query_for_decimal(&result, conn) {
                Ok(rewritten) => {
                    if rewritten != result.as_ref() {
                        result = Cow::Owned(rewritten);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to rewrite SELECT query for decimal: {}", e);
                }
            }
        }
    }
    
    Ok(result)
}

// Helper functions
fn extract_table_name(query: &str) -> Option<String> {
    crate::session::db_handler::extract_insert_table_name(query)
}

fn rewrite_query_for_decimal(query: &str, conn: &Connection) -> Result<String, rusqlite::Error> {
    crate::session::db_handler::rewrite_query_for_decimal(query, conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simple_queries_zero_allocation() {
        // These should all return Cow::Borrowed (no allocation)
        let queries = [
            "SELECT * FROM users",
            "SELECT id, name FROM users WHERE id = $1",
            "INSERT INTO users (name, email) VALUES ($1, $2)",
            "UPDATE users SET name = $1 WHERE id = $2",
            "DELETE FROM users WHERE id = $1",
            "SELECT * FROM benchmark_table_pg WHERE int_col > %s",
        ];
        
        for query in &queries {
            let result = process_query(query, &Connection::open_in_memory().unwrap(), &SchemaCache::new(300));
            assert!(matches!(result, Ok(Cow::Borrowed(_))));
        }
    }
    
    #[test]
    fn test_simple_returning_passthrough() {
        // Simple RETURNING should pass through
        let queries = [
            "INSERT INTO users (name) VALUES ('test') RETURNING id",
            "UPDATE users SET name = 'test' WHERE id = 1 RETURNING *",
            "DELETE FROM users WHERE id = 1 RETURNING id, name",
        ];
        
        for query in &queries {
            let result = process_query(query, &Connection::open_in_memory().unwrap(), &SchemaCache::new(300));
            assert!(matches!(result, Ok(Cow::Borrowed(_))));
        }
    }
    
    #[test]
    fn test_complex_queries_need_translation() {
        // These should need translation
        let queries = [
            "SELECT * FROM users WHERE created_at::date = $1",
            "SELECT * FROM pg_catalog.pg_tables",
            "SELECT * FROM users WHERE email ~ '@gmail.com'",
            "INSERT INTO logs (created) VALUES ('2024-01-01')",
        ];
        
        for query in &queries {
            // Would need actual translation logic to test properly
            // For now just ensure they're detected as complex
            let processor = UnifiedProcessor::analyze(query);
            assert_ne!(processor._complexity, ComplexityLevel::Simple);
        }
    }
}