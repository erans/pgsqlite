use rusqlite::Connection;
use crate::cache::SchemaCache;

// Feature flag to switch between old and new implementation
#[cfg(feature = "unified_processor")]
use super::unified_processor;

#[cfg(not(feature = "unified_processor"))]
use super::lazy_processor::LazyQueryProcessor;
#[cfg(not(feature = "unified_processor"))]
use super::simple_query_detector::is_fast_path_simple_query;

use tracing::debug;

/// Process a query, using fast path when possible
#[inline(always)]
pub fn process_query(
    query: &str,
    conn: &Connection,
    schema_cache: &SchemaCache,
) -> Result<String, rusqlite::Error> {
    // Handle CREATE TABLE statements first - they need special translation regardless of processor type
    if query.trim_start().to_uppercase().starts_with("CREATE TABLE") {
        use crate::translator::CreateTableTranslator;
        match CreateTableTranslator::translate_with_connection(query, Some(conn)) {
            Ok((translated, _type_mappings)) => {
                debug!("CREATE TABLE translated in process_query: {}", translated);
                return Ok(translated);
            }
            Err(e) => {
                tracing::warn!("Failed to translate CREATE TABLE in process_query: {}", e);
                // Fall through to normal processing
            }
        }
    }
    #[cfg(feature = "unified_processor")]
    {
        // New unified processor - returns Cow to avoid allocations
        match unified_processor::process_query(query, conn, schema_cache) {
            Ok(cow) => {
                let is_borrowed = matches!(&cow, std::borrow::Cow::Borrowed(_));
                if is_borrowed {
                    debug!("Using UNIFIED FAST PATH (zero-alloc) for query: {}", query);
                } else {
                    debug!("Using UNIFIED PROCESSOR (with translation) for query: {}", query);
                }
                Ok(cow.into_owned())
            }
            Err(e) => Err(e),
        }
    }
    
    #[cfg(not(feature = "unified_processor"))]
    {
        // Old implementation - kept for A/B testing
        if is_fast_path_simple_query(query) {
            debug!("Using OLD FAST PATH for query: {}", query);
            return Ok(query.to_string());
        }
        
        debug!("Using OLD SLOW PATH (LazyQueryProcessor) for query: {}", query);
        let mut processor = LazyQueryProcessor::new(query);
        Ok(processor.process(conn, schema_cache)?.to_string())
    }
}