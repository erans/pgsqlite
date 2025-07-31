use rusqlite::Connection;
use crate::cache::SchemaCache;
use super::lazy_processor::LazyQueryProcessor;
use super::simple_query_detector::is_fast_path_simple_query;

/// Process a query, using fast path when possible
#[inline(always)]
pub fn process_query<'a>(
    query: &'a str,
    conn: &Connection,
    schema_cache: &SchemaCache,
) -> Result<String, rusqlite::Error> {
    // Fast path: skip LazyQueryProcessor entirely for simple queries
    if is_fast_path_simple_query(query) {
        return Ok(query.to_string());
    }
    
    // Slow path: use LazyQueryProcessor for complex queries
    let mut processor = LazyQueryProcessor::new(query);
    Ok(processor.process(conn, schema_cache)?.to_string())
}