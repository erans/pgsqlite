/// Helper module to fix aggregate type detection when connection is not available
use regex::Regex;
use lazy_static::lazy_static;

lazy_static! {
    /// Regex to extract table name from a simple SELECT ... FROM table query
    static ref TABLE_REGEX: Regex = Regex::new(r"(?i)\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b").unwrap();
    
    /// Regex to detect MAX/MIN aggregates with DECIMAL columns
    static ref MAX_MIN_DECIMAL_REGEX: Regex = Regex::new(r"(?i)\b(MAX|MIN)\s*\(\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\)").unwrap();
}

/// Extract table name from a query
pub fn extract_table_from_query(query: &str) -> Option<String> {
    TABLE_REGEX.captures(query)
        .and_then(|cap| cap.get(1))
        .map(|m| m.as_str().to_string())
}

/// Check if this is a MAX/MIN aggregate on a DECIMAL column
/// Returns the proper type OID if we can determine it should be DECIMAL
pub fn fix_aggregate_type_for_decimal(
    function_name: &str,
    query: Option<&str>,
) -> Option<i32> {
    // Only handle MAX/MIN functions
    let upper = function_name.to_uppercase();
    if !upper.starts_with("MAX(") && !upper.starts_with("MIN(") {
        return None;
    }
    
    // If we have the query, check if the column is likely DECIMAL
    if let Some(query_str) = query {
        // Check if the query contains DECIMAL type definition
        if query_str.contains("DECIMAL") || query_str.contains("NUMERIC") {
            // For MAX/MIN on DECIMAL columns, return NUMERIC type OID
            return Some(crate::types::PgType::Numeric.to_oid());
        }
        
        // Check for common decimal column names
        if let Some(column_name) = crate::types::QueryContextAnalyzer::extract_column_from_aggregation(function_name) {
            let col_lower = column_name.to_lowercase();
            if col_lower.contains("balance") || 
               col_lower.contains("amount") || 
               col_lower.contains("price") ||
               col_lower.contains("cost") ||
               col_lower.contains("total") ||
               col_lower.contains("salary") ||
               col_lower.contains("revenue") {
                // These are likely decimal columns
                return Some(crate::types::PgType::Numeric.to_oid());
            }
        }
    }
    
    None
}