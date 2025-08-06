use bitflags::bitflags;

bitflags! {
    /// Flags indicating which translators need to be applied to a query
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TranslationFlags: u32 {
        const NONE = 0;
        const CAST = 1 << 0;
        const NUMERIC_FORMAT = 1 << 1;
        const BATCH_UPDATE = 1 << 2;
        const BATCH_DELETE = 1 << 3;
        const FTS = 1 << 4;
        const INSERT_DATETIME = 1 << 5;
        const DATETIME = 1 << 6;
        const JSON = 1 << 7;
        const ARRAY = 1 << 8;
        const ARRAY_AGG = 1 << 9;
        const UNNEST = 1 << 10;
        const JSON_EACH = 1 << 11;
        const ROW_TO_JSON = 1 << 12;
        const ARITHMETIC = 1 << 13;
    }
}

/// Analyzes a query in a single pass to determine which translators need to be applied
pub struct QueryAnalyzer;

impl QueryAnalyzer {
    /// Analyze query and return flags indicating which translators are needed
    pub fn analyze(query: &str) -> TranslationFlags {
        let mut flags = TranslationFlags::NONE;
        let query_lower = query.to_lowercase();
        
        // Check for cast operations (:: and CAST(...AS...))
        if query.contains("::") || query_lower.contains("cast(") {
            flags |= TranslationFlags::CAST;
            
            // Check for numeric format casts
            if query_lower.contains("::numeric") || query_lower.contains("::decimal") ||
               query_lower.contains("::double precision") || query_lower.contains("::real") ||
               query_lower.contains("::float") ||
               query_lower.contains(" as numeric") || query_lower.contains(" as decimal") ||
               query_lower.contains(" as double precision") || query_lower.contains(" as real") ||
               query_lower.contains(" as float") {
                flags |= TranslationFlags::NUMERIC_FORMAT;
            }
        }
        
        // Check for batch operations
        if query_lower.starts_with("update") && query.contains("FROM (VALUES") {
            flags |= TranslationFlags::BATCH_UPDATE;
        }
        if query_lower.starts_with("delete") && query.contains("IN (") && query.contains("VALUES") {
            flags |= TranslationFlags::BATCH_DELETE;
        }
        
        // Check for FTS operations
        if query_lower.contains("fts5") || query_lower.contains("match") {
            flags |= TranslationFlags::FTS;
        }
        
        // Check for INSERT with datetime/array values
        if query_lower.starts_with("insert") || query_lower.contains("insert into") {
            // Check for datetime patterns
            if query.contains('-') || query.contains(':') || 
               query.contains("NOW()") || query.contains("now()") ||
               query.contains("CURRENT_DATE") || query.contains("current_date") ||
               query.contains("CURRENT_TIME") || query.contains("current_time") ||
               query.contains("CURRENT_TIMESTAMP") || query.contains("current_timestamp") {
                flags |= TranslationFlags::INSERT_DATETIME;
            }
            
            // Check for array patterns in INSERT
            if query.contains('{') || query.contains("ARRAY[") || query.contains("array[") {
                flags |= TranslationFlags::INSERT_DATETIME; // InsertTranslator handles arrays too
            }
        }
        
        // Check for datetime functions (not in INSERT)
        if !flags.contains(TranslationFlags::INSERT_DATETIME)
            && (query_lower.contains("date(") || query_lower.contains("time(") ||
                query_lower.contains("timestamp") || query_lower.contains("interval") ||
                query_lower.contains("now()") || query_lower.contains("current_date") ||
                query_lower.contains("current_time") || query_lower.contains("extract(") ||
                query_lower.contains("date_trunc(") || query_lower.contains("age(") ||
                query_lower.contains("at time zone")) {
            flags |= TranslationFlags::DATETIME;
        }
        
        // Check for JSON operations
        if query.contains("->") || query.contains("->>") || query.contains("#>") ||
           query.contains("#>>") || query.contains("@>") || query.contains("<@") ||
           query.contains("?") || query.contains("?|") || query.contains("?&") ||
           query_lower.contains("json") || query_lower.contains("jsonb") {
            flags |= TranslationFlags::JSON;
        }
        
        // Check for array operations (more expensive check)
        if query.contains("@>") || query.contains("<@") || query.contains("&&") || 
           query.contains("||") || query.contains("[") || query.contains("ARRAY[") || 
           query.contains("array[") || query.contains(" ANY(") || query.contains(" any(") ||
           query.contains(" ALL(") || query.contains(" all(") {
            // Additional checks for array functions
            if query_lower.contains("array_") || query_lower.contains("unnest") ||
               query.contains("ARRAY[") || query.contains("array[") ||
               query.contains(" ANY(") || query.contains(" any(") ||
               query.contains(" ALL(") || query.contains(" all(") {
                flags |= TranslationFlags::ARRAY;
            }
        }
        
        // Check for array_agg
        if query_lower.contains("array_agg") {
            flags |= TranslationFlags::ARRAY_AGG;
        }
        
        // Check for unnest
        if query_lower.contains("unnest") {
            flags |= TranslationFlags::UNNEST;
        }
        
        // Check for json_each/jsonb_each
        if query_lower.contains("json_each") || query_lower.contains("jsonb_each") {
            flags |= TranslationFlags::JSON_EACH;
        }
        
        // Check for row_to_json
        if query_lower.contains("row_to_json") {
            flags |= TranslationFlags::ROW_TO_JSON;
        }
        
        // Check for arithmetic operations (only if SELECT query)
        if query_lower.starts_with("select") {
            // Simple check for arithmetic operators in SELECT clause
            // Look for FROM with various whitespace patterns
            let from_pos = query_lower.find(" from ")
                .or_else(|| query_lower.find("\nfrom "))
                .or_else(|| query_lower.find("\r\nfrom "))
                .or_else(|| query_lower.find("\tfrom "));
                
            if let Some(pos) = from_pos {
                let select_clause = &query[6..pos]; // Skip "SELECT"
                // Check for arithmetic operators but exclude "SELECT *" pattern
                if select_clause.contains('+') || select_clause.contains('-') || 
                   (select_clause.contains('*') && !select_clause.trim().starts_with('*')) || 
                   select_clause.contains('/') {
                    flags |= TranslationFlags::ARITHMETIC;
                }
            }
        }
        
        flags
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simple_select() {
        let flags = QueryAnalyzer::analyze("SELECT * FROM users");
        assert_eq!(flags, TranslationFlags::NONE);
    }
    
    #[test]
    fn test_cast_detection() {
        let flags = QueryAnalyzer::analyze("SELECT id::int FROM users");
        assert!(flags.contains(TranslationFlags::CAST));
        assert!(!flags.contains(TranslationFlags::NUMERIC_FORMAT));
    }
    
    #[test]
    fn test_cast_syntax_detection() {
        // Test :: syntax
        let flags = QueryAnalyzer::analyze("SELECT id::int FROM users");
        assert!(flags.contains(TranslationFlags::CAST));
        
        // Test CAST(...AS...) syntax
        let flags = QueryAnalyzer::analyze("SELECT CAST(id AS int) FROM users");
        assert!(flags.contains(TranslationFlags::CAST));
        
        // Test CAST with enum
        let flags = QueryAnalyzer::analyze("SELECT CAST('inactive' AS status) as casted_status");
        assert!(flags.contains(TranslationFlags::CAST));
    }
    
    #[test]
    fn test_numeric_cast_detection() {
        let flags = QueryAnalyzer::analyze("SELECT price::numeric(10,2) FROM products");
        assert!(flags.contains(TranslationFlags::CAST));
        assert!(flags.contains(TranslationFlags::NUMERIC_FORMAT));
    }
    
    #[test]
    fn test_datetime_detection() {
        let flags = QueryAnalyzer::analyze("SELECT NOW(), CURRENT_DATE FROM users");
        assert!(flags.contains(TranslationFlags::DATETIME));
    }
    
    #[test]
    fn test_insert_datetime_detection() {
        let flags = QueryAnalyzer::analyze("INSERT INTO events (time) VALUES ('2024-01-01')");
        assert!(flags.contains(TranslationFlags::INSERT_DATETIME));
        assert!(!flags.contains(TranslationFlags::DATETIME));
    }
    
    #[test]
    fn test_json_detection() {
        let flags = QueryAnalyzer::analyze("SELECT data->>'name' FROM users");
        assert!(flags.contains(TranslationFlags::JSON));
    }
    
    #[test]
    fn test_array_detection() {
        let flags = QueryAnalyzer::analyze("SELECT ARRAY[1,2,3] FROM users");
        assert!(flags.contains(TranslationFlags::ARRAY));
    }
    
    #[test]
    fn test_multiple_flags() {
        let flags = QueryAnalyzer::analyze("SELECT id::int, data->>'name', ARRAY[1,2,3] FROM users");
        assert!(flags.contains(TranslationFlags::CAST));
        assert!(flags.contains(TranslationFlags::JSON));
        assert!(flags.contains(TranslationFlags::ARRAY));
    }
    
    #[test]
    fn test_insert_array_detection() {
        let flags = QueryAnalyzer::analyze("INSERT INTO test_arrays (int_array) VALUES ('{1,2,3}')");
        assert!(flags.contains(TranslationFlags::INSERT_DATETIME));
    }
}