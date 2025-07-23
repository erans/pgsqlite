use crate::types::PgType;

/// Analyzes query context to help with type inference
pub struct QueryContextAnalyzer;

impl QueryContextAnalyzer {
    /// Analyze an INSERT query to get column types from the target table
    pub fn get_insert_column_info(query: &str) -> Option<(String, Vec<String>)> {
        let query_lower = query.to_lowercase();
        
        // Look for INSERT INTO table_name (columns) pattern
        if let Some(into_pos) = query_lower.find("insert into") {
            let after_into = &query[into_pos + 11..].trim();
            
            // Find table name
            let table_end = after_into.find(|c: char| c == '(' || c.is_whitespace())
                .unwrap_or(after_into.len());
            let table_name = after_into[..table_end].trim().to_string();
            
            // Find column list if present
            if let Some(paren_start) = after_into.find('(') {
                let rest = &after_into[paren_start + 1..];
                if let Some(paren_end) = rest.find(')') {
                    let columns_str = &rest[..paren_end];
                    let columns: Vec<String> = columns_str
                        .split(',')
                        .map(|s| s.trim().to_lowercase())
                        .collect();
                    return Some((table_name, columns));
                }
            }
            
            // No explicit columns, would need to query table schema
            return Some((table_name, vec![]));
        }
        
        None
    }
    
    /// Infer parameter types for common query patterns
    pub fn infer_parameter_types(query: &str, param_count: usize) -> Vec<i32> {
        let query_lower = query.to_lowercase();
        let mut types = Vec::new();
        
        // Check for explicit type casts
        for i in 1..=param_count {
            let param = format!("${i}");
            
            if query_lower.contains(&format!("{param}::int4")) {
                types.push(PgType::Int4.to_oid()); // Explicit cast to int4
            } else if query_lower.contains(&format!("{param}::int8")) ||
                      query_lower.contains(&format!("{param}::bigint")) {
                types.push(PgType::Int8.to_oid()); // Explicit cast to int8
            } else if query_lower.contains(&format!("{param}::text")) {
                types.push(PgType::Text.to_oid()); // Explicit cast to text
            } else if query_lower.contains(&format!("{param}::bytea")) {
                types.push(PgType::Bytea.to_oid()); // Explicit cast to bytea
            } else if query_lower.contains(&format!("{param}::bool")) ||
                      query_lower.contains(&format!("{param}::boolean")) {
                types.push(PgType::Bool.to_oid()); // Explicit cast to bool
            } else if query_lower.contains(&format!("{param}::float8")) ||
                      query_lower.contains(&format!("{param}::double precision")) {
                types.push(PgType::Float8.to_oid()); // Explicit cast to float8
            } else if query_lower.contains(&format!("{param}::numeric")) ||
                      query_lower.contains(&format!("{param}::decimal")) {
                types.push(PgType::Numeric.to_oid()); // Explicit cast to numeric
            } else {
                types.push(0); // Unknown - will need to be determined from schema
            }
        }
        
        types
    }
    
    /// Extract column name from aggregation function like SUM(column_name)
    pub fn extract_column_from_aggregation(agg_func: &str) -> Option<String> {
        let func_upper = agg_func.to_uppercase();
        
        // Find the opening parenthesis
        if let Some(start) = func_upper.find('(') {
            let after_paren = &agg_func[start + 1..];
            
            // Find the closing parenthesis
            if let Some(end) = after_paren.find(')') {
                let column_part = after_paren[..end].trim();
                
                // Handle COUNT(*) specially
                if column_part == "*" {
                    return None;
                }
                
                // Remove any whitespace and return
                return Some(column_part.to_string());
            }
        }
        
        None
    }
}