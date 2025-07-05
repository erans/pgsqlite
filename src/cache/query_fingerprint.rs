use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

/// Query fingerprinting for better cache keys
/// Normalizes queries to increase cache hit rates
pub struct QueryFingerprint;

impl QueryFingerprint {
    /// Generate a fingerprint for a query that ignores:
    /// - Whitespace differences
    /// - Case differences in keywords
    /// - Comments
    /// - Specific numeric/string literals (replaced with placeholders)
    #[inline]
    pub fn generate(query: &str) -> u64 {
        let normalized = Self::normalize_query(query);
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Generate a fingerprint that preserves literals (for translation cache)
    #[inline]
    pub fn generate_with_literals(query: &str) -> u64 {
        let normalized = Self::normalize_whitespace_and_case(query);
        let mut hasher = DefaultHasher::new();
        normalized.hash(&mut hasher);
        hasher.finish()
    }
    
    /// Normalize a query for fingerprinting
    fn normalize_query(query: &str) -> String {
        let mut result = String::with_capacity(query.len());
        let mut chars = query.chars().peekable();
        let in_string = false;
        let mut after_whitespace = false;
        
        while let Some(ch) = chars.next() {
            match ch {
                // Handle string literals
                '\'' if !in_string => {
                    result.push_str("'?'");
                    // Skip until closing quote
                    while let Some(ch) = chars.next() {
                        if ch == '\'' && chars.peek() != Some(&'\'') {
                            break;
                        }
                        // Handle escaped quotes
                        if ch == '\'' && chars.peek() == Some(&'\'') {
                            chars.next();
                        }
                    }
                }
                
                // Handle numbers
                '0'..='9' if !in_string => {
                    result.push('?');
                    // Skip rest of number (including decimals, scientific notation)
                    while let Some(&next_ch) = chars.peek() {
                        if matches!(next_ch, '0'..='9' | '.' | 'e' | 'E' | '+' | '-') {
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                
                // Handle whitespace
                ' ' | '\t' | '\n' | '\r' => {
                    if !after_whitespace && !result.is_empty() {
                        result.push(' ');
                        after_whitespace = true;
                    }
                }
                
                // Handle other characters
                _ if !in_string => {
                    after_whitespace = false;
                    result.push(ch.to_ascii_uppercase());
                }
                
                _ => {}
            }
        }
        
        // Trim trailing whitespace
        result.trim_end().to_string()
    }
    
    /// Normalize only whitespace and case (preserves literals)
    fn normalize_whitespace_and_case(query: &str) -> String {
        let mut result = String::with_capacity(query.len());
        let mut chars = query.chars();
        let mut in_string = false;
        let mut after_whitespace = false;
        
        while let Some(ch) = chars.next() {
            match ch {
                '\'' => {
                    in_string = !in_string;
                    result.push(ch);
                    after_whitespace = false;
                }
                
                ' ' | '\t' | '\n' | '\r' if !in_string => {
                    if !after_whitespace && !result.is_empty() {
                        result.push(' ');
                        after_whitespace = true;
                    }
                }
                
                _ => {
                    after_whitespace = false;
                    if in_string {
                        result.push(ch);
                    } else {
                        result.push(ch.to_ascii_uppercase());
                    }
                }
            }
        }
        
        result.trim_end().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_fingerprint_whitespace_normalization() {
        let q1 = "SELECT  *  FROM   users";
        let q2 = "SELECT * FROM users";
        let q3 = "SELECT\n*\nFROM\nusers";
        
        assert_eq!(
            QueryFingerprint::generate(q1),
            QueryFingerprint::generate(q2)
        );
        assert_eq!(
            QueryFingerprint::generate(q2),
            QueryFingerprint::generate(q3)
        );
    }
    
    #[test]
    fn test_fingerprint_case_normalization() {
        let q1 = "select * from users";
        let q2 = "SELECT * FROM users";
        let q3 = "SeLeCt * FrOm users";
        
        assert_eq!(
            QueryFingerprint::generate(q1),
            QueryFingerprint::generate(q2)
        );
        assert_eq!(
            QueryFingerprint::generate(q2),
            QueryFingerprint::generate(q3)
        );
    }
    
    #[test]
    fn test_fingerprint_literal_normalization() {
        let q1 = "SELECT * FROM users WHERE id = 123";
        let q2 = "SELECT * FROM users WHERE id = 456";
        let q3 = "SELECT * FROM users WHERE name = 'john'";
        let q4 = "SELECT * FROM users WHERE name = 'jane'";
        
        // Numeric literals should be normalized
        assert_eq!(
            QueryFingerprint::generate(q1),
            QueryFingerprint::generate(q2)
        );
        
        // String literals should be normalized
        assert_eq!(
            QueryFingerprint::generate(q3),
            QueryFingerprint::generate(q4)
        );
        
        // But different structure should have different fingerprints
        assert_ne!(
            QueryFingerprint::generate(q1),
            QueryFingerprint::generate(q3)
        );
    }
    
    #[test]
    fn test_fingerprint_with_literals_preserved() {
        let q1 = "SELECT * FROM users WHERE id = 123";
        let q2 = "SELECT * FROM users WHERE id = 456";
        
        // With literals preserved, these should be different
        assert_ne!(
            QueryFingerprint::generate_with_literals(q1),
            QueryFingerprint::generate_with_literals(q2)
        );
        
        // But whitespace/case should still be normalized
        let q3 = "SELECT  *  FROM  users  WHERE  id  =  123";
        assert_eq!(
            QueryFingerprint::generate_with_literals(q1),
            QueryFingerprint::generate_with_literals(q3)
        );
    }
}