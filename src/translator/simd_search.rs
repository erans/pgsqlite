use memchr::memmem;

/// SIMD-accelerated search for cast operators in SQL queries
pub struct SimdCastSearch;

impl SimdCastSearch {
    /// Check if a query contains :: cast operator using SIMD acceleration
    #[inline]
    pub fn contains_double_colon(query: &str) -> bool {
        // Use memchr's optimized substring search
        memmem::find(query.as_bytes(), b"::").is_some()
    }
    
    /// Find all positions of :: in the query
    #[inline]
    pub fn find_all_double_colons(query: &str) -> Vec<usize> {
        let finder = memmem::Finder::new(b"::");
        finder.find_iter(query.as_bytes()).collect()
    }
    
    /// Check if any :: is outside of string literals (SIMD-accelerated)
    pub fn has_cast_outside_strings(query: &str) -> bool {
        let bytes = query.as_bytes();
        
        // First, quick check if :: exists at all
        if !Self::contains_double_colon(query) {
            return false;
        }
        
        // Find all :: positions using SIMD
        let cast_positions = Self::find_all_double_colons(query);
        
        // Find all single quotes using SIMD
        let quote_positions: Vec<usize> = memchr::memchr_iter(b'\'', bytes).collect();
        
        // Now check if any :: is outside quotes
        for cast_pos in cast_positions {
            if !Self::is_position_in_string(cast_pos, &quote_positions, bytes) {
                return true;
            }
        }
        
        false
    }
    
    /// Check if a position is inside a string literal
    fn is_position_in_string(pos: usize, quote_positions: &[usize], bytes: &[u8]) -> bool {
        let mut in_string = false;
        let mut escaped = false;
        
        for &quote_pos in quote_positions {
            if quote_pos >= pos {
                break;
            }
            
            // Check if this quote is escaped
            if quote_pos > 0 && bytes[quote_pos - 1] == b'\\' && !escaped {
                escaped = true;
                continue;
            }
            
            if !escaped {
                in_string = !in_string;
            }
            escaped = false;
        }
        
        in_string
    }
    
    /// SIMD-accelerated search for CAST keyword
    pub fn contains_cast_keyword(query: &str) -> bool {
        let bytes = query.as_bytes();
        
        // Use SIMD to find all 'C' positions
        let c_positions: Vec<usize> = memchr::memchr_iter(b'C', bytes)
            .chain(memchr::memchr_iter(b'c', bytes))
            .collect();
        
        // Check each position to see if it starts "CAST("
        for pos in c_positions {
            if pos + 5 <= bytes.len() {
                let slice = &bytes[pos..pos + 5];
                if slice.eq_ignore_ascii_case(b"CAST(") {
                    return true;
                }
            }
        }
        
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_simd_double_colon_search() {
        assert!(SimdCastSearch::contains_double_colon("SELECT foo::text"));
        assert!(!SimdCastSearch::contains_double_colon("SELECT foo"));
        assert!(SimdCastSearch::contains_double_colon("::"));
    }
    
    #[test]
    fn test_simd_cast_outside_strings() {
        // Cast operator outside strings
        assert!(SimdCastSearch::has_cast_outside_strings("SELECT foo::text"));
        
        // Cast operator inside strings
        assert!(!SimdCastSearch::has_cast_outside_strings("SELECT 'foo::text'"));
        
        // Mixed case
        assert!(SimdCastSearch::has_cast_outside_strings("SELECT 'foo' || bar::text"));
        
        // IPv6 address in string
        assert!(!SimdCastSearch::has_cast_outside_strings("SELECT '::1' as ip"));
        assert!(!SimdCastSearch::has_cast_outside_strings("SELECT 'fe80::1/64' as ip"));
    }
    
    #[test]
    fn test_simd_cast_keyword() {
        assert!(SimdCastSearch::contains_cast_keyword("SELECT CAST(foo AS text)"));
        assert!(SimdCastSearch::contains_cast_keyword("select cast(foo as text)"));
        assert!(!SimdCastSearch::contains_cast_keyword("SELECT CASTLE"));
        assert!(!SimdCastSearch::contains_cast_keyword("SELECT foo"));
    }
}