/// Central OID generation module to ensure consistency across the codebase
/// Uses the same formula as the pg_class view in migrations

/// Generate a stable OID from a name using the same formula as SQLite views
/// This matches: (unicode(substr(name, 1, 1)) * 1000000) + (unicode(substr(name || ' ', 2, 1)) * 10000) + ...
pub fn generate_oid(name: &str) -> u32 {
    // For better uniqueness, sample characters from different positions
    let chars: Vec<char> = name.chars().collect();
    let len = chars.len();

    // Sample characters from different positions for better distribution
    // Use first, middle, and last characters to avoid collisions
    let char1 = chars.get(0).copied().unwrap_or(' ') as u32;
    let char2 = chars.get(1).copied().unwrap_or(' ') as u32;
    let char3 = chars.get(len / 3).copied().unwrap_or(' ') as u32;  // 1/3 position
    let char4 = chars.get(2 * len / 3).copied().unwrap_or(' ') as u32;  // 2/3 position
    let char5 = chars.get(len.saturating_sub(1)).copied().unwrap_or(' ') as u32;  // Last char
    let char6 = chars.get(len / 2).copied().unwrap_or(' ') as u32;  // Middle char
    let length = name.len() as u32;

    // Include characters from different positions for better uniqueness
    // This helps distinguish constraints with the same prefix
    ((char1 * 1000000) + (char2 * 10000) + (char3 * 100) +
     (char4 * 37) + (char5 * 23) + (char6 * 19) + (length * 7)) % 1000000 + 16384
}

/// Generate OID as i32 (for functions that need signed integers)
pub fn generate_oid_i32(name: &str) -> i32 {
    generate_oid(name) as i32
}

/// Generate OID as String (for database storage)
pub fn generate_oid_string(name: &str) -> String {
    generate_oid(name).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oid_generation_consistency() {
        // Test that same name produces same OID
        let oid1 = generate_oid("test_table");
        let oid2 = generate_oid("test_table");
        assert_eq!(oid1, oid2);

        // Test that different names produce different OIDs
        let oid3 = generate_oid("other_table");
        assert_ne!(oid1, oid3);
    }

    #[test]
    fn test_oid_formats() {
        let name = "users";
        let oid_u32 = generate_oid(name);
        let oid_i32 = generate_oid_i32(name);
        let oid_string = generate_oid_string(name);

        assert_eq!(oid_u32 as i32, oid_i32);
        assert_eq!(oid_u32.to_string(), oid_string);
    }
}