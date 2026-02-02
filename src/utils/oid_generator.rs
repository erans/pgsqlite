/// Central OID generation module to ensure consistency across the codebase
pub fn generate_oid(name: &str) -> u32 {
    let name_with_padding = format!("{}  ", name);
    let chars: Vec<char> = name_with_padding.chars().collect();
    let char1 = chars.first().copied().unwrap_or(' ') as u32;
    let char2 = chars.get(1).copied().unwrap_or(' ') as u32;
    let char3 = chars.get(2).copied().unwrap_or(' ') as u32;
    let length = name.len() as u32;

    ((char1 * 1_000_000) + (char2 * 10000) + (char3 * 100) + (length * 7)) % 1_000_000 + 16384
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
