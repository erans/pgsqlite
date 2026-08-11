/// Central OID generation module to ensure consistency across the codebase

/// Generate a stable OID from a name by sampling six character positions.
///
/// This is the *constraint/sequence* OID formula. It is deliberately NOT the table-identity
/// formula — see `generate_table_oid` for the one that must match the pg_class view.
pub fn generate_oid(name: &str) -> u32 {
    // For better uniqueness, sample characters from different positions
    let chars: Vec<char> = name.chars().collect();
    let len = chars.len();

    // Sample characters from different positions for better distribution
    // Use first, middle, and last characters to avoid collisions
    // Widened to u64 for the same reason as generate_table_oid: a high-codepoint
    // character times 1_000_000 overflows u32. The final `% 1_000_000 + 16384`
    // keeps the result far below u32::MAX, so the round trip is safe, and every
    // value that did not previously overflow is unchanged.
    let char1 = chars.first().copied().unwrap_or(' ') as u64;
    let char2 = chars.get(1).copied().unwrap_or(' ') as u64;
    let char3 = chars.get(len / 3).copied().unwrap_or(' ') as u64;  // 1/3 position
    let char4 = chars.get(2 * len / 3).copied().unwrap_or(' ') as u64;  // 2/3 position
    let char5 = chars.get(len.saturating_sub(1)).copied().unwrap_or(' ') as u64;  // Last char
    let char6 = chars.get(len / 2).copied().unwrap_or(' ') as u64;  // Middle char
    let length = name.len() as u64;

    // Include characters from different positions for better uniqueness
    // This helps distinguish constraints with the same prefix
    (((char1 * 1000000) + (char2 * 10000) + (char3 * 100) +
     (char4 * 37) + (char5 * 23) + (char6 * 19) + (length * 7)) % 1000000 + 16384) as u32
}

/// Generate OID as i32 (for functions that need signed integers)
pub fn generate_oid_i32(name: &str) -> i32 {
    generate_oid(name) as i32
}

/// Generate OID as String (for database storage)
pub fn generate_oid_string(name: &str) -> String {
    generate_oid(name).to_string()
}

/// Generate a stable table-identity OID from a name using the 3-character-prefix formula.
/// This is the canonical formula used by the pg_class/pg_namespace views (migration v28) and
/// by `constraint_populator::generate_table_oid`. Every producer of table-identity OIDs
/// (attrelid, tgrelid, seqrelid, etc.) must use this exact formula so that joins against
/// pg_class.oid resolve correctly.
pub fn generate_table_oid(name: &str) -> u32 {
    let name_with_padding = format!("{name}  ");
    let chars: Vec<char> = name_with_padding.chars().collect();
    // Widen to u64 for the arithmetic: a high-codepoint leading character (e.g. from
    // "日本語") times 1_000_000 overflows u32. The final `% 1_000_000 + 16_384` keeps
    // the result far below u32::MAX, so the u32 -> u64 -> u32 round trip is safe.
    let char1 = chars.first().copied().unwrap_or(' ') as u64;
    let char2 = chars.get(1).copied().unwrap_or(' ') as u64;
    let char3 = chars.get(2).copied().unwrap_or(' ') as u64;
    // Match SQLite's `length(name)`, which counts characters, not UTF-8 bytes.
    let length = name.chars().count() as u64;

    (((char1 * 1_000_000) + (char2 * 10_000) + (char3 * 100) + (length * 7)) % 1_000_000 + 16384) as u32
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

    /// The u64 widening of `generate_oid` must be strictly panic-eliminating: every
    /// input that already produced a value must still produce the same value. These
    /// were measured against the pre-widening body (with checked arithmetic to detect
    /// the overflow) and are unchanged by the widening, non-ASCII names included.
    #[test]
    fn test_generate_oid_values_unchanged_by_widening() {
        assert_eq!(generate_oid("users"), 186701);
        assert_eq!(generate_oid("customers"), 206538);
        assert_eq!(generate_oid("orders"), 175208);
        assert_eq!(generate_oid("a"), 353754);
        assert_eq!(generate_oid("ab"), 1013840);
        assert_eq!(generate_oid("café"), 1007190);
    }

    /// A high-codepoint leading character overflowed `char1 * 1_000_000` in u32,
    /// panicking in debug builds and silently wrapping in release. `generate_oid`
    /// reaches persisted catalog OIDs through migration v5's `populate_catalog_tables`,
    /// so this was a panic on the upgrade path for such a name.
    #[test]
    fn test_generate_oid_high_codepoint_no_panic() {
        assert_eq!(generate_oid("日本語"), 408635);
        assert_eq!(generate_oid("\u{10FFFF}x"), 636999);
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

    /// Pinned ASCII value: `customers` is persisted on disk today in
    /// `pg_constraint.conrelid` / `pg_index.indrelid` / `pg_attrdef.adrelid` /
    /// `pg_depend.objid` and `refobjid`, and `tests/catalog_join_test.rs:47` pins it too.
    /// This value must never change for ASCII input.
    #[test]
    fn test_generate_table_oid_ascii_pinned() {
        assert_eq!(generate_table_oid("customers"), 197947);
        assert_eq!(generate_table_oid("orders"), 166426);
    }

    /// Non-ASCII names must agree with the v28 SQL expression, which uses
    /// `length(name)` (character count). These expected values were computed by
    /// running the identical SQL expression through the `sqlite3` CLI:
    /// `café` -> 996612, `naïve_tbl` -> 1010347.
    #[test]
    fn test_generate_table_oid_non_ascii_matches_sqlite() {
        assert_eq!(generate_table_oid("café"), 996612);
        assert_eq!(generate_table_oid("naïve_tbl"), 1010347);
    }

    /// A high-codepoint leading character must not overflow/panic. Before the u64
    /// widening, `generate_table_oid("日本語")` panicked with "attempt to multiply
    /// with overflow" in a debug build. Expected value computed via the `sqlite3`
    /// CLI running the identical v28 SQL expression: 685005.
    #[test]
    fn test_generate_table_oid_high_codepoint_no_panic() {
        assert_eq!(generate_table_oid("日本語"), 685005);
    }
}