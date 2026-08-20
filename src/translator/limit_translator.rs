//! PATCH v22 —— PostgreSQL LIMIT / OFFSET semantics on top of SQLite.
//!
//! # Why this exists
//!
//! PostgreSQL and SQLite disagree on two points that GUI clients hit constantly:
//!
//! | clause                     | PostgreSQL           | SQLite                       |
//! |----------------------------|----------------------|------------------------------|
//! | `LIMIT NULL`               | no upper bound       | **error: datatype mismatch** |
//! | `LIMIT ALL`                | no upper bound       | **syntax error**             |
//! | `OFFSET n` without `LIMIT` | legal                | **syntax error**             |
//! | `LIMIT -1`                 | error (must be >= 0) | no upper bound               |
//!
//! # Real incident (dune, 2026-08-05)
//!
//! dbx loads the table list of a schema with:
//!
//! ```sql
//! SELECT c.relname, ... FROM pg_catalog.pg_class c
//!   JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
//!   ... ORDER BY ..., c.relname
//!   LIMIT CAST($4 AS BIGINT) OFFSET CAST($5 AS BIGINT)
//! ```
//!
//! When the user has no row cap configured dbx binds `$4 = NULL`, which pgsqlite
//! inlines to `LIMIT CAST(NULL AS INTEGER)`.  SQLite rejects the whole statement
//! with `datatype mismatch`, so the table tree never renders and the transaction
//! is poisoned for every follow-up metadata query.
//!
//! Single-table catalog queries masked the bug because the Rust catalog handler
//! ignores LIMIT entirely; only the JOIN form reaches real SQLite.
//!
//! # What this translator does
//!
//! 1. `LIMIT NULL` / `LIMIT CAST(NULL AS <type>)` / `LIMIT ALL`  ->  `LIMIT -1`
//! 2. `OFFSET NULL` / `OFFSET CAST(NULL AS <type>)`              ->  `OFFSET 0`
//! 3. `OFFSET <n>` with no `LIMIT`                               ->  `LIMIT -1 OFFSET <n>`
//!
//! # Deliberate conservatism
//!
//! * Only **top-level** clauses are rewritten (bracket depth 0).  A `LIMIT NULL`
//!   buried inside a sub-select is left alone: no GUI emits that, and touching it
//!   would widen the blast radius for no benefit.
//! * String literals, quoted identifiers and `--` comments are skipped, so a
//!   column literally named `limit` or a value of `'offset'` is never harmed.
//! * Unbound placeholders (`LIMIT $4`, `LIMIT ?`) are left untouched — at that
//!   point the value is unknown.  By the time `execute_select` runs, parameters
//!   have already been inlined, which is exactly where this runs.
//! * Returns `None` when nothing needs changing, so the overwhelmingly common
//!   `LIMIT 100` path costs one scan and zero allocations.
//! * PostgreSQL also accepts `OFFSET n LIMIT m` (reversed order), which SQLite
//!   rejects.  Reordering clauses is a much riskier edit and no observed client
//!   emits it, so that case is knowingly left as a limitation.

pub struct LimitTranslator;

impl LimitTranslator {
    /// Cheap pre-filter so callers can skip the scan (and the allocation) for the
    /// vast majority of statements.
    pub fn needs_translation(query: &str) -> bool {
        contains_ci(query, "limit") || contains_ci(query, "offset")
    }

    /// Rewrite PostgreSQL LIMIT/OFFSET semantics into SQLite-compatible form.
    ///
    /// Returns `None` when the query is already valid SQLite, so the caller can
    /// keep using the original `&str` without copying.
    pub fn translate(query: &str) -> Option<String> {
        let (limit_pos, offset_pos) = scan_top_level_clauses(query);
        if limit_pos.is_none() && offset_pos.is_none() {
            return None;
        }

        // (start, end, replacement); start == end means "pure insertion"
        let mut edits: Vec<(usize, usize, &'static str)> = Vec::new();

        if let Some(lp) = limit_pos
            && let Some(operand) = read_operand(query, lp + "limit".len())
            && operand.is_unbounded
        {
            edits.push((operand.start, operand.end, "-1"));
        }

        if let Some(op) = offset_pos
            && let Some(operand) = read_operand(query, op + "offset".len())
            && operand.is_unbounded
        {
            edits.push((operand.start, operand.end, "0"));
        }

        // SQLite requires OFFSET to be preceded by LIMIT.
        if let Some(op) = offset_pos
            && limit_pos.is_none()
        {
            edits.push((op, op, "LIMIT -1 "));
        }

        if edits.is_empty() {
            return None;
        }

        edits.sort_by_key(|e| e.0);
        let mut out = String::with_capacity(query.len() + 16);
        let mut cursor = 0usize;
        for (start, end, replacement) in edits {
            if start < cursor {
                continue; // overlapping edit; should not happen, but never panic
            }
            out.push_str(&query[cursor..start]);
            out.push_str(replacement);
            cursor = end;
        }
        out.push_str(&query[cursor..]);
        Some(out)
    }
}

/// Byte offsets of the last top-level `LIMIT` and `OFFSET` keywords.
fn scan_top_level_clauses(query: &str) -> (Option<usize>, Option<usize>) {
    let bytes = query.as_bytes();
    let mut depth: i32 = 0;
    let mut i = 0usize;
    let mut limit_pos = None;
    let mut offset_pos = None;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < bytes.len() {
                    if bytes[i] == b'\'' {
                        // '' is an escaped quote inside the literal
                        if i + 1 < bytes.len() && bytes[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            b'"' => {
                i += 1;
                while i < bytes.len() && bytes[i] != b'"' {
                    i += 1;
                }
                i += 1;
                continue;
            }
            b'-' if i + 1 < bytes.len() && bytes[i + 1] == b'-' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            b'(' => {
                depth += 1;
                i += 1;
                continue;
            }
            b')' => {
                depth -= 1;
                i += 1;
                continue;
            }
            _ => {}
        }

        if depth == 0 && is_word_start(bytes, i) {
            if word_matches(bytes, i, b"limit") {
                limit_pos = Some(i);
                i += "limit".len();
                continue;
            }
            if word_matches(bytes, i, b"offset") {
                offset_pos = Some(i);
                i += "offset".len();
                continue;
            }
        }
        i += 1;
    }

    (limit_pos, offset_pos)
}

struct Operand {
    start: usize,
    end: usize,
    /// true when this operand means "no bound" in PostgreSQL (NULL / ALL)
    is_unbounded: bool,
}

/// Read the expression that follows a LIMIT/OFFSET keyword.
fn read_operand(query: &str, from: usize) -> Option<Operand> {
    let bytes = query.as_bytes();
    let mut i = from;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }
    let start = i;

    // CAST( ... )
    if word_matches(bytes, i, b"cast") {
        let mut j = i + "cast".len();
        while j < bytes.len() && bytes[j].is_ascii_whitespace() {
            j += 1;
        }
        if j < bytes.len() && bytes[j] == b'(' {
            let mut depth = 0i32;
            while j < bytes.len() {
                if bytes[j] == b'(' {
                    depth += 1;
                } else if bytes[j] == b')' {
                    depth -= 1;
                    if depth == 0 {
                        j += 1;
                        break;
                    }
                }
                j += 1;
            }
            if depth != 0 {
                return None; // unbalanced; leave the query alone
            }
            let is_unbounded = cast_wraps_null(&query[start..j]);
            return Some(Operand { start, end: j, is_unbounded });
        }
    }

    if word_matches(bytes, i, b"null") {
        return Some(Operand { start, end: i + "null".len(), is_unbounded: true });
    }
    if word_matches(bytes, i, b"all") {
        return Some(Operand { start, end: i + "all".len(), is_unbounded: true });
    }

    // Numbers, placeholders, identifiers: read to the end of the token.
    let mut j = i;
    while j < bytes.len() {
        let c = bytes[j];
        if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' || c == b'?' || c == b'.' {
            j += 1;
        } else {
            break;
        }
    }
    if j == i {
        return None;
    }
    Some(Operand { start, end: j, is_unbounded: false })
}

/// Does `CAST(<expr> AS <type>)` wrap a bare NULL?
fn cast_wraps_null(cast_expr: &str) -> bool {
    let open = match cast_expr.find('(') {
        Some(p) => p,
        None => return false,
    };
    if cast_expr.len() < open + 2 {
        return false;
    }
    let inner = &cast_expr[open + 1..cast_expr.len() - 1];
    let trimmed = inner.trim_start();
    let bytes = trimmed.as_bytes();
    if bytes.len() < 4 || !trimmed[..4].eq_ignore_ascii_case("null") {
        return false;
    }
    bytes.len() == 4 || !(bytes[4].is_ascii_alphanumeric() || bytes[4] == b'_')
}

fn is_word_start(bytes: &[u8], i: usize) -> bool {
    if i == 0 {
        return true;
    }
    let prev = bytes[i - 1];
    !(prev.is_ascii_alphanumeric() || prev == b'_')
}

fn word_matches(bytes: &[u8], i: usize, needle: &[u8]) -> bool {
    let end = i + needle.len();
    if end > bytes.len() {
        return false;
    }
    if !bytes[i..end].eq_ignore_ascii_case(needle) {
        return false;
    }
    end == bytes.len() || !(bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_')
}

fn contains_ci(haystack: &str, needle: &str) -> bool {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    if n.is_empty() || h.len() < n.len() {
        return false;
    }
    h.windows(n.len()).any(|w| w.eq_ignore_ascii_case(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- the incident that motivated this patch --------------------------

    #[test]
    fn v22_dbx_table_list_limit_cast_null() {
        let q = "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 ORDER BY c.relname LIMIT CAST(NULL AS INTEGER) OFFSET CAST(0 AS INTEGER)";
        let out = LimitTranslator::translate(q).expect("should rewrite");
        assert!(out.contains("LIMIT -1"), "got: {out}");
        assert!(out.contains("OFFSET CAST(0 AS INTEGER)"), "got: {out}");
        assert!(!out.contains("CAST(NULL"), "got: {out}");
    }

    #[test]
    fn v22_limit_cast_null_bigint() {
        let out = LimitTranslator::translate("SELECT 1 LIMIT CAST(NULL AS BIGINT)").unwrap();
        assert_eq!(out, "SELECT 1 LIMIT -1");
    }

    #[test]
    fn v22_bare_limit_null() {
        let out = LimitTranslator::translate("SELECT 1 LIMIT NULL").unwrap();
        assert_eq!(out, "SELECT 1 LIMIT -1");
    }

    #[test]
    fn v22_limit_all() {
        let out = LimitTranslator::translate("SELECT 1 LIMIT ALL").unwrap();
        assert_eq!(out, "SELECT 1 LIMIT -1");
    }

    #[test]
    fn v22_offset_null_becomes_zero() {
        let out = LimitTranslator::translate("SELECT 1 LIMIT 10 OFFSET NULL").unwrap();
        assert_eq!(out, "SELECT 1 LIMIT 10 OFFSET 0");
    }

    #[test]
    fn v22_offset_without_limit_gets_one() {
        let out = LimitTranslator::translate("SELECT a FROM t ORDER BY a OFFSET 20").unwrap();
        assert_eq!(out, "SELECT a FROM t ORDER BY a LIMIT -1 OFFSET 20");
    }

    // ---- must not touch anything else ------------------------------------

    #[test]
    fn v22_plain_limit_is_untouched() {
        assert!(LimitTranslator::translate("SELECT * FROM daily_logs LIMIT 10").is_none());
    }

    #[test]
    fn v22_limit_offset_pair_is_untouched() {
        assert!(LimitTranslator::translate("SELECT * FROM t LIMIT 10 OFFSET 5").is_none());
    }

    #[test]
    fn v22_no_limit_clause_is_untouched() {
        assert!(LimitTranslator::translate("SELECT * FROM t WHERE a = 1").is_none());
    }

    #[test]
    fn v22_string_literal_named_limit_is_safe() {
        let q = "SELECT * FROM t WHERE kind = 'limit' AND note = 'offset'";
        assert!(LimitTranslator::translate(q).is_none());
    }

    #[test]
    fn v22_quoted_identifier_is_safe() {
        let q = "SELECT \"limit\", \"offset\" FROM t";
        assert!(LimitTranslator::translate(q).is_none());
    }

    #[test]
    fn v22_subquery_limit_is_left_alone() {
        // bracket depth > 0 -> deliberately not rewritten
        let q = "SELECT * FROM (SELECT a FROM t LIMIT NULL) x";
        assert!(LimitTranslator::translate(q).is_none());
    }

    #[test]
    fn v22_unbound_placeholder_is_left_alone() {
        assert!(LimitTranslator::translate("SELECT 1 LIMIT $4 OFFSET $5").is_none());
        assert!(LimitTranslator::translate("SELECT 1 LIMIT CAST($4 AS BIGINT)").is_none());
    }

    #[test]
    fn v22_comma_form_is_left_alone() {
        assert!(LimitTranslator::translate("SELECT 1 LIMIT 10, 20").is_none());
    }

    #[test]
    fn v22_is_idempotent() {
        let once = LimitTranslator::translate("SELECT 1 LIMIT NULL OFFSET NULL").unwrap();
        assert_eq!(once, "SELECT 1 LIMIT -1 OFFSET 0");
        assert!(LimitTranslator::translate(&once).is_none());
    }

    #[test]
    fn v22_case_insensitive() {
        let out = LimitTranslator::translate("select 1 limit cast(null as bigint)").unwrap();
        assert_eq!(out, "select 1 limit -1");
    }

    #[test]
    fn v22_needs_translation_prefilter() {
        assert!(LimitTranslator::needs_translation("SELECT 1 LIMIT 1"));
        assert!(LimitTranslator::needs_translation("SELECT 1 offset 1"));
        assert!(!LimitTranslator::needs_translation("SELECT * FROM t WHERE a = 1"));
    }

    #[test]
    fn v22_line_comment_is_skipped() {
        let q = "SELECT a FROM t -- LIMIT NULL\nWHERE a = 1";
        assert!(LimitTranslator::translate(q).is_none());
    }

    #[test]
    fn v22_escaped_quote_inside_literal() {
        let q = "SELECT * FROM t WHERE s = 'it''s a limit' AND x = 1";
        assert!(LimitTranslator::translate(q).is_none());
    }
}
