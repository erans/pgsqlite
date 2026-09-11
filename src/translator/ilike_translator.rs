// ILIKE -> LIKE translator.
//
// SQLite has no ILIKE operator. Its LIKE is already case-insensitive for ASCII,
// which matches PostgreSQL's ILIKE semantics closely enough for the catalog and
// metadata queries GUI clients issue (table-name filters, schema browsing, ...).
//
// Without this translation, any query containing ILIKE that reaches SQLite fails
// with `near "ILIKE": syntax error`. In the extended protocol that aborts the
// client's transaction and cascades into "current transaction is aborted" for
// every subsequent statement, which looks to the user like the whole connection
// broke. See PATCH v11 in catalog/query_interceptor.rs for why catalog JOIN
// queries now reach SQLite in the first place.

pub struct IlikeTranslator;

impl IlikeTranslator {
    #[inline]
    fn is_ident_byte(b: u8) -> bool {
        b.is_ascii_alphanumeric() || b == b'_' || b == b'$'
    }

    /// Cheap ASCII case-insensitive probe for the substring "ilike".
    /// Avoids allocating a lowercased copy of every query we see.
    pub fn contains_ilike(query: &str) -> bool {
        let b = query.as_bytes();
        let n = b.len();
        if n < 5 {
            return false;
        }
        let mut i = 0usize;
        while i + 5 <= n {
            if (b[i] | 0x20) == b'i'
                && (b[i + 1] | 0x20) == b'l'
                && (b[i + 2] | 0x20) == b'i'
                && (b[i + 3] | 0x20) == b'k'
                && (b[i + 4] | 0x20) == b'e'
            {
                return true;
            }
            i += 1;
        }
        false
    }

    /// Replace every standalone ILIKE keyword with LIKE.
    ///
    /// String literals ('...') and quoted identifiers ("...") are copied
    /// verbatim so an ILIKE appearing inside data is never touched. Doubled
    /// quotes ('' and "") are handled as escapes. Non-ASCII bytes are copied
    /// through untouched, so UTF-8 stays intact.
    pub fn translate_query(query: &str) -> String {
        if !Self::contains_ilike(query) {
            return query.to_string();
        }

        let b = query.as_bytes();
        let n = b.len();
        let mut out: Vec<u8> = Vec::with_capacity(n);
        let mut i = 0usize;

        while i < n {
            let c = b[i];

            // Single-quoted string literal
            if c == b'\'' {
                let start = i;
                i += 1;
                while i < n {
                    if b[i] == b'\'' {
                        if i + 1 < n && b[i + 1] == b'\'' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.extend_from_slice(&b[start..i]);
                continue;
            }

            // Double-quoted identifier
            if c == b'"' {
                let start = i;
                i += 1;
                while i < n {
                    if b[i] == b'"' {
                        if i + 1 < n && b[i + 1] == b'"' {
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                out.extend_from_slice(&b[start..i]);
                continue;
            }

            // Standalone ILIKE keyword
            if (c | 0x20) == b'i'
                && i + 5 <= n
                && (b[i + 1] | 0x20) == b'l'
                && (b[i + 2] | 0x20) == b'i'
                && (b[i + 3] | 0x20) == b'k'
                && (b[i + 4] | 0x20) == b'e'
            {
                let prev_ok = i == 0 || !Self::is_ident_byte(b[i - 1]);
                let next_ok = i + 5 >= n || !Self::is_ident_byte(b[i + 5]);
                if prev_ok && next_ok {
                    out.extend_from_slice(b"LIKE");
                    i += 5;
                    continue;
                }
            }

            out.push(c);
            i += 1;
        }

        String::from_utf8(out).unwrap_or_else(|_| query.to_string())
    }

    /// Cheap ASCII case-insensitive probe for the substring "like".
    /// Matches ILIKE too (it contains "like"), so one probe gates both passes.
    pub fn contains_like(query: &str) -> bool {
        let b = query.as_bytes();
        let n = b.len();
        if n < 4 {
            return false;
        }
        let mut i = 0usize;
        while i + 4 <= n {
            if (b[i] | 0x20) == b'l'
                && (b[i + 1] | 0x20) == b'i'
                && (b[i + 2] | 0x20) == b'k'
                && (b[i + 3] | 0x20) == b'e'
            {
                return true;
            }
            i += 1;
        }
        false
    }

    /// === PATCH v20 ===
    /// Give every bare LIKE the backslash escape character PostgreSQL applies
    /// by default.
    ///
    /// PostgreSQL: `LIKE 'pg\_%'` -> `\_` is a literal underscore.
    /// SQLite    : LIKE has NO default escape character, so `\_` means
    ///             "a backslash followed by any character" and the predicate
    ///             silently matches nothing. GUI clients escape underscores in
    ///             every object-name filter they build, so table search boxes
    ///             quietly returned zero rows instead of erroring out.
    ///
    /// Only a *simple* right-hand operand is rewritten: a string literal, a
    /// `$N` placeholder or a `?` placeholder. Anything else (concatenations,
    /// function calls, column refs) is left exactly as it was, so this pass can
    /// never change the shape of a query it does not fully understand.
    ///
    /// An existing ESCAPE clause always wins.
    pub fn add_default_like_escape(query: &str) -> String {
        let b = query.as_bytes();
        let n = b.len();
        let mut out: Vec<u8> = Vec::with_capacity(n + 16);
        let mut i = 0usize;

        while i < n {
            let c = b[i];

            // Single-quoted string literal — copy verbatim.
            if c == b'\'' {
                let start = i;
                i = Self::skip_single_quoted(b, n, i);
                out.extend_from_slice(&b[start..i]);
                continue;
            }

            // Double-quoted identifier — copy verbatim.
            if c == b'"' {
                let start = i;
                i = Self::skip_double_quoted(b, n, i);
                out.extend_from_slice(&b[start..i]);
                continue;
            }

            // Standalone LIKE keyword?
            if (c | 0x20) == b'l'
                && i + 4 <= n
                && (b[i + 1] | 0x20) == b'i'
                && (b[i + 2] | 0x20) == b'k'
                && (b[i + 3] | 0x20) == b'e'
                && (i == 0 || !Self::is_ident_byte(b[i - 1]))
                && (i + 4 >= n || !Self::is_ident_byte(b[i + 4]))
            {
                let after_kw = i + 4;
                let mut j = after_kw;
                while j < n && (b[j] as char).is_ascii_whitespace() {
                    j += 1;
                }

                // Right-hand operand: string literal / $N / ? — anything else is
                // too complex to append to safely, so we bail out untouched.
                let operand_end = if j < n && b[j] == b'\'' {
                    Some(Self::skip_single_quoted(b, n, j))
                } else if j < n && b[j] == b'$' && j + 1 < n && b[j + 1].is_ascii_digit() {
                    let mut k = j + 1;
                    while k < n && b[k].is_ascii_digit() {
                        k += 1;
                    }
                    Some(k)
                } else if j < n && b[j] == b'?' {
                    let mut k = j + 1;
                    while k < n && b[k].is_ascii_digit() {
                        k += 1;
                    }
                    Some(k)
                } else {
                    None
                };

                if let Some(end) = operand_end {
                    // Is an ESCAPE clause already present?
                    let mut p = end;
                    while p < n && (b[p] as char).is_ascii_whitespace() {
                        p += 1;
                    }
                    let has_escape = p + 6 <= n
                        && (b[p] | 0x20) == b'e'
                        && (b[p + 1] | 0x20) == b's'
                        && (b[p + 2] | 0x20) == b'c'
                        && (b[p + 3] | 0x20) == b'a'
                        && (b[p + 4] | 0x20) == b'p'
                        && (b[p + 5] | 0x20) == b'e'
                        && (p + 6 >= n || !Self::is_ident_byte(b[p + 6]));

                    out.extend_from_slice(&b[i..end]);
                    // === PATCH v20b ===
                    // Only append when the predicate genuinely ends here. In
                    // `LIKE '%' || $1 || '%'` the literal is merely the first
                    // slice of a concatenation, and hanging ESCAPE off it would
                    // produce broken SQL. Whitelist of terminators only.
                    if !has_escape && Self::is_predicate_end(b, n, p) {
                        out.extend_from_slice(b" ESCAPE '\\'");
                    }
                    i = end;
                    continue;
                }

                // Unknown operand shape: emit the keyword and carry on normally.
                out.extend_from_slice(&b[i..after_kw]);
                i = after_kw;
                continue;
            }

            out.push(c);
            i += 1;
        }

        String::from_utf8(out).unwrap_or_else(|_| query.to_string())
    }

    /// ILIKE -> LIKE, then give every bare LIKE PostgreSQL's default escape.
    /// This is the entry point call sites should use.
    pub fn normalize_like(query: &str) -> String {
        let step1 = Self::translate_query(query);
        Self::add_default_like_escape(&step1)
    }


    /// True when position `p` is where a LIKE predicate legitimately ends, i.e.
    /// appending an ESCAPE clause there is syntactically safe.
    ///
    /// Deliberately a whitelist: end-of-query, a closing paren / comma /
    /// semicolon, or a clause keyword. Operators that continue the pattern
    /// expression (`||`, `::`, `+`, ...) are NOT terminators, so those queries
    /// are left untouched rather than being rewritten incorrectly.
    fn is_predicate_end(b: &[u8], n: usize, p: usize) -> bool {
        if p >= n {
            return true;
        }
        match b[p] {
            b')' | b',' | b';' => return true,
            _ => {}
        }
        if !b[p].is_ascii_alphabetic() {
            return false;
        }
        let mut e = p;
        while e < n && Self::is_ident_byte(b[e]) {
            e += 1;
        }
        let word = &b[p..e];
        const TERMINATORS: &[&[u8]] = &[
            b"and", b"or", b"order", b"group", b"limit", b"offset", b"having",
            b"union", b"intersect", b"except", b"then", b"else", b"end",
            b"when", b"on", b"window", b"fetch", b"returning", b"for",
            b"escape", b"is", b"collate", b"asc", b"desc", b"where", b"from",
        ];
        TERMINATORS.iter().any(|t| {
            t.len() == word.len()
                && t.iter()
                    .zip(word.iter())
                    .all(|(a, c)| *a == (*c | 0x20))
        })
    }

    #[inline]
    fn skip_single_quoted(b: &[u8], n: usize, mut i: usize) -> usize {
        i += 1;
        while i < n {
            if b[i] == b'\'' {
                if i + 1 < n && b[i + 1] == b'\'' {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i += 1;
        }
        n
    }

    #[inline]
    fn skip_double_quoted(b: &[u8], n: usize, mut i: usize) -> usize {
        i += 1;
        while i < n {
            if b[i] == b'"' {
                if i + 1 < n && b[i + 1] == b'"' {
                    i += 2;
                    continue;
                }
                return i + 1;
            }
            i += 1;
        }
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rewrites_standalone_ilike() {
        assert_eq!(
            IlikeTranslator::translate_query("SELECT * FROM t WHERE a ILIKE '%x%'"),
            "SELECT * FROM t WHERE a LIKE '%x%'"
        );
    }

    #[test]
    fn rewrites_lowercase_and_not_ilike() {
        assert_eq!(
            IlikeTranslator::translate_query("WHERE a ilike 'p' AND b NOT ILIKE 'q'"),
            "WHERE a LIKE 'p' AND b NOT LIKE 'q'"
        );
    }

    #[test]
    fn preserves_escape_clause() {
        assert_eq!(
            IlikeTranslator::translate_query("WHERE relname ILIKE $1 ESCAPE '~'"),
            "WHERE relname LIKE $1 ESCAPE '~'"
        );
    }

    #[test]
    fn does_not_touch_string_literals() {
        let q = "SELECT 'this ILIKE that' AS s FROM t";
        assert_eq!(IlikeTranslator::translate_query(q), q);
    }

    #[test]
    fn does_not_touch_quoted_identifiers() {
        let q = "SELECT \"ILIKE\" FROM t";
        assert_eq!(IlikeTranslator::translate_query(q), q);
    }

    #[test]
    fn does_not_touch_substrings() {
        let q = "SELECT similake, xilike FROM t";
        assert_eq!(IlikeTranslator::translate_query(q), q);
    }

    #[test]
    fn no_ilike_is_a_noop() {
        let q = "SELECT * FROM t WHERE a LIKE 'x'";
        assert_eq!(IlikeTranslator::translate_query(q), q);
    }

    // ---------------------------------------------------------------- PATCH v20
    #[test]
    fn v20_adds_default_escape_to_literal() {
        assert_eq!(
            IlikeTranslator::normalize_like("SELECT 1 WHERE relname LIKE 'pg\\_%'"),
            "SELECT 1 WHERE relname LIKE 'pg\\_%' ESCAPE '\\'"
        );
    }

    #[test]
    fn v20_adds_default_escape_to_ilike_literal() {
        assert_eq!(
            IlikeTranslator::normalize_like("WHERE a ILIKE '%grow\\_award%'"),
            "WHERE a LIKE '%grow\\_award%' ESCAPE '\\'"
        );
    }

    #[test]
    fn v20_adds_default_escape_to_placeholders() {
        assert_eq!(
            IlikeTranslator::normalize_like("WHERE a LIKE $1 AND b LIKE ?"),
            "WHERE a LIKE $1 ESCAPE '\\' AND b LIKE ? ESCAPE '\\'"
        );
    }

    #[test]
    fn v20_keeps_existing_escape_clause() {
        let q = "WHERE relname LIKE $1 ESCAPE '~'";
        assert_eq!(IlikeTranslator::normalize_like(q), q);
        let q2 = "WHERE relname ILIKE 'a~_b' escape '~'";
        assert_eq!(
            IlikeTranslator::normalize_like(q2),
            "WHERE relname LIKE 'a~_b' escape '~'"
        );
    }

    #[test]
    fn v20_leaves_complex_operands_alone() {
        let q = "WHERE a LIKE lower(b)";
        assert_eq!(IlikeTranslator::normalize_like(q), q);
    }

    #[test]
    fn v20_does_not_touch_like_inside_literals() {
        let q = "SELECT 'x LIKE ''y''' AS s FROM t";
        assert_eq!(IlikeTranslator::normalize_like(q), q);
    }

    #[test]
    fn v20_not_like_is_covered() {
        assert_eq!(
            IlikeTranslator::normalize_like("WHERE nspname NOT LIKE 'pg\\_toast%'"),
            "WHERE nspname NOT LIKE 'pg\\_toast%' ESCAPE '\\'"
        );
    }

    #[test]
    fn v20_does_not_touch_substrings() {
        let q = "SELECT unlike, likeness FROM t";
        assert_eq!(IlikeTranslator::normalize_like(q), q);
    }

    #[test]
    fn v20_is_idempotent() {
        let once = IlikeTranslator::normalize_like("WHERE a LIKE 'x\\_y'");
        assert_eq!(IlikeTranslator::normalize_like(&once), once);
    }

    #[test]
    fn v20b_concatenated_pattern_is_untouched() {
        let q = "WHERE a LIKE '%' || $1 || '%'";
        assert_eq!(IlikeTranslator::normalize_like(q), q);
        let q2 = "WHERE a ILIKE '%' || ? || '%' AND b = 1";
        assert_eq!(
            IlikeTranslator::normalize_like(q2),
            "WHERE a LIKE '%' || ? || '%' AND b = 1"
        );
    }

    #[test]
    fn v20b_terminators_still_get_escape() {
        for (input, want) in [
            ("WHERE a LIKE 'x\\_y'", "WHERE a LIKE 'x\\_y' ESCAPE '\\'"),
            ("WHERE (a LIKE $1)", "WHERE (a LIKE $1 ESCAPE '\\')"),
            ("WHERE a LIKE $1 AND b = 2", "WHERE a LIKE $1 ESCAPE '\\' AND b = 2"),
            ("WHERE a LIKE $1 ORDER BY b", "WHERE a LIKE $1 ESCAPE '\\' ORDER BY b"),
            ("SELECT f(a LIKE 'x', 1)", "SELECT f(a LIKE 'x' ESCAPE '\\', 1)"),
            ("WHERE a LIKE 'x';", "WHERE a LIKE 'x' ESCAPE '\\';"),
        ] {
            assert_eq!(IlikeTranslator::normalize_like(input), want, "input={input}");
        }
    }

    #[test]
    fn v20b_unknown_trailing_token_is_left_alone() {
        let q = "WHERE a LIKE 'x' :: text";
        assert_eq!(IlikeTranslator::normalize_like(q), q);
    }
}
