// === PATCH v29d: PostgreSQL escape-string constants  E'...' ===
//
// DBeaver reads catalog metadata with queries such as
//
//     SELECT count(*) FROM pg_class WHERE relname LIKE E'pg\_class'
//
// SQLite has no `E'...'` syntax, so this used to die with
//
//     SQLite error: near "'pg\_class'": syntax error
//
// aborting the whole metadata read (navigator expand / columns / indexes /
// DDL panel all came up empty or errored).
//
// This translator decodes the escape-string constant into a plain SQLite
// string literal following PostgreSQL's documented rules, then re-quotes it
// with `''` doubling.  Ordinary `'...'` literals and `"..."` identifiers are
// copied through untouched.

pub struct EscapeStringTranslator;

impl EscapeStringTranslator {
    /// Cheap gate: does the statement contain a token-initial `E'` / `e'`?
    ///
    /// May return a false positive when an `e'` sequence sits inside an
    /// ordinary literal; that only costs one extra allocation because
    /// `translate` tracks literals properly.
    pub fn contains_escape_string(sql: &str) -> bool {
        let b = sql.as_bytes();
        if b.len() < 2 {
            return false;
        }
        for i in 0..b.len() - 1 {
            if (b[i] == b'E' || b[i] == b'e') && b[i + 1] == b'\'' {
                let prev_is_ident = i > 0 && {
                    let p = b[i - 1];
                    p.is_ascii_alphanumeric() || p == b'_' || p == b'$'
                };
                if !prev_is_ident {
                    return true;
                }
            }
        }
        false
    }

    /// Rewrite every escape-string constant into a plain SQLite literal.
    pub fn translate(sql: &str) -> String {
        let chars: Vec<char> = sql.chars().collect();
        let n = chars.len();
        let mut out = String::with_capacity(sql.len());
        let mut i = 0usize;

        while i < n {
            let c = chars[i];

            // Ordinary single-quoted literal -> copy verbatim ('' doubling).
            if c == '\'' {
                out.push(c);
                i += 1;
                while i < n {
                    if chars[i] == '\'' {
                        if i + 1 < n && chars[i + 1] == '\'' {
                            out.push('\'');
                            out.push('\'');
                            i += 2;
                            continue;
                        }
                        out.push('\'');
                        i += 1;
                        break;
                    }
                    out.push(chars[i]);
                    i += 1;
                }
                continue;
            }

            // Double-quoted identifier -> copy verbatim ("" doubling).
            if c == '"' {
                out.push(c);
                i += 1;
                while i < n {
                    if chars[i] == '"' {
                        if i + 1 < n && chars[i + 1] == '"' {
                            out.push('"');
                            out.push('"');
                            i += 2;
                            continue;
                        }
                        out.push('"');
                        i += 1;
                        break;
                    }
                    out.push(chars[i]);
                    i += 1;
                }
                continue;
            }

            // Escape-string constant.  `date'...'`-style type prefixes end in
            // an identifier character, so they are deliberately skipped.
            if (c == 'E' || c == 'e') && i + 1 < n && chars[i + 1] == '\'' {
                let prev_is_ident = i > 0 && {
                    let p = chars[i - 1];
                    p.is_alphanumeric() || p == '_' || p == '$'
                };
                if !prev_is_ident {
                    let (decoded, next) = Self::decode(&chars, i + 1);
                    out.push('\'');
                    for ch in decoded.chars() {
                        if ch == '\'' {
                            out.push('\'');
                        }
                        out.push(ch);
                    }
                    out.push('\'');
                    i = next;
                    continue;
                }
            }

            out.push(c);
            i += 1;
        }

        out
    }

    /// Decode the body of an escape string.  `open_quote` indexes the opening
    /// `'`.  Returns the decoded text and the index just past the closing `'`
    /// (or the end of input when the literal is unterminated).
    fn decode(chars: &[char], open_quote: usize) -> (String, usize) {
        let n = chars.len();
        let mut s = String::new();
        let mut i = open_quote + 1;

        while i < n {
            let c = chars[i];

            if c == '\'' {
                // PostgreSQL accepts both '' and \' inside an E-string.
                if i + 1 < n && chars[i + 1] == '\'' {
                    s.push('\'');
                    i += 2;
                    continue;
                }
                return (s, i + 1);
            }

            if c != '\\' {
                s.push(c);
                i += 1;
                continue;
            }

            // Backslash escape.
            if i + 1 >= n {
                s.push('\\');
                i += 1;
                continue;
            }
            let e = chars[i + 1];
            i += 2;
            match e {
                'b' => s.push('\u{08}'),
                'f' => s.push('\u{0C}'),
                'n' => s.push('\n'),
                'r' => s.push('\r'),
                't' => s.push('\t'),
                'x' => {
                    let mut v: u32 = 0;
                    let mut digits = 0;
                    while digits < 2 && i < n && chars[i].is_ascii_hexdigit() {
                        v = v * 16 + chars[i].to_digit(16).unwrap_or(0);
                        i += 1;
                        digits += 1;
                    }
                    if digits == 0 {
                        s.push('x');
                    } else {
                        Self::push_code_point(&mut s, v);
                    }
                }
                'u' | 'U' => {
                    let want = if e == 'u' { 4 } else { 8 };
                    let mut v: u32 = 0;
                    let mut digits = 0;
                    while digits < want && i < n && chars[i].is_ascii_hexdigit() {
                        v = v * 16 + chars[i].to_digit(16).unwrap_or(0);
                        i += 1;
                        digits += 1;
                    }
                    if digits == 0 {
                        s.push(e);
                    } else {
                        Self::push_code_point(&mut s, v);
                    }
                }
                '0'..='7' => {
                    let mut v: u32 = e.to_digit(8).unwrap_or(0);
                    let mut digits = 1;
                    while digits < 3 && i < n && chars[i].is_digit(8) {
                        v = v * 8 + chars[i].to_digit(8).unwrap_or(0);
                        i += 1;
                        digits += 1;
                    }
                    Self::push_code_point(&mut s, v);
                }
                // Any other character loses the backslash and is taken
                // literally -- this is what turns E'pg\_class' into pg_class.
                other => s.push(other),
            }
        }

        (s, i)
    }

    /// NUL cannot travel through a SQLite text value, so it is dropped rather
    /// than corrupting the statement.
    fn push_code_point(s: &mut String, v: u32) {
        if v == 0 {
            return;
        }
        if let Some(ch) = char::from_u32(v) {
            s.push(ch);
        }
    }
}

#[cfg(test)]
mod v29d_escape_string_tests {
    use super::EscapeStringTranslator as T;

    #[test]
    fn gate_detects_real_dbeaver_query() {
        let sql = r"SELECT count(*) FROM pg_class WHERE relname LIKE E'pg\_class'";
        assert!(T::contains_escape_string(sql));
    }

    #[test]
    fn gate_ignores_plain_sql() {
        assert!(!T::contains_escape_string("SELECT 1"));
        assert!(!T::contains_escape_string("SELECT * FROM users WHERE name = 'bob'"));
    }

    #[test]
    fn decodes_real_dbeaver_query() {
        let sql = r"SELECT count(*) FROM pg_class WHERE relname LIKE E'pg\_class'";
        assert_eq!(
            T::translate(sql),
            "SELECT count(*) FROM pg_class WHERE relname LIKE 'pg_class'"
        );
    }

    #[test]
    fn strips_prefix_when_no_escapes_present() {
        assert_eq!(
            T::translate("SELECT count(*) FROM pg_class WHERE relname LIKE E'users'"),
            "SELECT count(*) FROM pg_class WHERE relname LIKE 'users'"
        );
    }

    #[test]
    fn accepts_lowercase_e() {
        assert_eq!(T::translate(r"SELECT e'pg\_class'"), "SELECT 'pg_class'");
    }

    #[test]
    fn decodes_double_backslash() {
        assert_eq!(T::translate(r"SELECT E'a\\b'"), r"SELECT 'a\b'");
    }

    #[test]
    fn decodes_escaped_quote_into_doubled_quote() {
        assert_eq!(T::translate(r"SELECT E'it\'s'"), "SELECT 'it''s'");
    }

    #[test]
    fn decodes_doubled_quote_inside_e_string() {
        assert_eq!(T::translate("SELECT E'it''s'"), "SELECT 'it''s'");
    }

    #[test]
    fn decodes_control_escapes() {
        assert_eq!(T::translate(r"SELECT E'a\nb\tc'"), "SELECT 'a\nb\tc'");
    }

    #[test]
    fn decodes_hex_octal_unicode() {
        assert_eq!(T::translate(r"SELECT E'\x41'"), "SELECT 'A'");
        assert_eq!(T::translate(r"SELECT E'\101'"), "SELECT 'A'");
        assert_eq!(T::translate(r"SELECT E'\u0041'"), "SELECT 'A'");
    }

    #[test]
    fn leaves_ordinary_literal_untouched() {
        let sql = r"SELECT 'pg\_class' FROM t";
        assert_eq!(T::translate(sql), sql);
    }

    #[test]
    fn leaves_e_inside_literal_untouched() {
        let sql = "SELECT 'x e''y'' z' FROM t";
        assert_eq!(T::translate(sql), sql);
    }

    #[test]
    fn leaves_type_prefixed_literal_untouched() {
        // `date'...'` ends in an identifier char before the quote.
        let sql = "SELECT date'2020-01-01' FROM t";
        assert_eq!(T::translate(sql), sql);
    }

    #[test]
    fn handles_multiple_e_strings() {
        assert_eq!(
            T::translate(r"SELECT E'a\_b' || E'c\_d'"),
            "SELECT 'a_b' || 'c_d'"
        );
    }

    #[test]
    fn handles_e_string_next_to_quoted_identifier() {
        let sql = r#"SELECT "relname" FROM pg_class WHERE "relname" LIKE E'user\_%'"#;
        assert_eq!(
            T::translate(sql),
            r#"SELECT "relname" FROM pg_class WHERE "relname" LIKE 'user_%'"#
        );
    }

    #[test]
    fn unterminated_e_string_does_not_panic() {
        let _ = T::translate(r"SELECT E'abc\");
        let _ = T::translate("SELECT E'abc");
    }
}
