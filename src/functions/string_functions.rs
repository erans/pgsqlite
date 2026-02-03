use once_cell::sync::Lazy;
use rusqlite::{functions::FunctionFlags, Connection, Result};
use tracing::debug;
use unicode_normalization::UnicodeNormalization;

/// Register all PostgreSQL string functions
pub fn register_string_functions(conn: &Connection) -> Result<()> {
    debug!("Registering string functions");

    // Register split_part function
    conn.create_scalar_function(
        "split_part",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            let delimiter = ctx.get::<String>(1)?;
            let field_num = ctx.get::<i64>(2)?;

            if field_num < 1 {
                return Ok("".to_string());
            }

            let parts: Vec<&str> = string.split(&delimiter).collect();
            let index = (field_num - 1) as usize; // Convert to 0-based index

            if index < parts.len() {
                Ok(parts[index].to_string())
            } else {
                Ok("".to_string())
            }
        },
    )?;

    // Register string_agg function - this is an aggregate function
    conn.create_aggregate_function(
        "string_agg",
        2,
        FunctionFlags::SQLITE_UTF8,
        StringAggregator,
    )?;

    // Register translate function
    conn.create_scalar_function(
        "translate",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            let from_chars = ctx.get::<String>(1)?;
            let to_chars = ctx.get::<String>(2)?;

            let from_vec: Vec<char> = from_chars.chars().collect();
            let to_vec: Vec<char> = to_chars.chars().collect();

            let mut result = String::new();
            for ch in string.chars() {
                if let Some(pos) = from_vec.iter().position(|&c| c == ch) {
                    if pos < to_vec.len() {
                        result.push(to_vec[pos]);
                    }
                    // If to_chars is shorter than from_chars, characters are removed
                } else {
                    result.push(ch);
                }
            }

            Ok(result)
        },
    )?;

    // Register ascii function
    conn.create_scalar_function(
        "ascii",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            if let Some(first_char) = string.chars().next() {
                Ok(first_char as u32 as i64)
            } else {
                Ok(0i64)
            }
        },
    )?;

    // unaccent(text) -> text
    conn.create_scalar_function(
        "unaccent",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let input: Option<String> = ctx.get(0)?;
            Ok(input.map(|s| unaccent_text(&s)))
        },
    )?;

    // unaccent(regdictionary, text) -> text
    // NOTE: pgsqlite currently ignores the regdictionary selection and uses the same implementation.
    conn.create_scalar_function(
        "unaccent",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _dict: Option<String> = ctx.get(0)?;
            let input: Option<String> = ctx.get(1)?;
            Ok(input.map(|s| unaccent_text(&s)))
        },
    )?;

    // Register chr function
    conn.create_scalar_function(
        "chr",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let code = ctx.get::<i64>(0)?;
            if (0..=1_114_111).contains(&code) {
                // Valid Unicode range
                if let Some(ch) = char::from_u32(code as u32) {
                    Ok(ch.to_string())
                } else {
                    Ok("".to_string())
                }
            } else {
                Ok("".to_string())
            }
        },
    )?;

    // Register repeat function
    conn.create_scalar_function(
        "repeat",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            let count = ctx.get::<i64>(1)?;

            if count < 0 {
                return Ok("".to_string());
            }

            Ok(string.repeat(count as usize))
        },
    )?;

    // Register reverse function
    conn.create_scalar_function(
        "reverse",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            Ok(string.chars().rev().collect::<String>())
        },
    )?;

    // Register left function
    conn.create_scalar_function(
        "left",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            let length = ctx.get::<i64>(1)?;

            if length <= 0 {
                return Ok("".to_string());
            }

            let chars: Vec<char> = string.chars().collect();
            let end = std::cmp::min(length as usize, chars.len());
            Ok(chars[..end].iter().collect())
        },
    )?;

    // Register right function
    conn.create_scalar_function(
        "right",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            let length = ctx.get::<i64>(1)?;

            if length <= 0 {
                return Ok("".to_string());
            }

            let chars: Vec<char> = string.chars().collect();
            let start = chars.len().saturating_sub(length as usize);
            Ok(chars[start..].iter().collect())
        },
    )?;

    // Register lpad function
    conn.create_scalar_function(
        "lpad",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            let length = ctx.get::<i64>(1)?;
            let fill_text = ctx.get::<String>(2)?;

            if length <= 0 {
                return Ok("".to_string());
            }

            let string_len = string.chars().count();
            let target_len = length as usize;

            if string_len >= target_len {
                // Truncate if too long
                Ok(string.chars().take(target_len).collect())
            } else {
                // Pad on the left
                let padding_needed = target_len - string_len;
                let fill_chars: Vec<char> =
                    fill_text.chars().cycle().take(padding_needed).collect();
                Ok(format!(
                    "{}{}",
                    fill_chars.iter().collect::<String>(),
                    string
                ))
            }
        },
    )?;

    // Register rpad function
    conn.create_scalar_function(
        "rpad",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let string = ctx.get::<String>(0)?;
            let length = ctx.get::<i64>(1)?;
            let fill_text = ctx.get::<String>(2)?;

            if length <= 0 {
                return Ok("".to_string());
            }

            let string_len = string.chars().count();
            let target_len = length as usize;

            if string_len >= target_len {
                // Truncate if too long
                Ok(string.chars().take(target_len).collect())
            } else {
                // Pad on the right
                let padding_needed = target_len - string_len;
                let fill_chars: Vec<char> =
                    fill_text.chars().cycle().take(padding_needed).collect();
                Ok(format!(
                    "{}{}",
                    string,
                    fill_chars.iter().collect::<String>()
                ))
            }
        },
    )?;

    debug!("Successfully registered string functions");
    Ok(())
}

static UNACCENT_RULES: Lazy<Vec<(String, String)>> = Lazy::new(|| {
    let mut rules = Vec::new();
    if let Ok(path) = std::env::var("PGSQLITE_UNACCENT_RULES_PATH")
        && let Ok(contents) = std::fs::read_to_string(path) {
            for line in contents.lines() {
                let l = line.trim();
                if l.is_empty() || l.starts_with('#') {
                    continue;
                }
                let parts: Vec<&str> = l.split_whitespace().collect();
                if parts.is_empty() {
                    continue;
                }
                let from = parts[0].to_string();
                let to = if parts.len() >= 2 {
                    parts[1].to_string()
                } else {
                    "".to_string()
                };
                rules.push((from, to));
            }
        }
    rules
});

fn unaccent_text(input: &str) -> String {
    let input = maybe_repair_mojibake(input);

    // Step 1: compatibility decomposition then strip combining marks.
    let mut s: String = input.nfkd().filter(|c| !is_combining_mark(*c)).collect();

    // Step 2: common compatibility substitutions not guaranteed by nfkd.
    // Keep this small and deterministic.
    if s.contains('ß') {
        s = s.replace('ß', "ss");
    }

    // Step 3: optional additional rules (Postgres-like unaccent.rules support via env path).
    for (from, to) in UNACCENT_RULES.iter() {
        if s.contains(from) {
            s = s.replace(from, to);
        }
    }

    s
}

fn maybe_repair_mojibake(input: &str) -> std::borrow::Cow<'_, str> {
    // Some clients can effectively double-encode UTF-8, producing mojibake like "HÃ´tel".
    // If this looks like that pattern, try to reinterpret the Latin-1 bytes as UTF-8.
    if !(input.contains('Ã') || input.contains('Â') || input.contains('â')) {
        return std::borrow::Cow::Borrowed(input);
    }

    let mut bytes = Vec::with_capacity(input.len());
    for ch in input.chars() {
        let u = ch as u32;
        if u > 0xFF {
            return std::borrow::Cow::Borrowed(input);
        }
        bytes.push(u as u8);
    }

    match String::from_utf8(bytes) {
        Ok(s) => std::borrow::Cow::Owned(s),
        Err(_) => std::borrow::Cow::Borrowed(input),
    }
}

fn is_combining_mark(c: char) -> bool {
    let u = c as u32;
    matches!(
        u,
        0x0300..=0x036F
            | 0x1AB0..=0x1AFF
            | 0x1DC0..=0x1DFF
            | 0x20D0..=0x20FF
            | 0xFE20..=0xFE2F
    )
}

/// String aggregator for string_agg function
#[derive(Debug)]
struct StringAggregator;

impl rusqlite::functions::Aggregate<(Vec<String>, Option<String>), Option<String>>
    for StringAggregator
{
    fn init(
        &self,
        _ctx: &mut rusqlite::functions::Context<'_>,
    ) -> rusqlite::Result<(Vec<String>, Option<String>)> {
        Ok((Vec::new(), None))
    }

    fn step(
        &self,
        ctx: &mut rusqlite::functions::Context<'_>,
        agg: &mut (Vec<String>, Option<String>),
    ) -> rusqlite::Result<()> {
        let value = ctx.get::<String>(0)?;
        agg.0.push(value);

        if agg.1.is_none() {
            let delimiter = ctx.get::<String>(1)?;
            agg.1 = Some(delimiter);
        }

        Ok(())
    }

    fn finalize(
        &self,
        _ctx: &mut rusqlite::functions::Context<'_>,
        agg: Option<(Vec<String>, Option<String>)>,
    ) -> rusqlite::Result<Option<String>> {
        match agg {
            Some((values, delimiter)) => {
                if values.is_empty() {
                    Ok(None)
                } else {
                    let delimiter = delimiter.as_deref().unwrap_or(",");
                    Ok(Some(values.join(delimiter)))
                }
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_split_part() {
        let conn = Connection::open_in_memory().unwrap();
        register_string_functions(&conn).unwrap();

        let result: String = conn
            .query_row("SELECT split_part('abc,def,ghi', ',', 2)", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(result, "def");

        // Test out of bounds
        let result: String = conn
            .query_row("SELECT split_part('abc,def,ghi', ',', 5)", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(result, "");

        // Test negative index
        let result: String = conn
            .query_row("SELECT split_part('abc,def,ghi', ',', -1)", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_translate() {
        let conn = Connection::open_in_memory().unwrap();
        register_string_functions(&conn).unwrap();

        let result: String = conn
            .query_row("SELECT translate('hello', 'elo', 'xyz')", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(result, "hxyyz");

        // Test character removal (to_chars shorter than from_chars)
        let result: String = conn
            .query_row("SELECT translate('hello', 'elo', 'x')", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(result, "hx");
    }

    #[test]
    fn test_ascii_chr() {
        let conn = Connection::open_in_memory().unwrap();
        register_string_functions(&conn).unwrap();

        let result: i64 = conn
            .query_row("SELECT ascii('A')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, 65);

        let result: String = conn
            .query_row("SELECT chr(65)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "A");
    }

    #[test]
    fn test_repeat() {
        let conn = Connection::open_in_memory().unwrap();
        register_string_functions(&conn).unwrap();

        let result: String = conn
            .query_row("SELECT repeat('abc', 3)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "abcabcabc");
    }

    #[test]
    fn test_reverse() {
        let conn = Connection::open_in_memory().unwrap();
        register_string_functions(&conn).unwrap();

        let result: String = conn
            .query_row("SELECT reverse('hello')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "olleh");
    }

    #[test]
    fn test_left_right() {
        let conn = Connection::open_in_memory().unwrap();
        register_string_functions(&conn).unwrap();

        let result: String = conn
            .query_row("SELECT left('hello', 3)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "hel");

        let result: String = conn
            .query_row("SELECT right('hello', 3)", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "llo");
    }

    #[test]
    fn test_lpad_rpad() {
        let conn = Connection::open_in_memory().unwrap();
        register_string_functions(&conn).unwrap();

        let result: String = conn
            .query_row("SELECT lpad('hello', 8, 'x')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "xxxhello");

        let result: String = conn
            .query_row("SELECT rpad('hello', 8, 'x')", [], |row| row.get(0))
            .unwrap();
        assert_eq!(result, "helloxxx");
    }
}
