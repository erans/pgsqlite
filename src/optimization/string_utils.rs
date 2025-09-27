use std::borrow::Cow;
use std::collections::HashMap;
use once_cell::sync::Lazy;

/// Optimized string utilities using Cow<str> to reduce allocations
pub struct StringOptimizer {
    /// Cache for commonly used strings to avoid repeated allocations
    static_strings: HashMap<&'static str, &'static str>,
    /// Pre-allocated string patterns
    command_tags: HashMap<(&'static str, u32), Cow<'static, str>>,
}

impl Default for StringOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl StringOptimizer {
    pub fn new() -> Self {
        let mut optimizer = Self {
            static_strings: HashMap::new(),
            command_tags: HashMap::new(),
        };

        // Pre-populate common static strings
        optimizer.init_static_strings();
        optimizer.init_command_tags();

        optimizer
    }

    fn init_static_strings(&mut self) {
        // Common error messages and protocol strings
        self.static_strings.insert("Empty query", "Empty query");
        self.static_strings.insert("DEALLOCATE", "DEALLOCATE");
        self.static_strings.insert("Could not extract table name", "Could not extract table name");
        self.static_strings.insert("Could not extract column definitions", "Could not extract column definitions");
        self.static_strings.insert("Query contains potentially malicious quote injection pattern",
                                   "Query contains potentially malicious quote injection pattern");
        self.static_strings.insert("Query contains potentially malicious pattern",
                                   "Query contains potentially malicious pattern");
        self.static_strings.insert("Query contains dangerous multi-statement pattern",
                                   "Query contains dangerous multi-statement pattern");
    }

    fn init_command_tags(&mut self) {
        // Common PostgreSQL command completion tags
        self.command_tags.insert(("INSERT", 0), Cow::Borrowed("INSERT 0 0"));
        self.command_tags.insert(("INSERT", 1), Cow::Borrowed("INSERT 0 1"));
        self.command_tags.insert(("UPDATE", 0), Cow::Borrowed("UPDATE 0"));
        self.command_tags.insert(("UPDATE", 1), Cow::Borrowed("UPDATE 1"));
        self.command_tags.insert(("DELETE", 0), Cow::Borrowed("DELETE 0"));
        self.command_tags.insert(("DELETE", 1), Cow::Borrowed("DELETE 1"));
        self.command_tags.insert(("SELECT", 0), Cow::Borrowed("SELECT 0"));
    }

    /// Get a static error message or create one if not cached
    pub fn get_error_message(&self, key: &str) -> Cow<'static, str> {
        if let Some(&static_str) = self.static_strings.get(key) {
            Cow::Borrowed(static_str)
        } else {
            Cow::Owned(key.to_string())
        }
    }

    /// Get an optimized command completion tag
    pub fn get_command_tag(&self, command: &str, rows: u32) -> Cow<'static, str> {
        if let Some(tag) = self.command_tags.get(&(command, rows)) {
            tag.clone()
        } else if rows <= 1000 {
            // For small row counts, create optimized strings
            match command {
                "INSERT" => Cow::Owned(format!("INSERT 0 {}", rows)),
                "UPDATE" => Cow::Owned(format!("UPDATE {}", rows)),
                "DELETE" => Cow::Owned(format!("DELETE {}", rows)),
                "SELECT" => Cow::Owned(format!("SELECT {}", rows)),
                _ => Cow::Owned(format!("{} {}", command, rows)),
            }
        } else {
            Cow::Owned(format!("{} {}", command, rows))
        }
    }

    /// Optimize column name extraction avoiding unnecessary allocations
    pub fn extract_column_name<'a>(&self, input: &'a str) -> Cow<'a, str> {
        // Check if this is already a clean column name
        if input.chars().all(|c| c.is_alphanumeric() || c == '_') {
            Cow::Borrowed(input)
        } else {
            // Need to clean/extract the column name
            let trimmed = input.trim();
            if trimmed == input {
                Cow::Borrowed(input)
            } else {
                Cow::Owned(trimmed.to_string())
            }
        }
    }

    /// Optimize table name extraction
    pub fn extract_table_name<'a>(&self, input: &'a str) -> Option<Cow<'a, str>> {
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return None;
        }

        // Simple case: clean identifier
        if trimmed.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
            Some(if trimmed == input {
                Cow::Borrowed(input)
            } else {
                Cow::Borrowed(trimmed)
            })
        } else {
            // More complex parsing needed
            Some(Cow::Owned(trimmed.to_string()))
        }
    }

    /// Optimize SQL value to string conversion
    pub fn value_to_string<'a>(&self, value: &'a rusqlite::types::Value) -> Cow<'a, str> {
        match value {
            rusqlite::types::Value::Integer(i) => {
                // Cache common small integers
                match *i {
                    0 => Cow::Borrowed("0"),
                    1 => Cow::Borrowed("1"),
                    -1 => Cow::Borrowed("-1"),
                    _ => Cow::Owned(i.to_string()),
                }
            }
            rusqlite::types::Value::Real(f) => {
                // Cache common floating point values
                if *f == 0.0 {
                    Cow::Borrowed("0")
                } else if *f == 1.0 {
                    Cow::Borrowed("1")
                } else {
                    Cow::Owned(f.to_string())
                }
            }
            rusqlite::types::Value::Text(s) => Cow::Borrowed(s),
            rusqlite::types::Value::Blob(_) => Cow::Borrowed("[BLOB]"),
            rusqlite::types::Value::Null => Cow::Borrowed(""),
        }
    }

    /// Convert bytes to string with minimal allocations
    pub fn bytes_to_string_lossy<'a>(&self, bytes: &'a [u8]) -> Cow<'a, str> {
        match std::str::from_utf8(bytes) {
            Ok(s) => Cow::Borrowed(s),
            Err(_) => {
                // Only allocate if we truly need lossy conversion
                String::from_utf8_lossy(bytes)
            }
        }
    }

    /// Optimize column definition string processing
    pub fn process_column_definition<'a>(&self, def: &'a str) -> Cow<'a, str> {
        let trimmed = def.trim();
        if trimmed == def {
            Cow::Borrowed(def)
        } else {
            Cow::Borrowed(trimmed)
        }
    }
}

/// Global string optimizer instance
static GLOBAL_STRING_OPTIMIZER: Lazy<StringOptimizer> = Lazy::new(|| StringOptimizer::new());

/// Get the global string optimizer
pub fn global_string_optimizer() -> &'static StringOptimizer {
    &GLOBAL_STRING_OPTIMIZER
}

/// Trait for optimized string operations
pub trait StringOptimized {
    /// Convert to an optimized Cow<str>
    fn to_optimized_string(&self) -> Cow<'_, str>;

    /// Convert to owned string only if necessary
    fn to_string_if_needed(&self) -> Cow<'_, str>;
}

impl StringOptimized for str {
    fn to_optimized_string(&self) -> Cow<'_, str> {
        Cow::Borrowed(self)
    }

    fn to_string_if_needed(&self) -> Cow<'_, str> {
        Cow::Borrowed(self)
    }
}

impl StringOptimized for String {
    fn to_optimized_string(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_str())
    }

    fn to_string_if_needed(&self) -> Cow<'_, str> {
        Cow::Borrowed(self.as_str())
    }
}

/// Macro to help with optimized string creation
#[macro_export]
macro_rules! optimized_string {
    ($literal:literal) => {
        std::borrow::Cow::Borrowed($literal)
    };
    ($expr:expr) => {
        $crate::optimization::string_utils::global_string_optimizer()
            .extract_column_name($expr)
    };
}

/// Macro for creating optimized command tags
#[macro_export]
macro_rules! command_tag {
    ($command:literal, $rows:expr) => {
        $crate::optimization::string_utils::global_string_optimizer()
            .get_command_tag($command, $rows)
    };
}

/// Macro for optimized error messages
#[macro_export]
macro_rules! error_message {
    ($key:literal) => {
        $crate::optimization::string_utils::global_string_optimizer()
            .get_error_message($key)
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_optimizer_command_tags() {
        let optimizer = StringOptimizer::new();

        let tag = optimizer.get_command_tag("INSERT", 1);
        assert_eq!(tag, "INSERT 0 1");

        let tag = optimizer.get_command_tag("UPDATE", 0);
        assert_eq!(tag, "UPDATE 0");

        let tag = optimizer.get_command_tag("DELETE", 5);
        assert_eq!(tag, "DELETE 5");
    }

    #[test]
    fn test_string_optimizer_error_messages() {
        let optimizer = StringOptimizer::new();

        let msg = optimizer.get_error_message("Empty query");
        assert_eq!(msg, "Empty query");

        let msg = optimizer.get_error_message("Custom error");
        assert_eq!(msg, "Custom error");
    }

    #[test]
    fn test_value_to_string_optimization() {
        let optimizer = StringOptimizer::new();

        let val = rusqlite::types::Value::Integer(0);
        let s = optimizer.value_to_string(&val);
        assert_eq!(s, "0");

        let val = rusqlite::types::Value::Real(1.0);
        let s = optimizer.value_to_string(&val);
        assert_eq!(s, "1");

        let val = rusqlite::types::Value::Text("test".to_string());
        let s = optimizer.value_to_string(&val);
        assert_eq!(s, "test");
    }

    #[test]
    fn test_bytes_to_string_optimization() {
        let optimizer = StringOptimizer::new();

        let bytes = b"hello";
        let s = optimizer.bytes_to_string_lossy(bytes);
        assert_eq!(s, "hello");

        // Test with invalid UTF-8
        let bytes = &[0xFF, 0xFE];
        let s = optimizer.bytes_to_string_lossy(bytes);
        assert!(s.len() > 0); // Should handle gracefully
    }

    #[test]
    fn test_column_name_extraction() {
        let optimizer = StringOptimizer::new();

        let clean_name = optimizer.extract_column_name("user_id");
        assert_eq!(clean_name, "user_id");

        let dirty_name = optimizer.extract_column_name("  user_id  ");
        assert_eq!(dirty_name, "user_id");
    }
}