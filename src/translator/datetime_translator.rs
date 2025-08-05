use regex::Regex;
use once_cell::sync::Lazy;

/// Translates PostgreSQL datetime functions to our custom SQLite functions
pub struct DateTimeTranslator;

// Lazy static regex patterns for datetime function detection
static NOW_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(NOW|CURRENT_TIMESTAMP)\s*\(\s*\)").unwrap()
});

static CURRENT_DATE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bCURRENT_DATE\b").unwrap()
});

static CURRENT_TIME_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bCURRENT_TIME\b").unwrap()
});

// SQLite date/time functions that return text but should be DATE/TIME types
static DATE_FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bdate\s*\(([^)]+)\)").unwrap()
});

static TIME_FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\btime\s*\(([^)]+)\)").unwrap()
});

static DATETIME_FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bdatetime\s*\(([^)]+)\)").unwrap()
});

static EXTRACT_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // More specific pattern that won't over-capture
    Regex::new(r"(?i)\bEXTRACT\s*\(\s*(\w+)\s+FROM\s+([^)]+)\s*\)").unwrap()
});

static DATE_TRUNC_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bDATE_TRUNC\s*\(\s*'([^']+)'\s*,\s*([^)]+)\s*\)").unwrap()
});

static AGE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bAGE\s*\(").unwrap()
});

static AT_TIME_ZONE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // Match: expression AT TIME ZONE 'timezone' [as alias]
    // Captures: (1) expression, (2) timezone, (3) optional alias
    Regex::new(r"(?i)(\S+)\s+AT\s+TIME\s+ZONE\s+'([^']+)'(\s+as\s+(\w+))?").unwrap()
});

impl DateTimeTranslator {
    /// Check if the query contains datetime functions that need translation
    pub fn needs_translation(query: &str) -> bool {
        NOW_PATTERN.is_match(query) ||
        CURRENT_DATE_PATTERN.is_match(query) ||
        CURRENT_TIME_PATTERN.is_match(query) ||
        DATE_FUNCTION_PATTERN.is_match(query) ||
        TIME_FUNCTION_PATTERN.is_match(query) ||
        DATETIME_FUNCTION_PATTERN.is_match(query) ||
        EXTRACT_PATTERN.is_match(query) ||
        DATE_TRUNC_PATTERN.is_match(query) ||
        AGE_PATTERN.is_match(query) ||
        AT_TIME_ZONE_PATTERN.is_match(query) ||
        query.to_uppercase().contains("INTERVAL") ||
        query.to_uppercase().contains("TO_TIMESTAMP") ||
        query.to_uppercase().contains("TO_DATE") ||
        query.to_uppercase().contains("MAKE_DATE") ||
        query.to_uppercase().contains("MAKE_TIME")
    }
    
    /// Translate PostgreSQL datetime functions to SQLite-compatible versions
    pub fn translate_query(query: &str) -> String {
        let (translated, _) = Self::translate_with_metadata(query);
        translated
    }
    
    /// Translate query and return metadata about the translation
    pub fn translate_with_metadata(query: &str) -> (String, super::TranslationMetadata) {
        let mut result = query.to_string();
        let mut metadata = super::TranslationMetadata::new();
        
        let query_upper = query.to_uppercase();
        let is_create_table_with_default = query_upper.contains("CREATE TABLE") && query_upper.contains("DEFAULT");
        
        // Replace NOW() and CURRENT_TIMESTAMP 
        // In CREATE TABLE DEFAULT clauses, use SQLite's built-in datetime('now') 
        // In other contexts, use our custom now() function
        if is_create_table_with_default {
            // For CREATE TABLE with DEFAULT, use SQLite's built-in functions
            result = NOW_PATTERN.replace_all(&result, "datetime('now')").to_string();
            // Don't process datetime functions further for CREATE TABLE
            return (result, metadata);
        } else {
            // For other contexts, use our custom function
            result = NOW_PATTERN.replace_all(&result, "now()").to_string();
        }
        
        // Don't translate CURRENT_DATE - SQLite has its own built-in that returns text
        // We'll let the value converter handle the conversion if needed
        // result = CURRENT_DATE_PATTERN.replace_all(&result, "current_date").to_string();
        
        // Replace CURRENT_TIME (no parentheses in PostgreSQL)
        result = CURRENT_TIME_PATTERN.replace_all(&result, "current_time").to_string();
        
        // Wrap SQLite date() function to convert to epoch days (INTEGER)
        result = DATE_FUNCTION_PATTERN.replace_all(&result, |caps: &regex::Captures| {
            let args = &caps[1];
            // For parameterized queries, keep the date() function as-is
            // The SQLite date() function will handle the parameters correctly
            if args.contains('$') || args.contains("CAST") {
                // Return the original match - don't translate parameterized date functions
                caps[0].to_string()
            } else {
                // For literal values, wrap in julianday conversion
                format!("CAST(julianday(date({args})) - 2440587.5 AS INTEGER)")
            }
        }).to_string();
        
        // Wrap SQLite time() function to convert to microseconds since midnight (INTEGER)
        result = TIME_FUNCTION_PATTERN.replace_all(&result, |caps: &regex::Captures| {
            let args = &caps[1];
            format!("CAST((strftime('%s', '2000-01-01 ' || time({args})) - strftime('%s', '2000-01-01')) * 1000000 AS INTEGER)")
        }).to_string();
        
        // Wrap SQLite datetime() function to convert to microseconds since epoch (INTEGER)
        // But skip datetime('now') when used in CREATE TABLE DEFAULT clauses
        let is_create_table = query_upper.contains("CREATE TABLE");
        result = DATETIME_FUNCTION_PATTERN.replace_all(&result, |caps: &regex::Captures| {
            let args = &caps[1];
            // Don't process datetime('now') - leave it as-is for CREATE TABLE DEFAULT
            if args.trim() == "'now'" && is_create_table {
                format!("datetime({args})")
            } else {
                format!("CAST((julianday(datetime({args})) - 2440587.5) * 86400 * 1000000 AS INTEGER)")
            }
        }).to_string();
        
        // Handle EXTRACT(field FROM timestamp) -> extract(field, timestamp)
        result = EXTRACT_PATTERN.replace_all(&result, |caps: &regex::Captures| {
            let field = &caps[1];
            let timestamp = &caps[2];
            format!("extract('{}', {})", field.to_lowercase(), timestamp.trim())
        }).to_string();
        
        // Handle DATE_TRUNC('field', timestamp) -> date_trunc('field', timestamp)
        result = DATE_TRUNC_PATTERN.replace_all(&result, |caps: &regex::Captures| {
            let field = &caps[1];
            let timestamp = &caps[2];
            format!("date_trunc('{}', {})", field.to_lowercase(), timestamp.trim())
        }).to_string();
        
        // Handle INTERVAL literals (basic support)
        result = Self::translate_interval_literals(&result);
        
        // Handle AT TIME ZONE operator
        let at_time_zone_metadata = Self::translate_at_time_zone_with_metadata(&mut result);
        metadata.merge(at_time_zone_metadata);
        
        // Handle timestamp arithmetic with intervals
        result = Self::translate_interval_arithmetic(&result);
        
        (result, metadata)
    }
    
    /// Translate INTERVAL literals to microseconds
    fn translate_interval_literals(query: &str) -> String {
        let interval_pattern = Regex::new(r"(?i)INTERVAL\s+'([^']+)'").unwrap();
        
        interval_pattern.replace_all(query, |caps: &regex::Captures| {
            let interval_str = &caps[1];
            if let Some(microseconds) = Self::parse_interval_to_seconds(interval_str) {
                format!("{microseconds:.0}")
            } else {
                // If we can't parse it, leave it as is
                caps[0].to_string()
            }
        }).to_string()
    }
    
    /// Parse common interval formats to microseconds
    fn parse_interval_to_seconds(interval: &str) -> Option<f64> {
        let parts: Vec<&str> = interval.split_whitespace().collect();
        
        if parts.len() >= 2 {
            if let Ok(value) = parts[0].parse::<f64>() {
                match parts[1].to_lowercase().as_str() {
                    "second" | "seconds" | "sec" | "secs" => Some(value * 1_000_000.0),
                    "minute" | "minutes" | "min" | "mins" => Some(value * 60.0 * 1_000_000.0),
                    "hour" | "hours" | "hr" | "hrs" => Some(value * 3600.0 * 1_000_000.0),
                    "day" | "days" => Some(value * 86400.0 * 1_000_000.0),
                    "week" | "weeks" => Some(value * 604800.0 * 1_000_000.0),
                    "month" | "months" | "mon" | "mons" => Some(value * 2592000.0 * 1_000_000.0), // 30 days
                    "year" | "years" | "yr" | "yrs" => Some(value * 31536000.0 * 1_000_000.0), // 365 days
                    _ => None
                }
            } else {
                None
            }
        } else {
            None
        }
    }
    
    /// Translate interval arithmetic (timestamp + interval, etc.)
    fn translate_interval_arithmetic(query: &str) -> String {
        // This is a simplified version - full interval arithmetic is complex
        let mut result = query.to_string();
        
        // Handle simple cases like "timestamp + INTERVAL '1 day'"
        let arithmetic_pattern = Regex::new(r"(?i)(\w+)\s*([+-])\s*INTERVAL\s+'([^']+)'").unwrap();
        result = arithmetic_pattern.replace_all(&result, |caps: &regex::Captures| {
            let column = &caps[1];
            let operator = &caps[2];
            let interval_str = &caps[3];
            
            if let Some(microseconds) = Self::parse_interval_to_seconds(interval_str) {
                format!("({column} {operator} {microseconds:.0})")
            } else {
                caps[0].to_string()
            }
        }).to_string();
        
        result
    }
    
    
    /// Translate AT TIME ZONE operator with metadata
    fn translate_at_time_zone_with_metadata(query: &mut String) -> super::TranslationMetadata {
        let (result, metadata) = Self::translate_at_time_zone_with_metadata_impl(query);
        *query = result;
        metadata
    }
    
    /// Internal implementation of AT TIME ZONE translation with metadata
    fn translate_at_time_zone_with_metadata_impl(query: &str) -> (String, super::TranslationMetadata) {
        let mut metadata = super::TranslationMetadata::new();
        
        // Pattern to match expressions like "timestamp AT TIME ZONE 'timezone'"
        let result = AT_TIME_ZONE_PATTERN.replace_all(query, |caps: &regex::Captures| {
            let expression = &caps[1];
            let timezone = &caps[2];
            let alias_part = caps.get(3).map(|m| m.as_str()).unwrap_or("");
            let alias = caps.get(4).map(|m| m.as_str());
            let offset_seconds = Self::tz_to_offset_seconds(timezone);
            
            // If we have an alias, track the type hint
            if let Some(alias_name) = alias {
                // Try to extract the source column from the expression
                let source_column = if expression.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '.') {
                    Some(expression.to_string())
                } else {
                    None
                };
                
                // AT TIME ZONE operations should preserve the source column type
                // Don't force a specific type here - let the extended protocol look up the source column type
                let hint = super::ColumnTypeHint::expression(
                    source_column.clone(),
                    super::super::types::PgType::Float8, // This will be overridden by source column lookup
                    super::ExpressionType::DateTimeExpression
                );
                
                metadata.add_hint(alias_name.to_string(), hint);
            }
            
            // If timezone is UTC or offset is 0, just return the expression
            if offset_seconds == 0 {
                format!("{expression}{alias_part}")
            } else {
                // Apply offset to the timestamp (convert seconds to microseconds)
                format!("{} + {}{}", expression, offset_seconds as i64 * 1_000_000, alias_part)
            }
        }).to_string();
        
        (result, metadata)
    }
    
    /// Convert timezone name to offset in seconds
    fn tz_to_offset_seconds(tz: &str) -> i32 {
        match tz.to_uppercase().as_str() {
            "UTC" | "GMT" => 0,
            "EST" | "AMERICA/NEW_YORK" => -5 * 3600, // -5 hours
            "EDT" => -4 * 3600, // -4 hours (daylight saving)
            "PST" | "AMERICA/LOS_ANGELES" => -8 * 3600, // -8 hours
            "PDT" => -7 * 3600, // -7 hours (daylight saving)
            "CST" | "AMERICA/CHICAGO" => -6 * 3600, // -6 hours
            "CDT" => -5 * 3600, // -5 hours (daylight saving)
            "MST" | "AMERICA/DENVER" => -7 * 3600, // -7 hours
            "MDT" => -6 * 3600, // -6 hours (daylight saving)
            "CET" | "EUROPE/PARIS" | "EUROPE/BERLIN" => 3600, // +1 hour
            "CEST" => 2 * 3600, // +2 hours (daylight saving)
            "JST" | "ASIA/TOKYO" => 9 * 3600, // +9 hours
            "IST" | "ASIA/KOLKATA" => 5 * 3600 + 1800, // +5:30 hours
            _ => {
                // Try to parse offset format like '+05:30' or '-08:00'
                Self::parse_offset_string(tz).unwrap_or_default()
            }
        }
    }
    
    /// Parse offset string like "+05:30" or "-08:00" to seconds
    fn parse_offset_string(offset: &str) -> Option<i32> {
        let re = regex::Regex::new(r"^([+-])(\d{2}):(\d{2})$").ok()?;
        let caps = re.captures(offset)?;
        let sign = if &caps[1] == "+" { 1 } else { -1 };
        let hours = caps[2].parse::<i32>().ok()?;
        let minutes = caps[3].parse::<i32>().ok()?;
        Some(sign * (hours * 3600 + minutes * 60))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_datetime_translation() {
        // Test NOW() translation
        assert_eq!(
            DateTimeTranslator::translate_query("SELECT NOW()"),
            "SELECT now()"
        );
        
        // Test CURRENT_DATE translation (not translated since SQLite has built-in)
        assert_eq!(
            DateTimeTranslator::translate_query("SELECT CURRENT_DATE"),
            "SELECT CURRENT_DATE"
        );
        
        // Test EXTRACT translation
        assert_eq!(
            DateTimeTranslator::translate_query("SELECT EXTRACT(YEAR FROM created_at)"),
            "SELECT extract('year', created_at)"
        );
        
        // Test DATE_TRUNC translation
        assert_eq!(
            DateTimeTranslator::translate_query("SELECT DATE_TRUNC('month', created_at)"),
            "SELECT date_trunc('month', created_at)"
        );
        
        // Test INTERVAL translation
        assert_eq!(
            DateTimeTranslator::translate_query("SELECT created_at + INTERVAL '1 day'"),
            "SELECT created_at + 86400000000"
        );
        
        // Test AT TIME ZONE removal
        assert_eq!(
            DateTimeTranslator::translate_query("SELECT created_at AT TIME ZONE 'UTC'"),
            "SELECT created_at"
        );
    }
    
    #[test]
    fn test_interval_parsing() {
        assert_eq!(DateTimeTranslator::parse_interval_to_seconds("1 second"), Some(1_000_000.0));
        assert_eq!(DateTimeTranslator::parse_interval_to_seconds("2 minutes"), Some(120_000_000.0));
        assert_eq!(DateTimeTranslator::parse_interval_to_seconds("3 hours"), Some(10_800_000_000.0));
        assert_eq!(DateTimeTranslator::parse_interval_to_seconds("1 day"), Some(86_400_000_000.0));
        assert_eq!(DateTimeTranslator::parse_interval_to_seconds("1 week"), Some(604_800_000_000.0));
        assert_eq!(DateTimeTranslator::parse_interval_to_seconds("1 month"), Some(2_592_000_000_000.0));
        assert_eq!(DateTimeTranslator::parse_interval_to_seconds("1 year"), Some(31_536_000_000_000.0));
    }
}