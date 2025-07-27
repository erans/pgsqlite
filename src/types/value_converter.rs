use crate::types::type_mapper::PgType;
use std::net::{Ipv4Addr, Ipv6Addr};
use regex::Regex;
use chrono::{NaiveTime, Timelike};
use crate::types::datetime_utils;
use once_cell::sync::Lazy;

// Pre-compiled regex patterns
static MONEY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[\$€£¥]?-?\d+(\.\d{1,2})?$|^-[\$€£¥]\d+(\.\d{1,2})?$").unwrap()
});

static RANGE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[\[\(]-?\d+,-?\d+[\]\)]$").unwrap()
});

static TIMETZ_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(\d{2}:\d{2}:\d{2}(?:\.\d+)?)([-+]\d{2}:?\d{2})$").unwrap()
});

static TIMESTAMPTZ_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^(.+?)([-+]\d{2}:?\d{2})$").unwrap()
});

static INTERVAL_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?:(\d+)\s+days?\s*)?(?:(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?)?").unwrap()
});

static TIMEZONE_OFFSET_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^([-+])(\d{2}):?(\d{2})$").unwrap()
});

pub struct ValueConverter;

impl ValueConverter {
    /// Convert a PostgreSQL value to SQLite storage format
    pub fn pg_to_sqlite(value: &str, pg_type: PgType) -> Result<String, String> {
        match pg_type {
            PgType::Money => Self::convert_money(value),
            PgType::Int4range | PgType::Int8range | PgType::Numrange => Self::convert_range(value),
            PgType::Cidr => Self::convert_cidr(value),
            PgType::Inet => Self::convert_inet(value),
            PgType::Macaddr => Self::convert_macaddr(value),
            PgType::Macaddr8 => Self::convert_macaddr8(value),
            PgType::Bit | PgType::Varbit => Self::convert_bit(value),
            PgType::Date => Self::convert_date_to_unix(value),
            PgType::Time => Self::convert_time_to_seconds(value),
            PgType::Timetz => Self::convert_timetz_to_seconds(value),
            PgType::Timestamp => Self::convert_timestamp_to_unix(value),
            PgType::Timestamptz => Self::convert_timestamptz_to_unix(value),
            PgType::Interval => Self::convert_interval_to_seconds(value),
            _ => Ok(value.to_string()), // Pass through other types
        }
    }
    
    /// Convert a SQLite value back to PostgreSQL format
    pub fn sqlite_to_pg(value: &str, pg_type: PgType) -> Result<String, String> {
        match pg_type {
            PgType::Money => Ok(value.to_string()), // Money is stored as-is
            PgType::Int4range | PgType::Int8range | PgType::Numrange => Ok(value.to_string()), // Ranges stored as-is
            PgType::Cidr => Ok(value.to_string()), // CIDR stored as-is
            PgType::Inet => Ok(value.to_string()), // INET stored as-is
            PgType::Macaddr => Ok(value.to_string()), // MAC addresses stored as-is
            PgType::Macaddr8 => Ok(value.to_string()),
            PgType::Bit | PgType::Varbit => Ok(value.to_string()), // Bit strings stored as-is
            PgType::Date => Self::convert_unix_to_date(value),
            PgType::Time => Self::convert_seconds_to_time(value),
            PgType::Timetz => Self::convert_seconds_to_timetz(value),
            PgType::Timestamp => Self::convert_unix_to_timestamp(value),
            PgType::Timestamptz => Self::convert_unix_to_timestamptz(value, "UTC"), // TODO: Use session timezone
            PgType::Interval => Self::convert_seconds_to_interval(value),
            _ => Ok(value.to_string()),
        }
    }
    
    /// Validate and convert money values
    fn convert_money(value: &str) -> Result<String, String> {
        // Remove whitespace
        let trimmed = value.trim();
        
        // Check for currency symbols and valid decimal format
        if MONEY_REGEX.is_match(trimmed) {
            Ok(trimmed.to_string())
        } else {
            Err(format!("Invalid money format: {value}"))
        }
    }
    
    /// Validate and convert range values
    fn convert_range(value: &str) -> Result<String, String> {
        // Range format: [lower,upper) or (lower,upper] or [lower,upper] or (lower,upper)
        if RANGE_REGEX.is_match(value.trim()) {
            Ok(value.trim().to_string())
        } else {
            Err(format!("Invalid range format: {value}"))
        }
    }
    
    /// Validate and convert CIDR values
    fn convert_cidr(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Split on '/'
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid CIDR format: {value}"));
        }
        
        let ip_part = parts[0];
        let prefix_part = parts[1];
        
        // Validate IP address
        if !Self::is_valid_ip(ip_part) {
            return Err(format!("Invalid IP address in CIDR: {ip_part}"));
        }
        
        // Validate prefix length
        let prefix: u8 = prefix_part.parse()
            .map_err(|_| format!("Invalid prefix length: {prefix_part}"))?;
        
        if ip_part.contains(':') {
            // IPv6
            if prefix > 128 {
                return Err(format!("IPv6 prefix length cannot exceed 128: {prefix}"));
            }
        } else {
            // IPv4
            if prefix > 32 {
                return Err(format!("IPv4 prefix length cannot exceed 32: {prefix}"));
            }
        }
        
        Ok(trimmed.to_string())
    }
    
    /// Validate and convert INET values
    fn convert_inet(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // INET can be just an IP address or IP/prefix
        if trimmed.contains('/') {
            Self::convert_cidr(trimmed)
        } else if Self::is_valid_ip(trimmed) {
            Ok(trimmed.to_string())
        } else {
            Err(format!("Invalid INET format: {value}"))
        }
    }
    
    /// Validate and convert MAC address (6 bytes)
    fn convert_macaddr(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Support colon and hyphen separators
        let normalized = if trimmed.contains(':') {
            trimmed.to_string()
        } else if trimmed.contains('-') {
            trimmed.replace('-', ":")
        } else {
            return Err(format!("Invalid MAC address format: {value}"));
        };
        
        let parts: Vec<&str> = normalized.split(':').collect();
        if parts.len() != 6 {
            return Err(format!("MAC address must have 6 parts: {value}"));
        }
        
        for part in &parts {
            if part.len() != 2 || !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!("Invalid MAC address part: {part}"));
            }
        }
        
        Ok(normalized)
    }
    
    /// Validate and convert MAC address (8 bytes)
    fn convert_macaddr8(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Support colon and hyphen separators
        let normalized = if trimmed.contains(':') {
            trimmed.to_string()
        } else if trimmed.contains('-') {
            trimmed.replace('-', ":")
        } else {
            return Err(format!("Invalid MAC address format: {value}"));
        };
        
        let parts: Vec<&str> = normalized.split(':').collect();
        if parts.len() != 8 {
            return Err(format!("MAC address must have 8 parts: {value}"));
        }
        
        for part in &parts {
            if part.len() != 2 || !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!("Invalid MAC address part: {part}"));
            }
        }
        
        Ok(normalized)
    }
    
    /// Validate and convert bit strings
    fn convert_bit(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Remove B prefix if present (e.g., B'1010')
        let bit_string = if trimmed.starts_with("B'") && trimmed.ends_with('\'') {
            &trimmed[2..trimmed.len()-1]
        } else {
            trimmed
        };
        
        // Validate all characters are 0 or 1
        if bit_string.chars().all(|c| c == '0' || c == '1') {
            Ok(bit_string.to_string())
        } else {
            Err(format!("Invalid bit string: {value}"))
        }
    }
    
    /// Check if a string is a valid IP address (IPv4 or IPv6)
    fn is_valid_ip(s: &str) -> bool {
        s.parse::<Ipv4Addr>().is_ok() || s.parse::<Ipv6Addr>().is_ok()
    }
    
    // DateTime conversion functions
    
    /// Convert PostgreSQL DATE to epoch days (stored as INTEGER)
    pub fn convert_date_to_unix(value: &str) -> Result<String, String> {
        datetime_utils::parse_date_to_days(value.trim())
            .map(|days| days.to_string())
            .ok_or_else(|| format!("Invalid date format: {value}"))
    }
    
    /// Convert epoch days (INTEGER) to PostgreSQL DATE
    fn convert_unix_to_date(value: &str) -> Result<String, String> {
        let days = value.parse::<i64>()
            .map_err(|e| format!("Invalid days value: {value} ({e})"))?;
        Ok(datetime_utils::format_days_to_date(days))
    }
    
    /// Convert PostgreSQL TIME to microseconds since midnight (stored as INTEGER)
    pub fn convert_time_to_seconds(value: &str) -> Result<String, String> {
        datetime_utils::parse_time_to_microseconds(value.trim())
            .map(|micros| micros.to_string())
            .ok_or_else(|| format!("Invalid time format: {value}"))
    }
    
    /// Convert microseconds since midnight (INTEGER) to PostgreSQL TIME
    fn convert_seconds_to_time(value: &str) -> Result<String, String> {
        let micros = value.parse::<i64>()
            .map_err(|e| format!("Invalid microseconds value: {value} ({e})"))?;
        Ok(datetime_utils::format_microseconds_to_time(micros))
    }
    
    /// Convert PostgreSQL TIMETZ to microseconds since midnight UTC (stored as INTEGER)
    fn convert_timetz_to_seconds(value: &str) -> Result<String, String> {
        // Parse time and timezone offset
        if let Some(caps) = TIMETZ_REGEX.captures(value.trim()) {
            let time_str = &caps[1];
            let offset_str = &caps[2];
            
            // Parse time to microseconds
            let time_micros = datetime_utils::parse_time_to_microseconds(time_str)
                .ok_or_else(|| format!("Invalid time format: {time_str}"))?;
            
            // Parse offset (±HH:MM or ±HHMM)
            let offset_seconds = Self::parse_timezone_offset(offset_str)?;
            
            // Convert to microseconds since midnight UTC by adjusting for timezone
            let utc_micros = time_micros - (offset_seconds as i64 * 1_000_000);
            
            Ok(utc_micros.to_string())
        } else {
            Err(format!("Invalid TIMETZ format: {value}"))
        }
    }
    
    /// Convert microseconds since midnight UTC (INTEGER) to PostgreSQL TIMETZ
    fn convert_seconds_to_timetz(value: &str) -> Result<String, String> {
        let micros = value.parse::<i64>()
            .map_err(|e| format!("Invalid microseconds value: {value} ({e})"))?;
        
        // Normalize to 0-86400000000 range (microseconds in a day)
        let normalized_micros = micros.rem_euclid(86_400_000_000);
        
        // Format as time with UTC offset
        let time_str = datetime_utils::format_microseconds_to_time(normalized_micros);
        Ok(format!("{time_str}+00:00"))
    }
    
    /// Convert PostgreSQL TIMESTAMP to microseconds since epoch (stored as INTEGER)
    pub fn convert_timestamp_to_unix(value: &str) -> Result<String, String> {
        datetime_utils::parse_timestamp_to_microseconds(value.trim())
            .map(|micros| micros.to_string())
            .ok_or_else(|| format!("Invalid timestamp format: {value}"))
    }
    
    /// Convert microseconds since epoch (INTEGER) to PostgreSQL TIMESTAMP
    fn convert_unix_to_timestamp(value: &str) -> Result<String, String> {
        let micros = value.parse::<i64>()
            .map_err(|e| format!("Invalid microseconds value: {value} ({e})"))?;
        Ok(datetime_utils::format_microseconds_to_timestamp(micros))
    }
    
    /// Convert PostgreSQL TIMESTAMPTZ to microseconds since epoch in UTC (stored as INTEGER)
    fn convert_timestamptz_to_unix(value: &str) -> Result<String, String> {
        // Try parsing with timezone offset
        let (datetime_str, offset_seconds) = if let Some(caps) = TIMESTAMPTZ_REGEX.captures(value.trim()) {
            let dt_str = caps.get(1).unwrap().as_str();
            let offset_str = caps.get(2).unwrap().as_str();
            let offset = Self::parse_timezone_offset(offset_str)?;
            (dt_str.trim().to_string(), offset)
        } else {
            // No timezone specified, assume UTC
            (value.trim().to_string(), 0)
        };
        
        // Parse timestamp to microseconds
        let micros = datetime_utils::parse_timestamp_to_microseconds(&datetime_str)
            .ok_or_else(|| format!("Invalid timestamp format: {datetime_str}"))?;
        
        // Convert to UTC by subtracting the offset (in microseconds)
        let utc_micros = micros - (offset_seconds as i64 * 1_000_000);
        
        Ok(utc_micros.to_string())
    }
    
    /// Convert microseconds since epoch (INTEGER) to PostgreSQL TIMESTAMPTZ (with session timezone)
    fn convert_unix_to_timestamptz(value: &str, _timezone: &str) -> Result<String, String> {
        let micros = value.parse::<i64>()
            .map_err(|e| format!("Invalid microseconds value: {value} ({e})"))?;
        
        // Format timestamp
        let timestamp_str = datetime_utils::format_microseconds_to_timestamp(micros);
        
        // For now, always use UTC
        // TODO: Apply session timezone offset
        Ok(format!("{timestamp_str}+00:00"))
    }
    
    /// Convert PostgreSQL INTERVAL to microseconds (stored as INTEGER)
    fn convert_interval_to_seconds(value: &str) -> Result<String, String> {
        // Simple interval parsing for common formats
        // Full PostgreSQL interval parsing is complex, this handles basic cases
        let trimmed = value.trim();
        
        // Handle simple numeric intervals (e.g., "3600000000" microseconds)
        if let Ok(micros) = trimmed.parse::<i64>() {
            return Ok(micros.to_string());
        }
        
        // Handle HH:MM:SS format
        if let Ok(time) = NaiveTime::parse_from_str(trimmed, "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(trimmed, "%H:%M:%S")) {
            let micros = time.num_seconds_from_midnight() as i64 * 1_000_000 
                + (time.nanosecond() / 1000) as i64;
            return Ok(micros.to_string());
        }
        
        // Handle verbose format (e.g., "1 day 02:30:00")
        if let Some(caps) = INTERVAL_REGEX.captures(trimmed) {
            let days = caps.get(1).map(|m| m.as_str().parse::<i64>().unwrap_or(0)).unwrap_or(0);
            let hours = caps.get(2).map(|m| m.as_str().parse::<i64>().unwrap_or(0)).unwrap_or(0);
            let minutes = caps.get(3).map(|m| m.as_str().parse::<i64>().unwrap_or(0)).unwrap_or(0);
            let seconds = caps.get(4).map(|m| m.as_str().parse::<i64>().unwrap_or(0)).unwrap_or(0);
            let fraction = caps.get(5).map(|m| {
                let fraction_str = m.as_str();
                // Parse the fractional part and convert to microseconds
                
                if fraction_str.len() <= 6 {
                    // Pad with zeros if needed
                    let padded = format!("{fraction_str:0<6}");
                    padded.parse::<i64>().unwrap_or(0)
                } else {
                    // Truncate to 6 digits
                    fraction_str[..6].parse::<i64>().unwrap_or(0)
                }
            }).unwrap_or(0);
            
            let total_micros = (days * 86400 + hours * 3600 + minutes * 60 + seconds) * 1_000_000 + fraction;
            return Ok(total_micros.to_string());
        }
        
        Err(format!("Unsupported interval format: {value}"))
    }
    
    /// Convert microseconds to PostgreSQL INTERVAL
    fn convert_seconds_to_interval(value: &str) -> Result<String, String> {
        let total_micros = value.parse::<i64>()
            .map_err(|e| format!("Invalid microseconds value: {value} ({e})"))?;
        
        let days = total_micros / (86400 * 1_000_000);
        let remaining_micros = total_micros % (86400 * 1_000_000);
        let hours = remaining_micros / (3600 * 1_000_000);
        let minutes = (remaining_micros % (3600 * 1_000_000)) / (60 * 1_000_000);
        let seconds = (remaining_micros % (60 * 1_000_000)) / 1_000_000;
        let microseconds = remaining_micros % 1_000_000;
        
        let mut parts = Vec::new();
        if days > 0 {
            parts.push(format!("{} day{}", days, if days == 1 { "" } else { "s" }));
        }
        
        if microseconds > 0 {
            parts.push(format!("{hours:02}:{minutes:02}:{seconds:02}.{microseconds:06}"));
        } else {
            parts.push(format!("{hours:02}:{minutes:02}:{seconds:02}"));
        }
        
        Ok(parts.join(" "))
    }
    
    /// Parse timezone offset string (±HH:MM or ±HHMM) to seconds
    fn parse_timezone_offset(offset: &str) -> Result<i32, String> {
        if let Some(caps) = TIMEZONE_OFFSET_REGEX.captures(offset) {
            let sign = if &caps[1] == "+" { 1 } else { -1 };
            let hours = caps[2].parse::<i32>()
                .map_err(|e| format!("Invalid hours in offset: {} ({})", &caps[2], e))?;
            let minutes = caps[3].parse::<i32>()
                .map_err(|e| format!("Invalid minutes in offset: {} ({})", &caps[3], e))?;
            Ok(sign * (hours * 3600 + minutes * 60))
        } else {
            Err(format!("Invalid timezone offset format: {offset}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_money_conversion() {
        assert!(ValueConverter::convert_money("$123.45").is_ok());
        assert!(ValueConverter::convert_money("€100.00").is_ok());
        assert!(ValueConverter::convert_money("£50.5").is_ok());
        assert!(ValueConverter::convert_money("-$25.99").is_ok());
        assert!(ValueConverter::convert_money("invalid").is_err());
    }
    
    #[test]
    fn test_cidr_conversion() {
        assert!(ValueConverter::convert_cidr("192.168.1.0/24").is_ok());
        assert!(ValueConverter::convert_cidr("10.0.0.0/8").is_ok());
        assert!(ValueConverter::convert_cidr("2001:db8::/32").is_ok());
        assert!(ValueConverter::convert_cidr("192.168.1.0/33").is_err()); // Invalid prefix
        assert!(ValueConverter::convert_cidr("invalid/24").is_err());
    }
    
    #[test]
    fn test_inet_conversion() {
        assert!(ValueConverter::convert_inet("192.168.1.1").is_ok());
        assert!(ValueConverter::convert_inet("192.168.1.0/24").is_ok());
        assert!(ValueConverter::convert_inet("2001:db8::1").is_ok());
        assert!(ValueConverter::convert_inet("invalid").is_err());
    }
    
    #[test]
    fn test_macaddr_conversion() {
        assert!(ValueConverter::convert_macaddr("08:00:2b:01:02:03").is_ok());
        assert!(ValueConverter::convert_macaddr("08-00-2b-01-02-03").is_ok());
        assert!(ValueConverter::convert_macaddr("08:00:2b:01:02").is_err()); // Too few parts
        assert!(ValueConverter::convert_macaddr("invalid").is_err());
    }
    
    #[test]
    fn test_bit_conversion() {
        assert!(ValueConverter::convert_bit("1010").is_ok());
        assert!(ValueConverter::convert_bit("B'1010'").is_ok());
        assert!(ValueConverter::convert_bit("1012").is_err()); // Invalid character
    }
    
    #[test]
    fn test_date_conversion() {
        // Test DATE to epoch days
        let result = ValueConverter::convert_date_to_unix("2024-01-15").unwrap();
        let days = result.parse::<i64>().unwrap();
        assert_eq!(days, 19737); // Days since 1970-01-01
        
        // Test epoch days to DATE
        let result = ValueConverter::convert_unix_to_date("19737").unwrap();
        assert_eq!(result, "2024-01-15");
    }
    
    #[test]
    fn test_time_conversion() {
        // Test TIME to microseconds
        let result = ValueConverter::convert_time_to_seconds("14:30:45.123456").unwrap();
        let micros = result.parse::<i64>().unwrap();
        assert_eq!(micros, 52245123456); // 14:30:45.123456 as microseconds
        
        // Test microseconds to TIME
        let result = ValueConverter::convert_seconds_to_time("52245123456").unwrap();
        assert_eq!(result, "14:30:45.123456");
        
        // Test TIME without fractional seconds
        let result = ValueConverter::convert_time_to_seconds("14:30:45").unwrap();
        assert_eq!(result, "52245000000");
    }
    
    #[test]
    fn test_timestamp_conversion() {
        // Test TIMESTAMP to microseconds since epoch
        let result = ValueConverter::convert_timestamp_to_unix("2024-01-15 14:30:45.123456").unwrap();
        let micros = result.parse::<i64>().unwrap();
        assert_eq!(micros, 1705329045123456); // Microseconds since epoch
        
        // Test microseconds to TIMESTAMP
        let result = ValueConverter::convert_unix_to_timestamp("1705329045123456").unwrap();
        assert_eq!(result, "2024-01-15 14:30:45.123456");
        
        // Test without fractional seconds
        let result = ValueConverter::convert_timestamp_to_unix("2024-01-15 14:30:45").unwrap();
        let micros = result.parse::<i64>().unwrap();
        assert_eq!(micros, 1705329045000000);
    }
    
    #[test]
    fn test_interval_conversion() {
        // Test simple microseconds
        assert_eq!(ValueConverter::convert_interval_to_seconds("3600000000").unwrap(), "3600000000");
        
        // Test HH:MM:SS format
        assert_eq!(ValueConverter::convert_interval_to_seconds("01:30:00").unwrap(), "5400000000"); // 1.5 hours in microseconds
        
        // Test verbose format
        let result = ValueConverter::convert_interval_to_seconds("1 day 02:30:00").unwrap();
        assert_eq!(result, "95400000000"); // (86400 + 9000) seconds * 1_000_000 microseconds
        
        // Test microseconds to interval
        assert_eq!(ValueConverter::convert_seconds_to_interval("95400000000").unwrap(), "1 day 02:30:00");
        assert_eq!(ValueConverter::convert_seconds_to_interval("5400500000").unwrap(), "01:30:00.500000");
    }
}