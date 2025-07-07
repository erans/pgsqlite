use crate::types::type_mapper::PgType;
use std::net::{Ipv4Addr, Ipv6Addr};
use regex::Regex;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, DateTime, Timelike};

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
        let money_regex = Regex::new(r"^[\$€£¥]?-?\d+(\.\d{1,2})?$|^-[\$€£¥]\d+(\.\d{1,2})?$").unwrap();
        if money_regex.is_match(trimmed) {
            Ok(trimmed.to_string())
        } else {
            Err(format!("Invalid money format: {}", value))
        }
    }
    
    /// Validate and convert range values
    fn convert_range(value: &str) -> Result<String, String> {
        // Range format: [lower,upper) or (lower,upper] or [lower,upper] or (lower,upper)
        let range_regex = Regex::new(r"^[\[\(]-?\d+,-?\d+[\]\)]$").unwrap();
        if range_regex.is_match(value.trim()) {
            Ok(value.trim().to_string())
        } else {
            Err(format!("Invalid range format: {}", value))
        }
    }
    
    /// Validate and convert CIDR values
    fn convert_cidr(value: &str) -> Result<String, String> {
        let trimmed = value.trim();
        
        // Split on '/'
        let parts: Vec<&str> = trimmed.split('/').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid CIDR format: {}", value));
        }
        
        let ip_part = parts[0];
        let prefix_part = parts[1];
        
        // Validate IP address
        if !Self::is_valid_ip(ip_part) {
            return Err(format!("Invalid IP address in CIDR: {}", ip_part));
        }
        
        // Validate prefix length
        let prefix: u8 = prefix_part.parse()
            .map_err(|_| format!("Invalid prefix length: {}", prefix_part))?;
        
        if ip_part.contains(':') {
            // IPv6
            if prefix > 128 {
                return Err(format!("IPv6 prefix length cannot exceed 128: {}", prefix));
            }
        } else {
            // IPv4
            if prefix > 32 {
                return Err(format!("IPv4 prefix length cannot exceed 32: {}", prefix));
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
            Err(format!("Invalid INET format: {}", value))
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
            return Err(format!("Invalid MAC address format: {}", value));
        };
        
        let parts: Vec<&str> = normalized.split(':').collect();
        if parts.len() != 6 {
            return Err(format!("MAC address must have 6 parts: {}", value));
        }
        
        for part in &parts {
            if part.len() != 2 || !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!("Invalid MAC address part: {}", part));
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
            return Err(format!("Invalid MAC address format: {}", value));
        };
        
        let parts: Vec<&str> = normalized.split(':').collect();
        if parts.len() != 8 {
            return Err(format!("MAC address must have 8 parts: {}", value));
        }
        
        for part in &parts {
            if part.len() != 2 || !part.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(format!("Invalid MAC address part: {}", part));
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
            Err(format!("Invalid bit string: {}", value))
        }
    }
    
    /// Check if a string is a valid IP address (IPv4 or IPv6)
    fn is_valid_ip(s: &str) -> bool {
        s.parse::<Ipv4Addr>().is_ok() || s.parse::<Ipv6Addr>().is_ok()
    }
    
    // DateTime conversion functions
    
    /// Convert PostgreSQL DATE to Unix timestamp (at 00:00:00 UTC)
    fn convert_date_to_unix(value: &str) -> Result<String, String> {
        let date = NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d")
            .map_err(|e| format!("Invalid date format: {} ({})", value, e))?;
        let datetime = date.and_hms_opt(0, 0, 0)
            .ok_or_else(|| "Invalid date conversion".to_string())?;
        let timestamp = datetime.and_utc().timestamp() as f64;
        Ok(timestamp.to_string())
    }
    
    /// Convert Unix timestamp to PostgreSQL DATE
    fn convert_unix_to_date(value: &str) -> Result<String, String> {
        let timestamp = value.parse::<f64>()
            .map_err(|e| format!("Invalid timestamp: {} ({})", value, e))?;
        let datetime = DateTime::from_timestamp(timestamp as i64, 0)
            .ok_or_else(|| "Invalid timestamp".to_string())?;
        Ok(datetime.format("%Y-%m-%d").to_string())
    }
    
    /// Convert PostgreSQL TIME to seconds since midnight
    fn convert_time_to_seconds(value: &str) -> Result<String, String> {
        let time = NaiveTime::parse_from_str(value.trim(), "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(value.trim(), "%H:%M:%S"))
            .map_err(|e| format!("Invalid time format: {} ({})", value, e))?;
        let seconds = time.num_seconds_from_midnight() as f64 
            + (time.nanosecond() as f64 / 1_000_000_000.0);
        Ok(seconds.to_string())
    }
    
    /// Convert seconds since midnight to PostgreSQL TIME
    fn convert_seconds_to_time(value: &str) -> Result<String, String> {
        let seconds = value.parse::<f64>()
            .map_err(|e| format!("Invalid seconds value: {} ({})", value, e))?;
        let secs = seconds.trunc() as u32;
        let nanos = ((seconds.fract() * 1_000_000_000.0).round() as u32).min(999_999_999);
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
            .ok_or_else(|| format!("Invalid time value: {} seconds", value))?;
        
        // Format with microseconds if present
        if nanos > 0 {
            Ok(format!("{:02}:{:02}:{:02}.{:06}", 
                time.hour(), time.minute(), time.second(), 
                nanos / 1000))
        } else {
            Ok(time.format("%H:%M:%S").to_string())
        }
    }
    
    /// Convert PostgreSQL TIMETZ to seconds since midnight UTC
    fn convert_timetz_to_seconds(value: &str) -> Result<String, String> {
        // Parse time and timezone offset
        let re = Regex::new(r"^(\d{2}:\d{2}:\d{2}(?:\.\d+)?)([-+]\d{2}:?\d{2})$").unwrap();
        if let Some(caps) = re.captures(value.trim()) {
            let time_str = &caps[1];
            let offset_str = &caps[2];
            
            // Parse time
            let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
                .or_else(|_| NaiveTime::parse_from_str(time_str, "%H:%M:%S"))
                .map_err(|e| format!("Invalid time format: {} ({})", time_str, e))?;
            
            // Parse offset (±HH:MM or ±HHMM)
            let offset_seconds = Self::parse_timezone_offset(offset_str)?;
            
            // Convert to seconds since midnight and adjust for timezone
            let seconds = time.num_seconds_from_midnight() as f64 
                + (time.nanosecond() as f64 / 1_000_000_000.0)
                - offset_seconds as f64;
            
            Ok(seconds.to_string())
        } else {
            Err(format!("Invalid TIMETZ format: {}", value))
        }
    }
    
    /// Convert seconds since midnight UTC to PostgreSQL TIMETZ
    fn convert_seconds_to_timetz(value: &str) -> Result<String, String> {
        let seconds = value.parse::<f64>()
            .map_err(|e| format!("Invalid seconds value: {} ({})", value, e))?;
        
        // Normalize to 0-86400 range
        let normalized_seconds = seconds.rem_euclid(86400.0);
        let secs = normalized_seconds.trunc() as u32;
        let nanos = ((normalized_seconds.fract() * 1_000_000_000.0).round() as u32).min(999_999_999);
        
        let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
            .ok_or_else(|| format!("Invalid time value: {} seconds", value))?;
        
        // Format with UTC offset
        if nanos > 0 {
            Ok(format!("{:02}:{:02}:{:02}.{:06}+00:00", 
                time.hour(), time.minute(), time.second(), 
                nanos / 1000))
        } else {
            Ok(format!("{}+00:00", time.format("%H:%M:%S")))
        }
    }
    
    /// Convert PostgreSQL TIMESTAMP to Unix timestamp
    fn convert_timestamp_to_unix(value: &str) -> Result<String, String> {
        let datetime = NaiveDateTime::parse_from_str(value.trim(), "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(value.trim(), "%Y-%m-%d %H:%M:%S"))
            .map_err(|e| format!("Invalid timestamp format: {} ({})", value, e))?;
        let timestamp = datetime.and_utc().timestamp() as f64
            + (datetime.nanosecond() as f64 / 1_000_000_000.0);
        Ok(timestamp.to_string())
    }
    
    /// Convert Unix timestamp to PostgreSQL TIMESTAMP
    fn convert_unix_to_timestamp(value: &str) -> Result<String, String> {
        let timestamp = value.parse::<f64>()
            .map_err(|e| format!("Invalid timestamp: {} ({})", value, e))?;
        let secs = timestamp.trunc() as i64;
        let nanos = ((timestamp.fract() * 1_000_000_000.0).round() as u32).min(999_999_999);
        let datetime = DateTime::from_timestamp(secs, nanos)
            .ok_or_else(|| "Invalid timestamp".to_string())?;
        
        // Format with microseconds if present
        if nanos > 0 {
            Ok(datetime.format("%Y-%m-%d %H:%M:%S.%6f").to_string())
        } else {
            Ok(datetime.format("%Y-%m-%d %H:%M:%S").to_string())
        }
    }
    
    /// Convert PostgreSQL TIMESTAMPTZ to Unix timestamp in UTC
    fn convert_timestamptz_to_unix(value: &str) -> Result<String, String> {
        // Try parsing with timezone offset
        let re = Regex::new(r"^(.+?)([-+]\d{2}:?\d{2})$").unwrap();
        
        let (datetime_str, offset_seconds) = if let Some(caps) = re.captures(value.trim()) {
            let dt_str = caps.get(1).unwrap().as_str();
            let offset_str = caps.get(2).unwrap().as_str();
            let offset = Self::parse_timezone_offset(offset_str)?;
            (dt_str.trim().to_string(), offset)
        } else {
            // No timezone specified, assume UTC
            (value.trim().to_string(), 0)
        };
        
        let datetime = NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M:%S%.f")
            .or_else(|_| NaiveDateTime::parse_from_str(&datetime_str, "%Y-%m-%d %H:%M:%S"))
            .map_err(|e| format!("Invalid timestamp format: {} ({})", datetime_str, e))?;
        
        // Convert to UTC by subtracting the offset
        let timestamp = datetime.and_utc().timestamp() as f64
            + (datetime.nanosecond() as f64 / 1_000_000_000.0)
            - offset_seconds as f64;
        
        Ok(timestamp.to_string())
    }
    
    /// Convert Unix timestamp to PostgreSQL TIMESTAMPTZ (with session timezone)
    fn convert_unix_to_timestamptz(value: &str, _timezone: &str) -> Result<String, String> {
        let timestamp = value.parse::<f64>()
            .map_err(|e| format!("Invalid timestamp: {} ({})", value, e))?;
        let secs = timestamp.trunc() as i64;
        let nanos = ((timestamp.fract() * 1_000_000_000.0).round() as u32).min(999_999_999);
        let datetime = DateTime::from_timestamp(secs, nanos)
            .ok_or_else(|| "Invalid timestamp".to_string())?;
        
        // For now, always use UTC
        // TODO: Apply session timezone offset
        if nanos > 0 {
            Ok(datetime.format("%Y-%m-%d %H:%M:%S.%6f+00:00").to_string())
        } else {
            Ok(datetime.format("%Y-%m-%d %H:%M:%S+00:00").to_string())
        }
    }
    
    /// Convert PostgreSQL INTERVAL to seconds
    fn convert_interval_to_seconds(value: &str) -> Result<String, String> {
        // Simple interval parsing for common formats
        // Full PostgreSQL interval parsing is complex, this handles basic cases
        let trimmed = value.trim();
        
        // Handle simple numeric intervals (e.g., "3600" seconds)
        if let Ok(seconds) = trimmed.parse::<f64>() {
            return Ok(seconds.to_string());
        }
        
        // Handle HH:MM:SS format
        if let Ok(time) = NaiveTime::parse_from_str(trimmed, "%H:%M:%S%.f")
            .or_else(|_| NaiveTime::parse_from_str(trimmed, "%H:%M:%S")) {
            let seconds = time.num_seconds_from_midnight() as f64 
                + (time.nanosecond() as f64 / 1_000_000_000.0);
            return Ok(seconds.to_string());
        }
        
        // Handle verbose format (e.g., "1 day 02:30:00")
        let re = Regex::new(r"(?:(\d+)\s+days?\s*)?(?:(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?)?").unwrap();
        if let Some(caps) = re.captures(trimmed) {
            let days = caps.get(1).map(|m| m.as_str().parse::<i64>().unwrap_or(0)).unwrap_or(0);
            let hours = caps.get(2).map(|m| m.as_str().parse::<i64>().unwrap_or(0)).unwrap_or(0);
            let minutes = caps.get(3).map(|m| m.as_str().parse::<i64>().unwrap_or(0)).unwrap_or(0);
            let seconds = caps.get(4).map(|m| m.as_str().parse::<i64>().unwrap_or(0)).unwrap_or(0);
            let fraction = caps.get(5).map(|m| format!("0.{}", m.as_str()).parse::<f64>().unwrap_or(0.0)).unwrap_or(0.0);
            
            let total_seconds = (days * 86400 + hours * 3600 + minutes * 60 + seconds) as f64 + fraction;
            return Ok(total_seconds.to_string());
        }
        
        Err(format!("Unsupported interval format: {}", value))
    }
    
    /// Convert seconds to PostgreSQL INTERVAL
    fn convert_seconds_to_interval(value: &str) -> Result<String, String> {
        let total_seconds = value.parse::<f64>()
            .map_err(|e| format!("Invalid seconds value: {} ({})", value, e))?;
        
        let days = (total_seconds / 86400.0).trunc() as i64;
        let remaining_seconds = total_seconds - (days as f64 * 86400.0);
        let hours = (remaining_seconds / 3600.0).trunc() as i64;
        let minutes = ((remaining_seconds - hours as f64 * 3600.0) / 60.0).trunc() as i64;
        let seconds = remaining_seconds - (hours as f64 * 3600.0) - (minutes as f64 * 60.0);
        
        let mut parts = Vec::new();
        if days > 0 {
            parts.push(format!("{} day{}", days, if days == 1 { "" } else { "s" }));
        }
        
        if seconds.fract() > 0.0 {
            parts.push(format!("{:02}:{:02}:{:06.3}", hours, minutes, seconds));
        } else {
            parts.push(format!("{:02}:{:02}:{:02}", hours, minutes, seconds.trunc() as i64));
        }
        
        Ok(parts.join(" "))
    }
    
    /// Parse timezone offset string (±HH:MM or ±HHMM) to seconds
    fn parse_timezone_offset(offset: &str) -> Result<i32, String> {
        let re = Regex::new(r"^([-+])(\d{2}):?(\d{2})$").unwrap();
        if let Some(caps) = re.captures(offset) {
            let sign = if &caps[1] == "+" { 1 } else { -1 };
            let hours = caps[2].parse::<i32>()
                .map_err(|e| format!("Invalid hours in offset: {} ({})", &caps[2], e))?;
            let minutes = caps[3].parse::<i32>()
                .map_err(|e| format!("Invalid minutes in offset: {} ({})", &caps[3], e))?;
            Ok(sign * (hours * 3600 + minutes * 60))
        } else {
            Err(format!("Invalid timezone offset format: {}", offset))
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
        // Test DATE to Unix timestamp
        let result = ValueConverter::convert_date_to_unix("2024-01-15").unwrap();
        let timestamp = result.parse::<f64>().unwrap();
        assert_eq!(timestamp, 1705276800.0); // 2024-01-15 00:00:00 UTC
        
        // Test Unix timestamp to DATE
        let result = ValueConverter::convert_unix_to_date("1705276800").unwrap();
        assert_eq!(result, "2024-01-15");
    }
    
    #[test]
    fn test_time_conversion() {
        // Test TIME to seconds
        let result = ValueConverter::convert_time_to_seconds("14:30:45.123456").unwrap();
        let seconds = result.parse::<f64>().unwrap();
        assert!((seconds - 52245.123456).abs() < 0.000001);
        
        // Test seconds to TIME
        let result = ValueConverter::convert_seconds_to_time("52245.123456").unwrap();
        assert_eq!(result, "14:30:45.123456");
        
        // Test TIME without fractional seconds
        let result = ValueConverter::convert_time_to_seconds("14:30:45").unwrap();
        assert_eq!(result, "52245");
    }
    
    #[test]
    fn test_timestamp_conversion() {
        // Test TIMESTAMP to Unix timestamp
        let result = ValueConverter::convert_timestamp_to_unix("2024-01-15 14:30:45.123456").unwrap();
        let timestamp = result.parse::<f64>().unwrap();
        assert!((timestamp - 1705329045.123456).abs() < 0.000001);
        
        // Test Unix timestamp to TIMESTAMP
        let result = ValueConverter::convert_unix_to_timestamp("1705329045.123456").unwrap();
        assert_eq!(result, "2024-01-15 14:30:45.123456");
        
        // Test without fractional seconds
        let result = ValueConverter::convert_timestamp_to_unix("2024-01-15 14:30:45").unwrap();
        let timestamp = result.parse::<f64>().unwrap();
        assert_eq!(timestamp, 1705329045.0);
    }
    
    #[test]
    fn test_interval_conversion() {
        // Test simple seconds
        assert_eq!(ValueConverter::convert_interval_to_seconds("3600").unwrap(), "3600");
        
        // Test HH:MM:SS format
        assert_eq!(ValueConverter::convert_interval_to_seconds("01:30:00").unwrap(), "5400");
        
        // Test verbose format
        let result = ValueConverter::convert_interval_to_seconds("1 day 02:30:00").unwrap();
        assert_eq!(result, "95400"); // 86400 + 9000
        
        // Test seconds to interval
        assert_eq!(ValueConverter::convert_seconds_to_interval("95400").unwrap(), "1 day 02:30:00");
        assert_eq!(ValueConverter::convert_seconds_to_interval("5400.5").unwrap(), "01:30:00.500");
    }
}