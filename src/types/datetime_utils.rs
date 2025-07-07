/// Utility functions for datetime conversions using INTEGER microseconds
use chrono::{NaiveDate, NaiveTime, NaiveDateTime, Datelike, Timelike};

/// Unix epoch as a date (1970-01-01)
const UNIX_EPOCH_DATE: i32 = 719163; // Days from 0000-01-01 to 1970-01-01

/// Convert epoch days to year, month, day
pub fn epoch_days_to_date(days: i64) -> (i32, u32, u32) {
    // Convert to Julian days (days since 0000-01-01)
    let julian_days = days + UNIX_EPOCH_DATE as i64;
    
    // Use chrono for the conversion
    if let Some(date) = NaiveDate::from_num_days_from_ce_opt(julian_days as i32) {
        (date.year(), date.month(), date.day())
    } else {
        // Fallback for out-of-range dates
        (1970, 1, 1)
    }
}

/// Convert year, month, day to epoch days
pub fn date_to_epoch_days(year: i32, month: u32, day: u32) -> Option<i64> {
    NaiveDate::from_ymd_opt(year, month, day)
        .map(|date| date.num_days_from_ce() as i64 - UNIX_EPOCH_DATE as i64)
}

/// Convert microseconds since midnight to hours, minutes, seconds, microseconds
pub fn microseconds_to_time(micros: i64) -> (u32, u32, u32, u32) {
    let total_seconds = micros / 1_000_000;
    let microseconds = (micros % 1_000_000) as u32;
    
    let hours = (total_seconds / 3600) as u32;
    let minutes = ((total_seconds % 3600) / 60) as u32;
    let seconds = (total_seconds % 60) as u32;
    
    (hours % 24, minutes, seconds, microseconds) // Ensure hours wrap at 24
}

/// Convert time components to microseconds since midnight
pub fn time_to_microseconds(hours: u32, minutes: u32, seconds: u32, microseconds: u32) -> i64 {
    let total_seconds = (hours as i64) * 3600 + (minutes as i64) * 60 + (seconds as i64);
    total_seconds * 1_000_000 + (microseconds as i64)
}

/// Convert microseconds since epoch to NaiveDateTime
pub fn microseconds_to_datetime(micros: i64) -> Option<NaiveDateTime> {
    let seconds = micros / 1_000_000;
    let nanoseconds = ((micros % 1_000_000) * 1000) as u32;
    use chrono::DateTime;
    DateTime::from_timestamp(seconds, nanoseconds).map(|dt| dt.naive_utc())
}

/// Convert NaiveDateTime to microseconds since epoch
pub fn datetime_to_microseconds(dt: &NaiveDateTime) -> i64 {
    let seconds = dt.and_utc().timestamp();
    let microseconds = dt.and_utc().timestamp_subsec_micros() as i64;
    seconds * 1_000_000 + microseconds
}

/// Parse PostgreSQL date string to epoch days
pub fn parse_date_to_days(date_str: &str) -> Option<i64> {
    // Handle special values
    match date_str {
        "infinity" | "+infinity" => return Some(i64::MAX / 86400_000_000), // Max days
        "-infinity" => return Some(i64::MIN / 86400_000_000), // Min days
        _ => {}
    }
    
    // Parse ISO date format (YYYY-MM-DD)
    if let Ok(date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
        return date_to_epoch_days(date.year(), date.month(), date.day());
    }
    
    None
}

/// Parse PostgreSQL time string to microseconds since midnight
pub fn parse_time_to_microseconds(time_str: &str) -> Option<i64> {
    // Parse various time formats
    let formats = [
        "%H:%M:%S%.f",     // HH:MM:SS.ffffff
        "%H:%M:%S",        // HH:MM:SS
        "%H:%M",           // HH:MM
    ];
    
    for format in &formats {
        if let Ok(time) = NaiveTime::parse_from_str(time_str, format) {
            let micros = time.num_seconds_from_midnight() as i64 * 1_000_000 
                + (time.nanosecond() / 1000) as i64;
            return Some(micros);
        }
    }
    
    None
}

/// Parse PostgreSQL timestamp string to microseconds since epoch
pub fn parse_timestamp_to_microseconds(timestamp_str: &str) -> Option<i64> {
    // Handle special values
    match timestamp_str {
        "infinity" | "+infinity" => return Some(i64::MAX),
        "-infinity" => return Some(i64::MIN),
        _ => {}
    }
    
    // Parse various timestamp formats
    let formats = [
        "%Y-%m-%d %H:%M:%S%.f",     // YYYY-MM-DD HH:MM:SS.ffffff
        "%Y-%m-%d %H:%M:%S",        // YYYY-MM-DD HH:MM:SS
        "%Y-%m-%dT%H:%M:%S%.f",     // ISO format with T
        "%Y-%m-%dT%H:%M:%S",        // ISO format with T
    ];
    
    for format in &formats {
        if let Ok(dt) = NaiveDateTime::parse_from_str(timestamp_str, format) {
            return Some(datetime_to_microseconds(&dt));
        }
    }
    
    None
}

/// Format epoch days as PostgreSQL date string
pub fn format_days_to_date(days: i64) -> String {
    // Handle special values
    if days >= i64::MAX / 86400_000_000 {
        return "infinity".to_string();
    }
    if days <= i64::MIN / 86400_000_000 {
        return "-infinity".to_string();
    }
    
    let (year, month, day) = epoch_days_to_date(days);
    format!("{:04}-{:02}-{:02}", year, month, day)
}

/// Optimized format epoch days as PostgreSQL date string into a buffer
/// Returns the number of bytes written
pub fn format_days_to_date_buf(days: i32, buf: &mut [u8]) -> usize {
    // Handle special values efficiently
    if days >= (i64::MAX / 86400_000_000) as i32 {
        let inf = b"infinity";
        buf[..inf.len()].copy_from_slice(inf);
        return inf.len();
    }
    if days <= (i64::MIN / 86400_000_000) as i32 {
        let ninf = b"-infinity";
        buf[..ninf.len()].copy_from_slice(ninf);
        return ninf.len();
    }
    
    let (year, month, day) = epoch_days_to_date(days as i64);
    
    // Format directly into buffer: YYYY-MM-DD (10 bytes)
    // Year (4 digits)
    let year_abs = year.abs() as u32;
    buf[0] = b'0' + ((year_abs / 1000) % 10) as u8;
    buf[1] = b'0' + ((year_abs / 100) % 10) as u8;
    buf[2] = b'0' + ((year_abs / 10) % 10) as u8;
    buf[3] = b'0' + (year_abs % 10) as u8;
    buf[4] = b'-';
    // Month (2 digits)
    buf[5] = b'0' + ((month / 10) % 10) as u8;
    buf[6] = b'0' + (month % 10) as u8;
    buf[7] = b'-';
    // Day (2 digits)
    buf[8] = b'0' + ((day / 10) % 10) as u8;
    buf[9] = b'0' + (day % 10) as u8;
    
    10
}

/// Format microseconds since midnight as PostgreSQL time string
pub fn format_microseconds_to_time(micros: i64) -> String {
    let (hours, minutes, seconds, microseconds) = microseconds_to_time(micros);
    
    if microseconds > 0 {
        format!("{:02}:{:02}:{:02}.{:06}", hours, minutes, seconds, microseconds)
    } else {
        format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
    }
}

/// Optimized format microseconds since midnight into a buffer
/// Returns the number of bytes written
pub fn format_microseconds_to_time_buf(micros: i64, buf: &mut [u8]) -> usize {
    let (hours, minutes, seconds, microseconds) = microseconds_to_time(micros);
    
    // Format HH:MM:SS (8 bytes minimum)
    buf[0] = b'0' + ((hours / 10) % 10) as u8;
    buf[1] = b'0' + (hours % 10) as u8;
    buf[2] = b':';
    buf[3] = b'0' + ((minutes / 10) % 10) as u8;
    buf[4] = b'0' + (minutes % 10) as u8;
    buf[5] = b':';
    buf[6] = b'0' + ((seconds / 10) % 10) as u8;
    buf[7] = b'0' + (seconds % 10) as u8;
    
    if microseconds > 0 {
        // Add .ffffff (7 more bytes)
        buf[8] = b'.';
        let mut us = microseconds;
        for i in (0..6).rev() {
            buf[9 + i] = b'0' + (us % 10) as u8;
            us /= 10;
        }
        15
    } else {
        8
    }
}

/// Format microseconds since epoch as PostgreSQL timestamp string
pub fn format_microseconds_to_timestamp(micros: i64) -> String {
    // Handle special values
    if micros == i64::MAX {
        return "infinity".to_string();
    }
    if micros == i64::MIN {
        return "-infinity".to_string();
    }
    
    if let Some(dt) = microseconds_to_datetime(micros) {
        let micros = dt.and_utc().timestamp_subsec_micros();
        if micros > 0 {
            format!("{}.{:06}", dt.format("%Y-%m-%d %H:%M:%S"), micros)
        } else {
            dt.format("%Y-%m-%d %H:%M:%S").to_string()
        }
    } else {
        "1970-01-01 00:00:00".to_string()
    }
}

/// Optimized format microseconds since epoch into a buffer  
/// Returns the number of bytes written
pub fn format_microseconds_to_timestamp_buf(micros: i64, buf: &mut [u8]) -> usize {
    // Handle special values
    if micros == i64::MAX {
        let inf = b"infinity";
        buf[..inf.len()].copy_from_slice(inf);
        return inf.len();
    }
    if micros == i64::MIN {
        let ninf = b"-infinity";
        buf[..ninf.len()].copy_from_slice(ninf);
        return ninf.len();
    }
    
    // Convert to date and time components
    let seconds = micros / 1_000_000;
    let microseconds = (micros % 1_000_000) as u32;
    
    // Convert seconds to days and time
    let days = seconds / 86400;
    let time_seconds = seconds % 86400;
    
    // Format date part
    let date_len = format_days_to_date_buf(days as i32, buf);
    
    // Add space separator
    buf[date_len] = b' ';
    
    // Format time part
    let time_micros = time_seconds * 1_000_000 + microseconds as i64;
    let time_len = format_microseconds_to_time_buf(time_micros, &mut buf[date_len + 1..]);
    
    date_len + 1 + time_len
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_date_conversions() {
        // Test epoch date
        assert_eq!(date_to_epoch_days(1970, 1, 1), Some(0));
        assert_eq!(epoch_days_to_date(0), (1970, 1, 1));
        
        // Test a known date
        assert_eq!(date_to_epoch_days(2023, 6, 15), Some(19523));
        assert_eq!(epoch_days_to_date(19523), (2023, 6, 15));
        
        // Test date formatting
        assert_eq!(format_days_to_date(0), "1970-01-01");
        assert_eq!(format_days_to_date(19523), "2023-06-15");
        
        // Test date parsing
        assert_eq!(parse_date_to_days("1970-01-01"), Some(0));
        assert_eq!(parse_date_to_days("2023-06-15"), Some(19523));
    }
    
    #[test]
    fn test_time_conversions() {
        // Test midnight
        assert_eq!(time_to_microseconds(0, 0, 0, 0), 0);
        assert_eq!(microseconds_to_time(0), (0, 0, 0, 0));
        
        // Test a specific time
        let micros = time_to_microseconds(14, 30, 45, 123456);
        assert_eq!(microseconds_to_time(micros), (14, 30, 45, 123456));
        
        // Test time formatting
        assert_eq!(format_microseconds_to_time(0), "00:00:00");
        assert_eq!(format_microseconds_to_time(micros), "14:30:45.123456");
        
        // Test time parsing
        assert_eq!(parse_time_to_microseconds("00:00:00"), Some(0));
        assert_eq!(parse_time_to_microseconds("14:30:45.123456"), Some(micros));
    }
    
    #[test]
    fn test_timestamp_conversions() {
        // Test epoch timestamp
        assert_eq!(format_microseconds_to_timestamp(0), "1970-01-01 00:00:00");
        
        // Test a specific timestamp (2023-06-15 14:30:45.123456)
        let timestamp_micros = 1686839445_123456;
        let formatted = format_microseconds_to_timestamp(timestamp_micros);
        assert!(formatted.starts_with("2023-06-15"));
        
        // Test parsing
        assert_eq!(parse_timestamp_to_microseconds("1970-01-01 00:00:00"), Some(0));
    }
}