use rusqlite::{Connection, Result, Error};
use rusqlite::functions::FunctionFlags;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc, Datelike, Timelike};

/// Register datetime-related functions in SQLite
pub fn register_datetime_functions(conn: &Connection) -> Result<()> {
    // now() / current_timestamp - Return current timestamp as formatted string
    // PostgreSQL clients expect NOW() to return formatted timestamp strings
    conn.create_scalar_function(
        "now",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            let now = Utc::now();
            Ok(now.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        },
    )?;
    
    conn.create_scalar_function(
        "current_timestamp",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            let now = Utc::now();
            Ok(now.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        },
    )?;
    
    // Don't override SQLite's built-in CURRENT_DATE function
    // SQLite's CURRENT_DATE returns text in YYYY-MM-DD format
    
    // current_time - Return microseconds since midnight
    conn.create_scalar_function(
        "current_time",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            let now = Utc::now();
            let time = now.time();
            let micros = time.num_seconds_from_midnight() as i64 * 1_000_000 
                + (time.nanosecond() / 1000) as i64;
            Ok(micros)
        },
    )?;
    
    // date_part(field, timestamp) / extract(field FROM timestamp)
    // Extract a specific part from a timestamp
    conn.create_scalar_function(
        "date_part",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let field: String = ctx.get(0)?;
            let timestamp: i64 = ctx.get(1)?;
            extract_date_part(&field, timestamp)
        },
    )?;
    
    conn.create_scalar_function(
        "extract",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let field: String = ctx.get(0)?;
            let timestamp: i64 = ctx.get(1)?;
            extract_date_part(&field, timestamp)
        },
    )?;
    
    // date_trunc(field, timestamp) - Truncate timestamp to specified precision
    conn.create_scalar_function(
        "date_trunc",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let field: String = ctx.get(0)?;
            let timestamp: i64 = ctx.get(1)?;
            truncate_date(&field, timestamp)
        },
    )?;
    
    // age(timestamp1, timestamp2) - Calculate interval between timestamps
    conn.create_scalar_function(
        "age",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let ts1: i64 = ctx.get(0)?;
            let ts2: i64 = ctx.get(1)?;
            Ok(ts1 - ts2) // Return difference in microseconds
        },
    )?;
    
    // age(timestamp) - Calculate interval from current time
    conn.create_scalar_function(
        "age",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let ts: i64 = ctx.get(0)?;
            let now = Utc::now().timestamp_micros();
            Ok(now - ts) // Return difference in microseconds
        },
    )?;
    
    // to_timestamp(double) - Convert seconds to microseconds
    conn.create_scalar_function(
        "to_timestamp",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let ts: f64 = ctx.get(0)?;
            Ok((ts * 1_000_000.0) as i64) // Convert seconds to microseconds
        },
    )?;
    
    // epoch() - Unix epoch timestamp (0)
    conn.create_scalar_function(
        "epoch",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            Ok(0i64)
        },
    )?;
    
    // make_date(year, month, day) - Create date from components
    conn.create_scalar_function(
        "make_date",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let year: i32 = ctx.get(0)?;
            let month: u32 = ctx.get::<i32>(1)? as u32;
            let day: u32 = ctx.get::<i32>(2)? as u32;
            
            match NaiveDate::from_ymd_opt(year, month, day) {
                Some(date) => {
                    // Return days since epoch
                    Ok(date.num_days_from_ce() as i64 - 719163)
                }
                None => Err(Error::UserFunctionError(
                    format!("Invalid date: {year}-{month}-{day}").into()
                ))
            }
        },
    )?;
    
    // make_time(hour, min, sec) - Create time from components
    conn.create_scalar_function(
        "make_time",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let hour: u32 = ctx.get::<i32>(0)? as u32;
            let min: u32 = ctx.get::<i32>(1)? as u32;
            let sec: f64 = ctx.get(2)?;
            
            let secs = sec.trunc() as u32;
            let nanos = ((sec.fract() * 1_000_000_000.0).round() as u32).min(999_999_999);
            
            match NaiveTime::from_hms_nano_opt(hour, min, secs, nanos) {
                Some(time) => {
                    let micros = time.num_seconds_from_midnight() as i64 * 1_000_000 
                        + (time.nanosecond() / 1000) as i64;
                    Ok(micros)
                }
                None => Err(Error::UserFunctionError(
                    format!("Invalid time: {hour}:{min}:{sec}").into()
                ))
            }
        },
    )?;
    
    Ok(())
}

/// Extract a date part from microseconds since epoch
fn extract_date_part(field: &str, timestamp: i64) -> Result<f64> {
    let secs = timestamp / 1_000_000;
    let micros = timestamp % 1_000_000;
    let nanos = (micros * 1000) as u32;
    
    let datetime = DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| Error::UserFunctionError("Invalid timestamp".into()))?;
    
    match field.to_lowercase().as_str() {
        "year" => Ok(datetime.year() as f64),
        "month" => Ok(datetime.month() as f64),
        "day" => Ok(datetime.day() as f64),
        "hour" => Ok(datetime.hour() as f64),
        "minute" => Ok(datetime.minute() as f64),
        "second" => Ok(datetime.second() as f64 + (datetime.nanosecond() as f64 / 1_000_000_000.0)),
        "microseconds" => Ok(datetime.nanosecond() as f64 / 1000.0),
        "milliseconds" => Ok(datetime.nanosecond() as f64 / 1_000_000.0),
        "epoch" => Ok(timestamp as f64 / 1_000_000.0), // Return seconds for epoch
        "dow" | "dayofweek" => Ok(datetime.weekday().num_days_from_sunday() as f64),
        "doy" | "dayofyear" => Ok(datetime.ordinal() as f64),
        "quarter" => Ok(((datetime.month() - 1) / 3 + 1) as f64),
        "week" => Ok(datetime.iso_week().week() as f64),
        "isoyear" => Ok(datetime.iso_week().year() as f64),
        "decade" => Ok((datetime.year() / 10) as f64),
        "century" => Ok(((datetime.year() - 1) / 100 + 1) as f64),
        "millennium" => Ok(((datetime.year() - 1) / 1000 + 1) as f64),
        _ => Err(Error::UserFunctionError(
            format!("Unknown date part: {field}").into()
        ))
    }
}

/// Truncate microseconds since epoch to the specified precision
fn truncate_date(field: &str, timestamp: i64) -> Result<i64> {
    let secs = timestamp / 1_000_000;
    let micros = timestamp % 1_000_000;
    let nanos = (micros * 1000) as u32;
    
    let datetime = DateTime::from_timestamp(secs, nanos)
        .ok_or_else(|| Error::UserFunctionError("Invalid timestamp".into()))?;
    
    let truncated = match field.to_lowercase().as_str() {
        "microseconds" => {
            // Already at microsecond precision
            timestamp
        }
        "milliseconds" => {
            // Truncate to millisecond
            (timestamp / 1000) * 1000
        }
        "second" => (timestamp / 1_000_000) * 1_000_000,
        "minute" => {
            let dt = datetime.date_naive().and_hms_opt(datetime.hour(), datetime.minute(), 0).unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "hour" => {
            let dt = datetime.date_naive().and_hms_opt(datetime.hour(), 0, 0).unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "day" => {
            let dt = datetime.date_naive().and_hms_opt(0, 0, 0).unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "week" => {
            // Truncate to start of week (Monday)
            let days_from_monday = datetime.weekday().num_days_from_monday();
            let start_of_week = datetime.date_naive() - chrono::Duration::days(days_from_monday as i64);
            let dt = start_of_week.and_hms_opt(0, 0, 0).unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "month" => {
            let dt = NaiveDate::from_ymd_opt(datetime.year(), datetime.month(), 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "quarter" => {
            let quarter_month = ((datetime.month() - 1) / 3) * 3 + 1;
            let dt = NaiveDate::from_ymd_opt(datetime.year(), quarter_month, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "year" => {
            let dt = NaiveDate::from_ymd_opt(datetime.year(), 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "decade" => {
            let decade_year = (datetime.year() / 10) * 10;
            let dt = NaiveDate::from_ymd_opt(decade_year, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "century" => {
            let century_year = ((datetime.year() - 1) / 100) * 100 + 1;
            let dt = NaiveDate::from_ymd_opt(century_year, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        "millennium" => {
            let millennium_year = ((datetime.year() - 1) / 1000) * 1000 + 1;
            let dt = NaiveDate::from_ymd_opt(millennium_year, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap();
            dt.and_utc().timestamp() * 1_000_000
        }
        _ => return Err(Error::UserFunctionError(
            format!("Unknown truncation field: {field}").into()
        ))
    };
    
    Ok(truncated)
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_date_functions() {
        // Test will be implemented when integrated
    }
}