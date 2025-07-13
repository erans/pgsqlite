use rusqlite::{Connection, Result, functions::{FunctionFlags, Context}};
use rust_decimal::Decimal;
use std::str::FromStr;
use std::panic::AssertUnwindSafe;

/// Register all decimal-related functions with the SQLite connection
pub fn register_decimal_functions(conn: &Connection) -> Result<()> {
    // Storage functions
    conn.create_scalar_function(
        "decimal_from_text",
        1,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_from_text,
    )?;
    
    conn.create_scalar_function(
        "decimal_to_text",
        1,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_to_text,
    )?;
    
    // Arithmetic functions
    conn.create_scalar_function(
        "decimal_add",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_add,
    )?;
    
    conn.create_scalar_function(
        "decimal_sub",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_sub,
    )?;
    
    conn.create_scalar_function(
        "decimal_mul",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_mul,
    )?;
    
    conn.create_scalar_function(
        "decimal_div",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_div,
    )?;
    
    // Comparison functions
    conn.create_scalar_function(
        "decimal_eq",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_eq,
    )?;
    
    conn.create_scalar_function(
        "decimal_lt",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_lt,
    )?;
    
    conn.create_scalar_function(
        "decimal_gt",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_gt,
    )?;
    
    // Note: Aggregate functions will be implemented using SQLite's built-in aggregates
    // with proper wrapping of decimal values. For now, we provide the scalar functions
    // that can be used with GROUP BY for basic aggregation.
    
    // Formatting function for NUMERIC type with precision and scale
    conn.create_scalar_function(
        "numeric_format",
        3,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        numeric_format,
    )?;
    
    // Numeric cast function that validates constraints
    conn.create_scalar_function(
        "numeric_cast",
        3,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        numeric_cast,
    )?;
    
    // Decimal math functions
    conn.create_scalar_function(
        "decimal_round",
        2,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_round,
    )?;
    
    conn.create_scalar_function(
        "decimal_abs",
        1,
        FunctionFlags::SQLITE_DETERMINISTIC | FunctionFlags::SQLITE_INNOCUOUS,
        decimal_abs,
    )?;
    
    Ok(())
}

// Storage functions

fn decimal_from_text(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match ctx.get_raw(0) {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Text(s) => {
            let text = std::str::from_utf8(s).map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
            
            // Check if this is a very large number that might exceed rust_decimal's capacity
            // rust_decimal can handle approximately 28-29 significant digits
            let trimmed = text.trim();
            let without_sign = trimmed.trim_start_matches('-').trim_start_matches('+');
            let parts: Vec<&str> = without_sign.split('.').collect();
            
            let total_digits = if parts.len() == 2 {
                let int_digits = parts[0].trim_start_matches('0').len();
                let dec_digits = parts[1].trim_end_matches('0').len();
                int_digits + dec_digits
            } else {
                parts[0].trim_start_matches('0').len()
            };
            
            if total_digits > 28 {
                // For very large numbers, return an error instead of panicking
                return Err(rusqlite::Error::UserFunctionError(
                    format!("Numeric value has {} significant digits, exceeding the maximum of 28 digits supported for calculations", total_digits).into()
                ));
            }
            
            // Try to parse, but catch panics from rust_decimal
            match std::panic::catch_unwind(AssertUnwindSafe(|| Decimal::from_str(text))) {
                Ok(Ok(decimal)) => {
                    // Store as binary representation
                    let bytes = decimal.serialize();
                    Ok(Some(bytes.to_vec()))
                }
                Ok(Err(e)) => Err(rusqlite::Error::UserFunctionError(Box::new(e))),
                Err(_) => {
                    // rust_decimal panicked - likely due to capacity error
                    Err(rusqlite::Error::UserFunctionError(
                        "Numeric value exceeds maximum supported precision of 28 significant digits".into()
                    ))
                }
            }
        }
        rusqlite::types::ValueRef::Integer(i) => {
            let decimal = Decimal::new(i, 0);
            Ok(Some(decimal.serialize().to_vec()))
        }
        rusqlite::types::ValueRef::Real(f) => {
            match Decimal::try_from(f) {
                Ok(decimal) => Ok(Some(decimal.serialize().to_vec())),
                Err(e) => Err(rusqlite::Error::UserFunctionError(format!("Cannot convert float to decimal: {}", e).into()))
            }
        }
        _ => Err(rusqlite::Error::UserFunctionError("Expected text, integer, or real value".into()))
    }
}

fn decimal_to_text(ctx: &Context<'_>) -> Result<Option<String>> {
    match ctx.get_raw(0) {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Blob(bytes) => {
            if bytes.len() == 16 {
                let mut array = [0u8; 16];
                array.copy_from_slice(bytes);
                let decimal = Decimal::deserialize(array);
                Ok(Some(decimal.to_string()))
            } else {
                Err(rusqlite::Error::UserFunctionError("Invalid decimal binary format".into()))
            }
        }
        rusqlite::types::ValueRef::Text(s) => {
            // Allow direct text input for compatibility
            let text = std::str::from_utf8(s).map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
            Ok(Some(text.to_string()))
        }
        _ => Err(rusqlite::Error::UserFunctionError("Expected blob or text value".into()))
    }
}

// Helper to get decimal from context
fn get_decimal(ctx: &Context<'_>, idx: usize) -> Result<Option<Decimal>> {
    match ctx.get_raw(idx) {
        rusqlite::types::ValueRef::Null => Ok(None),
        rusqlite::types::ValueRef::Blob(bytes) => {
            if bytes.len() == 16 {
                let mut array = [0u8; 16];
                array.copy_from_slice(bytes);
                Ok(Some(Decimal::deserialize(array)))
            } else {
                Err(rusqlite::Error::UserFunctionError("Invalid decimal binary format".into()))
            }
        }
        rusqlite::types::ValueRef::Text(s) => {
            let text = std::str::from_utf8(s).map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?;
            Decimal::from_str(text)
                .map(Some)
                .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))
        }
        rusqlite::types::ValueRef::Integer(i) => {
            Ok(Some(Decimal::new(i, 0)))
        }
        rusqlite::types::ValueRef::Real(f) => {
            // Check for special floating point values
            if f.is_nan() {
                return Err(rusqlite::Error::UserFunctionError("Cannot convert NaN to decimal".into()));
            }
            if f.is_infinite() {
                return Err(rusqlite::Error::UserFunctionError("Cannot convert infinity to decimal".into()));
            }
            
            // Try conversion and provide detailed error information
            Decimal::try_from(f)
                .map(Some)
                .map_err(|e| rusqlite::Error::UserFunctionError(format!("Invalid function parameter type Real at index 0: Cannot convert float {} to decimal: {}", f, e).into()))
        }
    }
}

// Arithmetic functions

fn decimal_add(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => {
            let result = a + b;
            Ok(Some(result.serialize().to_vec()))
        }
        _ => Ok(None)
    }
}

fn decimal_sub(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => {
            let result = a - b;
            Ok(Some(result.serialize().to_vec()))
        }
        _ => Ok(None)
    }
}

fn decimal_mul(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => {
            let result = a * b;
            Ok(Some(result.serialize().to_vec()))
        }
        _ => Ok(None)
    }
}

fn decimal_div(ctx: &Context<'_>) -> Result<Option<Vec<u8>>> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => {
            if b.is_zero() {
                Err(rusqlite::Error::UserFunctionError("Division by zero".into()))
            } else {
                let result = a / b;
                Ok(Some(result.serialize().to_vec()))
            }
        }
        _ => Ok(None)
    }
}

// Comparison functions

fn decimal_eq(ctx: &Context<'_>) -> Result<bool> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => Ok(a == b),
        (None, None) => Ok(true),
        _ => Ok(false)
    }
}

fn decimal_lt(ctx: &Context<'_>) -> Result<bool> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => Ok(a < b),
        _ => Ok(false)
    }
}

fn decimal_gt(ctx: &Context<'_>) -> Result<bool> {
    match (get_decimal(ctx, 0)?, get_decimal(ctx, 1)?) {
        (Some(a), Some(b)) => Ok(a > b),
        _ => Ok(false)
    }
}

// Formatting function for NUMERIC type
fn numeric_format(ctx: &Context<'_>) -> Result<Option<String>> {
    // Get the value to format
    let value = match ctx.get_raw(0) {
        rusqlite::types::ValueRef::Null => return Ok(None),
        rusqlite::types::ValueRef::Text(s) => {
            std::str::from_utf8(s)
                .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?
                .to_string()
        }
        rusqlite::types::ValueRef::Integer(i) => i.to_string(),
        rusqlite::types::ValueRef::Real(f) => f.to_string(),
        rusqlite::types::ValueRef::Blob(bytes) => {
            // Handle decimal stored as blob (from decimal_from_text)
            if bytes.len() == 16 {
                let mut array = [0u8; 16];
                array.copy_from_slice(bytes);
                let decimal = Decimal::deserialize(array);
                decimal.to_string()
            } else {
                return Err(rusqlite::Error::UserFunctionError("Invalid decimal binary format".into()))
            }
        }
    };
    
    // Get precision and scale
    let _precision = ctx.get::<i32>(1)?;
    let scale = ctx.get::<i32>(2)?;
    
    // For very large numbers that exceed rust_decimal's capacity,
    // we'll do string-based formatting
    
    // First check if it's a simple integer
    if scale == 0 {
        // For scale 0, just remove any decimal part
        if let Some(dot_pos) = value.find('.') {
            Ok(Some(value[..dot_pos].to_string()))
        } else {
            Ok(Some(value))
        }
    } else {
        // Try to use Decimal for normal-sized numbers
        match Decimal::from_str(&value) {
            Ok(decimal) => {
                // Round to the specified scale
                let rounded = decimal.round_dp(scale as u32);
                let formatted = format!("{:.prec$}", rounded, prec = scale as usize);
                Ok(Some(formatted))
            }
            Err(_) => {
                // For very large numbers or invalid decimals, do string-based formatting
                if let Some(dot_pos) = value.find('.') {
                    let integer_part = &value[..dot_pos];
                    let decimal_part = &value[dot_pos + 1..];
                    
                    // Pad or truncate decimal part to match scale
                    let formatted_decimal = if decimal_part.len() > scale as usize {
                        // Truncate (no rounding for very large numbers)
                        decimal_part[..scale as usize].to_string()
                    } else {
                        // Pad with zeros
                        let mut padded = decimal_part.to_string();
                        padded.push_str(&"0".repeat(scale as usize - decimal_part.len()));
                        padded
                    };
                    
                    Ok(Some(format!("{}.{}", integer_part, formatted_decimal)))
                } else {
                    // No decimal point, add one with zeros
                    Ok(Some(format!("{}.{}", value, "0".repeat(scale as usize))))
                }
            }
        }
    }
}

/// numeric_cast function that validates and formats numeric values with constraints
fn numeric_cast(ctx: &Context<'_>) -> Result<Option<String>> {
    let value = match ctx.get_raw(0) {
        rusqlite::types::ValueRef::Null => return Ok(None),
        rusqlite::types::ValueRef::Text(s) => {
            std::str::from_utf8(s)
                .map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))?
                .to_string()
        }
        rusqlite::types::ValueRef::Integer(i) => i.to_string(),
        rusqlite::types::ValueRef::Real(f) => f.to_string(),
        _ => return Ok(None),
    };
    
    let precision = ctx.get::<i32>(1)?;
    let scale = ctx.get::<i32>(2)?;
    
    // Validate the value against numeric constraints
    use crate::validator::NumericValidator;
    match NumericValidator::validate_value(&value, precision, scale) {
        Ok(()) => {
            // Value is valid, now format it to the correct scale
            numeric_format(ctx)
        }
        Err(e) => {
            // Return an error that will be caught by SQLite
            Err(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some(e.to_string())
            ))
        }
    }
}

/// decimal_round function that rounds a decimal value to specified decimal places
fn decimal_round(ctx: &Context<'_>) -> Result<Option<String>> {
    let decimal_opt = get_decimal(ctx, 0)?;
    let scale = ctx.get::<i32>(1)?;
    
    match decimal_opt {
        Some(decimal) => {
            let rounded = decimal.round_dp(scale as u32);
            Ok(Some(rounded.to_string()))
        }
        None => Ok(None)
    }
}

/// decimal_abs function that returns the absolute value of a decimal
fn decimal_abs(ctx: &Context<'_>) -> Result<Option<String>> {
    let decimal_opt = get_decimal(ctx, 0)?;
    
    match decimal_opt {
        Some(decimal) => {
            let absolute = decimal.abs();
            Ok(Some(absolute.to_string()))
        }
        None => Ok(None)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    #[test]
    fn test_decimal_arithmetic() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        register_decimal_functions(&conn)?;

        // Test basic arithmetic
        let result: String = conn.query_row(
            "SELECT decimal_to_text(decimal_add(decimal_from_text('123.45'), decimal_from_text('67.89')))",
            [],
            |row| row.get(0)
        )?;
        assert_eq!(result, "191.34");

        let result: String = conn.query_row(
            "SELECT decimal_to_text(decimal_sub(decimal_from_text('100.00'), decimal_from_text('25.50')))",
            [],
            |row| row.get(0)
        )?;
        assert_eq!(result, "74.50");

        let result: String = conn.query_row(
            "SELECT decimal_to_text(decimal_mul(decimal_from_text('10.5'), decimal_from_text('2')))",
            [],
            |row| row.get(0)
        )?;
        assert_eq!(result, "21.0");

        let result: String = conn.query_row(
            "SELECT decimal_to_text(decimal_div(decimal_from_text('100'), decimal_from_text('3')))",
            [],
            |row| row.get(0)
        )?;
        assert!(result.starts_with("33.333333"));

        Ok(())
    }

}