use rusqlite::{Connection, Result, functions::FunctionFlags};
use tracing::debug;

/// Register all PostgreSQL math functions
pub fn register_math_functions(conn: &Connection) -> Result<()> {
    debug!("Registering math functions");
    
    // Register trunc function (truncate towards zero)
    conn.create_scalar_function(
        "trunc",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.trunc())
        },
    )?;
    
    // Register trunc function with precision
    conn.create_scalar_function(
        "trunc",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            let precision = ctx.get::<i64>(1)?;
            
            if precision == 0 {
                Ok(value.trunc())
            } else {
                let multiplier = 10_f64.powi(precision as i32);
                Ok((value * multiplier).trunc() / multiplier)
            }
        },
    )?;
    
    // Register round function with precision
    conn.create_scalar_function(
        "round",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            let precision = ctx.get::<i64>(1)?;
            
            if precision == 0 {
                Ok(value.round())
            } else {
                let multiplier = 10_f64.powi(precision as i32);
                Ok((value * multiplier).round() / multiplier)
            }
        },
    )?;
    
    // Register ceil function (ceiling)
    conn.create_scalar_function(
        "ceil",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.ceil())
        },
    )?;
    
    // Register ceiling function (alias for ceil)
    conn.create_scalar_function(
        "ceiling",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.ceil())
        },
    )?;
    
    // Register floor function
    conn.create_scalar_function(
        "floor",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.floor())
        },
    )?;
    
    // Register sign function
    conn.create_scalar_function(
        "sign",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            if value > 0.0 {
                Ok(1.0)
            } else if value < 0.0 {
                Ok(-1.0)
            } else {
                Ok(0.0)
            }
        },
    )?;
    
    // Register abs function (absolute value)
    conn.create_scalar_function(
        "abs",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.abs())
        },
    )?;
    
    // Register mod function (modulo)
    conn.create_scalar_function(
        "mod",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let dividend = ctx.get::<f64>(0)?;
            let divisor = ctx.get::<f64>(1)?;
            
            if divisor == 0.0 {
                return Err(rusqlite::Error::UserFunctionError("division by zero".into()));
            }
            
            Ok(dividend % divisor)
        },
    )?;
    
    // Register power function
    conn.create_scalar_function(
        "power",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let base = ctx.get::<f64>(0)?;
            let exponent = ctx.get::<f64>(1)?;
            Ok(base.powf(exponent))
        },
    )?;
    
    // Register pow function (alias for power)
    conn.create_scalar_function(
        "pow",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let base = ctx.get::<f64>(0)?;
            let exponent = ctx.get::<f64>(1)?;
            Ok(base.powf(exponent))
        },
    )?;
    
    // Register sqrt function (square root)
    conn.create_scalar_function(
        "sqrt",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            if value < 0.0 {
                return Err(rusqlite::Error::UserFunctionError("square root of negative number".into()));
            }
            Ok(value.sqrt())
        },
    )?;
    
    // Register exp function (e^x)
    conn.create_scalar_function(
        "exp",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.exp())
        },
    )?;
    
    // Register ln function (natural logarithm)
    conn.create_scalar_function(
        "ln",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            if value <= 0.0 {
                return Err(rusqlite::Error::UserFunctionError("logarithm of non-positive number".into()));
            }
            Ok(value.ln())
        },
    )?;
    
    // Register log function (base 10 logarithm)
    conn.create_scalar_function(
        "log",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            if value <= 0.0 {
                return Err(rusqlite::Error::UserFunctionError("logarithm of non-positive number".into()));
            }
            Ok(value.log10())
        },
    )?;
    
    // Register log function with custom base
    conn.create_scalar_function(
        "log",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let base = ctx.get::<f64>(0)?;
            let value = ctx.get::<f64>(1)?;
            
            if base <= 0.0 || base == 1.0 {
                return Err(rusqlite::Error::UserFunctionError("invalid logarithm base".into()));
            }
            if value <= 0.0 {
                return Err(rusqlite::Error::UserFunctionError("logarithm of non-positive number".into()));
            }
            
            Ok(value.log(base))
        },
    )?;
    
    // Register trigonometric functions
    
    // sin function
    conn.create_scalar_function(
        "sin",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.sin())
        },
    )?;
    
    // cos function
    conn.create_scalar_function(
        "cos",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.cos())
        },
    )?;
    
    // tan function
    conn.create_scalar_function(
        "tan",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.tan())
        },
    )?;
    
    // asin function (inverse sine)
    conn.create_scalar_function(
        "asin",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            if !(-1.0..=1.0).contains(&value) {
                return Err(rusqlite::Error::UserFunctionError("asin domain error".into()));
            }
            Ok(value.asin())
        },
    )?;
    
    // acos function (inverse cosine)
    conn.create_scalar_function(
        "acos",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            if !(-1.0..=1.0).contains(&value) {
                return Err(rusqlite::Error::UserFunctionError("acos domain error".into()));
            }
            Ok(value.acos())
        },
    )?;
    
    // atan function (inverse tangent)
    conn.create_scalar_function(
        "atan",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value = ctx.get::<f64>(0)?;
            Ok(value.atan())
        },
    )?;
    
    // atan2 function (two-argument arctangent)
    conn.create_scalar_function(
        "atan2",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let y = ctx.get::<f64>(0)?;
            let x = ctx.get::<f64>(1)?;
            Ok(y.atan2(x))
        },
    )?;
    
    // Register radians function (degrees to radians)
    conn.create_scalar_function(
        "radians",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let degrees = ctx.get::<f64>(0)?;
            Ok(degrees.to_radians())
        },
    )?;
    
    // Register degrees function (radians to degrees)
    conn.create_scalar_function(
        "degrees",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let radians = ctx.get::<f64>(0)?;
            Ok(radians.to_degrees())
        },
    )?;
    
    // Register pi function
    conn.create_scalar_function(
        "pi",
        0,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |_ctx| {
            Ok(std::f64::consts::PI)
        },
    )?;
    
    // Register random function (0.0 to 1.0)
    conn.create_scalar_function(
        "random",
        0,
        FunctionFlags::SQLITE_UTF8,
        |_ctx| {
            use rand::Rng;
            let mut rng = rand::rng();
            Ok(rng.random::<f64>())
        },
    )?;
    
    debug!("Successfully registered math functions");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    #[test]
    fn test_trunc() {
        let conn = Connection::open_in_memory().unwrap();
        register_math_functions(&conn).unwrap();
        
        let result: f64 = conn.query_row(
            "SELECT trunc(3.7)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 3.0);
        
        let result: f64 = conn.query_row(
            "SELECT trunc(-3.7)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, -3.0);
        
        // Test with precision
        let result: f64 = conn.query_row(
            "SELECT trunc(3.789, 2)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 3.78);
    }
    
    #[test]
    fn test_round_with_precision() {
        let conn = Connection::open_in_memory().unwrap();
        register_math_functions(&conn).unwrap();
        
        let result: f64 = conn.query_row(
            "SELECT round(3.789, 2)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 3.79);
        
        let result: f64 = conn.query_row(
            "SELECT round(3.784, 2)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 3.78);
    }
    
    #[test]
    fn test_ceil_floor() {
        let conn = Connection::open_in_memory().unwrap();
        register_math_functions(&conn).unwrap();
        
        let result: f64 = conn.query_row(
            "SELECT ceil(3.2)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 4.0);
        
        let result: f64 = conn.query_row(
            "SELECT floor(3.7)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 3.0);
    }
    
    #[test]
    fn test_sign() {
        let conn = Connection::open_in_memory().unwrap();
        register_math_functions(&conn).unwrap();
        
        let result: f64 = conn.query_row(
            "SELECT sign(5.5)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 1.0);
        
        let result: f64 = conn.query_row(
            "SELECT sign(-3.2)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, -1.0);
        
        let result: f64 = conn.query_row(
            "SELECT sign(0.0)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 0.0);
    }
    
    #[test]
    fn test_power_sqrt() {
        let conn = Connection::open_in_memory().unwrap();
        register_math_functions(&conn).unwrap();
        
        let result: f64 = conn.query_row(
            "SELECT power(2, 3)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 8.0);
        
        let result: f64 = conn.query_row(
            "SELECT sqrt(16)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 4.0);
    }
    
    #[test]
    fn test_trigonometric() {
        let conn = Connection::open_in_memory().unwrap();
        register_math_functions(&conn).unwrap();
        
        // Test sin(pi/2) = 1
        let result: f64 = conn.query_row(
            "SELECT sin(pi() / 2)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!((result - 1.0).abs() < 1e-10);
        
        // Test cos(0) = 1
        let result: f64 = conn.query_row(
            "SELECT cos(0)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 1.0);
    }
    
    #[test]
    fn test_logarithms() {
        let conn = Connection::open_in_memory().unwrap();
        register_math_functions(&conn).unwrap();
        
        // Test ln(e) = 1
        let result: f64 = conn.query_row(
            "SELECT ln(exp(1))",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!((result - 1.0).abs() < 1e-10);
        
        // Test log base 10
        let result: f64 = conn.query_row(
            "SELECT log(100)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, 2.0);
    }
    
    #[test]
    fn test_angle_conversion() {
        let conn = Connection::open_in_memory().unwrap();
        register_math_functions(&conn).unwrap();
        
        // Test radians(180) = pi
        let result: f64 = conn.query_row(
            "SELECT radians(180)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!((result - std::f64::consts::PI).abs() < 1e-10);
        
        // Test degrees(pi) = 180
        let result: f64 = conn.query_row(
            "SELECT degrees(pi())",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!((result - 180.0).abs() < 1e-10);
    }
}