use rusqlite::{Connection, Result, functions::FunctionFlags};
use regex::Regex;
use tracing::{debug, trace};

/// Register PostgreSQL-compatible regular expression functions
pub fn register_regex_functions(conn: &Connection) -> Result<()> {
    debug!("Registering regex functions");
    
    // Register case-sensitive REGEXP function
    conn.create_scalar_function(
        "regexp",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let pattern: String = ctx.get(0)?;
            let text: String = ctx.get(1)?;
            
            trace!("regexp('{}', '{}')", pattern, text);
            
            match Regex::new(&pattern) {
                Ok(re) => Ok(re.is_match(&text)),
                Err(e) => {
                    debug!("Invalid regex pattern '{}': {}", pattern, e);
                    // PostgreSQL returns NULL for invalid patterns
                    Ok(false)
                }
            }
        },
    )?;
    
    // Register case-insensitive REGEXPI function
    conn.create_scalar_function(
        "regexpi",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let pattern: String = ctx.get(0)?;
            let text: String = ctx.get(1)?;
            
            trace!("regexpi('{}', '{}')", pattern, text);
            
            // Add (?i) flag for case-insensitive matching
            let case_insensitive_pattern = format!("(?i){pattern}");
            
            match Regex::new(&case_insensitive_pattern) {
                Ok(re) => Ok(re.is_match(&text)),
                Err(e) => {
                    debug!("Invalid regex pattern '{}': {}", case_insensitive_pattern, e);
                    // PostgreSQL returns NULL for invalid patterns
                    Ok(false)
                }
            }
        },
    )?;
    
    // Also register the standard SQLite REGEXP operator handler
    // This enables "text REGEXP pattern" syntax in SQLite
    conn.create_scalar_function(
        "regexp",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Note: SQLite calls this with (pattern, text) order
            let pattern: String = ctx.get(0)?;
            let text: String = ctx.get(1)?;
            
            match Regex::new(&pattern) {
                Ok(re) => Ok(re.is_match(&text)),
                Err(_) => Ok(false),
            }
        },
    )?;
    
    debug!("Regex functions registered successfully");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    #[test]
    fn test_regexp_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_regex_functions(&conn).unwrap();
        
        // Test basic match
        let result: bool = conn
            .query_row("SELECT regexp('^test', 'testing')", [], |row| row.get(0))
            .unwrap();
        assert!(result);
        
        // Test no match
        let result: bool = conn
            .query_row("SELECT regexp('^test', 'nottest')", [], |row| row.get(0))
            .unwrap();
        assert!(!result);
        
        // Test email pattern
        let result: bool = conn
            .query_row("SELECT regexp('@gmail\\.com$', 'user@gmail.com')", [], |row| row.get(0))
            .unwrap();
        assert!(result);
    }
    
    #[test]
    fn test_regexpi_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_regex_functions(&conn).unwrap();
        
        // Test case-insensitive match
        let result: bool = conn
            .query_row("SELECT regexpi('TEST', 'testing')", [], |row| row.get(0))
            .unwrap();
        assert!(result);
        
        // Test with mixed case
        let result: bool = conn
            .query_row("SELECT regexpi('TeSt', 'TEST')", [], |row| row.get(0))
            .unwrap();
        assert!(result);
    }
    
    #[test]
    fn test_invalid_regex() {
        let conn = Connection::open_in_memory().unwrap();
        register_regex_functions(&conn).unwrap();
        
        // Test invalid regex pattern - should return false
        let result: bool = conn
            .query_row("SELECT regexp('[invalid', 'test')", [], |row| row.get(0))
            .unwrap();
        assert!(!result);
    }
}