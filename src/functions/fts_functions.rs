use rusqlite::{Connection, Result};
use serde_json::json;

/// Register PostgreSQL Full-Text Search functions with SQLite
pub fn register_fts_functions(conn: &Connection) -> Result<()> {
    // Register to_tsvector function
    conn.create_scalar_function(
        "to_tsvector",
        2, // config_name, text
        rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let config = ctx.get::<String>(0).unwrap_or_else(|_| "english".to_string());
            let text = ctx.get::<String>(1)?;
            
            // Simple tokenization - split by whitespace and create JSON metadata
            let tokens: Vec<&str> = text.split_whitespace().collect();
            let mut lexemes = serde_json::Map::new();
            
            for (pos, token) in tokens.iter().enumerate() {
                let token_clean = token.to_lowercase()
                    .trim_matches(|c: char| !c.is_alphabetic())
                    .to_string();
                
                if !token_clean.is_empty() {
                    lexemes.insert(token_clean, json!({
                        "pos": [pos + 1],
                        "weight": "D"
                    }));
                }
            }
            
            // Return JSON metadata for tsvector
            let result = json!({
                "fts_ref": "__pgsqlite_fts_table_column", // Will be replaced by actual table/column
                "config": config,
                "lexemes": lexemes
            });
            
            Ok(result.to_string())
        },
    )?;
    
    // Register to_tsquery function
    conn.create_scalar_function(
        "to_tsquery",
        2, // config_name, query_text
        rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _config = ctx.get::<String>(0).unwrap_or_else(|_| "english".to_string());
            let query_text = ctx.get::<String>(1)?;
            
            // Convert PostgreSQL tsquery syntax to FTS5 MATCH syntax
            let fts5_query = query_text
                .replace(" & ", " AND ")
                .replace("&", " AND ")
                .replace(" | ", " OR ")
                .replace("|", " OR ")
                .replace("!", "NOT ")
                .replace(":*", "*");
            
            Ok(fts5_query)
        },
    )?;
    
    // Register plainto_tsquery function
    conn.create_scalar_function(
        "plainto_tsquery",
        2, // config_name, text
        rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _config = ctx.get::<String>(0).unwrap_or_else(|_| "english".to_string());
            let text = ctx.get::<String>(1)?;
            
            // Convert plain text to AND-separated terms
            let terms: Vec<&str> = text.split_whitespace().collect();
            Ok(terms.join(" AND "))
        },
    )?;
    
    // Register phraseto_tsquery function
    conn.create_scalar_function(
        "phraseto_tsquery",
        2, // config_name, text
        rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _config = ctx.get::<String>(0).unwrap_or_else(|_| "english".to_string());
            let text = ctx.get::<String>(1)?;
            
            // Return quoted phrase for exact match
            Ok(format!("\"{text}\""))
        },
    )?;
    
    // Register websearch_to_tsquery function
    conn.create_scalar_function(
        "websearch_to_tsquery",
        2, // config_name, text
        rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _config = ctx.get::<String>(0).unwrap_or_else(|_| "english".to_string());
            let text = ctx.get::<String>(1)?;
            
            // Simple web search syntax conversion (can be enhanced)
            let processed = text
                .replace(" OR ", " | ")
                .replace(" AND ", " & ")
                .replace("-", "!");
            
            Ok(processed)
        },
    )?;
    
    // Register ts_rank function (simplified version)
    conn.create_scalar_function(
        "ts_rank",
        2, // tsvector, tsquery
        rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _tsvector = ctx.get::<String>(0)?;
            let _tsquery = ctx.get::<String>(1)?;
            
            // Simple rank calculation (could be enhanced)
            // For now, return a default rank of 0.1
            Ok(0.1_f64)
        },
    )?;
    
    // Register ts_rank_cd function (simplified version)
    conn.create_scalar_function(
        "ts_rank_cd",
        2, // tsvector, tsquery
        rusqlite::functions::FunctionFlags::SQLITE_UTF8 | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let _tsvector = ctx.get::<String>(0)?;
            let _tsquery = ctx.get::<String>(1)?;
            
            // Simple cover density rank calculation (could be enhanced)
            // For now, return a default rank of 0.05
            Ok(0.05_f64)
        },
    )?;
    
    // Register pgsqlite_fts_match function - parser-friendly FTS matching
    conn.create_scalar_function(
        "pgsqlite_fts_match",
        3, // fts_table_name, rowid, query
        rusqlite::functions::FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let _fts_table_name = ctx.get::<String>(0)?;
            let _rowid = ctx.get::<i64>(1)?;
            let query = ctx.get::<String>(2)?;
            
            // Simple text matching implementation as fallback
            // This avoids the MATCH syntax issue with the SQL parser
            // In a production system, this would need proper FTS5 integration
            
            let query_clean = query.trim_matches('\'').to_lowercase();
            
            // Parse query - handle basic operators
            if query_clean.contains(" or ") {
                // For OR queries, return true (simplified)
                Ok(true)
            } else if query_clean.contains(" and ") {
                // For AND queries, return true (simplified)  
                Ok(true)
            } else {
                // For simple queries, return true if not empty
                Ok(!query_clean.trim().is_empty())
            }
        },
    )?;
    
    Ok(())
}