use pgsqlite::translator::{CastTranslator, ArrayTranslator};

fn main() {
    env_logger::init();
    
    // Test the exact query that's failing
    let query = r#"
        SELECT pg_catalog.pg_class.relname 
        FROM pg_catalog.pg_class 
        JOIN pg_catalog.pg_namespace ON pg_catalog.pg_namespace.oid = pg_catalog.pg_class.relnamespace 
        WHERE pg_catalog.pg_class.relname = $1::VARCHAR 
        AND pg_catalog.pg_class.relkind = ANY (ARRAY[$2::VARCHAR, $3::VARCHAR, $4::VARCHAR, $5::VARCHAR, $6::VARCHAR]) 
        AND pg_catalog.pg_table_is_visible(pg_catalog.pg_class.oid) 
        AND pg_catalog.pg_namespace.nspname != $7::VARCHAR
    "#;
    
    println!("=== Testing translation order ===");
    println!("Original query: {}", query);
    
    // Test what CastTranslator does
    println!("\n1. CastTranslator:");
    let after_cast = CastTranslator::translate_query(query, None);
    println!("After CastTranslator: {}", after_cast);
    
    // Check if ARRAY content was modified
    if after_cast.contains("ARRAY[$2::VARCHAR") {
        println!("✅ CastTranslator correctly skipped casts inside ARRAY");
    } else if after_cast.contains("ARRAY[") {
        println!("❌ CastTranslator modified ARRAY content");
        // Find what it became
        if let Some(start) = after_cast.find("ARRAY[") {
            let end = after_cast[start..].find("]").unwrap_or(50);
            println!("   ARRAY content: {}", &after_cast[start..start+end+1]);
        }
    }
    
    // Test what ArrayTranslator does with original
    println!("\n2. ArrayTranslator on original:");
    match ArrayTranslator::translate_array_operators(query) {
        Ok(translated) => {
            println!("After ArrayTranslator: {}", translated);
            if translated.contains("IN (") {
                println!("✅ ArrayTranslator successfully converted ANY(ARRAY[...]) to IN(...)");
            }
        }
        Err(e) => println!("❌ ArrayTranslator error: {}", e),
    }
    
    // Test what ArrayTranslator does after CastTranslator
    println!("\n3. ArrayTranslator on CastTranslator output:");
    match ArrayTranslator::translate_array_operators(&after_cast) {
        Ok(translated) => {
            println!("After ArrayTranslator: {}", translated);
            if translated.contains("IN (") {
                println!("✅ ArrayTranslator successfully converted ANY(ARRAY[...]) to IN(...)");
            } else {
                println!("❌ ArrayTranslator failed to convert ANY(ARRAY[...])");
            }
        }
        Err(e) => println!("❌ ArrayTranslator error: {}", e),
    }
}