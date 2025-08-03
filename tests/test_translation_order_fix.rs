use pgsqlite::translator::{CastTranslator, ArrayTranslator};

fn main() {
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
    
    println!("=== Testing translation order fix ===");
    println!("Original query: {}", query);
    
    // Simulate what happens in handle_parse now (ArrayTranslator first)
    println!("\n1. ArrayTranslator FIRST:");
    let after_array = match ArrayTranslator::translate_array_operators(query) {
        Ok(translated) => {
            println!("After ArrayTranslator: {}", translated);
            if translated.contains("IN (") {
                println!("✅ ArrayTranslator successfully converted ANY(ARRAY[...]) to IN(...)");
            }
            translated
        }
        Err(e) => {
            println!("❌ ArrayTranslator error: {}", e);
            query.to_string()
        }
    };
    
    // Then CastTranslator
    println!("\n2. CastTranslator SECOND:");
    let final_result = CastTranslator::translate_query(&after_array, None);
    println!("Final result: {}", final_result);
    
    // Check the final result
    if final_result.contains("IN (") && final_result.contains("CAST($1 AS VARCHAR)") {
        println!("\n✅ SUCCESS: Translation order fix works correctly!");
        println!("   - ANY(ARRAY[...]) was converted to IN(...)");
        println!("   - Parameter casts were properly translated");
    } else {
        println!("\n❌ FAILED: Translation still has issues");
    }
    
    // Also check that the IN clause doesn't have casts
    if final_result.contains("IN ($2, $3, $4, $5, $6)") {
        println!("✅ IN clause parameters are clean (no casts)");
    } else if final_result.contains("IN (CAST") {
        println!("❌ IN clause parameters have unwanted casts");
    }
}