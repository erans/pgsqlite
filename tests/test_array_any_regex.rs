use pgsqlite::translator::ArrayTranslator;

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
    
    println!("=== Testing ANY(ARRAY[...]) with parameter casts ===");
    println!("Original query: {}", query);
    
    match ArrayTranslator::translate_array_operators(query) {
        Ok(translated) => {
            println!("\nTranslated query: {}", translated);
            
            // Check if the translation worked correctly
            if translated.contains("IN (") {
                println!("\n✅ SUCCESS: ANY(ARRAY[...]) was correctly translated to IN(...)");
            } else if translated.contains("ANY") && translated.contains("ARRAY") {
                println!("\n❌ FAILED: ANY(ARRAY[...]) was not translated");
            }
        }
        Err(e) => {
            println!("\n❌ ERROR: {}", e);
        }
    }
    
    // Test a simpler case
    let simple_query = "SELECT * WHERE relkind = ANY (ARRAY[$1::VARCHAR, $2::VARCHAR])";
    println!("\n\n=== Testing simpler ANY(ARRAY[...]) ===");
    println!("Original query: {}", simple_query);
    
    match ArrayTranslator::translate_array_operators(simple_query) {
        Ok(translated) => {
            println!("Translated query: {}", translated);
        }
        Err(e) => {
            println!("ERROR: {}", e);
        }
    }
}