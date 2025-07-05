use pgsqlite::translator::CastTranslator;

fn main() {
    let query = "SELECT (SUM(price))::text as total_price FROM prices";
    println!("Original: {}", query);
    
    let needs_translation = CastTranslator::needs_translation(query);
    println!("Needs translation: {}", needs_translation);
    
    if needs_translation {
        let translated = CastTranslator::translate_query(query, None);
        println!("Translated: {}", translated);
    }
}