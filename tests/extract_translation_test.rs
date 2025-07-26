use pgsqlite::translator::DateTimeTranslator;

#[test]
fn test_extract_translation() {
    let query1 = "SELECT EXTRACT(YEAR FROM 1686840645) as year";
    let query2 = "SELECT EXTRACT(YEAR FROM 1686840645.0) as year";
    
    println!("Query 1: {query1}");
    println!("Needs translation: {}", DateTimeTranslator::needs_translation(query1));
    println!("Translated: {}", DateTimeTranslator::translate_query(query1));
    
    println!("\nQuery 2: {query2}");
    println!("Needs translation: {}", DateTimeTranslator::needs_translation(query2));
    println!("Translated: {}", DateTimeTranslator::translate_query(query2));
}