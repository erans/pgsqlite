#[cfg(test)]
mod test_fts_translator {
    use pgsqlite::translator::FtsTranslator;
    
    #[test]
    fn test_create_table_with_tsvector() {
        let translator = FtsTranslator::new();
        
        let query = "CREATE TABLE documents (
            id SERIAL PRIMARY KEY,
            title TEXT,
            content TEXT,
            search_vector tsvector
        )";
        
        let result = translator.translate(query, None).unwrap();
        
        // Should produce multiple queries:
        // 1. CREATE TABLE with tsvector as TEXT
        // 2. CREATE VIRTUAL TABLE for FTS5
        // 3. INSERT INTO __pgsqlite_fts_metadata
        // 4. UPDATE __pgsqlite_schema
        assert_eq!(result.len(), 4);
        
        // Check main table creation
        assert!(result[0].contains("CREATE TABLE documents"));
        assert!(result[0].contains("search_vector TEXT"));
        
        // Check FTS5 table creation
        assert!(result[1].contains("CREATE VIRTUAL TABLE __pgsqlite_fts_documents_search_vector"));
        assert!(result[1].contains("USING fts5"));
        
        // Check metadata insertion
        assert!(result[2].contains("INSERT INTO __pgsqlite_fts_metadata"));
        assert!(result[2].contains("'documents'"));
        assert!(result[2].contains("'search_vector'"));
        
        // Check schema update
        assert!(result[3].contains("UPDATE __pgsqlite_schema"));
        assert!(result[3].contains("fts_table_name = '__pgsqlite_fts_documents_search_vector'"));
    }
    
    #[test]
    fn test_select_with_fts_match() {
        let translator = FtsTranslator::new();
        
        let query = "SELECT * FROM documents WHERE search_vector @@ to_tsquery('english', 'quick & fox')";
        
        let result = translator.translate(query, None).unwrap();
        assert_eq!(result.len(), 1);
        
        let translated = &result[0];
        
        // Should translate @@ to pgsqlite_fts_match function call
        assert!(translated.contains("pgsqlite_fts_match"));
        assert!(translated.contains("'quick AND fox'"));
    }
    
    #[test]
    fn test_tsquery_functions() {
        let translator = FtsTranslator::new();
        
        // Test to_tsquery
        let query = "SELECT to_tsquery('english', 'cat & dog')";
        let result = translator.translate(query, None).unwrap();
        assert!(result[0].contains("'cat AND dog'"));
        
        // Test plainto_tsquery
        let query = "SELECT plainto_tsquery('english', 'quick brown fox')";
        let result = translator.translate(query, None).unwrap();
        assert!(result[0].contains("'quick AND brown AND fox'"));
        
        // Test phraseto_tsquery
        let query = "SELECT phraseto_tsquery('english', 'quick brown fox')";
        let result = translator.translate(query, None).unwrap();
        assert!(result[0].contains("\"quick brown fox\""));
    }
}