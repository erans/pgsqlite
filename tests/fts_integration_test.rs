use pgsqlite::session::DbHandler;
use tempfile::NamedTempFile;
use uuid::Uuid;

#[tokio::test]
async fn test_fts_create_table_integration() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();
    
    let db = DbHandler::new(db_path).unwrap();
    
    // Test CREATE TABLE with tsvector column
    let create_query = "CREATE TABLE articles (
        id SERIAL PRIMARY KEY,
        title TEXT,
        content TEXT,
        search_vector tsvector
    )";
    
    // This should create both the main table and FTS5 shadow table
    let result = db.execute(create_query).await;
    assert!(result.is_ok(), "CREATE TABLE with tsvector should succeed");
    
    // Verify the main table was created
    let check_result = db.query("SELECT name FROM sqlite_master WHERE type='table' AND name='articles'").await.unwrap();
    assert_eq!(check_result.rows.len(), 1);
    assert_eq!(String::from_utf8_lossy(check_result.rows[0][0].as_ref().unwrap()), "articles");
    
    // Note: Full FTS integration would require the executor integration 
    // which is currently commented out due to async issues
}

#[tokio::test]
async fn test_fts_functions_registration() {
    let temp_file = NamedTempFile::new().unwrap();
    let db_path = temp_file.path().to_str().unwrap();
    
    let db = DbHandler::new(db_path).unwrap();
    
    // Test that FTS functions are registered and work
    // Create a session for testing
    let session_id = Uuid::new_v4();
    db.create_session_connection(session_id).await.unwrap();
    
    // Test to_tsvector function
    let result = db.query_with_session(
        "SELECT to_tsvector('english', 'hello world')",
        &session_id
    ).await.unwrap();
    
    let text = String::from_utf8_lossy(result.rows[0][0].as_ref().unwrap());
    // Should return JSON metadata
    assert!(text.contains("fts_ref"));
    assert!(text.contains("english"));
    
    // Test to_tsquery function  
    let result = db.query_with_session(
        "SELECT to_tsquery('english', 'hello & world')",
        &session_id
    ).await.unwrap();
    
    let text = String::from_utf8_lossy(result.rows[0][0].as_ref().unwrap());
    // Should convert to FTS5 syntax
    assert_eq!(text, "hello AND world");
    
    // Test plainto_tsquery function
    let result = db.query_with_session(
        "SELECT plainto_tsquery('english', 'hello world')",
        &session_id
    ).await.unwrap();
    
    let text = String::from_utf8_lossy(result.rows[0][0].as_ref().unwrap());
    assert_eq!(text, "hello AND world");
    
    // Test phraseto_tsquery function
    let result = db.query_with_session(
        "SELECT phraseto_tsquery('english', 'hello world')",
        &session_id
    ).await.unwrap();
    
    let text = String::from_utf8_lossy(result.rows[0][0].as_ref().unwrap());
    assert_eq!(text, "\"hello world\"");
    
    // Test ts_rank function
    let result = db.query_with_session(
        "SELECT ts_rank('dummy', 'dummy')",
        &session_id
    ).await.unwrap();
    
    let text = String::from_utf8_lossy(result.rows[0][0].as_ref().unwrap());
    // ts_rank returns a float, but we get it as text
    assert_eq!(text, "0.1");
    
    // Clean up session
    db.remove_session_connection(&session_id);
}

#[test]
fn test_fts_translator_unit() {
    use pgsqlite::translator::FtsTranslator;
    
    let translator = FtsTranslator::new();
    
    // Test detection of FTS operations
    assert!(FtsTranslator::contains_fts_operations("SELECT * FROM docs WHERE content @@ to_tsquery('search')"));
    assert!(FtsTranslator::contains_fts_operations("INSERT INTO docs (content) VALUES (to_tsvector('hello world'))"));
    assert!(FtsTranslator::contains_fts_operations("CREATE TABLE docs (id INT, content tsvector)"));
    assert!(!FtsTranslator::contains_fts_operations("SELECT * FROM docs WHERE id = 1"));
    
    // Test CREATE TABLE translation
    let create_query = "CREATE TABLE documents (
        id SERIAL PRIMARY KEY,
        title TEXT,
        search_vector tsvector
    )";
    
    let result = translator.translate(create_query, None).unwrap();
    assert_eq!(result.len(), 4); // Main table + FTS table + metadata + schema update
    
    assert!(result[0].contains("CREATE TABLE documents"));
    assert!(result[0].contains("search_vector TEXT"));
    assert!(result[1].contains("CREATE VIRTUAL TABLE __pgsqlite_fts_documents_search_vector"));
    assert!(result[1].contains("USING fts5"));
    
    // Test SELECT translation
    let select_query = "SELECT * FROM documents WHERE search_vector @@ to_tsquery('english', 'test')";
    let result = translator.translate(select_query, None).unwrap();
    assert_eq!(result.len(), 1);
    
    let translated = &result[0];
    assert!(translated.contains("pgsqlite_fts_match"));
}

#[test]
fn test_fts_query_conversion() {
    use pgsqlite::translator::FtsTranslator;
    
    let translator = FtsTranslator::new();
    
    // Test different tsquery conversions
    let test_cases = vec![
        ("SELECT to_tsquery('english', 'cat & dog')", "cat AND dog"),
        ("SELECT to_tsquery('english', 'cat | dog')", "cat OR dog"),
        ("SELECT to_tsquery('english', '!cat')", "NOT cat"),
        ("SELECT to_tsquery('english', 'cat:*')", "cat*"),
        ("SELECT plainto_tsquery('english', 'quick brown fox')", "quick AND brown AND fox"),
        ("SELECT phraseto_tsquery('english', 'quick brown fox')", "\"quick brown fox\""),
    ];
    
    for (input, expected) in test_cases {
        let result = translator.translate(input, None).unwrap();
        assert_eq!(result.len(), 1);
        assert!(result[0].contains(expected), 
               "Query '{}' should contain '{}', but got '{}'", input, expected, result[0]);
    }
}

#[test]
fn test_type_mapping_integration() {
    use pgsqlite::types::{TypeMapper, PgType};
    
    let mapper = TypeMapper::new();
    
    // Test FTS type mappings
    assert_eq!(mapper.pg_to_sqlite("tsvector"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("tsquery"), "TEXT");
    assert_eq!(mapper.pg_to_sqlite("regconfig"), "TEXT");
    
    // Test PgType enum includes FTS types
    assert_eq!(PgType::Tsvector.to_oid(), 3614);
    assert_eq!(PgType::Tsquery.to_oid(), 3615);
    assert_eq!(PgType::Regconfig.to_oid(), 3734);
    
    // Test conversion from OID
    assert_eq!(PgType::from_oid(3614), Some(PgType::Tsvector));
    assert_eq!(PgType::from_oid(3615), Some(PgType::Tsquery));
    assert_eq!(PgType::from_oid(3734), Some(PgType::Regconfig));
}