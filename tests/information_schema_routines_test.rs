use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_information_schema_routines_basic() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_routines.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test basic routines query - use simple SELECT instead of COUNT to avoid aggregation issues
    let result = db_handler.query_with_session("SELECT routine_name FROM information_schema.routines", &session_id).await.unwrap();
    assert!(!result.rows.is_empty(), "Should get function results");

    let count = result.rows.len();

    // Should have many built-in functions
    assert!(count >= 40, "Should have at least 40 built-in functions, got {}", count);
    println!("✅ information_schema.routines contains {} functions", count);
}

#[tokio::test]
async fn test_information_schema_routines_column_structure() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_routines_structure.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test column structure
    let result = db_handler.query_with_session(
        "SELECT routine_catalog, routine_schema, routine_name, routine_type, data_type FROM information_schema.routines LIMIT 1",
        &session_id
    ).await.unwrap();

    assert_eq!(result.columns.len(), 5, "Should have 5 columns");
    assert_eq!(result.columns, vec!["routine_catalog", "routine_schema", "routine_name", "routine_type", "data_type"]);
    println!("✅ information_schema.routines has correct column structure");
}

#[tokio::test]
async fn test_information_schema_routines_function_filtering() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_routines_filtering.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test filtering by function name (Django/SQLAlchemy pattern)
    let result = db_handler.query_with_session(
        "SELECT routine_name, routine_type, data_type FROM information_schema.routines WHERE routine_name = 'length'",
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Should find length function");
    assert_eq!(result.rows.len(), 1, "Should find exactly one length function");

    let routine_name_bytes = result.rows[0][0].as_ref().unwrap();
    let routine_name = String::from_utf8(routine_name_bytes.clone()).unwrap();
    assert_eq!(routine_name, "length", "Should return length function");

    let routine_type_bytes = result.rows[0][1].as_ref().unwrap();
    let routine_type = String::from_utf8(routine_type_bytes.clone()).unwrap();
    assert_eq!(routine_type, "FUNCTION", "Should be a FUNCTION");

    let data_type_bytes = result.rows[0][2].as_ref().unwrap();
    let data_type = String::from_utf8(data_type_bytes.clone()).unwrap();
    assert_eq!(data_type, "integer", "length() should return integer");

    println!("✅ Function filtering works correctly");
}

#[tokio::test]
async fn test_information_schema_routines_function_types() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_routines_types.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test string functions
    let result = db_handler.query_with_session(
        "SELECT routine_name, data_type FROM information_schema.routines WHERE routine_name IN ('lower', 'upper', 'trim')",
        &session_id
    ).await.unwrap();

    assert_eq!(result.rows.len(), 3, "Should find 3 string functions");
    for row in &result.rows {
        let data_type_bytes = row[1].as_ref().unwrap();
        let data_type = String::from_utf8(data_type_bytes.clone()).unwrap();
        assert_eq!(data_type, "text", "String functions should return text");
    }
    println!("✅ String function types correct");

    // Test aggregate functions
    let result = db_handler.query_with_session(
        "SELECT routine_name, data_type FROM information_schema.routines WHERE routine_name IN ('count', 'sum', 'avg')",
        &session_id
    ).await.unwrap();

    assert_eq!(result.rows.len(), 3, "Should find 3 aggregate functions");

    // Check count function specifically
    let count_row = result.rows.iter().find(|row| {
        let name_bytes = row[0].as_ref().unwrap();
        let name = String::from_utf8(name_bytes.clone()).unwrap();
        name == "count"
    }).unwrap();

    let count_data_type_bytes = count_row[1].as_ref().unwrap();
    let count_data_type = String::from_utf8(count_data_type_bytes.clone()).unwrap();
    assert_eq!(count_data_type, "bigint", "count() should return bigint");

    println!("✅ Aggregate function types correct");
}

#[tokio::test]
async fn test_information_schema_routines_metadata_attributes() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_routines_metadata.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test comprehensive metadata for a function
    let result = db_handler.query_with_session(
        r#"SELECT
            routine_catalog, routine_schema, routine_name, routine_type,
            external_language, parameter_style, is_deterministic,
            sql_data_access, security_type, routine_body
        FROM information_schema.routines
        WHERE routine_name = 'now'"#,
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Should find now() function");
    let row = &result.rows[0];

    let catalog_bytes = row[0].as_ref().unwrap();
    let catalog = String::from_utf8(catalog_bytes.clone()).unwrap();
    assert_eq!(catalog, "main", "Should be in main catalog");

    let schema_bytes = row[1].as_ref().unwrap();
    let schema = String::from_utf8(schema_bytes.clone()).unwrap();
    assert_eq!(schema, "pg_catalog", "Should be in pg_catalog schema");

    let language_bytes = row[4].as_ref().unwrap();
    let language = String::from_utf8(language_bytes.clone()).unwrap();
    assert_eq!(language, "SQL", "Should be SQL language");

    let parameter_style_bytes = row[5].as_ref().unwrap();
    let parameter_style = String::from_utf8(parameter_style_bytes.clone()).unwrap();
    assert_eq!(parameter_style, "SQL", "Should use SQL parameter style");

    let deterministic_bytes = row[6].as_ref().unwrap();
    let deterministic = String::from_utf8(deterministic_bytes.clone()).unwrap();
    assert_eq!(deterministic, "NO", "now() should not be deterministic");

    let data_access_bytes = row[7].as_ref().unwrap();
    let data_access = String::from_utf8(data_access_bytes.clone()).unwrap();
    assert_eq!(data_access, "CONTAINS_SQL", "Should contain SQL");

    let security_bytes = row[8].as_ref().unwrap();
    let security = String::from_utf8(security_bytes.clone()).unwrap();
    assert_eq!(security, "INVOKER", "Should use invoker security");

    let body_bytes = row[9].as_ref().unwrap();
    let body = String::from_utf8(body_bytes.clone()).unwrap();
    assert_eq!(body, "EXTERNAL", "Should be external function");

    println!("✅ Function metadata attributes correct");
}

#[tokio::test]
async fn test_information_schema_routines_orm_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_routines_orm.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Django ORM introspection pattern
    let result = db_handler.query_with_session(
        r#"SELECT r.routine_name, r.routine_type, r.data_type, r.routine_schema
        FROM information_schema.routines r
        WHERE r.routine_schema = 'pg_catalog'
        AND r.routine_type = 'FUNCTION'
        ORDER BY r.routine_name"#,
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Django pattern should work");
    println!("✅ Django ORM introspection pattern works (found {} functions)", result.rows.len());

    // SQLAlchemy function discovery pattern
    let result = db_handler.query_with_session(
        r#"SELECT routine_name, specific_name, routine_catalog, routine_schema
        FROM information_schema.routines
        WHERE routine_name LIKE '%agg%'
        AND routine_type = 'FUNCTION'"#,
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "SQLAlchemy pattern should work");
    println!("✅ SQLAlchemy function discovery pattern works (found {} aggregate functions)", result.rows.len());

    // Rails ActiveRecord function metadata pattern
    let result = db_handler.query_with_session(
        r#"SELECT f.routine_name, f.data_type, f.external_language, f.is_deterministic
        FROM information_schema.routines f
        WHERE f.routine_name IN ('current_timestamp', 'now', 'version')
        ORDER BY f.routine_name"#,
        &session_id
    ).await.unwrap();

    assert!(result.rows.len() >= 3, "Rails pattern should find system functions");
    println!("✅ Rails ActiveRecord function metadata pattern works");

    // Ecto database introspection pattern
    let result = db_handler.query_with_session(
        r#"SELECT DISTINCT routine_schema, COUNT(*) as function_count
        FROM information_schema.routines
        WHERE routine_type = 'FUNCTION'
        GROUP BY routine_schema
        ORDER BY routine_schema"#,
        &session_id
    ).await.unwrap();

    assert!(!result.rows.is_empty(), "Ecto pattern should work");
    println!("✅ Ecto database introspection pattern works");
}

#[tokio::test]
async fn test_information_schema_routines_comprehensive_coverage() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_routines_coverage.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test coverage of different function categories
    let categories = vec![
        ("String functions", vec!["length", "lower", "upper", "substr", "replace"]),
        ("Math functions", vec!["abs", "round", "ceil", "floor", "sqrt"]),
        ("Aggregate functions", vec!["count", "sum", "avg", "max", "min"]),
        ("Date/time functions", vec!["now", "current_timestamp", "current_date", "current_time"]),
        ("JSON functions", vec!["json_agg", "jsonb_agg", "json_object_agg"]),
        ("Array functions", vec!["array_agg", "unnest", "array_length"]),
        ("System functions", vec!["version", "current_user", "session_user"]),
        ("Full-text search", vec!["to_tsvector", "to_tsquery", "plainto_tsquery"]),
    ];

    for (category_name, function_names) in categories {
        let function_list = function_names.iter()
            .map(|f| format!("'{}'", f))
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "SELECT routine_name FROM information_schema.routines WHERE routine_name IN ({})",
            function_list
        );

        let result = db_handler.query_with_session(&query, &session_id).await.unwrap();
        let count = result.rows.len() as i32;

        assert!(count >= function_names.len() as i32 / 2,
            "{} should have at least half of expected functions, got {}/{}",
            category_name, count, function_names.len());

        println!("✅ {} coverage: {}/{} functions found", category_name, count, function_names.len());
    }
}

#[tokio::test]
async fn test_information_schema_routines_specific_function_details() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_routines_details.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Test specific function details that ORMs care about
    let test_cases = vec![
        ("length", "text", "integer", "SQL"),
        ("count", "bigint", "bigint", "SQL"),
        ("now", "timestamp with time zone", "timestamp with time zone", "SQL"),
        ("json_agg", "json", "json", "SQL"),
        ("array_agg", "anyarray", "anyarray", "SQL"),
        ("uuid_generate_v4", "uuid", "uuid", "SQL"),
    ];

    for (func_name, _expected_param_type, expected_return_type, expected_language) in test_cases {
        let result = db_handler.query_with_session(
            &format!(
                "SELECT routine_name, data_type, external_language, routine_type FROM information_schema.routines WHERE routine_name = '{}'",
                func_name
            ),
            &session_id
        ).await.unwrap();

        assert!(!result.rows.is_empty(), "Should find {} function", func_name);
        let row = &result.rows[0];

        let name_bytes = row[0].as_ref().unwrap();
        let name = String::from_utf8(name_bytes.clone()).unwrap();
        assert_eq!(name, func_name, "Function name should match");

        let return_type_bytes = row[1].as_ref().unwrap();
        let return_type = String::from_utf8(return_type_bytes.clone()).unwrap();
        assert_eq!(return_type, expected_return_type, "{} should return {}", func_name, expected_return_type);

        let language_bytes = row[2].as_ref().unwrap();
        let language = String::from_utf8(language_bytes.clone()).unwrap();
        assert_eq!(language, expected_language, "{} should use {} language", func_name, expected_language);

        let routine_type_bytes = row[3].as_ref().unwrap();
        let routine_type = String::from_utf8(routine_type_bytes.clone()).unwrap();
        assert_eq!(routine_type, "FUNCTION", "{} should be a FUNCTION", func_name);

        println!("✅ {} function details correct", func_name);
    }
}