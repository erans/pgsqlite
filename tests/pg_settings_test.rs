use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_pg_settings_basic() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_settings.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let result = db_handler.query("SELECT name, setting FROM pg_settings").await.unwrap();

    assert!(result.rows.len() >= 10, "Should have many settings, got {}", result.rows.len());
    assert_eq!(result.columns, vec!["name", "setting"]);

    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| {
            row.get(0)
                .and_then(|opt| opt.as_ref())
                .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
        })
        .collect();

    assert!(names.contains(&"server_version".to_string()), "Should contain server_version");
    assert!(names.contains(&"server_encoding".to_string()), "Should contain server_encoding");

    println!("✅ pg_settings returns {} settings", result.rows.len());
}

#[tokio::test]
async fn test_pg_settings_filter_by_name() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_settings2.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let result = db_handler
        .query("SELECT setting FROM pg_settings WHERE name = 'server_version'")
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1, "Should have exactly one row");

    let setting_bytes = result.rows[0][0].as_ref().unwrap();
    let setting = String::from_utf8(setting_bytes.clone()).unwrap();
    assert_eq!(setting, "16.0");

    println!("✅ pg_settings WHERE filter works correctly");
}

#[tokio::test]
async fn test_pg_settings_all_columns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_settings3.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let result = db_handler
        .query("SELECT name, setting, category, short_desc, context, vartype FROM pg_settings WHERE name = 'server_version'")
        .await
        .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.columns.len(), 6);

    let name = String::from_utf8(result.rows[0][0].as_ref().unwrap().clone()).unwrap();
    let setting = String::from_utf8(result.rows[0][1].as_ref().unwrap().clone()).unwrap();
    let category = String::from_utf8(result.rows[0][2].as_ref().unwrap().clone()).unwrap();
    let context = String::from_utf8(result.rows[0][4].as_ref().unwrap().clone()).unwrap();
    let vartype = String::from_utf8(result.rows[0][5].as_ref().unwrap().clone()).unwrap();

    assert_eq!(name, "server_version");
    assert_eq!(setting, "16.0");
    assert_eq!(category, "Preset Options");
    assert_eq!(context, "internal");
    assert_eq!(vartype, "string");

    println!("✅ pg_settings returns correct column values");
}

#[tokio::test]
async fn test_pg_settings_common_settings() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_settings4.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    let result = db_handler.query("SELECT name FROM pg_settings ORDER BY name").await.unwrap();

    let names: Vec<String> = result
        .rows
        .iter()
        .filter_map(|row| {
            row.get(0)
                .and_then(|opt| opt.as_ref())
                .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
        })
        .collect();

    let expected_settings = vec![
        "server_version",
        "server_version_num",
        "server_encoding",
        "client_encoding",
        "DateStyle",
        "TimeZone",
        "max_connections",
        "standard_conforming_strings",
        "integer_datetimes",
    ];

    for expected in expected_settings {
        assert!(
            names.contains(&expected.to_string()),
            "Should contain setting: {}",
            expected
        );
    }

    println!("✅ pg_settings contains all common PostgreSQL settings");
}
