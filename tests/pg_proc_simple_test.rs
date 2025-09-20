use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_pg_proc_view_exists() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_proc_view.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Test direct SQLite query to pg_proc view
    let result = db_handler.query("SELECT COUNT(*) FROM pg_proc").await.unwrap();
    assert!(!result.rows.is_empty(), "Should get a count result");

    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = String::from_utf8(count_bytes.clone()).unwrap();
    let count: i32 = count_str.parse().unwrap();

    assert!(count > 0, "Should have functions in pg_proc, got {}", count);
    println!("✅ pg_proc view contains {} functions", count);

    // Test specific function lookup
    let result = db_handler.query("SELECT proname, prokind FROM pg_proc WHERE proname = 'length' LIMIT 1").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find length function");

    let proname_bytes = result.rows[0][0].as_ref().unwrap();
    let proname = String::from_utf8(proname_bytes.clone()).unwrap();
    assert_eq!(proname, "length");
    println!("✅ Found function: {}", proname);

    println!("🎉 pg_proc SQLite view test passed!");
}