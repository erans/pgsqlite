use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;

#[tokio::test]
async fn test_pg_sequence_basic() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_sequence.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    db_handler.execute("CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT
    )").await.unwrap();

    db_handler.execute("INSERT INTO users (name) VALUES ('Alice')").await.unwrap();
    db_handler.execute("INSERT INTO users (name) VALUES ('Bob')").await.unwrap();

    let result = db_handler.query("SELECT * FROM pg_sequence").await.unwrap();

    assert!(!result.rows.is_empty(), "Should find at least one sequence");

    println!("✅ pg_sequence returns {} sequences", result.rows.len());
}

#[tokio::test]
async fn test_pg_sequence_columns() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_sequence_columns.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    db_handler.execute("CREATE TABLE products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT
    )").await.unwrap();

    db_handler.execute("INSERT INTO products (title) VALUES ('Widget')").await.unwrap();

    let result = db_handler.query("SELECT seqrelid, seqtypid, seqstart, seqincrement FROM pg_sequence").await.unwrap();

    assert_eq!(result.columns.len(), 4, "Should have 4 columns");
    assert_eq!(result.columns, vec!["seqrelid", "seqtypid", "seqstart", "seqincrement"]);
    assert!(!result.rows.is_empty(), "Should have at least one sequence");

    println!("✅ pg_sequence has correct column structure");
}

#[tokio::test]
async fn test_pg_sequence_no_autoincrement() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_sequence_no_auto.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    db_handler.execute("CREATE TABLE simple_table (
        id INTEGER PRIMARY KEY,
        name TEXT
    )").await.unwrap();

    let result = db_handler.query("SELECT * FROM pg_sequence").await.unwrap();

    assert_eq!(result.rows.len(), 0, "Should have no sequences without AUTOINCREMENT");

    println!("✅ pg_sequence correctly returns empty for tables without AUTOINCREMENT");
}

#[tokio::test]
async fn test_pg_sequence_current_value() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_sequence_value.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    db_handler.execute("CREATE TABLE orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        amount REAL
    )").await.unwrap();

    db_handler.execute("INSERT INTO orders (amount) VALUES (100.0)").await.unwrap();
    db_handler.execute("INSERT INTO orders (amount) VALUES (200.0)").await.unwrap();
    db_handler.execute("INSERT INTO orders (amount) VALUES (300.0)").await.unwrap();

    let result = db_handler.query("SELECT seqrelid FROM pg_sequence WHERE seqrelid > 0").await.unwrap();

    assert!(!result.rows.is_empty(), "Should find the orders sequence");

    if let Some(Some(seqrelid_bytes)) = result.rows[0].first() {
        let seqrelid_str = String::from_utf8(seqrelid_bytes.clone()).unwrap();
        let seqrelid: u32 = seqrelid_str.parse().unwrap();
        assert!(seqrelid >= 16384, "seqrelid should be >= 16384 (user OID range)");
    }

    println!("✅ pg_sequence returns valid sequence OIDs");
}

#[tokio::test]
async fn test_pg_sequence_multiple_tables() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_sequence_multi.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    db_handler.execute("CREATE TABLE table1 (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE table2 (id INTEGER PRIMARY KEY AUTOINCREMENT, info TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE table3 (id INTEGER PRIMARY KEY, value TEXT)").await.unwrap();

    db_handler.execute("INSERT INTO table1 (data) VALUES ('test1')").await.unwrap();
    db_handler.execute("INSERT INTO table2 (info) VALUES ('test2')").await.unwrap();

    let result = db_handler.query("SELECT COUNT(*) FROM pg_sequence").await.unwrap();

    assert!(!result.rows.is_empty(), "Should have at least one row with count");
    assert!(result.rows[0][0].is_some(), "Count result should not be None");

    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = String::from_utf8(count_bytes.clone()).unwrap();
    let count: i32 = count_str.parse().unwrap();

    assert_eq!(count, 2, "Should have exactly 2 sequences (table1 and table2)");

    println!("✅ pg_sequence correctly handles multiple tables with AUTOINCREMENT");
}
