use pgsqlite::session::db_handler::DbHandler;
use std::sync::Arc;
use uuid::Uuid;

#[tokio::test]
async fn test_pg_stats_view_exists() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_stats.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create session for catalog queries
    let session_id = Uuid::new_v4();
    db_handler.create_session_connection(session_id).await.unwrap();

    // Create a sample table with data for statistics
    db_handler.execute("CREATE TABLE sample_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50),
        age INTEGER,
        email TEXT,
        status VARCHAR(20) DEFAULT 'active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        is_verified BOOLEAN DEFAULT FALSE
    )").await.unwrap();

    // Insert some sample data
    db_handler.execute("INSERT INTO sample_table (name, age, email, status, is_verified) VALUES
        ('Alice Johnson', 25, 'alice@example.com', 'active', 1),
        ('Bob Smith', 30, 'bob@example.com', 'inactive', 0),
        ('Carol Davis', 28, 'carol@example.com', 'active', 1),
        ('David Wilson', 35, 'david@example.com', 'pending', 0),
        ('Eve Brown', 22, 'eve@example.com', 'active', 1)
    ").await.unwrap();

    // Test basic pg_stats view query
    let result = db_handler.query_with_session("SELECT COUNT(*) FROM pg_stats", &session_id).await.unwrap();
    assert!(!result.rows.is_empty(), "Should get a count result");

    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count_str = String::from_utf8(count_bytes.clone()).unwrap();
    let count: i32 = count_str.parse().unwrap();

    // Should have statistics for all columns in our sample table (7 columns)
    assert!(count >= 7, "Should have statistics for at least 7 columns, got {}", count);
    println!("✅ pg_stats view contains {} column statistics", count);
}

#[tokio::test]
async fn test_pg_stats_column_structure() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_stats_structure.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create a test table
    db_handler.execute("CREATE TABLE test_stats (id INTEGER, name TEXT)").await.unwrap();

    // Test column structure
    let result = db_handler.query("SELECT schemaname, tablename, attname, null_frac, n_distinct FROM pg_stats LIMIT 1").await.unwrap();
    assert_eq!(result.columns.len(), 5, "Should have 5 columns");
    assert_eq!(result.columns, vec!["schemaname", "tablename", "attname", "null_frac", "n_distinct"]);
    println!("✅ pg_stats has correct column structure");
}

#[tokio::test]
async fn test_pg_stats_table_filtering() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_stats_filtering.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create test tables
    db_handler.execute("CREATE TABLE users (id INTEGER, name TEXT, email TEXT)").await.unwrap();
    db_handler.execute("CREATE TABLE products (id INTEGER, title TEXT, price REAL)").await.unwrap();

    // Test filtering by table name (common Django pattern)
    let result = db_handler.query("SELECT tablename, attname FROM pg_stats WHERE tablename = 'users'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find statistics for users table");

    for row in &result.rows {
        let tablename_bytes = row[0].as_ref().unwrap();
        let tablename = String::from_utf8(tablename_bytes.clone()).unwrap();
        assert_eq!(tablename, "users", "Should only return users table statistics");
    }
    println!("✅ Table filtering works correctly");

    // Test filtering by column name (common SQLAlchemy pattern)
    let result = db_handler.query("SELECT tablename, attname FROM pg_stats WHERE attname = 'id'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find statistics for id columns");

    for row in &result.rows {
        let attname_bytes = row[1].as_ref().unwrap();
        let attname = String::from_utf8(attname_bytes.clone()).unwrap();
        assert_eq!(attname, "id", "Should only return id column statistics");
    }
    println!("✅ Column filtering works correctly");
}

#[tokio::test]
async fn test_pg_stats_data_type_inference() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_stats_types.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create table with various data types
    db_handler.execute("CREATE TABLE type_test (
        user_id INTEGER PRIMARY KEY,
        username VARCHAR(50),
        email_address TEXT,
        user_count INTEGER,
        price_amount DECIMAL(10,2),
        is_active BOOLEAN,
        created_date DATE,
        user_status VARCHAR(20)
    )").await.unwrap();

    // Test ID column statistics (should have unique characteristics)
    let result = db_handler.query("SELECT null_frac, n_distinct, correlation FROM pg_stats WHERE tablename = 'type_test' AND attname = 'user_id'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find statistics for user_id");

    let null_frac_bytes = result.rows[0][0].as_ref().unwrap();
    let null_frac = String::from_utf8(null_frac_bytes.clone()).unwrap();
    assert_eq!(null_frac, "0.0", "Primary key should have 0.0 null_frac");

    let n_distinct_bytes = result.rows[0][1].as_ref().unwrap();
    let n_distinct = String::from_utf8(n_distinct_bytes.clone()).unwrap();
    assert_eq!(n_distinct, "-1", "Primary key should have -1 n_distinct (unique)");
    println!("✅ ID column statistics generated correctly");

    // Test email column statistics (should be near-unique)
    let result = db_handler.query("SELECT null_frac, n_distinct FROM pg_stats WHERE tablename = 'type_test' AND attname = 'email_address'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find statistics for email_address");

    let n_distinct_bytes = result.rows[0][1].as_ref().unwrap();
    let n_distinct = String::from_utf8(n_distinct_bytes.clone()).unwrap();
    assert_eq!(n_distinct, "-0.9", "Email should have -0.9 n_distinct (90% unique)");
    println!("✅ Email column statistics generated correctly");

    // Test status column statistics (should be categorical)
    let result = db_handler.query("SELECT null_frac, n_distinct, most_common_vals FROM pg_stats WHERE tablename = 'type_test' AND attname = 'user_status'").await.unwrap();
    assert!(!result.rows.is_empty(), "Should find statistics for user_status");

    let n_distinct_bytes = result.rows[0][1].as_ref().unwrap();
    let n_distinct = String::from_utf8(n_distinct_bytes.clone()).unwrap();
    assert_eq!(n_distinct, "10", "Status should have few distinct values");

    let most_common_vals_bytes = result.rows[0][2].as_ref().unwrap();
    let most_common_vals = String::from_utf8(most_common_vals_bytes.clone()).unwrap();
    assert_eq!(most_common_vals, "{active,inactive,pending}", "Status should have common values");
    println!("✅ Status column statistics generated correctly");
}

#[tokio::test]
async fn test_pg_stats_orm_compatibility() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_stats_orm.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create tables similar to what Django would create
    db_handler.execute("CREATE TABLE auth_user (
        id INTEGER PRIMARY KEY,
        username VARCHAR(150),
        email VARCHAR(254),
        is_active BOOLEAN DEFAULT 1,
        date_joined TIMESTAMP
    )").await.unwrap();

    db_handler.execute("CREATE TABLE blog_post (
        id INTEGER PRIMARY KEY,
        title VARCHAR(200),
        content TEXT,
        author_id INTEGER,
        status VARCHAR(20) DEFAULT 'draft',
        view_count INTEGER DEFAULT 0
    )").await.unwrap();

    // Django ORM statistics query pattern
    let result = db_handler.query(r#"
        SELECT s.schemaname, s.tablename, s.attname, s.n_distinct, s.correlation
        FROM pg_stats s
        WHERE s.schemaname = 'public'
        ORDER BY s.tablename, s.attname
    "#).await.unwrap();
    assert!(!result.rows.is_empty(), "Django pattern should work");
    println!("✅ Django ORM statistics query pattern works");

    // SQLAlchemy optimizer hint pattern
    let result = db_handler.query(r#"
        SELECT tablename, attname, null_frac, most_common_vals, histogram_bounds
        FROM pg_stats
        WHERE tablename IN ('auth_user', 'blog_post')
        AND attname LIKE '%id%'
    "#).await.unwrap();
    assert!(!result.rows.is_empty(), "SQLAlchemy pattern should work");
    println!("✅ SQLAlchemy optimizer hint pattern works");

    // Rails query optimization pattern
    let result = db_handler.query(r#"
        SELECT t.tablename, t.attname, t.n_distinct
        FROM pg_stats t
        WHERE t.tablename = 'auth_user'
        AND t.n_distinct > 100
    "#).await.unwrap();
    // May or may not have results depending on statistics, but should not error
    println!("✅ Rails query optimization pattern works (found {} high-cardinality columns)", result.rows.len());

    // Ecto database introspection pattern
    let result = db_handler.query(r#"
        SELECT DISTINCT tablename
        FROM pg_stats
        WHERE schemaname = 'public'
        ORDER BY tablename
    "#).await.unwrap();
    assert!(result.rows.len() >= 2, "Should find at least 2 tables");
    println!("✅ Ecto database introspection pattern works");
}

#[tokio::test]
async fn test_pg_stats_performance_hints() {
    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_pg_stats_performance.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Create a table with various column types for performance analysis
    db_handler.execute("CREATE TABLE performance_test (
        pk_id INTEGER PRIMARY KEY,
        lookup_code VARCHAR(10),
        description TEXT,
        amount DECIMAL(15,2),
        percentage REAL,
        flag BOOLEAN,
        category VARCHAR(50),
        timestamp_col TIMESTAMP
    )").await.unwrap();

    // Test correlation analysis (important for query planners)
    let result = db_handler.query("SELECT attname, correlation FROM pg_stats WHERE tablename = 'performance_test' AND correlation != '0.0'").await.unwrap();
    println!("✅ Correlation analysis available for {} columns", result.rows.len());

    // Test histogram bounds (important for range queries)
    let result = db_handler.query("SELECT attname, histogram_bounds FROM pg_stats WHERE tablename = 'performance_test' AND histogram_bounds != ''").await.unwrap();
    println!("✅ Histogram bounds available for {} columns", result.rows.len());

    // Test most common values (important for equality queries)
    let result = db_handler.query("SELECT attname, most_common_vals, most_common_freqs FROM pg_stats WHERE tablename = 'performance_test' AND most_common_vals != ''").await.unwrap();
    println!("✅ Most common values available for {} columns", result.rows.len());

    // Test null fraction analysis (important for NULL handling)
    let result = db_handler.query("SELECT COUNT(*) as total_columns, AVG(CAST(null_frac AS REAL)) as avg_null_frac FROM pg_stats WHERE tablename = 'performance_test'").await.unwrap();
    let count_bytes = result.rows[0][0].as_ref().unwrap();
    let count = String::from_utf8(count_bytes.clone()).unwrap();
    println!("✅ Null fraction analysis complete for {} columns", count);
}