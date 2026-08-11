mod common;
use common::setup_test_server_with_init;

async fn server_with_two_tables() -> common::TestServer {
    setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY)").await?;
            db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY)").await?;
            Ok(())
        })
    }).await
}

#[tokio::test]
async fn test_regex_match_actually_filters() {
    let _ = env_logger::builder().is_test(true).try_init();
    let server = server_with_two_tables().await;

    let rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname ~ '^cust'", &[]
    ).await.expect("~ query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(names.iter().any(|n| n == "customers"), "~ must match customers, got {names:?}");
    assert!(!names.iter().any(|n| n == "orders"),
        "~ must actually filter -- 'orders' does not match '^cust', got {names:?}");
}

#[tokio::test]
async fn test_regex_not_match_does_not_exclude_everything() {
    let _ = env_logger::builder().is_test(true).try_init();
    let server = server_with_two_tables().await;

    let rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname !~ '^pg_toast'", &[]
    ).await.expect("!~ query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(names.iter().any(|n| n == "customers"),
        "issue #87: !~ must not exclude non-matching rows, got {names:?}");
    assert!(names.iter().any(|n| n == "orders"),
        "!~ must not exclude non-matching rows, got {names:?}");
}

#[tokio::test]
async fn test_regex_not_match_still_excludes_matches() {
    let _ = env_logger::builder().is_test(true).try_init();
    let server = server_with_two_tables().await;

    let rows = server.client.query(
        "SELECT relname FROM pg_catalog.pg_class WHERE relname !~ '^cust'", &[]
    ).await.expect("!~ query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

    assert!(!names.iter().any(|n| n == "customers"),
        "!~ '^cust' must exclude customers, got {names:?}");
    assert!(names.iter().any(|n| n == "orders"),
        "!~ '^cust' must keep orders, got {names:?}");
}
