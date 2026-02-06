mod common;

use crate::common::setup_test_server;

#[tokio::test]
async fn test_unaccent_visible_in_pg_proc_and_information_schema() {
    let server = setup_test_server().await;
    let client = &server.client;

    // pg_proc should list it
    let msgs = client
        .simple_query("SELECT count(*) FROM pg_proc WHERE proname = 'unaccent'")
        .await
        .unwrap();
    let mut count: Option<i64> = None;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
            && let Some(v) = row.get(0usize).or_else(|| row.get("count"))
        {
            count = v.parse::<i64>().ok();
        }
    }
    let count = count.unwrap_or(0);
    assert!(count >= 1);

    // information_schema.routines should list it
    let msgs = client
        .simple_query(
            "SELECT count(*) FROM information_schema.routines WHERE routine_name = 'unaccent'",
        )
        .await
        .unwrap();
    let mut count: Option<i64> = None;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg
            && let Some(v) = row.get(0usize).or_else(|| row.get("count"))
        {
            count = v.parse::<i64>().ok();
        }
    }
    let count = count.unwrap_or(0);
    assert!(count >= 1);
}
