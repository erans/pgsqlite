mod common;

use crate::common::setup_test_server;

#[tokio::test]
async fn test_select_exists_returns_t_f_in_text_mode() {
    let server = setup_test_server().await;
    let client = &server.client;

    let msgs = client
        .simple_query("select exists(select 1)")
        .await
        .unwrap();

    let mut saw_row = false;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("t"));
            saw_row = true;
        }
    }
    assert!(saw_row);

    let msgs = client
        .simple_query("select exists(select 1 where 1=0)")
        .await
        .unwrap();

    let mut saw_row = false;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            assert_eq!(row.get(0), Some("f"));
            saw_row = true;
        }
    }
    assert!(saw_row);
}
