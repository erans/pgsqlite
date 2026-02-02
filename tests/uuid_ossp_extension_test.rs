use crate::common::setup_test_server;

mod common;

#[tokio::test]
async fn test_uuid_ossp_extension_and_functions() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"", &[])
        .await
        .unwrap();

    // Idempotent
    client
        .execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"", &[])
        .await
        .unwrap();

    let v4: String = client
        .query_one("SELECT uuid_generate_v4()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(&v4[14..15], "4");

    let v1: String = client
        .query_one("SELECT uuid_generate_v1()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(&v1[14..15], "1");

    let v1mc: String = client
        .query_one("SELECT uuid_generate_v1mc()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(&v1mc[14..15], "1");

    let nil: String = client
        .query_one("SELECT uuid_nil()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(nil, "00000000-0000-0000-0000-000000000000");

    let ns_dns: String = client
        .query_one("SELECT uuid_ns_dns()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(ns_dns, "6ba7b810-9dad-11d1-80b4-00c04fd430c8");

    let ns_url: String = client
        .query_one("SELECT uuid_ns_url()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(ns_url, "6ba7b811-9dad-11d1-80b4-00c04fd430c8");

    let ns_oid: String = client
        .query_one("SELECT uuid_ns_oid()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(ns_oid, "6ba7b812-9dad-11d1-80b4-00c04fd430c8");

    let ns_x500: String = client
        .query_one("SELECT uuid_ns_x500()::text", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(ns_x500, "6ba7b814-9dad-11d1-80b4-00c04fd430c8");

    // Deterministic v3/v5 for same inputs
    let v3a: String = client
        .query_one(
            "SELECT uuid_generate_v3(uuid_ns_url(), 'http://www.postgresql.org')::text",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let v3b: String = client
        .query_one(
            "SELECT uuid_generate_v3(uuid_ns_url(), 'http://www.postgresql.org')::text",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(v3a, v3b);
    assert_eq!(&v3a[14..15], "3");

    let v5a: String = client
        .query_one(
            "SELECT uuid_generate_v5(uuid_ns_url(), 'http://www.postgresql.org')::text",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let v5b: String = client
        .query_one(
            "SELECT uuid_generate_v5(uuid_ns_url(), 'http://www.postgresql.org')::text",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(v5a, v5b);
    assert_eq!(&v5a[14..15], "5");
}
