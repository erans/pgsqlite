mod common;

use crate::common::setup_test_server;

#[tokio::test]
async fn test_unaccent_extension_and_wrapper_function() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .execute("CREATE EXTENSION IF NOT EXISTS unaccent", &[])
        .await
        .unwrap();

    // Basic unaccent(text)
    let v: String = client
        .query_one("SELECT unaccent('Hôtel')", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(v, "Hotel");

    // unaccent(regdictionary, text) overload
    let v: String = client
        .query_one("SELECT unaccent('public.unaccent'::regdictionary, 'Hôtel')", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(v, "Hotel");

    // App-style wrapper DDL should succeed and register a user SQL function
    client
        .simple_query(
            "CREATE OR REPLACE FUNCTION public.unaccent_immutable(input text) RETURNS text \
             LANGUAGE sql IMMUTABLE AS $$ SELECT public.unaccent('public.unaccent'::regdictionary, input) $$;",
        )
        .await
        .unwrap();

    // public. prefix should be accepted on calls
    let v: String = client
        .query_one("SELECT public.unaccent_immutable('Hôtel')", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(v, "Hotel");

    // Parameter binding with Unicode should work
    let v: String = client
        .query_one("SELECT public.unaccent_immutable($1)", &[&"Hôtel"])
        .await
        .unwrap()
        .get(0);
    assert_eq!(v, "Hotel");

    // Ensure it shows up in pg_proc as a user function
    let count: i64 = client
        .query_one("SELECT count(*) FROM pg_proc WHERE proname = 'unaccent_immutable'", &[])
        .await
        .unwrap()
        .get(0);
    assert!(count >= 1);
}
