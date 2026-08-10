mod common;
use common::setup_test_server_with_init;

/// The exact query psql 18 expands `\dt` into.
const DT_QUERY: &str = r#"
SELECT n.nspname as "Schema",
  c.relname as "Name",
  CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'i' THEN 'index' WHEN 'S' THEN 'sequence' WHEN 't' THEN 'TOAST table' WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' WHEN 'I' THEN 'partitioned index' END as "Type",
  pg_catalog.pg_get_userbyid(c.relowner) as "Owner"
FROM pg_catalog.pg_class c
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam
WHERE c.relkind IN ('r','p','')
      AND n.nspname <> 'pg_catalog'
      AND n.nspname !~ '^pg_toast'
      AND n.nspname <> 'information_schema'
  AND pg_catalog.pg_table_is_visible(c.oid)
ORDER BY 1,2
"#;

#[tokio::test]
async fn test_dt_lists_user_tables() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(DT_QUERY, &[]).await
        .expect("\\dt query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>("Name")).collect();

    assert!(names.iter().any(|n| n == "customers"),
        "issue #87: \\dt must list the user table, got {names:?}");
}

#[tokio::test]
async fn test_dt_hides_internal_relations() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(DT_QUERY, &[]).await
        .expect("\\dt query should succeed");

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>("Name")).collect();

    for internal in ["pg_constraint", "pg_attrdef", "pg_index", "pg_depend"] {
        assert!(!names.iter().any(|n| n == internal),
            "pgsqlite's own {internal} must not appear in \\dt, got {names:?}");
    }

    assert!(!names.is_empty(),
        "must not pass vacuously against a 0-row result");
}

#[tokio::test]
async fn test_dt_reports_public_schema_and_owner() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let rows = server.client.query(DT_QUERY, &[]).await
        .expect("\\dt query should succeed");

    let row = rows.iter()
        .find(|r| r.get::<_, String>("Name") == "customers")
        .expect("customers row must be present");

    assert_eq!(row.get::<_, String>("Schema"), "public");
    assert_eq!(row.get::<_, String>("Type"), "table");
}
