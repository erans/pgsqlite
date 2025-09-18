mod common;
use common::{setup_test_server, setup_test_server_with_init};

#[tokio::test]
async fn test_information_schema_comprehensive() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create test objects
            db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)").await?;
            db.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER, title TEXT)").await?;
            db.execute("CREATE VIEW user_posts AS SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id").await?;
            Ok(())
        })
    }).await;
    let client = &server.client;

    println!("=== Testing information_schema.schemata ===");

    // Test schemata
    let schema_rows = client.query("SELECT schema_name FROM information_schema.schemata ORDER BY schema_name", &[]).await.unwrap();
    println!("Found {} schemas", schema_rows.len());

    let schema_names: Vec<String> = schema_rows.iter()
        .map(|row| row.get::<_, String>(0))
        .collect();
    println!("Schema names: {:?}", schema_names);

    // Should have the three default schemas
    assert!(schema_names.contains(&"information_schema".to_string()));
    assert!(schema_names.contains(&"pg_catalog".to_string()));
    assert!(schema_names.contains(&"public".to_string()));

    println!("=== Testing information_schema.tables ===");

    // Test tables with type filtering
    let table_rows = client.query(
        "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
        &[]
    ).await.unwrap();
    println!("Found {} tables/views", table_rows.len());

    let mut found_users_table = false;
    let mut found_posts_table = false;
    let mut found_user_posts_view = false;

    for row in &table_rows {
        let table_name: String = row.get(0);
        let table_type: String = row.get(1);
        println!("  {}: {}", table_name, table_type);

        match table_name.as_str() {
            "users" => {
                assert_eq!(table_type, "BASE TABLE");
                found_users_table = true;
            }
            "posts" => {
                assert_eq!(table_type, "BASE TABLE");
                found_posts_table = true;
            }
            "user_posts" => {
                assert_eq!(table_type, "VIEW");
                found_user_posts_view = true;
            }
            _ => {}
        }
    }

    assert!(found_users_table, "Should find users table");
    assert!(found_posts_table, "Should find posts table");
    assert!(found_user_posts_view, "Should find user_posts view");

    println!("=== Testing information_schema.tables with is_insertable_into ===");

    // Test insertability
    let insertable_rows = client.query(
        "SELECT table_name, table_type, is_insertable_into FROM information_schema.tables WHERE table_name IN ('users', 'user_posts')",
        &[]
    ).await.unwrap();

    for row in &insertable_rows {
        let table_name: String = row.get(0);
        let table_type: String = row.get(1);
        let is_insertable: String = row.get(2);

        println!("  {}: {} (insertable: {})", table_name, table_type, is_insertable);

        if table_name == "users" {
            assert_eq!(table_type, "BASE TABLE");
            assert_eq!(is_insertable, "YES");
        } else if table_name == "user_posts" {
            assert_eq!(table_type, "VIEW");
            assert_eq!(is_insertable, "NO");
        }
    }

    println!("=== Testing wildcard queries ===");

    // Test wildcard queries work
    let all_schemata = client.query("SELECT * FROM information_schema.schemata", &[]).await.unwrap();
    assert!(all_schemata.len() >= 3);
    assert!(all_schemata[0].len() >= 3); // Should have multiple columns

    let all_tables = client.query("SELECT * FROM information_schema.tables LIMIT 1", &[]).await.unwrap();
    assert!(all_tables.len() >= 1);
    assert!(all_tables[0].len() >= 4); // Should have multiple columns

    println!("✓ All information_schema tests passed!");
}