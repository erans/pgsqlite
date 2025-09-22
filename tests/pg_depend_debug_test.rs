mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn test_pg_depend_debug() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Create table with INTEGER PRIMARY KEY
            println!("CREATING debug_table with INTEGER PRIMARY KEY");
            db.execute("CREATE TABLE debug_table (id INTEGER PRIMARY KEY, name TEXT)").await?;
            println!("FINISHED creating debug_table");
            Ok(())
        })
    }).await;
    let client = &server.client;

    // First, let's check what PRAGMA table_info returns
    println!("=== Testing direct database access ===");

    // Test if our table was created
    let tables = client.query("SELECT name FROM sqlite_master WHERE type='table' AND name='debug_table'", &[]).await.unwrap();
    println!("Tables found: {}", tables.len());
    for table_row in &tables {
        let table_name: &str = table_row.get(0);
        println!("  Table: {}", table_name);
    }

    // Test direct pg_depend access (should go to catalog handler)
    println!("\n=== Testing pg_depend query ===");
    let deps = client.query("SELECT * FROM pg_depend", &[]).await.unwrap();
    println!("Dependencies found: {}", deps.len());

    // Test if catalog interceptor is working
    println!("\n=== Testing catalog routing ===");
    let basic_query = client.query("SELECT classid, objid FROM pg_depend", &[]).await.unwrap();
    println!("Basic pg_depend query returned {} rows", basic_query.len());

    for (i, row) in basic_query.iter().enumerate() {
        println!("Row {}: {} columns", i, row.len());
        if row.len() >= 2 {
            let classid: &str = row.get(0);
            let objid: &str = row.get(1);
            println!("  classid: {}, objid: {}", classid, objid);
        }
    }

    // Check if pg_depend table exists
    println!("\n=== Testing pg_depend table existence ===");
    let table_check = client.query("SELECT name FROM sqlite_master WHERE type='table' AND name='pg_depend'", &[]).await.unwrap();
    println!("pg_depend table exists: {}", table_check.len() > 0);

    if table_check.len() > 0 {
        // Check table structure
        let structure_result = client.query("SELECT sql FROM sqlite_master WHERE name = 'pg_depend'", &[]).await;
        if let Ok(structure) = structure_result {
            println!("pg_depend table schema: {}", structure.len());
            if !structure.is_empty() {
                let sql: &str = structure[0].get(0);
                println!("  CREATE statement: {}", sql);
            }
        }

        // Check table columns using PRAGMA table_info
        let pragma_result = client.query("PRAGMA table_info(pg_depend)", &[]).await;
        if let Ok(columns) = pragma_result {
            println!("pg_depend table columns: {}", columns.len());
            for (i, col) in columns.iter().enumerate() {
                let cid: i32 = col.get(0);
                let name: &str = col.get(1);
                let col_type: &str = col.get(2);
                let notnull: i32 = col.get(3);
                let _dflt_value: Option<&str> = col.get(4);
                let pk: i32 = col.get(5);
                println!("  Column {}: {} {} (cid={}, notnull={}, pk={})", i, name, col_type, cid, notnull, pk);
            }
        }

        // Check if there are any records in pg_depend
        let count = client.query("SELECT COUNT(*) FROM pg_depend", &[]).await.unwrap();
        let count_val: &str = count[0].get(0);
        let count_parsed = count_val.parse::<i64>().unwrap_or(0);
        println!("Records in pg_depend table: {}", count_parsed);

        if count_parsed > 0 {
            let sample = client.query("SELECT * FROM pg_depend LIMIT 3", &[]).await.unwrap();
            println!("Sample records:");
            for (i, row) in sample.iter().enumerate() {
                println!("  Row {}: {} columns", i, row.len());
            }
        }
    }

    // Check migration version
    println!("\n=== Testing migration version ===");
    let version_check = client.query("SELECT value FROM __pgsqlite_metadata WHERE key = 'schema_version'", &[]).await.unwrap();
    if !version_check.is_empty() {
        let version: &str = version_check[0].get(0);
        println!("Current schema version: {}", version);
    }

    // The test should at least not fail - we're just debugging
    assert!(true, "Debug test should always pass");
}