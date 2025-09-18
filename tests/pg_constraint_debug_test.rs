mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn debug_pg_constraint() {
    let _ = env_logger::builder().is_test(true).try_init();

    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            // Enable foreign keys in SQLite
            db.execute("PRAGMA foreign_keys = ON").await?;

            // Create simple tables
            db.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)").await?;
            db.execute("CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, FOREIGN KEY(parent_id) REFERENCES parent(id))").await?;

            // Debug: Check if foreign keys are detected by SQLite
            let fk_result = db.query("PRAGMA foreign_key_list(child)").await?;
            println!("SQLite foreign key list for 'child': {} rows", fk_result.rows.len());
            for (i, row) in fk_result.rows.iter().enumerate() {
                println!("  FK row {}: {} columns", i, row.len());
                for (j, col) in row.iter().enumerate() {
                    if let Some(val) = col {
                        println!("    Column {}: {}", j, String::from_utf8_lossy(val));
                    } else {
                        println!("    Column {}: NULL", j);
                    }
                }
            }

            Ok(())
        })
    }).await;
    let client = &server.client;

    // Debug: Check what constraints we actually get
    let rows = client.query("SELECT * FROM pg_constraint", &[]).await.unwrap();

    println!("Total constraints found: {}", rows.len());
    println!("Columns per row: {}", if !rows.is_empty() { rows[0].len() } else { 0 });

    for (i, row) in rows.iter().enumerate() {
        println!("Constraint {}: {} columns", i, row.len());
        for j in 0..row.len().min(10) { // Only show first 10 columns
            if let Ok(val) = row.try_get::<_, Option<String>>(j) {
                println!("  Column {}: {:?}", j, val);
            } else if let Ok(val) = row.try_get::<_, String>(j) {
                println!("  Column {} (non-null): {}", j, val);
            } else {
                println!("  Column {}: <type error>", j);
            }
        }
        println!();
    }

    // Test specific fields with their actual types
    if !rows.is_empty() {
        let row = &rows[0];
        println!("Testing first constraint row:");

        // Test each column type
        for i in 0..row.len() {
            print!("Column {}: ", i);
            if let Ok(val) = row.try_get::<_, String>(i) {
                println!("String: {}", val);
            } else if let Ok(val) = row.try_get::<_, i32>(i) {
                println!("i32: {}", val);
            } else if let Ok(val) = row.try_get::<_, bool>(i) {
                println!("bool: {}", val);
            } else if let Ok(val) = row.try_get::<_, Option<String>>(i) {
                println!("Option<String>: {:?}", val);
            } else {
                println!("Unknown type");
            }
        }
    }
}