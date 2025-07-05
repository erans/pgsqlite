mod common;
use common::setup_test_server_with_init;

#[tokio::test] 
async fn catalog_simple_repro() {
    let _ = env_logger::builder().is_test(true).try_init();
    eprintln!("Starting test...");
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test_table1 (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // This is the exact query from the failing test - updated to include all 33 columns
    let all_cols = "oid, relname, relnamespace, reltype, reloftype, relowner, relam, relfilenode, \
                    reltablespace, relpages, reltuples, relallvisible, reltoastrelid, relhasindex, \
                    relisshared, relpersistence, relkind, relnatts, relchecks, \
                    relhasrules, relhastriggers, relhassubclass, relrowsecurity, \
                    relforcerowsecurity, relispopulated, relreplident, relispartition, \
                    relrewrite, relfrozenxid, relminmxid, relacl, reloptions, relpartbound";
    
    eprintln!("Testing explicit columns query...");
    match client.query(
        &format!("SELECT {} FROM pg_catalog.pg_class WHERE relkind = 'r'", all_cols),
        &[]
    ).await {
        Ok(rows) => {
            eprintln!("✓ Explicit columns works! Got {} rows", rows.len());
        }
        Err(e) => {
            eprintln!("✗ Explicit columns failed: {:?}", e);
            panic!("Test failed!");
        }
    }
    
    server.abort();
}