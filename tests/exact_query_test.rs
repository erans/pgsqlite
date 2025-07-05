mod common;
use common::setup_test_server_with_init;

#[tokio::test]
async fn exact_query_test() {
    let _ = env_logger::builder().is_test(true).try_init();
    
    let server = setup_test_server_with_init(|db| {
        Box::pin(async move {
            db.execute("CREATE TABLE test_table1 (id INTEGER PRIMARY KEY, name TEXT)").await?;
            Ok(())
        })
    }).await;

    let client = &server.client;

    // This is the EXACT query from the failing test (without extra spaces after commas)
    let query_compact = "SELECT oid,relname,relnamespace,reltype,relowner,relam,relfilenode,reltablespace,relpages,reltuples,reltoastrelid,relhasindex,relisshared,relpersistence,relkind,relnatts,relchecks,relhasrules,relhastriggers,relhassubclass,relrowsecurity,relforcerowsecurity,relispopulated,relreplident,relispartition,relrewrite,relfrozenxid,relminmxid FROM pg_catalog.pg_class WHERE relkind = 'r'";
    
    // The same query with spaces after commas like in debug_column_count
    let query_spaced = "SELECT oid, relname, relnamespace, reltype, relowner, relam, relfilenode, reltablespace, relpages, reltuples, reltoastrelid, relhasindex, relisshared, relpersistence, relkind, relnatts, relchecks, relhasrules, relhastriggers, relhassubclass, relrowsecurity, relforcerowsecurity, relispopulated, relreplident, relispartition, relrewrite, relfrozenxid, relminmxid FROM pg_catalog.pg_class WHERE relkind = 'r'";
    
    eprintln!("Testing compact query (no spaces)...");
    eprintln!("Query: {}", query_compact);
    
    match client.query(query_compact, &[]).await {
        Ok(rows) => {
            eprintln!("✓ Compact query succeeded: {} rows", rows.len());
        }
        Err(e) => {
            eprintln!("✗ Compact query failed: {:?}", e);
        }
    }
    
    eprintln!("\nTesting spaced query...");
    eprintln!("Query: {}", query_spaced);
    
    match client.query(query_spaced, &[]).await {
        Ok(rows) => {
            eprintln!("✓ Spaced query succeeded: {} rows", rows.len());
            if !rows.is_empty() {
                eprintln!("  First row has {} columns", rows[0].len());
            }
        }
        Err(e) => {
            eprintln!("✗ Spaced query failed: {:?}", e);
        }
    }
    
    server.abort();
}