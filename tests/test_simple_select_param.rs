mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_simple_select_param() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    eprintln!("=== Starting test_simple_select_param ===");
    
    // Test 1: SELECT with explicit cast
    let row = client.query_one("SELECT $1::text", &[&"test"]).await.unwrap();
    println!("Test 1 - Row columns: {:?}", row.columns());
    println!("Test 1 - Column type: {:?}", row.columns()[0].type_());
    
    match row.try_get::<_, String>(0) {
        Ok(v) => {
            println!("Test 1 - Got string: '{}'", v);
            if v == "1952805748" {
                println!("ERROR: SELECT $1::text returned '1952805748' instead of 'test'!");
            } else {
                println!("SELECT $1::text works correctly");
            }
            assert_eq!(v, "test");
        }
        Err(e) => {
            println!("Test 1 - Failed to get as string: {:?}", e);
        }
    }
    
    // Test 2: SELECT without cast
    let row = client.query_one("SELECT $1", &[&"test"]).await.unwrap();
    println!("Row columns: {:?}", row.columns());
    println!("Column type: {:?}", row.columns()[0].type_());
    
    // Try to get as string
    match row.try_get::<_, String>(0) {
        Ok(v) => {
            println!("Got string: '{}'", v);
            if v == "1952805748" {
                panic!("BUG: Got '1952805748' instead of 'test'!");
            }
            assert_eq!(v, "test");
        }
        Err(e) => {
            println!("Failed to get as string: {:?}", e);
            // Try other types
            match row.try_get::<_, i32>(0) {
                Ok(v) => println!("Got i32: {}", v),
                Err(_) => {}
            }
            match row.try_get::<_, Vec<u8>>(0) {
                Ok(v) => println!("Got bytes: {:?}", v),
                Err(_) => {}
            }
        }
    }
    
    server.abort();
}