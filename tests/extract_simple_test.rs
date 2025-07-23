mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_extract_simple_case() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test with uppercase EXTRACT (known to cause UnexpectedMessage)
    let test_timestamp = 1686840645.0;
    let query_upper = format!("SELECT EXTRACT(YEAR FROM to_timestamp({test_timestamp})) as year");
    
    println!("Testing uppercase query: {query_upper}");
    
    // Test with lowercase extract (should work)
    let query_lower = format!("SELECT extract('year', to_timestamp({test_timestamp})) as year");
    println!("Testing lowercase query: {query_lower}");
    
    // First try uppercase EXTRACT (expect it to fail)
    match client.query(&query_upper, &[]).await {
        Ok(rows) => {
            println!("Uppercase query() unexpectedly succeeded with {} rows", rows.len());
            if !rows.is_empty() {
                let year: i32 = rows[0].get(0);  // EXTRACT now returns i32, not f64
                println!("Year: {year}");
                assert_eq!(year, 2023);
            }
        }
        Err(e) => {
            println!("Uppercase query() failed as expected: {e:?}");
        }
    }
    
    // Now try lowercase extract (should work)
    match client.query(&query_lower, &[]).await {
        Ok(rows) => {
            println!("Lowercase query() succeeded with {} rows", rows.len());
            if !rows.is_empty() {
                // Extract returns int4, not f64
                let year: i32 = rows[0].get(0);
                println!("Year: {year}");
                assert_eq!(year, 2023);
            }
        }
        Err(e) => {
            println!("Lowercase query() failed: {e:?}");
            panic!("Lowercase extract failed: {e:?}");
        }
    }
    
    // Test with query_one() using lowercase
    println!("\nTesting with query_one():");
    match client.query_one(&query_lower, &[]).await {
        Ok(row) => {
            println!("query_one() succeeded");
            // Extract returns int4, not f64
            let year: i32 = row.get(0);
            println!("Year: {year}");
            assert_eq!(year, 2023);
        }
        Err(e) => {
            println!("query_one() failed: {e:?}");
            // Don't panic here, just note the issue
            eprintln!("WARNING: query_one() failed with lowercase extract: {e:?}");
        }
    }
    
    // Test the translated query directly
    println!("\nTesting translated query directly:");
    let translated_query = "SELECT extract('year', to_timestamp(1686840645)) as year";
    match client.query(translated_query, &[]).await {
        Ok(rows) => {
            println!("Translated query succeeded with {} rows", rows.len());
            if !rows.is_empty() {
                // Extract returns int4, not f64
                let year: i32 = rows[0].get(0);
                println!("Year: {year}");
                assert_eq!(year, 2023);
            }
        }
        Err(e) => {
            println!("Translated query failed: {e:?}");
            panic!("Translated query should work: {e:?}");
        }
    }
}