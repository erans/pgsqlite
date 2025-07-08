mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_extract_debug() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    let test_timestamp = 1686840645.0;
    
    // First, let's see what the formatted query looks like
    let query = format!("SELECT EXTRACT(YEAR FROM {}) as year", test_timestamp);
    println!("Query: {}", query);
    
    // Try the query with query() instead of query_one()
    match client.query(&query, &[]).await {
        Ok(rows) => {
            println!("query() succeeded with {} rows", rows.len());
            if let Some(row) = rows.get(0) {
                match row.try_get::<_, f64>(0) {
                    Ok(year) => println!("Year as f64: {}", year),
                    Err(e) => println!("Error getting as f64: {}", e),
                }
            }
        }
        Err(e) => {
            println!("query() failed: {}", e);
        }
    }
    
    // Now try with query_one()
    match client.query_one(&query, &[]).await {
        Ok(row) => {
            println!("query_one() succeeded");
            match row.try_get::<_, f64>(0) {
                Ok(year) => println!("Year as f64: {}", year),
                Err(e) => println!("Error getting as f64: {}", e),
            }
        }
        Err(e) => {
            println!("query_one() failed: {}", e);
        }
    }
    
    // Try with a simpler query
    match client.query_one("SELECT 1", &[]).await {
        Ok(_) => println!("Simple query_one works"),
        Err(e) => println!("Simple query_one failed: {}", e),
    }
    
    // Try extract with a direct value
    match client.query_one("SELECT extract('year', 1686840645.0) as year", &[]).await {
        Ok(row) => {
            println!("Direct extract succeeded");
            match row.try_get::<_, f64>(0) {
                Ok(year) => println!("Year: {}", year),
                Err(e) => println!("Error getting year: {}", e),
            }
        }
        Err(e) => {
            println!("Direct extract failed: {}", e);
        }
    }
}