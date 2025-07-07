mod common;
use common::setup_test_server;

#[tokio::test]
async fn test_extract_simple() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test the extract function is available
    let result = client.query("SELECT extract('year', 1686840645.0)", &[]).await;
    
    match result {
        Ok(rows) => {
            eprintln!("Success! Got {} rows", rows.len());
            if let Some(row) = rows.get(0) {
                let val: f64 = row.get(0);
                eprintln!("Value: {}", val);
            }
        }
        Err(e) => {
            eprintln!("Error executing extract: {}", e);
        }
    }
}

#[tokio::test]
async fn test_date_trunc_simple() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test the date_trunc function is available
    let result = client.query("SELECT date_trunc('hour', 1686840645.0)", &[]).await;
    
    match result {
        Ok(rows) => {
            eprintln!("Success! Got {} rows", rows.len());
            if let Some(row) = rows.get(0) {
                let val: f64 = row.get(0);
                eprintln!("Value: {}", val);
            }
        }
        Err(e) => {
            eprintln!("Error executing date_trunc: {}", e);
        }
    }
}

#[tokio::test]
async fn test_extract_uppercase() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test EXTRACT with uppercase (as it would come from the test)
    let result = client.query("SELECT EXTRACT(YEAR FROM 1686840645.0) as year", &[]).await;
    
    match result {
        Ok(rows) => {
            eprintln!("EXTRACT uppercase success! Got {} rows", rows.len());
            if let Some(row) = rows.get(0) {
                let val: f64 = row.get(0);
                eprintln!("Value: {}", val);
            }
        }
        Err(e) => {
            eprintln!("Error executing EXTRACT uppercase: {}", e);
        }
    }
}

#[tokio::test]
async fn test_multiple_extracts() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test multiple EXTRACT calls in one query (like the failing test)
    let result = client.query(
        "SELECT EXTRACT(YEAR FROM 1686840645.0) as year, 
                EXTRACT(MONTH FROM 1686840645.0) as month",
        &[]
    ).await;
    
    match result {
        Ok(rows) => {
            eprintln!("Multiple EXTRACTs success! Got {} rows", rows.len());
            if let Some(row) = rows.get(0) {
                let year: f64 = row.get(0);
                let month: f64 = row.get(1);
                eprintln!("Year: {}, Month: {}", year, month);
            }
        }
        Err(e) => {
            eprintln!("Error executing multiple EXTRACTs: {}", e);
        }
    }
}

#[tokio::test]
async fn test_extract_query_one() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    // Test EXTRACT with query_one (like the failing test)
    let result = client.query_one(
        "SELECT EXTRACT(YEAR FROM 1686840645.0) as year",
        &[]
    ).await;
    
    match result {
        Ok(row) => {
            eprintln!("query_one EXTRACT success!");
            let year: f64 = row.get(0);
            eprintln!("Year: {}", year);
        }
        Err(e) => {
            eprintln!("Error with query_one EXTRACT: {}", e);
        }
    }
}

#[tokio::test]
async fn test_extract_with_format() {
    let server = setup_test_server().await;
    let client = &server.client;
    
    let test_timestamp = 1686840645.0;
    
    // Test EXTRACT using format! (exactly like the failing test)
    let result = client.query_one(
        &format!("SELECT EXTRACT(YEAR FROM {}) as year", test_timestamp),
        &[]
    ).await;
    
    match result {
        Ok(row) => {
            eprintln!("format! EXTRACT success!");
            let year: f64 = row.get("year");
            eprintln!("Year: {}", year);
        }
        Err(e) => {
            eprintln!("Error with format! EXTRACT: {}", e);
            eprintln!("Query was: SELECT EXTRACT(YEAR FROM {}) as year", test_timestamp);
        }
    }
}