use pgsqlite::{handle_test_connection_with_pool, session::db_handler::DbHandler};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use tokio_postgres::types::Type;

#[tokio::test]
async fn test_array_binary_protocol_support() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_array_binary.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Start a test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn server
    tokio::spawn(async move {
        while let Ok((stream, client_addr)) = listener.accept().await {
            let db_clone = db_handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_test_connection_with_pool(stream, client_addr, db_clone).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    });

    // Connect client in binary mode
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", addr.port()),
        NoTls,
    ).await.unwrap();

    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create table with array columns
    client.execute("
        CREATE TABLE array_test (
            id INTEGER PRIMARY KEY,
            bool_array BOOLEAN[],
            int2_array SMALLINT[],
            int4_array INTEGER[],
            int8_array BIGINT[],
            float4_array REAL[],
            float8_array DOUBLE PRECISION[],
            text_array TEXT[],
            varchar_array VARCHAR(50)[],
            numeric_array NUMERIC(10,2)[]
        )
    ", &[]).await.unwrap();

    // Insert test data
    client.execute("
        INSERT INTO array_test (id, bool_array, int2_array, int4_array, int8_array, float4_array, float8_array, text_array, varchar_array, numeric_array)
        VALUES (
            1,
            ARRAY[true, false, true],
            ARRAY[1, 2, 3],
            ARRAY[100, 200, 300],
            ARRAY[1000000, 2000000, 3000000],
            ARRAY[1.5, 2.5, 3.5],
            ARRAY[1.111, 2.222, 3.333],
            ARRAY['hello', 'world', 'test'],
            ARRAY['varchar1', 'varchar2', 'varchar3'],
            ARRAY[123.45, 678.90, 999.99]
        )
    ", &[]).await.unwrap();

    // Test binary format queries - prepare statement to force binary protocol
    let stmt = client.prepare_typed("
        SELECT bool_array, int2_array, int4_array, int8_array, float4_array, float8_array, text_array, varchar_array, numeric_array
        FROM array_test WHERE id = $1
    ", &[Type::INT4]).await.unwrap();

    // Execute with binary result format
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // Test boolean array
    let bool_array: Vec<bool> = row.get(0);
    assert_eq!(bool_array, vec![true, false, true]);
    println!("✅ BOOLEAN[] binary encoding works: {:?}", bool_array);

    // Test smallint array
    let int2_array: Vec<i16> = row.get(1);
    assert_eq!(int2_array, vec![1, 2, 3]);
    println!("✅ SMALLINT[] binary encoding works: {:?}", int2_array);

    // Test integer array
    let int4_array: Vec<i32> = row.get(2);
    assert_eq!(int4_array, vec![100, 200, 300]);
    println!("✅ INTEGER[] binary encoding works: {:?}", int4_array);

    // Test bigint array
    let int8_array: Vec<i64> = row.get(3);
    assert_eq!(int8_array, vec![1000000, 2000000, 3000000]);
    println!("✅ BIGINT[] binary encoding works: {:?}", int8_array);

    // Test real array
    let float4_array: Vec<f32> = row.get(4);
    assert_eq!(float4_array, vec![1.5, 2.5, 3.5]);
    println!("✅ REAL[] binary encoding works: {:?}", float4_array);

    // Test double precision array
    let float8_array: Vec<f64> = row.get(5);
    assert_eq!(float8_array, vec![1.111, 2.222, 3.333]);
    println!("✅ DOUBLE PRECISION[] binary encoding works: {:?}", float8_array);

    // Test text array
    let text_array: Vec<String> = row.get(6);
    assert_eq!(text_array, vec!["hello".to_string(), "world".to_string(), "test".to_string()]);
    println!("✅ TEXT[] binary encoding works: {:?}", text_array);

    // Test varchar array
    let varchar_array: Vec<String> = row.get(7);
    assert_eq!(varchar_array, vec!["varchar1".to_string(), "varchar2".to_string(), "varchar3".to_string()]);
    println!("✅ VARCHAR[] binary encoding works: {:?}", varchar_array);

    // Test numeric array - using rust_decimal for precision
    let numeric_array: Vec<rust_decimal::Decimal> = row.get(8);
    use std::str::FromStr;
    let expected = vec![
        rust_decimal::Decimal::from_str("123.45").unwrap(),
        rust_decimal::Decimal::from_str("678.90").unwrap(),
        rust_decimal::Decimal::from_str("999.99").unwrap(),
    ];
    assert_eq!(numeric_array, expected);
    println!("✅ NUMERIC[] binary encoding works: {:?}", numeric_array);

    println!("🎉 All array binary protocol tests passed!");
}

#[tokio::test]
async fn test_array_binary_with_nulls() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_array_nulls.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Start a test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn server
    tokio::spawn(async move {
        while let Ok((stream, client_addr)) = listener.accept().await {
            let db_clone = db_handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_test_connection_with_pool(stream, client_addr, db_clone).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    });

    // Connect client
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", addr.port()),
        NoTls,
    ).await.unwrap();

    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create table with array columns
    client.execute("
        CREATE TABLE array_null_test (
            id INTEGER PRIMARY KEY,
            int_array_with_nulls INTEGER[]
        )
    ", &[]).await.unwrap();

    // Insert array with NULL values (represented as JSON with null)
    client.execute("
        INSERT INTO array_null_test (id, int_array_with_nulls)
        VALUES (1, '[1, null, 3]'::INTEGER[])
    ", &[]).await.unwrap();

    // Test binary format with NULLs
    println!("Preparing SELECT statement for array with nulls...");
    let stmt = client.prepare_typed("
        SELECT int_array_with_nulls FROM array_null_test WHERE id = $1
    ", &[Type::INT4]).await.unwrap();

    // Check what type OID the server reported for the column
    println!("Statement columns: {:?}", stmt.columns());

    let rows = client.query(&stmt, &[&1i32]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // Test integer array with nulls
    let int_array: Vec<Option<i32>> = row.get(0);
    assert_eq!(int_array, vec![Some(1), None, Some(3)]);
    println!("✅ INTEGER[] with NULLs binary encoding works: {:?}", int_array);

    println!("🎉 Array with NULLs binary protocol test passed!");
}

#[tokio::test]
async fn test_empty_arrays_binary() {
    // Initialize logging
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .try_init();

    // Create a temporary database
    let temp_dir = tempfile::tempdir().unwrap();
    let db_path = temp_dir.path().join("test_empty_arrays.db");
    let db_handler = Arc::new(DbHandler::new(db_path.to_str().unwrap()).unwrap());

    // Start a test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn server
    tokio::spawn(async move {
        while let Ok((stream, client_addr)) = listener.accept().await {
            let db_clone = db_handler.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_test_connection_with_pool(stream, client_addr, db_clone).await {
                    eprintln!("Connection error: {}", e);
                }
            });
        }
    });

    // Connect client
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", addr.port()),
        NoTls,
    ).await.unwrap();

    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create table
    client.execute("
        CREATE TABLE empty_array_test (
            id INTEGER PRIMARY KEY,
            empty_int_array INTEGER[],
            empty_text_array TEXT[]
        )
    ", &[]).await.unwrap();

    // Insert empty arrays
    client.execute("
        INSERT INTO empty_array_test (id, empty_int_array, empty_text_array)
        VALUES (1, ARRAY[]::INTEGER[], ARRAY[]::TEXT[])
    ", &[]).await.unwrap();

    // Test binary format
    let stmt = client.prepare_typed("
        SELECT empty_int_array, empty_text_array FROM empty_array_test WHERE id = $1
    ", &[Type::INT4]).await.unwrap();

    let rows = client.query(&stmt, &[&1i32]).await.unwrap();

    assert_eq!(rows.len(), 1);
    let row = &rows[0];

    // Test empty arrays
    let empty_int_array: Vec<i32> = row.get(0);
    assert_eq!(empty_int_array, Vec::<i32>::new());
    println!("✅ Empty INTEGER[] binary encoding works: {:?}", empty_int_array);

    let empty_text_array: Vec<String> = row.get(1);
    assert_eq!(empty_text_array, Vec::<String>::new());
    println!("✅ Empty TEXT[] binary encoding works: {:?}", empty_text_array);

    println!("🎉 Empty arrays binary protocol test passed!");
}