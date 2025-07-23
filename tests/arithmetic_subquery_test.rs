use tokio::net::TcpListener;
use tokio_postgres::NoTls;

#[tokio::test]
#[ignore] // SQLite type affinity issues - subquery results are inferred as INT4 instead of FLOAT8
async fn test_arithmetic_in_subqueries() {
    // Start test server
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // Create test tables
        db_handler.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER, amount REAL)").await.unwrap();
        db_handler.execute("INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 150.0), (3, 2, 200.0), (4, 2, 250.0)").await.unwrap();
        
        db_handler.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT, discount REAL)").await.unwrap();
        db_handler.execute("INSERT INTO customers VALUES (1, 'Alice', 0.1), (2, 'Bob', 0.15)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    // Connect client
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test arithmetic in scalar subquery - use 1.0 to ensure float result
    let rows = client.query(
        "SELECT name, (SELECT SUM(amount * (1.0 - c.discount)) FROM orders o WHERE o.customer_id = c.id) AS total_after_discount 
         FROM customers c 
         ORDER BY id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let alice_total: Option<f64> = rows[0].try_get(1).ok();
    let bob_total: Option<f64> = rows[1].try_get(1).ok();
    let alice_total = alice_total.unwrap();
    let bob_total = bob_total.unwrap();
    // Alice: (100 + 150) * 0.9 = 225
    // Bob: (200 + 250) * 0.85 = 382.5
    assert!((alice_total - 225.0).abs() < 0.01);
    assert!((bob_total - 382.5).abs() < 0.01);
    
    // Test arithmetic in derived table
    let rows = client.query(
        "SELECT customer_id, avg_amount * 1.1 AS projected_avg 
         FROM (SELECT customer_id, AVG(amount) AS avg_amount FROM orders GROUP BY customer_id) t
         ORDER BY customer_id",
        &[]
    ).await.unwrap();
    assert_eq!(rows.len(), 2);
    let cust1_proj: f64 = rows[0].get(1);
    let cust2_proj: f64 = rows[1].get(1);
    // Customer 1: avg(100, 150) * 1.1 = 125 * 1.1 = 137.5
    // Customer 2: avg(200, 250) * 1.1 = 225 * 1.1 = 247.5
    assert!((cust1_proj - 137.5).abs() < 0.01);
    assert!((cust2_proj - 247.5).abs() < 0.01);
    
    server_handle.abort();
}

#[tokio::test]
#[ignore] // CTE queries produce results that are mistakenly treated as execute statements
async fn test_arithmetic_in_ctes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        db_handler.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, quantity REAL, unit_price REAL)").await.unwrap();
        db_handler.execute("INSERT INTO sales VALUES 
            (1, 'Widget', 10.0, 25.0),
            (2, 'Gadget', 5.0, 50.0),
            (3, 'Widget', 15.0, 25.0),
            (4, 'Gadget', 8.0, 50.0)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test arithmetic in CTE
    let rows = client.query(
        "WITH product_totals AS (
            SELECT product, 
                   SUM(quantity) AS total_quantity,
                   SUM(quantity * unit_price) AS total_revenue
            FROM sales
            GROUP BY product
        )
        SELECT product, 
               total_revenue / total_quantity AS avg_price_per_unit,
               total_revenue * 1.2 AS projected_revenue
        FROM product_totals
        ORDER BY product",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    
    // Gadget: revenue = 650, quantity = 13, avg = 50
    let gadget_avg: f64 = rows[0].get(1);
    let gadget_proj: f64 = rows[0].get(2);
    assert!((gadget_avg - 50.0).abs() < 0.01);
    assert!((gadget_proj - 780.0).abs() < 0.01); // 650 * 1.2 = 780
    
    // Widget: revenue = 625, quantity = 25, avg = 25
    let widget_avg: f64 = rows[1].get(1);
    let widget_proj: f64 = rows[1].get(2);
    assert!((widget_avg - 25.0).abs() < 0.01);
    assert!((widget_proj - 750.0).abs() < 0.01); // 625 * 1.2 = 750
    
    server_handle.abort();
}

#[tokio::test]
#[ignore] // Recursive CTE queries produce results that are mistakenly treated as execute statements
async fn test_recursive_cte_arithmetic() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        // No need for base table, using recursive CTE to generate series
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test arithmetic in recursive CTE - factorial calculation
    let rows = client.query(
        "WITH RECURSIVE factorial AS (
            SELECT 1 AS n, 1.0 AS fact
            UNION ALL
            SELECT n + 1, fact * (n + 1)
            FROM factorial
            WHERE n < 5
        )
        SELECT n, fact AS factorial_value
        FROM factorial
        ORDER BY n",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 5);
    
    // Check factorial values
    let fact1: f64 = rows[0].get(1);
    let fact2: f64 = rows[1].get(1);
    let fact3: f64 = rows[2].get(1);
    let fact4: f64 = rows[3].get(1);
    let fact5: f64 = rows[4].get(1);
    
    assert!((fact1 - 1.0).abs() < 0.01);
    assert!((fact2 - 2.0).abs() < 0.01);
    assert!((fact3 - 6.0).abs() < 0.01);
    assert!((fact4 - 24.0).abs() < 0.01);
    assert!((fact5 - 120.0).abs() < 0.01);
    
    server_handle.abort();
}

#[tokio::test]
#[ignore] // SQLite type affinity issues - arithmetic results in JOIN subqueries are inferred as INT4
async fn test_arithmetic_aliases_in_join_subqueries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        db_handler.execute("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, base_price REAL)").await.unwrap();
        db_handler.execute("INSERT INTO products VALUES (1, 'Item A', 100.0), (2, 'Item B', 200.0)").await.unwrap();
        
        db_handler.execute("CREATE TABLE regions (id INTEGER PRIMARY KEY, name TEXT, tax_rate REAL, shipping_cost REAL)").await.unwrap();
        db_handler.execute("INSERT INTO regions VALUES (1, 'North', 0.08, 10.0), (2, 'South', 0.06, 15.0)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test arithmetic aliases in JOIN with subqueries
    let rows = client.query(
        "SELECT p.name, r.name AS region, 
                price_with_tax, 
                price_with_tax + r.shipping_cost AS final_price
         FROM products p
         CROSS JOIN regions r
         JOIN (
            SELECT p2.id AS product_id, r2.id AS region_id,
                   p2.base_price * (1 + r2.tax_rate) AS price_with_tax
            FROM products p2
            CROSS JOIN regions r2
         ) calc ON calc.product_id = p.id AND calc.region_id = r.id
         ORDER BY p.id, r.id",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 4);
    
    // Item A in North: 100 * 1.08 = 108, + 10 shipping = 118
    let a_north_tax: Option<f64> = rows[0].try_get(2).ok();
    let a_north_final: Option<f64> = rows[0].try_get(3).ok();
    let a_north_tax = a_north_tax.unwrap();
    let a_north_final = a_north_final.unwrap();
    assert!((a_north_tax - 108.0).abs() < 0.01);
    assert!((a_north_final - 118.0).abs() < 0.01);
    
    // Item B in South: 200 * 1.06 = 212, + 15 shipping = 227
    let b_south_tax: Option<f64> = rows[3].try_get(2).ok();
    let b_south_final: Option<f64> = rows[3].try_get(3).ok();
    let b_south_tax = b_south_tax.unwrap();
    let b_south_final = b_south_final.unwrap();
    assert!((b_south_tax - 212.0).abs() < 0.01);
    assert!((b_south_final - 227.0).abs() < 0.01);
    
    server_handle.abort();
}

#[tokio::test]
#[ignore] // SQLite type affinity issues - nested subquery arithmetic results are inferred as INT4
async fn test_nested_subquery_arithmetic() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    
    let server_handle = tokio::spawn(async move {
        let db_handler = std::sync::Arc::new(
            pgsqlite::session::DbHandler::new(":memory:").unwrap()
        );
        
        db_handler.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, category TEXT, value REAL)").await.unwrap();
        db_handler.execute("INSERT INTO metrics VALUES 
            (1, 'A', 10.0), (2, 'A', 20.0), (3, 'A', 30.0),
            (4, 'B', 15.0), (5, 'B', 25.0), (6, 'B', 35.0)").await.unwrap();
        
        let (stream, addr) = listener.accept().await.unwrap();
        pgsqlite::handle_test_connection_with_pool(stream, addr, db_handler).await.unwrap();
    });
    
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} dbname=test user=test"),
        NoTls,
    ).await.unwrap();
    
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });
    
    // Test deeply nested subqueries with arithmetic
    let rows = client.query(
        "SELECT category,
                avg_value,
                avg_value * 2 AS double_avg,
                (SELECT (t2.avg_value - t1.avg_value) * 100 / t1.avg_value
                 FROM (SELECT AVG(value) AS avg_value FROM metrics) t2) AS pct_diff_from_overall
         FROM (SELECT category, AVG(value) AS avg_value FROM metrics GROUP BY category) t1
         ORDER BY category",
        &[]
    ).await.unwrap();
    
    assert_eq!(rows.len(), 2);
    
    // Category A: avg = 20, double = 40
    let a_avg: Option<f64> = rows[0].try_get(1).ok();
    let a_double: Option<f64> = rows[0].try_get(2).ok();
    let a_pct_diff: Option<f64> = rows[0].try_get(3).ok();
    let a_avg = a_avg.unwrap();
    let a_double = a_double.unwrap();
    let a_pct_diff = a_pct_diff.unwrap();
    assert!((a_avg - 20.0).abs() < 0.01);
    assert!((a_double - 40.0).abs() < 0.01);
    // Overall avg = 22.5, so (22.5 - 20) * 100 / 20 = 12.5%
    assert!((a_pct_diff - 12.5).abs() < 0.1);
    
    // Category B: avg = 25, double = 50
    let b_avg: Option<f64> = rows[1].try_get(1).ok();
    let b_double: Option<f64> = rows[1].try_get(2).ok();
    let b_pct_diff: Option<f64> = rows[1].try_get(3).ok();
    let b_avg = b_avg.unwrap();
    let b_double = b_double.unwrap();
    let b_pct_diff = b_pct_diff.unwrap();
    assert!((b_avg - 25.0).abs() < 0.01);
    assert!((b_double - 50.0).abs() < 0.01);
    // (22.5 - 25) * 100 / 25 = -10%
    assert!((b_pct_diff - (-10.0)).abs() < 0.1);
    
    server_handle.abort();
}