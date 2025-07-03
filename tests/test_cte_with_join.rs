use pgsqlite::query::{QueryTypeDetector, QueryType};

#[test]
fn test_cte_query_detection() {
    // Test basic WITH queries
    assert_eq!(QueryTypeDetector::detect_query_type("WITH cte AS (SELECT 1) SELECT * FROM cte"), QueryType::Select);
    assert_eq!(QueryTypeDetector::detect_query_type("with cte as (select 1) select * from cte"), QueryType::Select);
    assert_eq!(QueryTypeDetector::detect_query_type("With Cte As (Select 1) Select * From Cte"), QueryType::Select);
    assert_eq!(QueryTypeDetector::detect_query_type("WiTh cte AS (SELECT 1) SELECT * FROM cte"), QueryType::Select);
    
    // Test WITH RECURSIVE
    assert_eq!(QueryTypeDetector::detect_query_type("WITH RECURSIVE t(n) AS (SELECT 1) SELECT * FROM t"), QueryType::Select);
    
    // Test complex CTE with JOIN
    let complex_cte = "WITH user_stats AS (
        SELECT user_id, COUNT(*) as orders, SUM(total_price) as spent
        FROM orders
        GROUP BY user_id
    )
    SELECT u.username, COALESCE(s.orders, 0) as order_count
    FROM users u
    LEFT JOIN user_stats s ON u.id = s.user_id";
    assert_eq!(QueryTypeDetector::detect_query_type(complex_cte), QueryType::Select);
    
    // Test multiple CTEs
    let multiple_ctes = "WITH 
        cte1 AS (SELECT 1 as n),
        cte2 AS (SELECT 2 as n)
    SELECT * FROM cte1 UNION SELECT * FROM cte2";
    assert_eq!(QueryTypeDetector::detect_query_type(multiple_ctes), QueryType::Select);
    
    // Test that non-WITH queries are not affected
    assert_eq!(QueryTypeDetector::detect_query_type("SELECT * FROM users"), QueryType::Select);
    assert_eq!(QueryTypeDetector::detect_query_type("INSERT INTO users VALUES (1)"), QueryType::Insert);
    assert_eq!(QueryTypeDetector::detect_query_type("UPDATE users SET name = 'test'"), QueryType::Update);
    assert_eq!(QueryTypeDetector::detect_query_type("DELETE FROM users"), QueryType::Delete);
}