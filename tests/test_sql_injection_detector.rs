use pgsqlite::security::SqlInjectionDetector;

#[test]
fn test_sql_injection_detector_simple() {
    let detector = SqlInjectionDetector::new();

    // Test legitimate queries first with simple query
    let simple_query = "SELECT * FROM users";
    let result = detector.analyze_query(simple_query);
    if let Err(e) = &result {
        println!("Error analyzing simple query: {:?}", e);
    }
    assert!(result.is_ok(), "Simple query should succeed");
}

#[test]
fn test_sql_injection_detector_parameterized() {
    let detector = SqlInjectionDetector::new();

    // Test with parameterized query
    let safe_query = "SELECT * FROM users WHERE id = $1";
    let result = detector.analyze_query(safe_query);
    if let Err(e) = &result {
        println!("Error analyzing safe query: {:?}", e);
    }
    assert!(result.is_ok(), "Parameterized query should succeed");
}

#[test]
fn test_sql_injection_detector_tautology() {
    let detector = SqlInjectionDetector::new();

    // Test tautology injection - should be rejected
    let tautology_query = "SELECT * FROM users WHERE 1=1";
    let result = detector.analyze_query(tautology_query);
    assert!(result.is_err(), "Tautology query should be rejected");

    // Verify it's the right kind of error
    if let Err(e) = result {
        let error_msg = format!("{:?}", e);
        assert!(error_msg.contains("tautology"), "Error should mention tautology");
    }
}

#[test]
fn test_sql_injection_detector_dangerous_function() {
    let detector = SqlInjectionDetector::new();

    // Test dangerous function usage - should be rejected
    let dangerous_query = "SELECT exec('DROP TABLE users')";
    let result = detector.analyze_query(dangerous_query);
    assert!(result.is_err(), "Dangerous function query should be rejected");

    // Verify it's the right kind of error
    if let Err(e) = result {
        let error_msg = format!("{:?}", e);
        assert!(error_msg.contains("Dangerous function") || error_msg.contains("dangerous") || error_msg.contains("exec"), "Error should mention dangerous function");
    }
}

#[test]
fn test_sql_injection_detector_unions_normal() {
    let detector = SqlInjectionDetector::new();

    // Test normal UNION operations (should be allowed - 4 unions = 5 SELECTs, limit is 5)
    let union_query = "SELECT * FROM users UNION SELECT * FROM orders UNION SELECT * FROM products UNION SELECT * FROM inventory UNION SELECT * FROM logs";
    let result = detector.analyze_query(union_query);
    assert!(result.is_ok(), "Normal union query should be allowed");
}

#[test]
fn test_sql_injection_detector_unions_excessive() {
    let detector = SqlInjectionDetector::new();

    // Test excessive UNION operations (should be rejected - 6 unions = 7 SELECs, limit is 5)
    let excessive_union_query = "SELECT * FROM users UNION SELECT * FROM orders UNION SELECT * FROM products UNION SELECT * FROM inventory UNION SELECT * FROM logs UNION SELECT * FROM admin UNION SELECT * FROM config";
    let result = detector.analyze_query(excessive_union_query);
    assert!(result.is_err(), "Excessive union query should be rejected");

    // Verify it's the right kind of error
    if let Err(e) = result {
        let error_msg = format!("{:?}", e);
        assert!(error_msg.contains("UNION") || error_msg.contains("union"), "Error should mention unions");
    }
}

#[test]
fn test_sql_injection_detector_fallback() {
    let detector = SqlInjectionDetector::new();

    // Test invalid SQL that should fall back to pattern matching
    let invalid_sql = "INVALID SQL SYNTAX WITHOUT INJECTION";
    let result = detector.analyze_query(invalid_sql);
    // Should not error, should fall back to pattern analysis
    assert!(result.is_ok());

    // Test invalid SQL with injection pattern - should be rejected
    let invalid_sql_with_injection = "INVALID SQL SYNTAX '; DROP TABLE users;";
    let result = detector.analyze_query(invalid_sql_with_injection);
    // Should error due to injection pattern
    assert!(result.is_err());
}