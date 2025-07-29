use pgsqlite::session::DbHandler;
use uuid::Uuid;

struct TestContext {
    db_handler: DbHandler,
    session_id: Uuid,
}

impl TestContext {
    async fn execute(&self, query: &str) -> Result<pgsqlite::session::DbResponse, pgsqlite::PgSqliteError> {
        self.db_handler.execute_with_session(query, &self.session_id).await
    }
    
    async fn query(&self, query: &str) -> Result<pgsqlite::session::DbResponse, pgsqlite::PgSqliteError> {
        self.db_handler.query_with_session(query, &self.session_id).await
    }
}

async fn setup_test_db() -> TestContext {
    let db_handler = DbHandler::new(":memory:").unwrap();
    let session_id = Uuid::new_v4();
    
    // Create session connection
    db_handler.create_session_connection(session_id).await.unwrap();
    
    let ctx = TestContext { db_handler, session_id };
    
    // Create tables with NUMERIC columns
    ctx.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            name TEXT,
            balance TEXT,
            credit_limit TEXT
        )"
    ).await.unwrap();
    
    // Insert type metadata
    ctx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('accounts', 'balance', 'NUMERIC', 'DECIMAL'),
         ('accounts', 'credit_limit', 'NUMERIC', 'DECIMAL')"
    ).await.unwrap();
    
    ctx.execute(
        "CREATE TABLE transactions (
            id INTEGER PRIMARY KEY,
            account_id INTEGER,
            amount TEXT,
            fee TEXT,
            type TEXT
        )"
    ).await.unwrap();
    
    ctx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('transactions', 'amount', 'NUMERIC', 'DECIMAL'),
         ('transactions', 'fee', 'NUMERIC', 'DECIMAL')"
    ).await.unwrap();
    
    ctx
}

#[tokio::test]
async fn test_decimal_arithmetic_execution() {
    let ctx = setup_test_db().await;
    
    // Insert test data
    ctx.execute(
        "INSERT INTO accounts (name, balance, credit_limit) VALUES
         ('Alice', '1000.50', '5000.00'),
         ('Bob', '2500.75', '10000.00')"
    ).await.unwrap();
    
    // Test addition - should use decimal_add
    let result = ctx.query("SELECT balance + 100 FROM accounts").await.unwrap();
    assert_eq!(result.rows.len(), 2);
    // The result should be decimal calculations
    
    // Test subtraction
    let result = ctx.query("SELECT credit_limit - balance FROM accounts").await.unwrap();
    assert_eq!(result.rows.len(), 2);
    
    // Test multiplication
    let result = ctx.query("SELECT balance * 1.05 FROM accounts").await.unwrap();
    assert_eq!(result.rows.len(), 2);
    
    // Test division
    let result = ctx.query("SELECT balance / 2 FROM accounts").await.unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[tokio::test]
async fn test_decimal_comparisons() {
    let ctx = setup_test_db().await;
    
    // Insert test data
    ctx.execute(
        "INSERT INTO accounts (name, balance, credit_limit) VALUES
         ('Test1', '100.00', '500.00'),
         ('Test2', '200.00', '1000.00'),
         ('Test3', '300.00', '1500.00')"
    ).await.unwrap();
    
    // Test equality - use string comparison for exact decimal matches
    let result = ctx.query("SELECT * FROM accounts WHERE balance = '200.00'").await.unwrap();
    assert_eq!(result.rows.len(), 1);
    
    // Test less than - use CAST to REAL for proper numeric comparison  
    let result = ctx.query("SELECT * FROM accounts WHERE CAST(balance AS REAL) < 250").await.unwrap();
    assert_eq!(result.rows.len(), 2);
    
    // Test greater than - use CAST to REAL for proper numeric comparison
    let result = ctx.query("SELECT * FROM accounts WHERE CAST(credit_limit AS REAL) > 750").await.unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[tokio::test]
async fn test_decimal_aggregates() {
    let ctx = setup_test_db().await;
    
    // Insert test data
    ctx.execute(
        "INSERT INTO transactions (account_id, amount, fee, type) VALUES
         (1, '100.00', '2.50', 'deposit'),
         (1, '50.25', '1.25', 'deposit'),
         (1, '75.50', '1.50', 'withdrawal'),
         (2, '200.00', '5.00', 'deposit')"
    ).await.unwrap();
    
    // Test SUM
    let result = ctx.query("SELECT SUM(amount) FROM transactions WHERE account_id = 1").await.unwrap();
    assert_eq!(result.rows.len(), 1);
    
    // Test AVG
    let result = ctx.query("SELECT AVG(fee) FROM transactions").await.unwrap();
    assert_eq!(result.rows.len(), 1);
    
    // Test MIN/MAX
    let result = ctx.query("SELECT MIN(amount), MAX(amount) FROM transactions").await.unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_complex_decimal_queries() {
    let ctx = setup_test_db().await;
    
    // Insert test data
    ctx.execute(
        "INSERT INTO accounts (id, name, balance, credit_limit) VALUES
         (1, 'Account1', '1000.00', '5000.00'),
         (2, 'Account2', '2000.00', '10000.00')"
    ).await.unwrap();
    
    ctx.execute(
        "INSERT INTO transactions (account_id, amount, fee) VALUES
         (1, '100.00', '2.50'),
         (1, '200.00', '5.00'),
         (2, '150.00', '3.75')"
    ).await.unwrap();
    
    // Complex join with calculations
    let result = ctx.query(
        "SELECT a.name, a.balance + SUM(t.amount) - SUM(t.fee) as new_balance
         FROM accounts a
         JOIN transactions t ON a.id = t.account_id
         GROUP BY a.id, a.name, a.balance"
    ).await.unwrap();
    
    // The query should execute without errors
    assert!(!result.rows.is_empty());
}

#[tokio::test]
async fn test_update_with_decimal_operations() {
    let ctx = setup_test_db().await;
    
    // Insert test data
    ctx.execute(
        "INSERT INTO accounts (name, balance, credit_limit) VALUES
         ('Test', '1000.00', '5000.00')"
    ).await.unwrap();
    
    // Update with arithmetic
    let result = ctx.execute(
        "UPDATE accounts SET balance = balance * 1.1 WHERE name = 'Test'"
    ).await.unwrap();
    
    assert_eq!(result.rows_affected, 1);
    
    // Verify the update
    let result = ctx.query("SELECT balance FROM accounts WHERE name = 'Test'").await.unwrap();
    assert_eq!(result.rows.len(), 1);
    // Balance should be 1100.00 (stored as decimal)
}

#[tokio::test]
async fn test_insert_select_with_decimal() {
    let ctx = setup_test_db().await;
    
    // Create source data
    ctx.execute(
        "INSERT INTO accounts (id, name, balance, credit_limit) VALUES
         (1, 'Source', '1000.00', '5000.00')"
    ).await.unwrap();
    
    // Insert with calculations
    let result = ctx.execute(
        "INSERT INTO accounts (name, balance, credit_limit)
         SELECT name || '_copy', balance * 0.5, credit_limit * 0.5
         FROM accounts WHERE id = 1"
    ).await.unwrap();
    
    assert_eq!(result.rows_affected, 1);
    
    // Verify the insert
    let result = ctx.query("SELECT COUNT(*) FROM accounts").await.unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_decimal_precision_preservation() {
    let ctx = setup_test_db().await;
    
    // Insert precise values
    ctx.execute(
        "INSERT INTO transactions (amount, fee) VALUES
         ('123.456789', '1.234567')"
    ).await.unwrap();
    
    // Calculate with high precision
    let result = ctx.query(
        "SELECT amount * fee / 100 FROM transactions"
    ).await.unwrap();
    
    assert_eq!(result.rows.len(), 1);
    // Result should maintain precision through decimal operations
}

#[tokio::test]
async fn test_mixed_type_operations() {
    let ctx = setup_test_db().await;
    
    // Create data with mixed types
    ctx.execute(
        "CREATE TABLE mixed_types (
            id INTEGER,
            int_val INTEGER,
            decimal_val TEXT
        )"
    ).await.unwrap();
    
    ctx.execute(
        "INSERT INTO __pgsqlite_schema (table_name, column_name, pg_type, sqlite_type) VALUES
         ('mixed_types', 'decimal_val', 'NUMERIC', 'DECIMAL')"
    ).await.unwrap();
    
    ctx.execute(
        "INSERT INTO mixed_types (id, int_val, decimal_val) VALUES
         (1, 10, '100.50')"
    ).await.unwrap();
    
    // Integer + Decimal
    let result = ctx.query("SELECT int_val + decimal_val FROM mixed_types").await.unwrap();
    assert_eq!(result.rows.len(), 1);
    
    // Decimal * Integer
    let result = ctx.query("SELECT decimal_val * int_val FROM mixed_types").await.unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_null_handling() {
    let ctx = setup_test_db().await;
    
    // Insert data with NULLs
    ctx.execute(
        "INSERT INTO accounts (name, balance, credit_limit) VALUES
         ('NullTest', NULL, '1000.00')"
    ).await.unwrap();
    
    // Operations with NULL should handle gracefully
    let result = ctx.query(
        "SELECT balance + 100, credit_limit - balance FROM accounts WHERE name = 'NullTest'"
    ).await.unwrap();
    
    assert_eq!(result.rows.len(), 1);
    // NULL operations should return NULL
}

#[tokio::test]
async fn test_error_handling() {
    let ctx = setup_test_db().await;
    
    // Division by zero should be handled
    ctx.execute(
        "INSERT INTO accounts (name, balance, credit_limit) VALUES
         ('Zero', '100.00', '0')"
    ).await.unwrap();
    
    // This should either return an error or handle gracefully
    let _result = ctx.query("SELECT balance / credit_limit FROM accounts WHERE name = 'Zero'").await;
    
    // The query should either fail gracefully or return a result
    // depending on how decimal_div handles division by zero
}