# Transaction State Management Architecture

## Overview

This document outlines the architectural improvements needed for proper transaction state tracking in pgsqlite. Currently, transaction state is not properly maintained, causing issues with PostgreSQL client libraries like SQLAlchemy that rely on accurate transaction status reporting.

## Current Issues

### 1. Missing Transaction State Updates
- Transaction status is initialized to `Idle` and never updated
- BEGIN, COMMIT, and ROLLBACK commands execute but don't update session state
- `Session::in_transaction()` always returns false (unless lock fails)

### 2. Incorrect Wire Protocol Status
- ReadyForQuery message always reports 'I' (Idle)
- Should report 'T' (InTransaction) when inside a transaction
- Should report 'E' (InFailedTransaction) when transaction has errors

### 3. Connection Pool Transaction Routing
- QueryRouter relies on `in_transaction()` to maintain connection affinity
- Since this always returns false, transaction queries may use different connections
- Breaks transaction isolation and consistency

### 4. SQLAlchemy Compatibility
- SQLAlchemy expects committed changes to be visible across sessions
- Transaction isolation is broken due to incorrect state tracking
- Causes test failures in transaction handling scenarios

## Proposed Architecture

### Phase 1: Core Transaction State Management

#### 1.1 Update Transaction Status on Commands

Location: `src/query/executor.rs::execute_transaction()`

```rust
pub async fn execute_transaction(
    db: &Arc<Mutex<Connection>>,
    session: &Arc<Session>,
    query_type: QueryType,
) -> Result<QueryResponse, Box<dyn Error + Send + Sync>> {
    let db = db.lock();
    
    match query_type {
        QueryType::Begin => {
            db.execute("BEGIN", [])?;
            session.set_transaction_status(TransactionStatus::InTransaction).await;
            Ok(QueryResponse::CommandComplete("BEGIN".to_string()))
        }
        QueryType::Commit => {
            db.execute("COMMIT", [])?;
            session.set_transaction_status(TransactionStatus::Idle).await;
            Ok(QueryResponse::CommandComplete("COMMIT".to_string()))
        }
        QueryType::Rollback => {
            db.execute("ROLLBACK", [])?;
            session.set_transaction_status(TransactionStatus::Idle).await;
            Ok(QueryResponse::CommandComplete("ROLLBACK".to_string()))
        }
        _ => unreachable!("Invalid transaction command type"),
    }
}
```

#### 1.2 Fix Transaction Detection

Location: `src/session/mod.rs`

```rust
impl Session {
    pub async fn in_transaction(&self) -> bool {
        matches!(
            *self.transaction_status.read().await,
            TransactionStatus::InTransaction | TransactionStatus::InFailedTransaction
        )
    }
    
    pub async fn set_transaction_status(&self, status: TransactionStatus) {
        *self.transaction_status.write().await = status;
    }
    
    pub async fn get_transaction_status(&self) -> TransactionStatus {
        *self.transaction_status.read().await
    }
}
```

### Phase 2: Wire Protocol Transaction Status

#### 2.1 Correct ReadyForQuery Status

Location: `src/protocol/response.rs` or similar

```rust
pub async fn send_ready_for_query(
    writer: &mut (impl AsyncWrite + Unpin),
    session: &Arc<Session>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let tx_status = match session.get_transaction_status().await {
        TransactionStatus::Idle => b'I',
        TransactionStatus::InTransaction => b'T',
        TransactionStatus::InFailedTransaction => b'E',
    };
    
    // 'Z' (ReadyForQuery) | Length (5) | Status
    writer.write_all(&[b'Z', 0, 0, 0, 5, tx_status]).await?;
    writer.flush().await?;
    Ok(())
}
```

### Phase 3: Error Handling Within Transactions

#### 3.1 Track Failed Transactions

Location: `src/query/executor.rs`

```rust
// In execute_query or similar error handling paths
match result {
    Ok(response) => Ok(response),
    Err(e) => {
        if session.in_transaction().await {
            session.set_transaction_status(TransactionStatus::InFailedTransaction).await;
        }
        Err(e)
    }
}
```

#### 3.2 Enforce Rollback in Failed State

```rust
// Before executing any non-ROLLBACK query
if matches!(session.get_transaction_status().await, TransactionStatus::InFailedTransaction) {
    if !matches!(query_type, QueryType::Rollback) {
        return Err("current transaction is aborted, commands ignored until end of transaction block".into());
    }
}
```

### Phase 4: Connection Pooling Integration

The existing `QueryRouter` implementation should work correctly once transaction state is properly tracked:

```rust
// In QueryRouter::route_query()
if session.in_transaction().await {
    // Route to write connection for transaction consistency
    return Ok(QueryDestination::Write);
}
```

### Phase 5: Extended Transaction Support

#### 5.1 Savepoint Support (Future)

```rust
enum TransactionStatus {
    Idle,
    InTransaction,
    InFailedTransaction,
    InSavepoint(String),  // Savepoint name
}
```

#### 5.2 Transaction Characteristics

- Support for `SET TRANSACTION` characteristics
- Read-only transaction optimization
- Deferrable transaction support

## Testing Strategy

### Unit Tests

1. **State Transitions**
   - Test BEGIN → InTransaction
   - Test COMMIT → Idle
   - Test ROLLBACK → Idle
   - Test error → InFailedTransaction

2. **Wire Protocol**
   - Verify ReadyForQuery reports correct status
   - Test status changes after each command

### Integration Tests

1. **SQLAlchemy Compatibility**
   - Transaction commit visibility
   - Rollback behavior
   - Nested transaction support

2. **Connection Pool Behavior**
   - Transaction queries use same connection
   - New transactions get fresh connections

3. **Error Scenarios**
   - Failed transaction requires rollback
   - Syntax errors don't affect transaction state
   - Constraint violations mark transaction failed

## Implementation Timeline

1. **Week 1**: Core transaction state (Phase 1 & 2)
   - Update execute_transaction
   - Fix wire protocol status
   - Basic unit tests

2. **Week 2**: Error handling and testing (Phase 3 & 5)
   - Failed transaction tracking
   - Comprehensive test suite
   - SQLAlchemy validation

3. **Future**: Extended features
   - Savepoint support
   - Transaction characteristics
   - Performance optimizations

## Success Criteria

1. SQLAlchemy transaction tests pass
2. Transaction state correctly reported in wire protocol
3. Connection pool maintains transaction affinity
4. Error states handled properly
5. No performance regression

## Notes

- SQLite only supports SERIALIZABLE isolation level
- Autocommit mode is SQLite's default
- PostgreSQL clients expect accurate transaction status
- This fix is critical for ORM compatibility