# START TRANSACTION / END TRANSACTION Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Recognize `START TRANSACTION` as `BEGIN` and `END`/`END TRANSACTION` as `COMMIT` so postgres_fdw can connect.

**Architecture:** Add `START` and `END` keyword detection in `QueryTypeDetector` (both fast path and fallback) and in the extended query protocol's transaction dispatch in `extended.rs`. No new translation or preprocessing needed — existing transaction handlers already call session methods without passing raw SQL to SQLite.

**Tech Stack:** Rust

**Spec:** `docs/superpowers/specs/2026-03-27-start-transaction-design.md`

---

## File Structure

| File | Role |
|------|------|
| `src/query/query_type_detection.rs` | Query type classifier — add `START` → `Begin`, `END` → `Commit` |
| `src/query/extended.rs` | Extended query protocol — add `START`/`END` to transaction dispatch and handler |

---

### Task 1: Add START TRANSACTION detection in QueryTypeDetector

**Files:**
- Modify: `src/query/query_type_detection.rs:37-43` (fast path 5-byte block)
- Modify: `src/query/query_type_detection.rs:85-86` (fallback path)
- Modify: `src/query/query_type_detection.rs:197-243` (tests)

- [ ] **Step 1: Add failing tests**

Add to the existing `mod tests` block in `src/query/query_type_detection.rs`, inside `test_query_type_detection`:

```rust
// START TRANSACTION (issue #70)
assert_eq!(QueryTypeDetector::detect_query_type("START TRANSACTION"), QueryType::Begin);
assert_eq!(QueryTypeDetector::detect_query_type("start transaction"), QueryType::Begin);
assert_eq!(QueryTypeDetector::detect_query_type("Start Transaction"), QueryType::Begin);
assert_eq!(QueryTypeDetector::detect_query_type("START TRANSACTION ISOLATION LEVEL REPEATABLE READ"), QueryType::Begin);
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib query_type_detection::tests -- --nocapture`
Expected: FAIL — `START TRANSACTION` returns `QueryType::Other`

- [ ] **Step 3: Add START to fast path**

In `src/query/query_type_detection.rs`, add `START` to the existing `if bytes.len() >= 5` block at line 37-43:

```rust
if bytes.len() >= 5 {
    match &bytes[0..5] {
        b"ALTER" | b"alter" | b"Alter" => return QueryType::Alter,
        b"BEGIN" | b"begin" | b"Begin" => return QueryType::Begin,
        b"START" | b"start" | b"Start" => return QueryType::Begin,
        _ => {}
    }
}
```

- [ ] **Step 4: Add START to fallback path**

After the existing `BEGIN` check at line 85-86, add:

```rust
} else if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("START") {
    QueryType::Begin
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib query_type_detection::tests -- --nocapture`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/query/query_type_detection.rs
git commit -m "feat: recognize START TRANSACTION as BEGIN (#70)"
```

---

### Task 2: Add END / END TRANSACTION detection in QueryTypeDetector

**Files:**
- Modify: `src/query/query_type_detection.rs:37-65` (fast path — new 3-byte block)
- Modify: `src/query/query_type_detection.rs:85-90` (fallback path)
- Modify: `src/query/query_type_detection.rs:197-243` (tests)

- [ ] **Step 1: Add failing tests**

Add to `test_query_type_detection`:

```rust
// END / END TRANSACTION
assert_eq!(QueryTypeDetector::detect_query_type("END"), QueryType::Commit);
assert_eq!(QueryTypeDetector::detect_query_type("end"), QueryType::Commit);
assert_eq!(QueryTypeDetector::detect_query_type("END TRANSACTION"), QueryType::Commit);
assert_eq!(QueryTypeDetector::detect_query_type("end transaction"), QueryType::Commit);
// Word boundary guard — END must not match identifiers starting with "END"
assert_ne!(QueryTypeDetector::detect_query_type("ENDLESS"), QueryType::Commit);
assert_ne!(QueryTypeDetector::detect_query_type("ENDTABLE"), QueryType::Commit);
```

Also add a new test for the `is_transaction` helper:

```rust
#[test]
fn test_is_transaction_with_synonyms() {
    assert!(QueryTypeDetector::is_transaction("BEGIN"));
    assert!(QueryTypeDetector::is_transaction("START TRANSACTION"));
    assert!(QueryTypeDetector::is_transaction("COMMIT"));
    assert!(QueryTypeDetector::is_transaction("END"));
    assert!(QueryTypeDetector::is_transaction("END TRANSACTION"));
    assert!(QueryTypeDetector::is_transaction("ROLLBACK"));
    assert!(!QueryTypeDetector::is_transaction("SELECT 1"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib query_type_detection::tests -- --nocapture`
Expected: FAIL — `END` returns `QueryType::Other`

- [ ] **Step 3: Add END to fast path with word boundary guard**

Add a new block after the existing `if bytes.len() >= 5` block (after line 43), before `if bytes.len() >= 6`:

```rust
if bytes.len() >= 3 {
    let first3 = &bytes[0..3];
    if (first3 == b"END" || first3 == b"end" || first3 == b"End")
        && (bytes.len() == 3 || bytes[3].is_ascii_whitespace())
    {
        return QueryType::Commit;
    }
}
```

- [ ] **Step 4: Add END to fallback path with word boundary guard**

After the `START` check added in Task 1, add:

```rust
} else if trimmed.len() >= 3 && trimmed[..3].eq_ignore_ascii_case("END")
    && (trimmed.len() == 3 || trimmed.as_bytes()[3].is_ascii_whitespace()) {
    QueryType::Commit
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib query_type_detection::tests -- --nocapture`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add src/query/query_type_detection.rs
git commit -m "feat: recognize END / END TRANSACTION as COMMIT (#70)"
```

---

### Task 3: Add START/END to extended query protocol

**Files:**
- Modify: `src/query/extended.rs:1865-1868` (dispatch block)
- Modify: `src/query/extended.rs:5635-5647` (execute_transaction handler)

- [ ] **Step 1: Update dispatch block**

In `src/query/extended.rs`, change lines 1865-1868 from:

```rust
} else if query_starts_with_ignore_case(&final_query, "BEGIN")
    || query_starts_with_ignore_case(&final_query, "COMMIT")
    || query_starts_with_ignore_case(&final_query, "ROLLBACK") {
    Self::execute_transaction(framed, db, session, &final_query).await?;
```

to:

```rust
} else if query_starts_with_ignore_case(&final_query, "BEGIN")
    || query_starts_with_ignore_case(&final_query, "START")
    || query_starts_with_ignore_case(&final_query, "COMMIT")
    || query_starts_with_ignore_case(&final_query, "END")
    || query_starts_with_ignore_case(&final_query, "ROLLBACK") {
    Self::execute_transaction(framed, db, session, &final_query).await?;
```

- [ ] **Step 2: Update execute_transaction handler**

In `src/query/extended.rs`, change lines 5635-5643 from:

```rust
if query_starts_with_ignore_case(query, "BEGIN") {
    db.begin_with_session(&session.id).await?;
    framed.send(BackendMessage::CommandComplete { tag: "BEGIN".to_string() }).await
        .map_err(PgSqliteError::Io)?;
} else if query_starts_with_ignore_case(query, "COMMIT") {
    db.commit_with_session(&session.id).await?;
    framed.send(BackendMessage::CommandComplete { tag: "COMMIT".to_string() }).await
        .map_err(PgSqliteError::Io)?;
```

to:

```rust
if query_starts_with_ignore_case(query, "BEGIN")
    || query_starts_with_ignore_case(query, "START") {
    db.begin_with_session(&session.id).await?;
    framed.send(BackendMessage::CommandComplete { tag: "BEGIN".to_string() }).await
        .map_err(PgSqliteError::Io)?;
} else if query_starts_with_ignore_case(query, "COMMIT")
    || query_starts_with_ignore_case(query, "END") {
    db.commit_with_session(&session.id).await?;
    framed.send(BackendMessage::CommandComplete { tag: "COMMIT".to_string() }).await
        .map_err(PgSqliteError::Io)?;
```

- [ ] **Step 3: Run full test suite**

Run: `cargo test`
Expected: All PASS

Run: `cargo clippy`
Expected: No new warnings

- [ ] **Step 4: Commit**

```bash
git add src/query/extended.rs
git commit -m "feat: add START/END transaction support to extended protocol (#70)"
```

---

### Task 4: Final verification

- [ ] **Step 1: Run pre-commit checklist**

```bash
cargo check && cargo clippy && cargo build && cargo test
```

Expected: All pass with no errors.

- [ ] **Step 2: Verify the postgres_fdw query is handled**

The query from issue #70:
```sql
START TRANSACTION ISOLATION LEVEL REPEATABLE READ
```

After our changes:
1. `QueryTypeDetector::detect_query_type("START TRANSACTION ISOLATION LEVEL REPEATABLE READ")` → `QueryType::Begin` (Task 1)
2. Routes to `execute_transaction` which calls `db.begin_with_session()` — isolation level clause never reaches SQLite
3. Extended protocol also handles `START` via dispatch block (Task 3)
