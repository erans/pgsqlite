# START TRANSACTION / END TRANSACTION Support

**Issue**: [#70](https://github.com/erans/pgsqlite/issues/70) - Support for START TRANSACTION
**Date**: 2026-03-27
**Scope**: Recognize PostgreSQL transaction synonyms: `START TRANSACTION` → `BEGIN`, `END` → `COMMIT`

## Problem

PostgreSQL's `START TRANSACTION` is a synonym for `BEGIN`, and `END` / `END TRANSACTION` is a synonym for `COMMIT`. The `postgres_fdw` extension sends:

```sql
START TRANSACTION ISOLATION LEVEL REPEATABLE READ
```

pgsqlite's `QueryTypeDetector` doesn't recognize `START` as a transaction keyword, so the raw SQL is passed to SQLite, which fails because `START` isn't a SQLite keyword. The same issue exists for `END TRANSACTION`.

Additionally, the extended query protocol path in `extended.rs` has its own transaction dispatch and `execute_transaction` that only check for `BEGIN`, `COMMIT`, and `ROLLBACK` — not `START` or `END`.

## Design

### 1. Detection changes in QueryTypeDetector

**File**: `src/query/query_type_detection.rs`

**Fast path** — add `START` to the existing `if bytes.len() >= 5` block (lines 37-43) which already matches `ALTER` and `BEGIN`:
```rust
b"START" | b"start" | b"Start" => return QueryType::Begin,
```

For `END`, add a new 3-byte check with word boundary guard:
```rust
if bytes.len() >= 3 {
    let first3 = &bytes[0..3];
    if (first3 == b"END" || first3 == b"end" || first3 == b"End")
        && (bytes.len() == 3 || bytes[3].is_ascii_whitespace()) {
        return QueryType::Commit;
    }
}
```

**Fallback path** — add alongside existing checks:
```rust
} else if trimmed.len() >= 5 && trimmed[..5].eq_ignore_ascii_case("START") {
    QueryType::Begin
} else if trimmed.len() >= 3 && trimmed[..3].eq_ignore_ascii_case("END")
    && (trimmed.len() == 3 || trimmed.as_bytes()[3].is_ascii_whitespace()) {
    QueryType::Commit
```

### 2. Extended query protocol dispatch

**File**: `src/query/extended.rs`

**Dispatch block** (~line 1865): Add `START` and `END` to the transaction dispatch:
```rust
} else if query_starts_with_ignore_case(&final_query, "BEGIN")
    || query_starts_with_ignore_case(&final_query, "START")
    || query_starts_with_ignore_case(&final_query, "COMMIT")
    || query_starts_with_ignore_case(&final_query, "END")
    || query_starts_with_ignore_case(&final_query, "ROLLBACK") {
    Self::execute_transaction(framed, db, session, &final_query).await?;
```

**execute_transaction** (~line 5635): Add `START` and `END` handling:
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
} else if query_starts_with_ignore_case(query, "ROLLBACK") {
```

Note: `query_starts_with_ignore_case` checks case-insensitively and only matches at word boundaries, so `END` won't match `ENDTABLE`.

### Why this is sufficient

Once detected as `QueryType::Begin` or `QueryType::Commit`, the existing `execute_transaction` handlers call `db.begin_with_session()` or `db.commit_with_session()` — they never pass the raw SQL to SQLite. The `ISOLATION LEVEL REPEATABLE READ` clause is naturally ignored because SQLite only supports one isolation level (serializable).

The ultra-simple query fast path (`simple_query_detector.rs`) only matches DML/SELECT queries, so `START TRANSACTION` and `END` are unaffected — they always fall through to the normal detection path.

Note: `query_router.rs` already handles `START` and `END` classification (lines 170-173), so no changes are needed there.

## Files Changed

| File | Change |
|------|--------|
| `src/query/query_type_detection.rs` | Add `START` → `Begin` and `END` → `Commit` in both detection paths + tests |
| `src/query/extended.rs` | Add `START` and `END` to transaction dispatch and `execute_transaction` |

## Testing

- `START TRANSACTION` → `QueryType::Begin`
- `start transaction` → `QueryType::Begin` (case insensitive)
- `START TRANSACTION ISOLATION LEVEL REPEATABLE READ` → `QueryType::Begin`
- `END` → `QueryType::Commit`
- `END TRANSACTION` → `QueryType::Commit`
- `end transaction` → `QueryType::Commit` (case insensitive)
- Verify `END` does NOT match identifiers starting with "END" (word boundary guard)
- Verify `START TRANSACTION` in a failed transaction returns the "commands ignored" error

## Known Limitations

- SQLite only supports serializable isolation, so `ISOLATION LEVEL REPEATABLE READ` (or any other level) is silently accepted but not enforced at that level
- `SET TRANSACTION ISOLATION LEVEL` is not handled (separate command, not yet reported as needed)
