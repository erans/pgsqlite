# Column Name Sanitization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Strip parenthesized suffixes from SQLite result column names to match PostgreSQL behavior (e.g., `version()` → `version`).

**Architecture:** Add a `sanitize_column_name()` function that strips everything from the first `(` onward when present. Apply it at every `stmt.column_name(i)` collection site so that `DbResponse.columns` and all downstream code (FieldDescription, type lookups) receive PostgreSQL-compatible column names.

**Tech Stack:** Rust, existing pgsqlite codebase patterns.

---

### Task 1: Create the `sanitize_column_name` module

**Files:**
- Create: `src/query/column_sanitizer.rs`
- Modify: `src/query/mod.rs`

- [ ] **Step 1: Create the sanitizer module**

```rust
// src/query/column_sanitizer.rs

/// Strip parenthesized arguments from column names to match PostgreSQL behavior.
///
/// PostgreSQL returns just the function name as the column name for function calls:
///   SELECT version()  → column "version"
///   SELECT count(*)   → column "count"
///   SELECT max(id)    → column "max"
///
/// SQLite returns the full expression:
///   SELECT version()  → column "version()"
///   SELECT count(*)   → column "count(*)"
///   SELECT max(id)    → column "max(id)"
///
/// This function normalizes SQLite's behavior to match PostgreSQL.
pub fn sanitize_column_name(name: &str) -> &str {
    if let Some(pos) = name.find('(') {
        &name[..pos]
    } else {
        name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_function_call() {
        assert_eq!(sanitize_column_name("version()"), "version");
    }

    #[test]
    fn test_function_with_star_arg() {
        assert_eq!(sanitize_column_name("count(*)"), "count");
    }

    #[test]
    fn test_function_with_column_arg() {
        assert_eq!(sanitize_column_name("max(id)"), "max");
    }

    #[test]
    fn test_nested_function_call() {
        assert_eq!(sanitize_column_name("COALESCE(max(id), 0)"), "COALESCE");
    }

    #[test]
    fn test_no_parens() {
        assert_eq!(sanitize_column_name("current_timestamp"), "current_timestamp");
    }

    #[test]
    fn test_regular_column() {
        assert_eq!(sanitize_column_name("my_column"), "my_column");
    }

    #[test]
    fn test_schema_qualified() {
        assert_eq!(sanitize_column_name("pg_catalog.version()"), "pg_catalog.version");
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(sanitize_column_name(""), "");
    }

    #[test]
    fn test_just_parens() {
        assert_eq!(sanitize_column_name("()"), "");
    }
}
```

- [ ] **Step 2: Register the module in mod.rs**

Add to `src/query/mod.rs`:

```rust
pub mod column_sanitizer;
```

- [ ] **Step 3: Run tests to verify module compiles and tests pass**

Run: `cargo test --lib column_sanitizer`
Expected: All 9 tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/query/column_sanitizer.rs src/query/mod.rs
git commit -m "feat: add column name sanitizer to strip function parentheses"
```

---

### Task 2: Apply sanitization at all `stmt.column_name(i)` sites in db_handler.rs

**Files:**
- Modify: `src/session/db_handler.rs`

There are 9 sites in db_handler.rs where `stmt.column_name(i)` is called. Each follows the pattern `columns.push(stmt.column_name(i)?.to_string())` or `column_names.push(stmt.column_name(i).unwrap_or("").to_string())`. Change each to wrap with `sanitize_column_name()`.

- [ ] **Step 1: Add import at top of db_handler.rs**

Add to the imports section of `src/session/db_handler.rs`:

```rust
use crate::query::column_sanitizer::sanitize_column_name;
```

- [ ] **Step 2: Apply at line ~529 — SELECT path**

Change:
```rust
columns.push(stmt.column_name(i)?.to_string());
```
To:
```rust
columns.push(sanitize_column_name(stmt.column_name(i)?).to_string());
```

- [ ] **Step 3: Apply at line ~664 — another SELECT path**

Same change as Step 2.

- [ ] **Step 4: Apply at line ~777 — catalog query path**

Same change.

- [ ] **Step 5: Apply at line ~895 — another path**

Same change.

- [ ] **Step 6: Apply at line ~1242 — another path**

Same change.

- [ ] **Step 7: Apply at line ~1453 — another path**

Same change.

- [ ] **Step 8: Apply at line ~1722 — another path**

Same change.

- [ ] **Step 9: Apply at line ~2326 — type inference path (uses unwrap_or)**

Change:
```rust
column_names.push(stmt.column_name(i).unwrap_or("").to_string());
```
To:
```rust
column_names.push(sanitize_column_name(stmt.column_name(i).unwrap_or("")).to_string());
```

- [ ] **Step 10: Apply at line ~2424 — type inference path (uses unwrap_or)**

Same change as Step 9.

- [ ] **Step 11: Run cargo check**

Run: `cargo check`
Expected: No errors.

- [ ] **Step 12: Commit**

```bash
git add src/session/db_handler.rs
git commit -m "feat: apply column name sanitization in db_handler"
```

---

### Task 3: Apply sanitization in fast_path.rs

**Files:**
- Modify: `src/query/fast_path.rs`

3 sites, all following `columns.push(stmt.column_name(i)?.to_string())`.

- [ ] **Step 1: Add import**

```rust
use crate::query::column_sanitizer::sanitize_column_name;
```

- [ ] **Step 2: Apply at line ~377**

Change:
```rust
columns.push(stmt.column_name(i)?.to_string());
```
To:
```rust
columns.push(sanitize_column_name(stmt.column_name(i)?).to_string());
```

- [ ] **Step 3: Apply at line ~619**

Same change.

- [ ] **Step 4: Apply at line ~737**

Same change.

- [ ] **Step 5: Run cargo check**

Run: `cargo check`
Expected: No errors.

- [ ] **Step 6: Commit**

```bash
git add src/query/fast_path.rs
git commit -m "feat: apply column name sanitization in fast_path"
```

---

### Task 4: Apply sanitization in remaining files

**Files:**
- Modify: `src/catalog/query_interceptor.rs`
- Modify: `src/cache/statement_pool.rs`
- Modify: `src/optimization/read_only_optimizer.rs`

- [ ] **Step 1: Apply in query_interceptor.rs (2 sites: ~274, ~415)**

Add import:
```rust
use crate::query::column_sanitizer::sanitize_column_name;
```

Change both sites from:
```rust
columns.push(stmt.column_name(i)?.to_string());
```
To:
```rust
columns.push(sanitize_column_name(stmt.column_name(i)?).to_string());
```

- [ ] **Step 2: Apply in statement_pool.rs (1 site: ~197)**

Add import:
```rust
use crate::query::column_sanitizer::sanitize_column_name;
```

Change:
```rust
column_names.push(stmt.column_name(i)?.to_string());
```
To:
```rust
column_names.push(sanitize_column_name(stmt.column_name(i)?).to_string());
```

- [ ] **Step 3: Apply in read_only_optimizer.rs (1 site: ~262)**

Add import:
```rust
use crate::query::column_sanitizer::sanitize_column_name;
```

Change:
```rust
columns.push(stmt.column_name(i)?.to_string());
```
To:
```rust
columns.push(sanitize_column_name(stmt.column_name(i)?).to_string());
```

- [ ] **Step 4: Run cargo check**

Run: `cargo check`
Expected: No errors.

- [ ] **Step 5: Commit**

```bash
git add src/catalog/query_interceptor.rs src/cache/statement_pool.rs src/optimization/read_only_optimizer.rs
git commit -m "feat: apply column name sanitization in remaining files"
```

---

### Task 5: Check extended query path for column name handling

**Files:**
- Modify: `src/query/extended.rs` (if needed)

The extended query path may get column names from cached `StatementMetadata` (from statement_pool.rs, which we already fixed) or from `DbResponse.columns`. Read the relevant code to determine if any additional sanitization is needed.

- [ ] **Step 1: Inspect extended.rs for column name sources**

Search for how column names enter the extended path. They come from:
1. `DbResponse.columns` — already sanitized via Task 2-4
2. `StatementMetadata.column_names` — already sanitized via statement_pool.rs in Task 4
3. Hardcoded column names in catalog queries — these don't need sanitization (they're already correct PostgreSQL names)

No additional changes expected in extended.rs.

- [ ] **Step 2: Run cargo check**

Run: `cargo check`
Expected: No errors.

---

### Task 6: Run full test suite and verify

- [ ] **Step 1: Run cargo test**

Run: `cargo test`
Expected: All tests pass.

- [ ] **Step 2: Run cargo clippy**

Run: `cargo clippy`
Expected: No new warnings.

- [ ] **Step 3: Build**

Run: `cargo build`
Expected: Successful build.

- [ ] **Step 4: Manual integration test — verify version() column name**

Start pgsqlite, connect with psql, and run:

```sql
SELECT version();
```

Verify the column name in the response is `version` (not `version()`).

- [ ] **Step 5: Manual integration test — verify other function calls**

```sql
SELECT count(*) FROM sqlite_master;
SELECT 1, current_timestamp;
SELECT max(1);
```

Verify column names are `count`, `current_timestamp`, `max` respectively.

- [ ] **Step 6: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: any fixes from integration testing"
```