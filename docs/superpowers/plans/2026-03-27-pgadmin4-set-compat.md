# pgAdmin4 SET Command Compatibility Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix SET command parsing and add `set_config()`/`pg_show_all_settings()` support so pgAdmin4 can connect to pgsqlite.

**Architecture:** Fix the SET regex to allow `=` without surrounding spaces. Add query preprocessing in the executor to rewrite `pg_show_all_settings()` → `pg_settings` and handle `set_config()` with a synthetic wire response. Align `server_version` across all 4 reporting locations.

**Tech Stack:** Rust, regex, PostgreSQL wire protocol

**Spec:** `docs/superpowers/specs/2026-03-27-pgadmin4-set-compat-design.md`

---

## File Structure

| File | Role |
|------|------|
| `src/query/set_handler.rs` | SET/SHOW command handler — regex fix + version alignment |
| `src/query/executor.rs` | Query executor — add `set_config()` handler + `pg_show_all_settings()` rewrite |
| `src/session/state.rs` | Session state — version alignment |
| `src/functions/system_functions.rs` | SQLite-registered functions — version alignment |

---

### Task 1: Fix SET_PARAMETER_PATTERN regex

**Files:**
- Modify: `src/query/set_handler.rs:15-17` (regex definition)
- Modify: `src/query/set_handler.rs:192-228` (tests)

- [ ] **Step 1: Add failing tests for SET without spaces**

Add these tests to the existing `mod tests` block in `src/query/set_handler.rs` (starts at line 192):

```rust
#[test]
fn test_set_parameter_pattern_equals_no_spaces() {
    // Issue #71: pgAdmin4 sends SET DateStyle=ISO
    assert!(SET_PARAMETER_PATTERN.is_match("SET DateStyle=ISO"));
    assert!(SET_PARAMETER_PATTERN.is_match("SET client_min_messages=notice"));
    assert!(SET_PARAMETER_PATTERN.is_match("SET client_encoding='utf-8'"));
}

#[test]
fn test_set_parameter_pattern_equals_with_spaces() {
    assert!(SET_PARAMETER_PATTERN.is_match("SET DateStyle = ISO"));
    assert!(SET_PARAMETER_PATTERN.is_match("SET client_encoding = 'UTF8'"));
}

#[test]
fn test_set_parameter_pattern_to_keyword() {
    assert!(SET_PARAMETER_PATTERN.is_match("SET search_path TO public"));
    assert!(SET_PARAMETER_PATTERN.is_match("SET client_encoding TO 'UTF8'"));
}

#[test]
fn test_set_parameter_pattern_captures() {
    let caps = SET_PARAMETER_PATTERN.captures("SET DateStyle=ISO").unwrap();
    assert_eq!(&caps[1], "DateStyle");
    assert_eq!(&caps[2], "ISO");

    let caps = SET_PARAMETER_PATTERN.captures("SET client_encoding = 'UTF8'").unwrap();
    assert_eq!(&caps[1], "client_encoding");
    assert_eq!(&caps[2], "'UTF8'");

    let caps = SET_PARAMETER_PATTERN.captures("SET search_path TO public").unwrap();
    assert_eq!(&caps[1], "search_path");
    assert_eq!(&caps[2], "public");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib set_handler::tests -- --nocapture`
Expected: `test_set_parameter_pattern_equals_no_spaces` FAILS

- [ ] **Step 3: Fix the regex**

In `src/query/set_handler.rs`, change line 16 from:

```rust
Regex::new(r"(?i)^\s*SET\s+(\w+)\s+(?:TO|=)\s+(.+)$").unwrap()
```

to:

```rust
Regex::new(r"(?i)^\s*SET\s+(\w+)(?:\s*=\s*|\s+TO\s+)(.+)$").unwrap()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --lib set_handler::tests -- --nocapture`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add src/query/set_handler.rs
git commit -m "fix: allow SET without spaces around equals sign (#71)"
```

---

### Task 2: Add pg_show_all_settings() rewrite and set_config() handler

**Files:**
- Modify: `src/query/executor.rs` — add static regexes, `preprocess_query` function, `set_config()` handler in `execute_single_statement`, and tests in existing `mod tests` block (line 2727)

- [ ] **Step 1: Write failing tests**

Add to the existing `mod tests` block in `src/query/executor.rs` (starts at line 2727):

```rust
#[test]
fn test_pg_show_all_settings_rewrite() {
    let query = "SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'";
    let rewritten = preprocess_query(query);
    assert!(rewritten.contains("pg_settings"));
    assert!(!rewritten.contains("pg_show_all_settings()"));
}

#[test]
fn test_pg_show_all_settings_case_insensitive() {
    let query = "SELECT * FROM PG_SHOW_ALL_SETTINGS() WHERE name = 'timezone'";
    let rewritten = preprocess_query(query);
    assert!(rewritten.contains("pg_settings"));
}

#[test]
fn test_no_rewrite_when_not_present() {
    let query = "SELECT * FROM pg_settings WHERE name = 'timezone'";
    let rewritten = preprocess_query(query);
    assert_eq!(rewritten, query);
}

#[test]
fn test_set_config_detection() {
    let query = "SELECT set_config('bytea_output','hex',false) FROM pg_settings WHERE name = 'bytea_output'";
    assert!(SET_CONFIG_PATTERN.is_match(query));
}

#[test]
fn test_set_config_captures() {
    let query = "SELECT set_config('bytea_output','hex',false)";
    let caps = SET_CONFIG_PATTERN.captures(query).unwrap();
    assert_eq!(&caps[1], "bytea_output");
    assert_eq!(&caps[2], "hex");
    assert_eq!(&caps[3], "false");
}

#[test]
fn test_set_config_empty_value() {
    let query = "SELECT set_config('application_name','',false)";
    let caps = SET_CONFIG_PATTERN.captures(query).unwrap();
    assert_eq!(&caps[1], "application_name");
    assert_eq!(&caps[2], "");
    assert_eq!(&caps[3], "false");
}

#[test]
fn test_set_config_with_spaces() {
    let query = "SELECT set_config( 'timezone' , 'UTC' , true )";
    let caps = SET_CONFIG_PATTERN.captures(query).unwrap();
    assert_eq!(&caps[1], "timezone");
    assert_eq!(&caps[2], "UTC");
    assert_eq!(&caps[3], "true");
}

#[test]
fn test_pgadmin4_full_query_preprocessing() {
    // The exact pgAdmin4 query after semicolon splitting yields this statement:
    let query = "SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'";

    // Step 1: preprocess rewrites pg_show_all_settings() → pg_settings
    let rewritten = preprocess_query(query);
    assert_eq!(
        rewritten,
        "SELECT set_config('bytea_output','hex',false) FROM pg_settings WHERE name = 'bytea_output'"
    );

    // Step 2: set_config pattern matches on the rewritten query
    let caps = SET_CONFIG_PATTERN.captures(&rewritten).unwrap();
    assert_eq!(&caps[1], "bytea_output");
    assert_eq!(&caps[2], "hex");
    assert_eq!(&caps[3], "false");
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib executor::tests -- --nocapture 2>&1 | grep -E "FAIL|not found|error"`
Expected: FAIL — `preprocess_query` and `SET_CONFIG_PATTERN` do not exist

- [ ] **Step 3: Implement the static regexes and preprocess_query function**

Add to `src/query/executor.rs`, after the existing static declarations (after line 23, before the struct definitions):

```rust
static PG_SHOW_ALL_SETTINGS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)pg_show_all_settings\(\s*\)").unwrap()
});

static SET_CONFIG_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)set_config\(\s*'([^']+)'\s*,\s*'([^']*)'\s*,\s*(true|false)\s*\)").unwrap()
});

fn preprocess_query(query: &str) -> String {
    if PG_SHOW_ALL_SETTINGS_PATTERN.is_match(query) {
        PG_SHOW_ALL_SETTINGS_PATTERN.replace_all(query, "pg_settings").to_string()
    } else {
        query.to_string()
    }
}
```

- [ ] **Step 4: Run regex and preprocessing tests**

Run: `cargo test --lib executor::tests -- --nocapture 2>&1 | grep -E "test_pg_show|test_set_config|test_pgadmin"`
Expected: All matching tests PASS

- [ ] **Step 5: Wire preprocessing and set_config handler into execute_single_statement**

In `execute_single_statement`, after the failed-transaction check (after line 263's closing `}`) and before the ultra-fast path comment (line 264), add:

```rust
// Preprocess query: rewrite pg_show_all_settings() → pg_settings
let query = preprocess_query(query);
let query: &str = query.as_str();

// Handle set_config() function calls
if let Some(caps) = SET_CONFIG_PATTERN.captures(query) {
    let param_name = caps[1].to_string();
    let param_value = caps[2].to_string();
    // is_local (caps[3]) is ignored — pgsqlite doesn't support transaction-scoped settings

    debug!("Handling set_config('{}', '{}', ...)", param_name, param_value);

    // Set the parameter in the session
    let mut params = session.parameters.write().await;
    params.insert(param_name.to_uppercase(), param_value.clone());
    drop(params);

    // Send synthetic response: RowDescription + DataRow + CommandComplete
    let field = FieldDescription {
        name: "set_config".to_string(),
        table_oid: 0,
        column_id: 1,
        type_oid: PgType::Text.to_oid(),
        type_size: -1,
        type_modifier: -1,
        format: 0,
    };
    framed.send(BackendMessage::RowDescription(vec![field])).await
        .map_err(PgSqliteError::Io)?;

    let row = vec![Some(param_value.as_bytes().to_vec())];
    framed.send(BackendMessage::DataRow(row)).await
        .map_err(PgSqliteError::Io)?;

    framed.send(BackendMessage::CommandComplete {
        tag: "SELECT 1".to_string()
    }).await.map_err(PgSqliteError::Io)?;

    return Ok(());
}
```

Note on parameter key casing: The `to_uppercase()` call is consistent with the existing SET handler (`set_handler.rs:85`). Session parameters initialized in `state.rs` use mixed case (`"DateStyle"`, `"server_version"`), while SET/SHOW operations uppercase. This is a pre-existing inconsistency — not introduced by this change.

- [ ] **Step 6: Run full test suite**

Run: `cargo test`
Expected: All PASS

Run: `cargo clippy`
Expected: No new warnings

- [ ] **Step 7: Commit**

```bash
git add src/query/executor.rs
git commit -m "feat: add pg_show_all_settings() rewrite and set_config() support (#71)"
```

---

### Task 3: Align server_version across all locations

**Files:**
- Modify: `src/query/set_handler.rs:110-111`
- Modify: `src/session/state.rs:58`
- Modify: `src/functions/system_functions.rs:16`

- [ ] **Step 1: Update set_handler.rs**

In `src/query/set_handler.rs`, change lines 110-111 from:

```rust
"SERVER_VERSION" => "15.0".to_string(),
"SERVER_VERSION_NUM" => "150000".to_string(),
```

to:

```rust
"SERVER_VERSION" => "16.0".to_string(),
"SERVER_VERSION_NUM" => "160000".to_string(),
```

- [ ] **Step 2: Update state.rs**

In `src/session/state.rs`, change line 58 from:

```rust
parameters.insert("server_version".to_string(), "14.0 (SQLite wrapper)".to_string());
```

to:

```rust
parameters.insert("server_version".to_string(), "16.0".to_string());
```

- [ ] **Step 3: Update system_functions.rs**

In `src/functions/system_functions.rs`, change line 16 from:

```rust
Ok(format!("PostgreSQL 15.0 (pgsqlite {}) on x86_64-pc-linux-gnu, compiled by rustc, 64-bit",
```

to:

```rust
Ok(format!("PostgreSQL 16.0 (pgsqlite {}) on x86_64-pc-linux-gnu, compiled by rustc, 64-bit",
```

- [ ] **Step 4: Run full test suite**

Run: `cargo test`
Expected: All PASS

Run: `cargo clippy`
Expected: No new warnings

- [ ] **Step 5: Commit**

```bash
git add src/query/set_handler.rs src/session/state.rs src/functions/system_functions.rs
git commit -m "fix: align server_version to 16.0 across all reporting locations"
```

---

### Task 4: Final verification

- [ ] **Step 1: Run pre-commit checklist**

```bash
cargo check && cargo clippy && cargo build && cargo test
```

Expected: All pass with no errors or warnings.

- [ ] **Step 2: Verify all pgAdmin4 compound query statements are handled**

The compound query from issue #71 is split by `;` in the executor. After our changes, each statement should succeed:

1. `SET DateStyle=ISO` — matches fixed regex (Task 1) ✓
2. `SET client_min_messages=notice` — matches fixed regex (Task 1) ✓
3. `SELECT set_config('bytea_output','hex',false) FROM pg_show_all_settings() WHERE name = 'bytea_output'` — `pg_show_all_settings()` rewritten to `pg_settings`, then `set_config()` handler intercepts and returns synthetic response (Task 2) ✓
4. `SET client_encoding='utf-8'` — matches fixed regex (Task 1) ✓

The `test_pgadmin4_full_query_preprocessing` test in Task 2 validates the combined rewrite + intercept flow for statement 3.
