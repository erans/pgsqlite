# Wire Protocol Type OID Fix Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix wire protocol type OIDs so pre-existing SQLite tables return correct OIDs (int4, float8, bool, etc.) instead of always returning OID 25 (TEXT).

**Architecture:** Add a `sqlite_type_to_pg_type_name` helper and a `get_column_types_from_pragma` DbHandler method that runs `PRAGMA table_info` to get SQLite declared types. Use this as a fallback in the schema_types population loops in executor.rs and extended.rs when `__pgsqlite_schema` has no entry.

**Tech Stack:** Rust

**Spec:** `docs/superpowers/specs/2026-03-27-wire-type-oid-fix-design.md`

---

## File Structure

| File | Role |
|------|------|
| `src/types/sqlite_type_info.rs` | Add `sqlite_type_to_pg_type_name` — maps SQLite declared types to PG type name strings |
| `src/session/db_handler.rs` | Add `get_column_types_from_pragma` — fetches PRAGMA table_info and returns column→PG type name map |
| `src/query/executor.rs` | Add PRAGMA fallback in schema_types population loop |
| `src/query/extended.rs` | Add PRAGMA fallback in 3 locations (schema_types x2, inferred_types x1) |

---

### Task 1: Add `sqlite_type_to_pg_type_name` helper

**Files:**
- Modify: `src/types/sqlite_type_info.rs:62-116`

- [ ] **Step 1: Add failing tests**

Add a `#[cfg(test)]` module at the end of `src/types/sqlite_type_info.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_type_to_pg_type_name() {
        assert_eq!(sqlite_type_to_pg_type_name("INTEGER"), "integer");
        assert_eq!(sqlite_type_to_pg_type_name("integer"), "integer");
        assert_eq!(sqlite_type_to_pg_type_name("INT"), "integer");
        assert_eq!(sqlite_type_to_pg_type_name("BIGINT"), "bigint");
        assert_eq!(sqlite_type_to_pg_type_name("INT8"), "bigint");
        assert_eq!(sqlite_type_to_pg_type_name("SMALLINT"), "smallint");
        assert_eq!(sqlite_type_to_pg_type_name("INT2"), "smallint");
        assert_eq!(sqlite_type_to_pg_type_name("REAL"), "double precision");
        assert_eq!(sqlite_type_to_pg_type_name("FLOAT"), "double precision");
        assert_eq!(sqlite_type_to_pg_type_name("DOUBLE PRECISION"), "double precision");
        assert_eq!(sqlite_type_to_pg_type_name("BOOLEAN"), "boolean");
        assert_eq!(sqlite_type_to_pg_type_name("BOOL"), "boolean");
        assert_eq!(sqlite_type_to_pg_type_name("TEXT"), "text");
        assert_eq!(sqlite_type_to_pg_type_name("VARCHAR(50)"), "text");
        assert_eq!(sqlite_type_to_pg_type_name("BLOB"), "bytea");
        assert_eq!(sqlite_type_to_pg_type_name("DATE"), "date");
        assert_eq!(sqlite_type_to_pg_type_name("TIMESTAMP"), "timestamp");
        assert_eq!(sqlite_type_to_pg_type_name("NUMERIC"), "numeric");
        assert_eq!(sqlite_type_to_pg_type_name("DECIMAL"), "numeric");
        assert_eq!(sqlite_type_to_pg_type_name("UUID"), "uuid");
        assert_eq!(sqlite_type_to_pg_type_name("JSON"), "json");
        assert_eq!(sqlite_type_to_pg_type_name("SOMETHING_UNKNOWN"), "text");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib sqlite_type_info::tests -- --nocapture`
Expected: FAIL — `sqlite_type_to_pg_type_name` not found

- [ ] **Step 3: Implement `sqlite_type_to_pg_type_name`**

Add after `sqlite_type_to_pg_oid` (after line 116) in `src/types/sqlite_type_info.rs`:

```rust
/// Convert SQLite type declaration to PostgreSQL type name string
pub fn sqlite_type_to_pg_type_name(sqlite_type: &str) -> &'static str {
    let type_upper = sqlite_type.to_uppercase();

    if type_upper.contains("BLOB") {
        return "bytea";
    }

    if type_upper.contains("REAL") || type_upper.contains("FLOAT") || type_upper.contains("DOUBLE") {
        return "double precision";
    }

    if type_upper.contains("INT") {
        if type_upper.contains("INT2") || type_upper.contains("SMALLINT") {
            return "smallint";
        } else if type_upper.contains("INT8") || type_upper.contains("BIGINT") {
            return "bigint";
        } else {
            return "integer";
        }
    }

    if type_upper.contains("BOOL") {
        return "boolean";
    }

    if type_upper.contains("DATE") && !type_upper.contains("TIME") {
        return "date";
    }

    if type_upper.contains("TIME") && !type_upper.contains("STAMP") {
        return "time";
    }

    if type_upper.contains("TIMESTAMP") {
        return "timestamp";
    }

    if type_upper.contains("NUMERIC") || type_upper.contains("DECIMAL") {
        return "numeric";
    }

    if type_upper.contains("UUID") {
        return "uuid";
    }

    if type_upper.contains("JSON") {
        return "json";
    }

    "text"
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib sqlite_type_info::tests -- --nocapture`
Expected: All PASS

- [ ] **Step 5: Commit**

```bash
git add src/types/sqlite_type_info.rs
git commit -m "feat: add sqlite_type_to_pg_type_name helper (#68)"
```

---

### Task 2: Add `get_column_types_from_pragma` to DbHandler

**Files:**
- Modify: `src/session/db_handler.rs:2272` (after `get_schema_type_with_session`)

- [ ] **Step 1: Add the method**

Add after the closing `}` of `get_schema_type_with_session` (after line 2272) in `src/session/db_handler.rs`:

```rust
    /// Get column types from PRAGMA table_info for tables without __pgsqlite_schema metadata.
    /// Returns a HashMap mapping column names to PostgreSQL type name strings.
    pub async fn get_column_types_from_pragma(
        &self,
        session_id: &Uuid,
        table_name: &str,
    ) -> Result<std::collections::HashMap<String, String>, PgSqliteError> {
        let table_name = table_name.to_string();
        self.with_session_connection(session_id, move |conn| {
            let mut result = std::collections::HashMap::new();
            let query = format!("PRAGMA table_info(\"{}\")", table_name);
            let mut stmt = conn.prepare(&query)?;
            let mut rows = stmt.query_map([], |row| {
                let col_name: String = row.get(1)?;
                let col_type: String = row.get(2)?;
                Ok((col_name, col_type))
            })?;
            while let Some(Ok((col_name, col_type))) = rows.next() {
                let pg_type_name = crate::types::sqlite_type_info::sqlite_type_to_pg_type_name(&col_type);
                result.insert(col_name, pg_type_name.to_string());
            }
            Ok(result)
        }).await
    }
```

- [ ] **Step 2: Run build to verify it compiles**

Run: `cargo check`
Expected: No errors (warnings OK)

- [ ] **Step 3: Commit**

```bash
git add src/session/db_handler.rs
git commit -m "feat: add get_column_types_from_pragma to DbHandler (#68)"
```

---

### Task 3: Add PRAGMA fallback in executor.rs

**Files:**
- Modify: `src/query/executor.rs:1088-1155` (schema_types population loop)

- [ ] **Step 1: Add PRAGMA cache and fallback**

In `src/query/executor.rs`, at line 1088, change the `schema_types` population block from:

```rust
            if let Some(ref table) = table_name {
                debug!("Type inference: Found table name '{}', looking up schema for {} columns", table, response.columns.len());

                // Extract column mappings from query if possible
                let column_mappings = extract_column_mappings_from_query(query, table);

                // Fetch types for actual columns
                for col_name in &response.columns {
                    // Try direct lookup first
                    if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, col_name).await {
```

to:

```rust
            if let Some(ref table) = table_name {
                debug!("Type inference: Found table name '{}', looking up schema for {} columns", table, response.columns.len());

                // Pre-fetch PRAGMA table_info as fallback for tables without __pgsqlite_schema
                let pragma_types = db.get_column_types_from_pragma(&session.id, table).await.unwrap_or_default();

                // Extract column mappings from query if possible
                let column_mappings = extract_column_mappings_from_query(query, table);

                // Fetch types for actual columns
                for col_name in &response.columns {
                    // Try direct lookup first
                    if let Ok(Some(pg_type)) = db.get_schema_type_with_session(&session.id, table, col_name).await {
```

Then add a PRAGMA fallback at the end of the `for col_name` loop body — after line 1163 (close of the `else` block) and before line 1164 (close of the `for` loop). This placement is at the `for` loop level so it catches columns that went through ANY branch (direct lookup, column_mappings, alias resolution) and still weren't found:

```rust
                    // PRAGMA table_info fallback for pre-existing SQLite tables
                    if !schema_types.contains_key(col_name) {
                        if let Some(pg_type) = pragma_types.get(col_name) {
                            debug!("Type inference: Found PRAGMA type for '{}.{}' -> {}", table, col_name, pg_type);
                            schema_types.insert(col_name.clone(), pg_type.clone());
                        }
                    }
```

This goes between lines 1163 and 1164 (just before the `for` loop's closing `}`).

- [ ] **Step 2: Run tests**

Run: `cargo test`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add src/query/executor.rs
git commit -m "feat: add PRAGMA type fallback in simple query protocol (#68)"
```

---

### Task 4: Add PRAGMA fallback in extended.rs schema_types (2 locations)

**Files:**
- Modify: `src/query/extended.rs:563-640` (Parse-time schema_types)
- Modify: `src/query/extended.rs:4487-4511` (Execute-time schema_types)

- [ ] **Step 1: Add PRAGMA fallback to Parse-time schema_types (line 563)**

In `src/query/extended.rs`, at line 564, change:

```rust
                        if let Some(ref table) = table_name {
                            info!("PARSE: Fetching schema types for table '{}'", table);
                            // For aliased columns, try to find the source column
                            for col_name in &response.columns {
```

to:

```rust
                        if let Some(ref table) = table_name {
                            info!("PARSE: Fetching schema types for table '{}'", table);
                            let pragma_types = db.get_column_types_from_pragma(&session.id, table).await.unwrap_or_default();
                            // For aliased columns, try to find the source column
                            for col_name in &response.columns {
```

Then add a PRAGMA fallback at the end of the `for col_name` loop body — after line 648 (close of the `else` block) and before line 649 (close of the `for` loop). This placement is at the `for` loop level:

```rust
                                // PRAGMA table_info fallback
                                if !schema_types.contains_key(col_name) {
                                    if let Some(pg_type) = pragma_types.get(col_name) {
                                        info!("PARSE: Found PRAGMA type for '{}.{}' -> {}", table, col_name, pg_type);
                                        schema_types.insert(col_name.clone(), pg_type.clone());
                                    }
                                }
```

This goes between lines 648 and 649 (just before the `for` loop's closing `}`).

- [ ] **Step 2: Add PRAGMA fallback to Execute-time schema_types (line 4487)**

In `src/query/extended.rs`, at line 4488, change:

```rust
                if let Some(ref table) = table_name {
                    for col_name in &response.columns {
```

to:

```rust
                if let Some(ref table) = table_name {
                    let pragma_types = db.get_column_types_from_pragma(&session.id, table).await.unwrap_or_default();
                    for col_name in &response.columns {
```

Then add a PRAGMA fallback at the end of the `for col_name` loop body — before line 4511 (the `for` loop's closing `}`):

```rust
                        // PRAGMA table_info fallback
                        if !schema_types.contains_key(col_name) {
                            if let Some(pg_type) = pragma_types.get(col_name) {
                                info!("Found PRAGMA type for '{}.{}' -> {}", table, col_name, pg_type);
                                schema_types.insert(col_name.clone(), pg_type.clone());
                            }
                        }
```

- [ ] **Step 3: Run tests**

Run: `cargo test`
Expected: All pass

- [ ] **Step 4: Commit**

```bash
git add src/query/extended.rs
git commit -m "feat: add PRAGMA type fallback in extended protocol schema_types (#68)"
```

---

### Task 5: Add PRAGMA fallback in extended.rs inferred_types

**Files:**
- Modify: `src/query/extended.rs:836-903` (Parse-time inferred_types fallback)

- [ ] **Step 1: Add PRAGMA lookup at inferred_types fallback points**

The `inferred_types` construction (lines 654-912) has 4 TEXT fallback points where a table name is known but `get_schema_type_with_session` returned `None`. At each of these, insert a PRAGMA lookup before defaulting to TEXT.

First, add a `pragma_types` cache at the start of the `inferred_types` loop. Find line 654:

```rust
                        let mut inferred_types = Vec::new();
```

Add after it:

```rust
                        let pragma_types_for_inferred = if let Some(ref table) = table_name {
                            db.get_column_types_from_pragma(&session.id, table).await.unwrap_or_default()
                        } else {
                            std::collections::HashMap::new()
                        };
```

Then change the 4 fallback points. At lines 848-856 (after `extract_source_table_column_for_alias` lookup), change:

```rust
                                        Ok(None) => {
                                            info!("Column '{}': no schema type found for '{}.{}', defaulting to text",
                                                  col_name, source_table, source_col);
                                            inferred_types.push(PgType::Text.to_oid());
                                        }
                                        Err(_) => {
                                            // Schema lookup error, defaulting to text
                                            inferred_types.push(PgType::Text.to_oid());
                                        }
```

to:

```rust
                                        Ok(None) => {
                                            if let Some(pg_type) = pragma_types_for_inferred.get(col_name) {
                                                let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type);
                                                info!("Column '{}': resolved type from PRAGMA -> {} (OID {})", col_name, pg_type, type_oid);
                                                inferred_types.push(type_oid);
                                            } else {
                                                info!("Column '{}': no schema type found for '{}.{}', defaulting to text",
                                                      col_name, source_table, source_col);
                                                inferred_types.push(PgType::Text.to_oid());
                                            }
                                        }
                                        Err(_) => {
                                            if let Some(pg_type) = pragma_types_for_inferred.get(col_name) {
                                                let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type);
                                                inferred_types.push(type_oid);
                                            } else {
                                                inferred_types.push(PgType::Text.to_oid());
                                            }
                                        }
```

At lines 896-904 (after `extract_table_name_from_select` + `get_schema_type_with_session`), change:

```rust
                                                Ok(None) => {
                                                    info!("Column '{}': no schema type found for '{}.{}', defaulting to text",
                                                          col_name, table_name, col_name);
                                                    inferred_types.push(PgType::Text.to_oid());
                                                }
                                                Err(_) => {
                                                    // Schema lookup error, defaulting to text
                                                    inferred_types.push(PgType::Text.to_oid());
                                                }
```

to:

```rust
                                                Ok(None) => {
                                                    if let Some(pg_type) = pragma_types_for_inferred.get(col_name) {
                                                        let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type);
                                                        info!("Column '{}': resolved type from PRAGMA -> {} (OID {})", col_name, pg_type, type_oid);
                                                        inferred_types.push(type_oid);
                                                    } else {
                                                        info!("Column '{}': no schema type found for '{}.{}', defaulting to text",
                                                              col_name, table_name, col_name);
                                                        inferred_types.push(PgType::Text.to_oid());
                                                    }
                                                }
                                                Err(_) => {
                                                    if let Some(pg_type) = pragma_types_for_inferred.get(col_name) {
                                                        let type_oid = crate::types::SchemaTypeMapper::pg_type_string_to_oid(pg_type);
                                                        inferred_types.push(type_oid);
                                                    } else {
                                                        inferred_types.push(PgType::Text.to_oid());
                                                    }
                                                }
```

- [ ] **Step 2: Run tests**

Run: `cargo test`
Expected: All pass

- [ ] **Step 3: Commit**

```bash
git add src/query/extended.rs
git commit -m "feat: add PRAGMA type fallback in extended protocol inferred_types (#68)"
```

---

### Task 6: Final verification

- [ ] **Step 1: Run pre-commit checklist**

```bash
cargo check && cargo clippy && cargo build && cargo test
```

Expected: All pass with no errors.

- [ ] **Step 2: Verify the fix conceptually**

After our changes, for a pre-existing SQLite table with `CREATE TABLE test (id INTEGER, name TEXT, score REAL, active BOOLEAN)`:

1. `get_column_types_from_pragma` returns `{"id": "integer", "name": "text", "score": "double precision", "active": "boolean"}`
2. In executor.rs, `schema_types` gets populated from PRAGMA when `__pgsqlite_schema` has no entry
3. `pg_type_string_to_oid("integer")` → 23, `pg_type_string_to_oid("double precision")` → 701, `pg_type_string_to_oid("boolean")` → 16
4. FieldDescription sends correct OIDs on the wire
