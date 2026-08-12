# Namespace-aware `information_schema` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `information_schema.tables` and `information_schema.columns` report pgsqlite's own catalog relations under `pg_catalog` / `information_schema` instead of as user tables in `public`, by serving both from SQLite views rather than hand-rolled Rust handlers.

**Architecture:** An exact-name registry of the relations pgsqlite's migrations create is exposed to SQL as a UDF. Migration v29 rebuilds the two `information_schema` views to use it, plus four more UDFs that resolve a column's PostgreSQL type from `__pgsqlite_schema`. `SchemaPrefixTranslator` then rewrites `information_schema.tables` → `information_schema_tables` so the existing fall-through path in `CatalogInterceptor` executes the query against the view, and the Rust handlers are deleted.

**Tech Stack:** Rust, rusqlite (scalar UDFs via `create_scalar_function`), SQLite views, tokio-postgres for integration tests.

**Spec:** `docs/superpowers/specs/2026-08-11-information-schema-namespace-design.md`
**Issue:** [#88](https://github.com/erans/pgsqlite/issues/88). Follow-up: [#102](https://github.com/erans/pgsqlite/issues/102).

## Global Constraints

- **Pre-commit checklist** (from `CLAUDE.md`) before every commit: `cargo check` (no errors or warnings), `cargo clippy`, `cargo build`, `cargo test`.
- **Never infer types from column names.** Only explicit PG type declarations, `__pgsqlite_schema`, `PRAGMA table_info`, or explicit casts.
- **Datetime storage is INTEGER** (microseconds/days since epoch). A catalog surface must never report `integer` for a `TIMESTAMP` column — that leaks storage detail.
- **`LIKE '__pgsqlite_%'` is wrong** — `_` is a LIKE wildcard, so it matches unrelated names. New SQL uses `substr(name, 1, 11) <> '__pgsqlite_'`, the form #85 established.
- **Do not modify `pg_class` or `pg_attribute`.** Both are deferred to #102. If a task seems to need them, stop and re-read the spec's Non-goals.
- **Migration v29 is unreleased** across these tasks, so Tasks 3 and 5 both edit it. Any local test database created between those tasks must be deleted, or v29 will be recorded as applied and the second half will not run. Integration tests create a fresh DB per run and are unaffected.
- **`Migration::down` is never executed.** There is no rollback path anywhere in `src/migration/` — the field is stored and never read. v29 supplies a `down` to match repo convention, but do not write a test that exercises it and do not claim the migration was verified reversible.
- Namespace OIDs are fixed by the v28 `pg_namespace` view: `11` = `pg_catalog`, `2200` = `public`, `13000` = `information_schema`.

## File Structure

**Created:**
- `src/catalog/internal_relations.rs` — the exact-name list of relations pgsqlite's migrations create, and `relnamespace()` over it. No SQL, no I/O; a pure lookup table so it can be unit-tested and read at a glance.
- `src/catalog/column_type_info.rs` — `pg_column_info()`, the PG-type-string → `information_schema` column metadata mapping moved out of the handler being deleted.
- `tests/information_schema_namespace_test.rs` — integration tests for #88.

**Modified:**
- `src/catalog/mod.rs` — declare the two new modules.
- `src/functions/catalog_functions.rs` — five new UDFs wrapping the two new modules.
- `src/migration/registry.rs` — migration v29; register it in the `MIGRATIONS` block.
- `src/translator/schema_prefix_translator.rs` — rewrite `information_schema.{tables,columns}` to the underscore view names.
- `src/catalog/query_interceptor.rs` — delete two dispatch arms, two handler functions, and one private helper.
- `src/session/db_handler.rs`, `src/query/extended.rs` — delete the now-unreachable duplicate handlers.
- `tests/migration_test.rs` — v29 added to the expected migration list.
- `CLAUDE.md` — Current Migrations list.

The two new `src/catalog/` modules are deliberately separate: one answers "is this relation ours?", the other "what does this type look like to `information_schema`?". Neither knows about the other, and both are pure functions over their input, which is what makes the UDF wrappers trivial.

---

### Task 1: Internal-relation registry and `__pgsqlite_relnamespace`

**Files:**
- Create: `src/catalog/internal_relations.rs`
- Modify: `src/catalog/mod.rs:17` (add module declaration)
- Modify: `src/functions/catalog_functions.rs` (add UDF at end of `register_catalog_functions`)
- Test: unit tests inline in `src/catalog/internal_relations.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pgsqlite::catalog::internal_relations::relnamespace(name: &str) -> i32`
  - `pgsqlite::catalog::internal_relations::INTERNAL_PG_CATALOG_RELATIONS: &[&str]`
  - `pgsqlite::catalog::internal_relations::INTERNAL_INFORMATION_SCHEMA_RELATIONS: &[&str]`
  - SQL function `__pgsqlite_relnamespace(name TEXT) -> INTEGER` returning `11`, `13000`, `2200`, or `NULL` for `NULL` input.

- [ ] **Step 1: Write the failing test**

Create `src/catalog/internal_relations.rs` containing only the test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_pg_relations_map_to_pg_catalog() {
        assert_eq!(relnamespace("pg_class"), PG_CATALOG_NAMESPACE);
        assert_eq!(relnamespace("pg_constraint"), PG_CATALOG_NAMESPACE);
        assert_eq!(relnamespace("idx_enum_values_label"), PG_CATALOG_NAMESPACE);
    }

    #[test]
    fn internal_information_schema_relations_map_to_information_schema() {
        assert_eq!(relnamespace("information_schema_tables"), INFORMATION_SCHEMA_NAMESPACE);
        assert_eq!(relnamespace("information_schema_columns"), INFORMATION_SCHEMA_NAMESPACE);
    }

    #[test]
    fn user_tables_map_to_public() {
        assert_eq!(relnamespace("customers"), PUBLIC_NAMESPACE);
    }

    /// The reason this is an exact-name list rather than a `LIKE 'pg\_%'` test:
    /// a user table whose name merely starts with `pg_` is not ours. See #102.
    #[test]
    fn user_tables_named_like_catalog_relations_map_to_public() {
        assert_eq!(relnamespace("pg_myreport"), PUBLIC_NAMESPACE);
        assert_eq!(relnamespace("information_schema_export"), PUBLIC_NAMESPACE);
        assert_eq!(relnamespace("idx_customers_email"), PUBLIC_NAMESPACE);
    }

    #[test]
    fn lists_have_no_duplicates_and_do_not_overlap() {
        let mut all: Vec<&str> = INTERNAL_PG_CATALOG_RELATIONS
            .iter()
            .chain(INTERNAL_INFORMATION_SCHEMA_RELATIONS.iter())
            .copied()
            .collect();
        let total = all.len();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), total, "duplicate entry in the internal relation lists");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib internal_relations`
Expected: FAIL — compile error, `relnamespace` and the constants are not defined.

- [ ] **Step 3: Write the implementation**

Prepend to `src/catalog/internal_relations.rs`, above the test module:

```rust
//! Exact names of the relations pgsqlite's own migrations create.
//!
//! Prefix matching is not sufficient in either direction: it claims user tables
//! that merely start with `pg_`, and it misses the `idx_*` indexes migrations
//! create on `__pgsqlite_*` tables. Both failure modes are user-visible, so this
//! list is exact and is guarded against drift by
//! `tests/information_schema_namespace_test.rs::internal_relation_list_matches_migrated_database`.
//!
//! Adding a catalog relation to `src/migration/registry.rs` means adding it here.

/// Namespace OIDs, fixed by the `pg_namespace` view in migration v28.
pub const PG_CATALOG_NAMESPACE: i32 = 11;
pub const PUBLIC_NAMESPACE: i32 = 2200;
pub const INFORMATION_SCHEMA_NAMESPACE: i32 = 13000;

/// Relations pgsqlite creates that PostgreSQL keeps in `pg_catalog`.
pub const INTERNAL_PG_CATALOG_RELATIONS: &[&str] = &[
    // Real tables
    "pg_attrdef",
    "pg_constraint",
    "pg_depend",
    "pg_index",
    // Views
    "pg_am",
    "pg_attribute",
    "pg_class",
    "pg_database",
    "pg_description",
    "pg_enum",
    "pg_foreign_data_wrapper",
    "pg_namespace",
    "pg_proc",
    "pg_roles",
    "pg_stat_activity",
    "pg_stat_all_indexes",
    "pg_stat_all_tables",
    "pg_stat_database",
    "pg_stat_user_indexes",
    "pg_stat_user_tables",
    "pg_type",
    "pg_user",
    // Indexes on __pgsqlite_* tables. Not named __pgsqlite_*, so no prefix
    // filter catches them; they leak into \di today (#102).
    "idx_array_types_table",
    "idx_comments_lookup",
    "idx_datetime_cache_table",
    "idx_enum_values_label",
    "idx_enum_values_type",
    "idx_fts_metadata_table",
    "idx_numeric_constraints_table",
    "idx_string_constraints_table",
];

/// Relations pgsqlite creates that PostgreSQL keeps in `information_schema`.
pub const INTERNAL_INFORMATION_SCHEMA_RELATIONS: &[&str] = &[
    "information_schema_columns",
    "information_schema_key_column_usage",
    "information_schema_referential_constraints",
    "information_schema_schemata",
    "information_schema_table_constraints",
    "information_schema_tables",
];

/// Namespace OID for a relation name. Anything unlisted is a user relation.
///
/// Unlisted defaults to `public` deliberately: the failure mode is showing an
/// internal relation, never hiding a user's table.
pub fn relnamespace(name: &str) -> i32 {
    if INTERNAL_PG_CATALOG_RELATIONS.contains(&name) {
        PG_CATALOG_NAMESPACE
    } else if INTERNAL_INFORMATION_SCHEMA_RELATIONS.contains(&name) {
        INFORMATION_SCHEMA_NAMESPACE
    } else {
        PUBLIC_NAMESPACE
    }
}
```

Add to `src/catalog/mod.rs` after line 17 (`pub mod constraint_populator;`):

```rust
pub mod internal_relations;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib internal_relations`
Expected: PASS, 5 tests.

- [ ] **Step 5: Register the UDF**

In `src/functions/catalog_functions.rs`, inside `register_catalog_functions`, before its final `Ok(())`:

```rust
    // __pgsqlite_relnamespace(name) - namespace OID for a relation.
    // Lets catalog views ask "is this relation ours?" by exact name rather than
    // by prefix. Returns NULL for NULL input so a view row degrades rather than
    // failing the query.
    conn.create_scalar_function(
        "__pgsqlite_relnamespace",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let name: Option<String> = ctx.get(0)?;
            Ok(name.map(|n| crate::catalog::internal_relations::relnamespace(&n)))
        },
    )?;
```

- [ ] **Step 6: Verify the UDF is reachable from SQL**

Add to the test module in `src/catalog/internal_relations.rs`:

```rust
    #[test]
    fn udf_is_callable_from_sql() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::functions::register_all_functions(&conn).unwrap();

        let ns: i32 = conn
            .query_row("SELECT __pgsqlite_relnamespace('pg_class')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ns, PG_CATALOG_NAMESPACE);

        let ns: i32 = conn
            .query_row("SELECT __pgsqlite_relnamespace('pg_myreport')", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ns, PUBLIC_NAMESPACE);

        let ns: Option<i32> = conn
            .query_row("SELECT __pgsqlite_relnamespace(NULL)", [], |r| r.get(0))
            .unwrap();
        assert_eq!(ns, None);
    }
```

Run: `cargo test --lib internal_relations`
Expected: PASS, 6 tests.

- [ ] **Step 7: Pre-commit checklist**

Run: `cargo check && cargo clippy && cargo build && cargo test`
Expected: no errors, no new warnings, all tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/catalog/internal_relations.rs src/catalog/mod.rs src/functions/catalog_functions.rs
git commit -m "feat: exact-name registry of pgsqlite's internal catalog relations (#88)

Prefix matching fails both ways: 'pg\_%' claims a user table named
pg_myreport, and nothing catches the 8 idx_* indexes migrations create on
__pgsqlite_* tables. Exact list of all 36, exposed to SQL as
__pgsqlite_relnamespace(name) for the catalog views to use."
```

---

### Task 2: `pg_column_info` and the four type UDFs

**Files:**
- Create: `src/catalog/column_type_info.rs`
- Modify: `src/catalog/mod.rs` (add module declaration)
- Modify: `src/functions/catalog_functions.rs` (four UDFs)
- Test: unit tests inline in `src/catalog/column_type_info.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pgsqlite::catalog::column_type_info::PgColumnInfo { data_type: String, character_maximum_length: Option<i32>, numeric_precision: Option<i32>, numeric_scale: Option<i32> }`
  - `pgsqlite::catalog::column_type_info::pg_column_info(pg_type: &str) -> PgColumnInfo`
  - SQL functions `__pgsqlite_pg_data_type(TEXT) -> TEXT`, `__pgsqlite_char_max_length(TEXT) -> INTEGER`, `__pgsqlite_numeric_precision(TEXT) -> INTEGER`, `__pgsqlite_numeric_scale(TEXT) -> INTEGER`.

This is a move-and-fix of the private `map_sqlite_type_to_pg_column_info` at `src/catalog/query_interceptor.rs:2190`, which Task 6 deletes. The move is what keeps `character_maximum_length`, `numeric_precision` and `numeric_scale` populated once the handler is gone. The fixes are the rows that measurement showed wrong: `SERIAL`, `TIMESTAMPTZ`, `TIME`/`TIMETZ`, and array types.

- [ ] **Step 1: Write the failing test**

Create `src/catalog/column_type_info.rs` containing only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn dt(pg_type: &str) -> String {
        pg_column_info(pg_type).data_type
    }

    /// Types the deleted handler already got right. These must not regress.
    #[test]
    fn preserves_correct_handler_behavior() {
        assert_eq!(dt("INTEGER"), "integer");
        assert_eq!(dt("TEXT"), "text");
        assert_eq!(dt("BOOLEAN"), "boolean");
        assert_eq!(dt("UUID"), "uuid");
        assert_eq!(dt("JSONB"), "jsonb");
        assert_eq!(dt("NUMERIC(10,2)"), "numeric");
        assert_eq!(dt("VARCHAR(50)"), "character varying");
        assert_eq!(dt("TIMESTAMP"), "timestamp without time zone");
        assert_eq!(dt("DATE"), "date");
        assert_eq!(dt("BLOB"), "bytea");
    }

    /// Types the deleted handler got wrong. Measured in the spec.
    #[test]
    fn fixes_types_the_handler_reported_as_text() {
        assert_eq!(dt("SERIAL"), "integer");
        assert_eq!(dt("BIGSERIAL"), "bigint");
        assert_eq!(dt("TIMESTAMPTZ"), "timestamp with time zone");
        assert_eq!(dt("TIMESTAMP WITH TIME ZONE"), "timestamp with time zone");
        assert_eq!(dt("TIMETZ"), "time with time zone");
        assert_eq!(dt("TIME WITH TIME ZONE"), "time with time zone");
        assert_eq!(dt("TIME"), "time without time zone");
    }

    #[test]
    fn array_types_report_array() {
        assert_eq!(dt("TEXT[]"), "ARRAY");
        assert_eq!(dt("INTEGER[]"), "ARRAY");
        assert_eq!(dt("NUMERIC(10,2)[]"), "ARRAY");
    }

    #[test]
    fn character_maximum_length_comes_from_the_modifier() {
        assert_eq!(pg_column_info("VARCHAR(50)").character_maximum_length, Some(50));
        assert_eq!(pg_column_info("CHAR(8)").character_maximum_length, Some(8));
        assert_eq!(pg_column_info("VARCHAR").character_maximum_length, None);
        assert_eq!(pg_column_info("TEXT").character_maximum_length, None);
    }

    #[test]
    fn numeric_precision_and_scale_come_from_the_modifier() {
        let info = pg_column_info("NUMERIC(10,2)");
        assert_eq!(info.numeric_precision, Some(10));
        assert_eq!(info.numeric_scale, Some(2));

        let info = pg_column_info("DECIMAL(38)");
        assert_eq!(info.numeric_precision, Some(38));
        assert_eq!(info.numeric_scale, Some(0));

        let info = pg_column_info("NUMERIC");
        assert_eq!(info.numeric_precision, None);
        assert_eq!(info.numeric_scale, None);
    }

    /// PostgreSQL reports precision and scale for integer types too.
    #[test]
    fn integer_types_report_binary_precision() {
        let info = pg_column_info("INTEGER");
        assert_eq!(info.numeric_precision, Some(32));
        assert_eq!(info.numeric_scale, Some(0));

        let info = pg_column_info("BIGINT");
        assert_eq!(info.numeric_precision, Some(64));
        assert_eq!(info.numeric_scale, Some(0));
    }

    /// Text is the safe fallback: matches both the deleted handler's final
    /// branch and the view's `ELSE 'text'`. ENUM types land here (#88 non-goal).
    #[test]
    fn unknown_types_fall_back_to_text() {
        assert_eq!(dt("my_enum_type"), "text");
        assert_eq!(dt(""), "text");
    }

    #[test]
    fn case_is_insensitive() {
        assert_eq!(dt("varchar(50)"), "character varying");
        assert_eq!(dt("TimestampTZ"), "timestamp with time zone");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib column_type_info`
Expected: FAIL — compile error, `pg_column_info` is not defined.

- [ ] **Step 3: Write the implementation**

Prepend to `src/catalog/column_type_info.rs`:

```rust
//! PostgreSQL type string to `information_schema` column metadata.
//!
//! Moved out of `CatalogInterceptor::map_sqlite_type_to_pg_column_info` when the
//! `information_schema.columns` handler was deleted, so the views could keep
//! reporting `character_maximum_length`, `numeric_precision` and `numeric_scale`.
//!
//! Input is the declared PostgreSQL type from `__pgsqlite_schema.pg_type`
//! (`VARCHAR(50)`, `NUMERIC(10,2)`, `TIMESTAMPTZ`), falling back to the SQLite
//! declared type for databases created outside pgsqlite. Deriving from the
//! string rather than an OID is deliberate — an OID cannot carry the modifier.

/// The four `information_schema.columns` fields that depend on a column's type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgColumnInfo {
    pub data_type: String,
    pub character_maximum_length: Option<i32>,
    pub numeric_precision: Option<i32>,
    pub numeric_scale: Option<i32>,
}

impl PgColumnInfo {
    fn plain(data_type: &str) -> Self {
        Self {
            data_type: data_type.to_string(),
            character_maximum_length: None,
            numeric_precision: None,
            numeric_scale: None,
        }
    }

    fn integral(data_type: &str, precision: i32) -> Self {
        Self {
            data_type: data_type.to_string(),
            character_maximum_length: None,
            numeric_precision: Some(precision),
            numeric_scale: Some(0),
        }
    }
}

/// Split `NUMERIC(10,2)` into `("NUMERIC", ["10", "2"])`.
fn split_modifier(upper: &str) -> (&str, Vec<&str>) {
    match (upper.find('('), upper.rfind(')')) {
        (Some(open), Some(close)) if close > open => {
            let base = upper[..open].trim();
            let params = upper[open + 1..close]
                .split(',')
                .map(|p| p.trim())
                .collect();
            (base, params)
        }
        _ => (upper.trim(), Vec::new()),
    }
}

pub fn pg_column_info(pg_type: &str) -> PgColumnInfo {
    let upper = pg_type.trim().to_uppercase();

    // Array types report `ARRAY`, with the element type in udt_name in real
    // PostgreSQL. Checked before modifiers so `NUMERIC(10,2)[]` is an array.
    if upper.ends_with("[]") {
        return PgColumnInfo::plain("ARRAY");
    }

    let (base, params) = split_modifier(&upper);
    let param = |i: usize| params.get(i).and_then(|p| p.parse::<i32>().ok());

    match base {
        "VARCHAR" | "CHARACTER VARYING" => PgColumnInfo {
            data_type: "character varying".to_string(),
            character_maximum_length: param(0),
            numeric_precision: None,
            numeric_scale: None,
        },
        "CHAR" | "CHARACTER" | "BPCHAR" => PgColumnInfo {
            data_type: "character".to_string(),
            character_maximum_length: param(0),
            numeric_precision: None,
            numeric_scale: None,
        },
        "NUMERIC" | "DECIMAL" => PgColumnInfo {
            data_type: "numeric".to_string(),
            character_maximum_length: None,
            numeric_precision: param(0),
            // A precision with no scale means scale 0; no precision means
            // unconstrained, where PostgreSQL reports NULL for both.
            numeric_scale: param(1).or(param(0).map(|_| 0)),
        },

        "SMALLINT" | "INT2" | "SMALLSERIAL" => PgColumnInfo::integral("smallint", 16),
        "INTEGER" | "INT" | "INT4" | "SERIAL" => PgColumnInfo::integral("integer", 32),
        "BIGINT" | "INT8" | "BIGSERIAL" => PgColumnInfo::integral("bigint", 64),

        "REAL" | "FLOAT4" => PgColumnInfo {
            data_type: "real".to_string(),
            character_maximum_length: None,
            numeric_precision: Some(24),
            numeric_scale: None,
        },
        "DOUBLE PRECISION" | "FLOAT8" | "FLOAT" | "DOUBLE" => PgColumnInfo {
            data_type: "double precision".to_string(),
            character_maximum_length: None,
            numeric_precision: Some(53),
            numeric_scale: None,
        },

        "TEXT" => PgColumnInfo::plain("text"),
        "BYTEA" | "BLOB" => PgColumnInfo::plain("bytea"),
        "BOOLEAN" | "BOOL" => PgColumnInfo::plain("boolean"),
        "UUID" => PgColumnInfo::plain("uuid"),
        "JSON" => PgColumnInfo::plain("json"),
        "JSONB" => PgColumnInfo::plain("jsonb"),
        "MONEY" => PgColumnInfo::plain("money"),
        "INTERVAL" => PgColumnInfo::plain("interval"),

        "DATE" => PgColumnInfo::plain("date"),
        "TIME" | "TIME WITHOUT TIME ZONE" => PgColumnInfo::plain("time without time zone"),
        "TIMETZ" | "TIME WITH TIME ZONE" => PgColumnInfo::plain("time with time zone"),
        "TIMESTAMP" | "DATETIME" | "TIMESTAMP WITHOUT TIME ZONE" => {
            PgColumnInfo::plain("timestamp without time zone")
        }
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => {
            PgColumnInfo::plain("timestamp with time zone")
        }

        // Unknown types, including ENUMs, report text. Matches the deleted
        // handler's final branch and the view's previous `ELSE 'text'`.
        _ => PgColumnInfo::plain("text"),
    }
}
```

Add to `src/catalog/mod.rs`:

```rust
pub mod column_type_info;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib column_type_info`
Expected: PASS, 8 tests.

- [ ] **Step 5: Register the four UDFs**

In `src/functions/catalog_functions.rs`, after the `__pgsqlite_relnamespace` registration:

```rust
    // Column type metadata for the information_schema.columns view. Four
    // scalar functions rather than one, because a SQLite scalar UDF returns a
    // single value and the view needs four fields from the same input.
    macro_rules! register_column_info_fn {
        ($name:literal, $field:ident, $ty:ty) => {
            conn.create_scalar_function(
                $name,
                1,
                FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
                |ctx| {
                    let pg_type: Option<String> = ctx.get(0)?;
                    let value: Option<$ty> = pg_type.and_then(|t| {
                        crate::catalog::column_type_info::pg_column_info(&t).$field.into()
                    });
                    Ok(value)
                },
            )?;
        };
    }

    conn.create_scalar_function(
        "__pgsqlite_pg_data_type",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let pg_type: Option<String> = ctx.get(0)?;
            Ok(pg_type.map(|t| crate::catalog::column_type_info::pg_column_info(&t).data_type))
        },
    )?;

    register_column_info_fn!("__pgsqlite_char_max_length", character_maximum_length, i32);
    register_column_info_fn!("__pgsqlite_numeric_precision", numeric_precision, i32);
    register_column_info_fn!("__pgsqlite_numeric_scale", numeric_scale, i32);
```

Note on the macro: `pg_column_info(...).$field` is already `Option<i32>`, so `.into()` is an identity conversion that lets `and_then` accept it. If the compiler objects, replace `.into()` with nothing and use `.and_then(|t| pg_column_info(&t).$field)` directly — that is the intended semantics either way: `NULL` in gives `NULL` out, and a type with no modifier gives `NULL`.

- [ ] **Step 6: Verify the UDFs are reachable from SQL**

Add to the test module in `src/catalog/column_type_info.rs`:

```rust
    #[test]
    fn udfs_are_callable_from_sql() {
        let conn = rusqlite::Connection::open_in_memory().unwrap();
        crate::functions::register_all_functions(&conn).unwrap();

        let (dt, len): (String, Option<i32>) = conn
            .query_row(
                "SELECT __pgsqlite_pg_data_type('VARCHAR(50)'), __pgsqlite_char_max_length('VARCHAR(50)')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(dt, "character varying");
        assert_eq!(len, Some(50));

        let (p, s): (Option<i32>, Option<i32>) = conn
            .query_row(
                "SELECT __pgsqlite_numeric_precision('NUMERIC(10,2)'), __pgsqlite_numeric_scale('NUMERIC(10,2)')",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!((p, s), (Some(10), Some(2)));

        let dt: Option<String> = conn
            .query_row("SELECT __pgsqlite_pg_data_type(NULL)", [], |r| r.get(0))
            .unwrap();
        assert_eq!(dt, None);
    }
```

Run: `cargo test --lib column_type_info`
Expected: PASS, 9 tests.

- [ ] **Step 7: Pre-commit checklist**

Run: `cargo check && cargo clippy && cargo build && cargo test`
Expected: no errors, no new warnings, all tests pass.

- [ ] **Step 8: Commit**

```bash
git add src/catalog/column_type_info.rs src/catalog/mod.rs src/functions/catalog_functions.rs
git commit -m "feat: pg_column_info and type UDFs for information_schema.columns (#88)

Moves map_sqlite_type_to_pg_column_info out of the handler about to be
deleted, so the view can keep populating character_maximum_length,
numeric_precision and numeric_scale. Fixes the rows measurement showed
wrong: SERIAL, TIMESTAMPTZ, TIME/TIMETZ and array types all reported text."
```

---

### Task 3: Migration v29 — rebuild both `information_schema` views

**Files:**
- Modify: `src/migration/registry.rs:37` (register v29) and end of file (define it)
- Modify: `tests/migration_test.rs:26,34` (expected migration list and version)
- Test: `tests/migration_test.rs`

**Interfaces:**
- Consumes: `__pgsqlite_relnamespace`, `__pgsqlite_pg_data_type`, `__pgsqlite_char_max_length`, `__pgsqlite_numeric_precision`, `__pgsqlite_numeric_scale` from Tasks 1 and 2.
- Produces: SQLite views `information_schema_tables` and `information_schema_columns` with corrected `table_schema` and column metadata. Column order and names are unchanged from v14 except where noted below.

Two column-order notes, both deliberate:

- `information_schema_tables` moves `is_insertable_into` from position 5 to position 10, where PostgreSQL puts it. The v14 view had it at 5; the Rust handler being deleted had it at 10. Matching the handler keeps `SELECT *` results stable for existing clients and matches real PostgreSQL.
- `information_schema_columns` keeps v14's 44 columns in v14's order, which already matches the handler and PostgreSQL.

**Both view bodies below were prototyped against a migrated database before this plan was written**, with SQL stand-ins for the UDFs. Confirmed: the lateral `JOIN pragma_table_info(m.name)` resolves, the `pg_namespace` join partitions relations 22 `pg_catalog` / 6 `information_schema` / 2 user, `COALESCE(s.pg_type, p.type)` yields the declared PostgreSQL types (`SERIAL`, `NUMERIC(10,2)`, `TEXT[]`, `TIMESTAMPTZ`) that `pg_column_info` expects, and `p.pk > 0` gives `is_nullable = 'NO'` for `SERIAL PRIMARY KEY`. If a query against these views returns nothing, suspect the UDFs or `trusted_schema`, not the join shape.

One environment note for manual poking: pgsqlite runs SQLite in WAL mode, so `cp foo.db bar.db` silently loses recent writes. Use `sqlite3 foo.db ".backup bar.db"`.

- [ ] **Step 1: Write the failing test**

Add to `tests/migration_test.rs`:

```rust
#[test]
fn test_v29_information_schema_views_are_namespace_aware() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("v29.db");

    let conn = Connection::open(&db_path).unwrap();
    let mut runner = MigrationRunner::new(conn);
    runner.run_pending_migrations().unwrap();
    let conn = runner.into_connection();

    // The views call UDFs, which the migration runner's connection does not have.
    pgsqlite::functions::register_all_functions(&conn).unwrap();
    conn.pragma_update(None, "trusted_schema", true).unwrap();

    conn.execute_batch(
        "CREATE TABLE customers (id INTEGER PRIMARY KEY, name VARCHAR(50), amount NUMERIC(10,2));",
    )
    .unwrap();

    // Internal relations are not in public.
    let internal_in_public: i32 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema_tables \
             WHERE table_schema = 'public' AND table_name LIKE 'pg\\_%' ESCAPE '\\'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(internal_in_public, 0, "pg_* relations still reported in public");

    let pg_class_schema: String = conn
        .query_row(
            "SELECT table_schema FROM information_schema_tables WHERE table_name = 'pg_class'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(pg_class_schema, "pg_catalog");

    let is_tables_schema: String = conn
        .query_row(
            "SELECT table_schema FROM information_schema_tables \
             WHERE table_name = 'information_schema_tables'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(is_tables_schema, "information_schema");

    // The user table is the only thing in public.
    let public_tables: Vec<String> = conn
        .prepare("SELECT table_name FROM information_schema_tables WHERE table_schema = 'public' ORDER BY table_name")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(public_tables, vec!["customers".to_string()]);

    // Column metadata the old view lost.
    let (dt, len): (String, Option<i32>) = conn
        .query_row(
            "SELECT data_type, character_maximum_length FROM information_schema_columns \
             WHERE table_name = 'customers' AND column_name = 'name'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(dt, "character varying");
    assert_eq!(len, Some(50));

    let (p, s): (Option<i32>, Option<i32>) = conn
        .query_row(
            "SELECT numeric_precision, numeric_scale FROM information_schema_columns \
             WHERE table_name = 'customers' AND column_name = 'amount'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!((p, s), (Some(10), Some(2)));

    // INTEGER PRIMARY KEY is NOT NULL, which the v14 view got wrong.
    let nullable: String = conn
        .query_row(
            "SELECT is_nullable FROM information_schema_columns \
             WHERE table_name = 'customers' AND column_name = 'id'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(nullable, "NO");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test migration_test test_v29_information_schema_views_are_namespace_aware`
Expected: FAIL — `assert_eq!(internal_in_public, 0)` reports a non-zero count, because v14's view hardcodes `'public'`.

- [ ] **Step 3: Register the migration**

In `src/migration/registry.rs`, after line 37 (`register_v28_pg_class_full_columns(&mut registry);`):

```rust
        register_v29_information_schema_namespace(&mut registry);
```

- [ ] **Step 4: Write the migration**

Append to `src/migration/registry.rs`:

```rust
/// Version 29: Namespace-aware information_schema views (#88)
///
/// The views are rebuilt rather than patched. `information_schema_tables`
/// derives `table_schema` from `__pgsqlite_relnamespace` rather than hardcoding
/// `'public'`, and reads the UDF directly rather than `pg_class.relnamespace` so
/// it does not inherit v28's `LIKE 'pg\_%'` heuristic (#102).
///
/// `information_schema_columns` reads sqlite_master, pragma_table_info and
/// __pgsqlite_schema directly rather than layering on pg_attribute, which keeps
/// this migration off a view every ORM reads and is what makes column_default,
/// is_nullable, character_maximum_length and numeric precision/scale reachable.
fn register_v29_information_schema_namespace(registry: &mut BTreeMap<u32, Migration>) {
    registry.insert(29, Migration {
        version: 29,
        name: "information_schema_namespace",
        description: "Namespace-aware information_schema.tables and .columns served from SQLite",
        up: MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_tables"#,
            r#"DROP VIEW IF EXISTS information_schema_columns"#,

            // Column order matches PostgreSQL: is_insertable_into is 10th, not
            // 5th as in v14. The deleted Rust handler already used this order.
            r#"
            CREATE VIEW information_schema_tables AS
            SELECT
                'main' as table_catalog,
                n.nspname as table_schema,
                c.relname as table_name,
                CASE c.relkind
                    WHEN 'r' THEN 'BASE TABLE'
                    WHEN 'v' THEN 'VIEW'
                    ELSE 'UNKNOWN'
                END as table_type,
                NULL as self_referencing_column_name,
                NULL as reference_generation,
                NULL as user_defined_type_catalog,
                NULL as user_defined_type_schema,
                NULL as user_defined_type_name,
                CASE c.relkind WHEN 'r' THEN 'YES' ELSE 'NO' END as is_insertable_into,
                'NO' as is_typed,
                NULL as commit_action
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = __pgsqlite_relnamespace(c.relname)
            WHERE c.relkind IN ('r', 'v')
            "#,

            // 44 columns in v14's order, which matches PostgreSQL.
            // substr(...) <> '__pgsqlite_' rather than LIKE '__pgsqlite_%':
            // `_` is a LIKE wildcard, so the LIKE form matches unrelated names.
            r#"
            CREATE VIEW information_schema_columns AS
            SELECT
                'main' as table_catalog,
                n.nspname as table_schema,
                m.name as table_name,
                p.name as column_name,
                p.cid + 1 as ordinal_position,
                p.dflt_value as column_default,
                CASE WHEN p."notnull" = 1 OR p.pk > 0 THEN 'NO' ELSE 'YES' END as is_nullable,
                __pgsqlite_pg_data_type(COALESCE(s.pg_type, p.type)) as data_type,
                __pgsqlite_char_max_length(COALESCE(s.pg_type, p.type)) as character_maximum_length,
                __pgsqlite_char_max_length(COALESCE(s.pg_type, p.type)) as character_octet_length,
                __pgsqlite_numeric_precision(COALESCE(s.pg_type, p.type)) as numeric_precision,
                CASE
                    WHEN __pgsqlite_numeric_precision(COALESCE(s.pg_type, p.type)) IS NULL THEN NULL
                    ELSE 10
                END as numeric_precision_radix,
                __pgsqlite_numeric_scale(COALESCE(s.pg_type, p.type)) as numeric_scale,
                NULL as datetime_precision,
                NULL as interval_type,
                NULL as interval_precision,
                NULL as character_set_catalog,
                NULL as character_set_schema,
                NULL as character_set_name,
                NULL as collation_catalog,
                NULL as collation_schema,
                NULL as collation_name,
                NULL as domain_catalog,
                NULL as domain_schema,
                NULL as domain_name,
                'main' as udt_catalog,
                'pg_catalog' as udt_schema,
                __pgsqlite_pg_data_type(COALESCE(s.pg_type, p.type)) as udt_name,
                NULL as scope_catalog,
                NULL as scope_schema,
                NULL as scope_name,
                NULL as maximum_cardinality,
                p.cid + 1 as dtd_identifier,
                'NO' as is_self_referencing,
                'NO' as is_identity,
                NULL as identity_generation,
                NULL as identity_start,
                NULL as identity_increment,
                NULL as identity_maximum,
                NULL as identity_minimum,
                'NO' as identity_cycle,
                'NEVER' as is_generated,
                NULL as generation_expression,
                'YES' as is_updatable
            FROM sqlite_master m
            JOIN pragma_table_info(m.name) p
            LEFT JOIN __pgsqlite_schema s
                ON s.table_name = m.name AND s.column_name = p.name
            JOIN pg_namespace n ON n.oid = __pgsqlite_relnamespace(m.name)
            WHERE m.type = 'table'
              AND m.name NOT LIKE 'sqlite_%'
              AND substr(m.name, 1, 11) <> '__pgsqlite_'
            "#,

            r#"
            UPDATE __pgsqlite_metadata
            SET value = '29', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ]),
        down: Some(MigrationAction::SqlBatch(&[
            r#"DROP VIEW IF EXISTS information_schema_tables"#,
            r#"DROP VIEW IF EXISTS information_schema_columns"#,

            // Restore the v14 views: copied verbatim from
            // register_v14_information_schema_views's `up`.
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_tables AS
            SELECT
                'main' as table_catalog,
                'public' as table_schema,
                relname as table_name,
                CASE relkind
                    WHEN 'r' THEN 'BASE TABLE'
                    WHEN 'v' THEN 'VIEW'
                    ELSE 'UNKNOWN'
                END as table_type,
                'YES' as is_insertable_into,
                NULL as self_referencing_column_name,
                NULL as reference_generation,
                NULL as user_defined_type_catalog,
                NULL as user_defined_type_schema,
                NULL as user_defined_type_name,
                'NO' as is_typed,
                'NO' as commit_action
            FROM pg_class
            WHERE relkind IN ('r', 'v');
            "#,

            // NOTE TO IMPLEMENTER: this `down` is never executed. No rollback
            // path exists in src/migration/ -- `Migration::down` is stored and
            // never read. It is populated to match repo convention so a future
            // rollback runner finds it, not because it can be tested.
            r#"
            CREATE VIEW IF NOT EXISTS information_schema_columns AS
            SELECT
                'main' as table_catalog,
                'public' as table_schema,
                c.relname as table_name,
                a.attname as column_name,
                a.attnum as ordinal_position,
                NULL as column_default,
                CASE WHEN a.attnotnull = 't' THEN 'NO' ELSE 'YES' END as is_nullable,
                CASE a.atttypid
                    WHEN 23 THEN 'integer'
                    WHEN 25 THEN 'text'
                    WHEN 700 THEN 'real'
                    WHEN 701 THEN 'double precision'
                    WHEN 17 THEN 'bytea'
                    WHEN 1043 THEN 'character varying'
                    WHEN 1042 THEN 'character'
                    WHEN 16 THEN 'boolean'
                    WHEN 1082 THEN 'date'
                    WHEN 1083 THEN 'time without time zone'
                    WHEN 1114 THEN 'timestamp without time zone'
                    WHEN 1184 THEN 'timestamp with time zone'
                    WHEN 1700 THEN 'numeric'
                    ELSE 'text'
                END as data_type,
                NULL as character_maximum_length,
                NULL as character_octet_length,
                NULL as numeric_precision,
                NULL as numeric_precision_radix,
                NULL as numeric_scale,
                NULL as datetime_precision,
                NULL as interval_type,
                NULL as interval_precision,
                NULL as character_set_catalog,
                NULL as character_set_schema,
                NULL as character_set_name,
                NULL as collation_catalog,
                NULL as collation_schema,
                NULL as collation_name,
                NULL as domain_catalog,
                NULL as domain_schema,
                NULL as domain_name,
                NULL as udt_catalog,
                NULL as udt_schema,
                NULL as udt_name,
                NULL as scope_catalog,
                NULL as scope_schema,
                NULL as scope_name,
                NULL as maximum_cardinality,
                NULL as dtd_identifier,
                'NO' as is_self_referencing,
                'NO' as is_identity,
                NULL as identity_generation,
                NULL as identity_start,
                NULL as identity_increment,
                NULL as identity_maximum,
                NULL as identity_minimum,
                NULL as identity_cycle,
                'NO' as is_generated,
                NULL as generation_expression,
                'NO' as is_updatable
            FROM pg_class c
            JOIN pg_attribute a ON c.oid = a.attrelid
            WHERE c.relkind = 'r'
              AND a.attnum > 0;
            "#,

            r#"
            UPDATE __pgsqlite_metadata
            SET value = '28', updated_at = strftime('%s', 'now')
            WHERE key = 'schema_version';
            "#,
        ])),
        dependencies: vec![28],
    });
}
```

- [ ] **Step 5: Update the migration list test**

In `tests/migration_test.rs`, change line 26 to append `29` to the expected vector, and line 34 to expect version `"29"`:

```rust
    assert_eq!(applied, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]);
```

```rust
    assert_eq!(version, "29");
```

Also search the file for any other hardcoded `28` and update it:

Run: `grep -n '\b28\b' tests/migration_test.rs`

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --test migration_test`
Expected: PASS, all tests including the new one.

- [ ] **Step 7: Pre-commit checklist**

Run: `cargo check && cargo clippy && cargo build && cargo test`
Expected: no errors, no new warnings, all tests pass. Some `information_schema` integration tests still exercise the Rust handlers and should be unaffected — the views are not routed to yet.

- [ ] **Step 8: Commit**

```bash
git add src/migration/registry.rs tests/migration_test.rs
git commit -m "feat: migration v29 rebuilds information_schema views namespace-aware (#88)

table_schema now comes from __pgsqlite_relnamespace rather than a hardcoded
'public'. columns reads sqlite_master/pragma_table_info/__pgsqlite_schema
directly instead of pg_attribute, which keeps v29 off a view every ORM reads
and recovers column_default, is_nullable for INTEGER PRIMARY KEY,
character_maximum_length and numeric precision/scale.

Views are not routed to yet -- the Rust handlers still intercept."
```

---

### Task 4: Route `information_schema.tables` to the view

**Files:**
- Modify: `src/translator/schema_prefix_translator.rs:11-44,76-93`
- Modify: `src/catalog/query_interceptor.rs:728-731` (delete arm), `:1844-1935` (delete handler)
- Modify: `src/session/db_handler.rs:1174-1176`, `:1650-1652`, `:2662-2729` (delete duplicates)
- Modify: `src/query/extended.rs:2262` (delete branch)
- Create: `tests/information_schema_namespace_test.rs`

**Interfaces:**
- Consumes: the `information_schema_tables` view from Task 3.
- Produces: `information_schema.tables` queries executing as SQL against the view. No Rust API surface.

- [ ] **Step 1: Write the failing test**

Create `tests/information_schema_namespace_test.rs`:

```rust
mod common;
use common::setup_test_server;

/// Issue #88: information_schema.tables listed pgsqlite's own catalog relations
/// as user tables in public, so Django inspectdb and SQLAlchemy automap would
/// generate models for pg_constraint and friends.
#[tokio::test]
async fn internal_relations_are_not_reported_in_public() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query("CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT)")
        .await
        .unwrap();

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["customers".to_string()]);
}

#[tokio::test]
async fn internal_relations_report_their_real_schema() {
    let server = setup_test_server().await;
    let client = &server.client;

    let rows = client
        .query(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = $1",
            &[&"pg_class"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "pg_catalog");

    let rows = client
        .query(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = $1",
            &[&"information_schema_tables"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "information_schema");
}

/// The deleted handler ignored ORDER BY entirely.
#[tokio::test]
async fn order_by_is_respected() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("CREATE TABLE zebra (id INTEGER)").await.unwrap();
    client.simple_query("CREATE TABLE alpha (id INTEGER)").await.unwrap();
    client.simple_query("CREATE TABLE middle (id INTEGER)").await.unwrap();

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["alpha".to_string(), "middle".to_string(), "zebra".to_string()]);
}

/// The deleted handler returned zero rows for any aggregate.
#[tokio::test]
async fn aggregates_work() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("CREATE TABLE a (id INTEGER)").await.unwrap();
    client.simple_query("CREATE TABLE b (id INTEGER)").await.unwrap();

    let row = client
        .query_one(
            "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 2);
}

/// The deleted handler supported equality on table_name and nothing else.
#[tokio::test]
async fn non_table_name_predicates_filter() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("CREATE TABLE t (id INTEGER)").await.unwrap();
    client.simple_query("CREATE VIEW v AS SELECT id FROM t").await.unwrap();

    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'public' AND table_type = 'VIEW'",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["v".to_string()]);
}

/// #102 divergence, asserted so it is documented rather than discovered.
/// pg_class still uses v28's LIKE 'pg\_%' heuristic, so a user table named
/// pg_* is misfiled there while information_schema.tables gets it right.
/// When #102 lands, the second assertion flips to "public".
#[tokio::test]
async fn user_table_named_like_a_catalog_relation_is_visible() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query("CREATE TABLE pg_myreport (id INTEGER PRIMARY KEY)")
        .await
        .unwrap();

    let rows = client
        .query(
            "SELECT table_schema FROM information_schema.tables WHERE table_name = $1",
            &[&"pg_myreport"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "public", "#88: exact-name matching");

    let rows = client
        .query(
            "SELECT n.nspname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relname = $1",
            &[&"pg_myreport"],
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get::<_, &str>(0),
        "pg_catalog",
        "known divergence until #102 lands; flip this to 'public' there"
    );
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test information_schema_namespace_test`
Expected: FAIL. `internal_relations_are_not_reported_in_public` reports 29 names instead of 1, because the Rust handler still intercepts.

- [ ] **Step 3: Add the rewrite to SchemaPrefixTranslator**

In `src/translator/schema_prefix_translator.rs`, inside `translate_query` after the `catalog_functions` loop (line 40) and before the `debug!`:

```rust
        // information_schema relations exist as SQLite views with underscores.
        // Rewriting here routes them to the views through the interceptor's
        // fall-through path, the same way pg_catalog.* reaches pg_class.
        // Only the two relations served by views; the rest still have handlers.
        result = result.replace("information_schema.tables", "information_schema_tables");
        result = result.replace("information_schema.columns", "information_schema_columns");
        result = result.replace("INFORMATION_SCHEMA.TABLES", "information_schema_tables");
        result = result.replace("INFORMATION_SCHEMA.COLUMNS", "information_schema_columns");
```

In `translate_object_name`, replace the comment at line 91 and add the AST path:

```rust
            if schema_name == "pg_catalog" {
                // Replace with just the table name
                name.0 = vec![table.clone()];
            } else if schema_name == "information_schema" {
                // The two relations served by SQLite views are rewritten to
                // their underscore names; the rest keep their Rust handlers.
                let table_name = match table {
                    ObjectNamePart::Identifier(ident) => ident.value.to_lowercase(),
                };
                if table_name == "tables" || table_name == "columns" {
                    let mut ident = match table {
                        ObjectNamePart::Identifier(ident) => ident.clone(),
                    };
                    ident.value = format!("information_schema_{table_name}");
                    name.0 = vec![ObjectNamePart::Identifier(ident)];
                }
            }
```

- [ ] **Step 4: Add translator unit tests**

Add to the `tests` module at the bottom of `src/translator/schema_prefix_translator.rs`:

```rust
    #[test]
    fn test_information_schema_tables_rewrite() {
        let query = "SELECT table_name FROM information_schema.tables ORDER BY 1";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(translated, "SELECT table_name FROM information_schema_tables ORDER BY 1");
    }

    #[test]
    fn test_information_schema_columns_rewrite() {
        let query = "SELECT * FROM information_schema.columns";
        let translated = SchemaPrefixTranslator::translate_query(query);
        assert_eq!(translated, "SELECT * FROM information_schema_columns");
    }

    /// Relations that still have Rust handlers must not be rewritten -- there is
    /// no `information_schema_routines` view to fall through to.
    #[test]
    fn test_other_information_schema_relations_are_untouched() {
        for relation in ["routines", "views", "triggers", "check_constraints"] {
            let query = format!("SELECT * FROM information_schema.{relation}");
            assert_eq!(SchemaPrefixTranslator::translate_query(&query), query);
        }
    }

    /// `table_constraints` shares a prefix with `tables` -- verify no collision.
    #[test]
    fn test_table_constraints_is_not_caught_by_the_tables_rewrite() {
        let query = "SELECT * FROM information_schema.table_constraints";
        assert_eq!(SchemaPrefixTranslator::translate_query(query), query);
    }
```

Run: `cargo test --lib schema_prefix_translator`
Expected: PASS.

- [ ] **Step 5: Delete the `tables` handler and its dispatch**

In `src/catalog/query_interceptor.rs`, delete the arm at lines 728-731:

```rust
            // Handle information_schema.tables queries
            if table_name.contains("information_schema.tables") {
                return Some(Ok(Self::handle_information_schema_tables_query(select, &db).await));
            }
```

and delete the whole `handle_information_schema_tables_query` function (lines 1844-1935, from `async fn handle_information_schema_tables_query` through its closing brace).

In `src/session/db_handler.rs`, delete the two dispatch checks:

```rust
            if lower_query.contains("information_schema.tables") {
                return self.handle_information_schema_tables_query(query, session_id).await;
            }
```

(at lines 1174-1176 and 1650-1652) and the whole `handle_information_schema_tables_query` method (lines 2662-2729).

In `src/query/extended.rs`, delete the `else if query.contains("information_schema.tables")` branch beginning at line 2262 — read the surrounding `if`/`else if` chain first and keep it well-formed.

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --test information_schema_namespace_test`
Expected: PASS, 6 tests.

If `internal_relations_report_their_real_schema` fails with "no such function: __pgsqlite_relnamespace", the query reached a connection without UDFs registered — check `src/session/connection_manager.rs:80`.

If it fails with "no such table: information_schema_tables", the rewrite did not fire; log the translated query at `src/catalog/query_interceptor.rs:97`.

- [ ] **Step 7: Pre-commit checklist**

Run: `cargo check && cargo clippy && cargo build && cargo test`
Expected: no errors, no new warnings. `cargo clippy` may now flag `extract_table_name_filters` as dead code if the columns handler was its only other caller — leave it until Task 5, which deletes that caller too.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "fix: serve information_schema.tables from SQLite, not a Rust handler (#88)

Closes the headline defect: internal relations now report pg_catalog and
information_schema for table_schema instead of appearing as user tables in
public.

Also fixes three defects the handler carried: ORDER BY was ignored,
aggregates returned zero rows, and any predicate other than equality on
table_name was silently dropped."
```

---

### Task 5: Route `information_schema.columns` to the view

**Files:**
- Modify: `src/catalog/query_interceptor.rs:733-740` (delete arm), `:1937-2190` (delete handler), `:2190-2245` (delete `map_sqlite_type_to_pg_column_info`)
- Modify: `tests/information_schema_namespace_test.rs` (add tests)

**Interfaces:**
- Consumes: the `information_schema_columns` view from Task 3; the rewrite added in Task 4 already covers `information_schema.columns`.
- Produces: `information_schema.columns` queries executing as SQL against the view.

- [ ] **Step 1: Write the failing test**

Add to `tests/information_schema_namespace_test.rs`:

```rust
/// The type fidelity table from the spec. The deleted handler got id, ts and
/// tags wrong; the pre-v29 view got six of eight wrong, including reporting
/// `integer` for TIMESTAMPTZ, which leaked the INTEGER datetime storage.
#[tokio::test]
async fn column_types_are_reported_faithfully() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query(
            "CREATE TABLE fidelity (
                id SERIAL PRIMARY KEY,
                amount NUMERIC(10,2),
                uid UUID,
                doc JSONB,
                tags TEXT[],
                ts TIMESTAMPTZ,
                flag BOOLEAN,
                nick VARCHAR(50)
            )",
        )
        .await
        .unwrap();

    let rows = client
        .query(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_name = 'fidelity' ORDER BY ordinal_position",
            &[],
        )
        .await
        .unwrap();

    let got: Vec<(String, String)> = rows
        .iter()
        .map(|r| (r.get::<_, String>(0), r.get::<_, String>(1)))
        .collect();

    assert_eq!(
        got,
        vec![
            ("id".to_string(), "integer".to_string()),
            ("amount".to_string(), "numeric".to_string()),
            ("uid".to_string(), "uuid".to_string()),
            ("doc".to_string(), "jsonb".to_string()),
            ("tags".to_string(), "ARRAY".to_string()),
            ("ts".to_string(), "timestamp with time zone".to_string()),
            ("flag".to_string(), "boolean".to_string()),
            ("nick".to_string(), "character varying".to_string()),
        ]
    );
}

/// The four columns the handler populated and the pre-v29 view left NULL.
/// Django and SQLAlchemy both read these.
#[tokio::test]
async fn column_modifiers_and_defaults_are_reported() {
    let server = setup_test_server().await;
    let client = &server.client;

    client
        .simple_query(
            "CREATE TABLE widgets (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                price NUMERIC(10,2),
                active BOOLEAN DEFAULT true
            )",
        )
        .await
        .unwrap();

    let row = client
        .query_one(
            "SELECT character_maximum_length FROM information_schema.columns \
             WHERE table_name = 'widgets' AND column_name = 'name'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<i32>>(0), Some(50));

    let row = client
        .query_one(
            "SELECT numeric_precision, numeric_scale FROM information_schema.columns \
             WHERE table_name = 'widgets' AND column_name = 'price'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<i32>>(0), Some(10));
    assert_eq!(row.get::<_, Option<i32>>(1), Some(2));

    let row = client
        .query_one(
            "SELECT column_default FROM information_schema.columns \
             WHERE table_name = 'widgets' AND column_name = 'active'",
            &[],
        )
        .await
        .unwrap();
    assert!(
        row.get::<_, Option<String>>(0).is_some(),
        "column_default should be populated for DEFAULT true"
    );

    // INTEGER PRIMARY KEY is NOT NULL in PostgreSQL. pragma_table_info reports
    // notnull=0 for it, so the view must also consider pk.
    let row = client
        .query_one(
            "SELECT is_nullable FROM information_schema.columns \
             WHERE table_name = 'widgets' AND column_name = 'id'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, &str>(0), "NO");
}

#[tokio::test]
async fn columns_of_internal_relations_are_not_in_public() {
    let server = setup_test_server().await;
    let client = &server.client;

    client.simple_query("CREATE TABLE t (id INTEGER)").await.unwrap();

    let rows = client
        .query(
            "SELECT DISTINCT table_name FROM information_schema.columns \
             WHERE table_schema = 'public' ORDER BY table_name",
            &[],
        )
        .await
        .unwrap();

    let names: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();
    assert_eq!(names, vec!["t".to_string()]);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test information_schema_namespace_test column_types_are_reported_faithfully`
Expected: FAIL — `id` reports `text` and `ts` reports `text`, because the Rust handler still intercepts.

- [ ] **Step 3: Delete the `columns` handler and its dispatch**

In `src/catalog/query_interceptor.rs`, delete the arm at lines 733-740:

```rust
            // Handle information_schema.columns queries
            if table_name.contains("information_schema.columns") {
                if let Some(ref session_state) = session {
                    return Some(Self::handle_information_schema_columns_query_with_session(select, &db, &session_state.id).await);
                } else {
                    return None;
                }
            }
```

Delete `handle_information_schema_columns_query_with_session` (starts line 1937) in full, and `map_sqlite_type_to_pg_column_info` (starts line 2190) in full — the latter now lives in `src/catalog/column_type_info.rs`.

Then check whether any other handler still calls the helpers those two used:

Run: `grep -n "extract_selected_columns\|extract_table_name_filters" src/catalog/query_interceptor.rs`

Keep them if other handlers call them; delete any that are now unused. `cargo clippy` will report dead code either way.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test --test information_schema_namespace_test`
Expected: PASS, 9 tests.

- [ ] **Step 5: Run the pre-existing information_schema tests**

Run: `cargo test --test information_schema_test --test information_schema_comprehensive_test`
Expected: PASS. These assert `data_type` for `INTEGER`, `VARCHAR`, `DECIMAL(10,2)`, `BOOLEAN`, `TIMESTAMP`, and `is_nullable = 'NO'` for `INTEGER PRIMARY KEY` — all covered by the new view.

If `test_information_schema_columns` fails on a column count, the view is emitting rows for a table the handler filtered. Compare its `WHERE` clause against the handler's `type='table' AND name NOT LIKE 'sqlite_%' AND substr(name,1,11) <> '__pgsqlite_'`.

- [ ] **Step 6: Pre-commit checklist**

Run: `cargo check && cargo clippy && cargo build && cargo test`
Expected: no errors, no new warnings, all tests pass.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "fix: serve information_schema.columns from SQLite, not a Rust handler (#88)

Types are now resolved from __pgsqlite_schema through pg_column_info, so
SERIAL reports integer and TIMESTAMPTZ reports timestamp with time zone
instead of text. character_maximum_length, numeric precision/scale,
column_default and is_nullable for INTEGER PRIMARY KEY are preserved from
the deleted handler rather than regressing to NULL."
```

---

### Task 6: Drift guard, docs, and full-suite verification

**Files:**
- Modify: `tests/information_schema_namespace_test.rs` (drift test)
- Modify: `CLAUDE.md` (Current Migrations list)

**Interfaces:**
- Consumes: everything from Tasks 1-5.
- Produces: nothing new; this task closes the loop.

- [ ] **Step 1: Write the drift test**

Add to `tests/information_schema_namespace_test.rs`:

```rust
/// The internal-relation list is maintained by hand, so it can drift when a
/// migration adds a catalog relation. Fail here rather than silently leaking
/// the new relation into information_schema.tables as a user table.
#[test]
fn internal_relation_list_matches_migrated_database() {
    use pgsqlite::catalog::internal_relations::{
        INTERNAL_INFORMATION_SCHEMA_RELATIONS, INTERNAL_PG_CATALOG_RELATIONS,
    };
    use pgsqlite::migration::MigrationRunner;
    use rusqlite::Connection;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("drift.db");
    let conn = Connection::open(&db_path).unwrap();
    let mut runner = MigrationRunner::new(conn);
    runner.run_pending_migrations().unwrap();
    let conn = runner.into_connection();

    let mut in_database: Vec<String> = conn
        .prepare(
            "SELECT name FROM sqlite_master \
             WHERE name NOT LIKE 'sqlite_%' AND substr(name, 1, 11) <> '__pgsqlite_' \
             ORDER BY name",
        )
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    in_database.sort();

    let mut in_list: Vec<String> = INTERNAL_PG_CATALOG_RELATIONS
        .iter()
        .chain(INTERNAL_INFORMATION_SCHEMA_RELATIONS.iter())
        .map(|s| s.to_string())
        .collect();
    in_list.sort();

    let missing: Vec<&String> = in_database.iter().filter(|n| !in_list.contains(n)).collect();
    assert!(
        missing.is_empty(),
        "migrations create relations absent from src/catalog/internal_relations.rs: {missing:?}. \
         Add them, or they will be reported as user tables in information_schema.tables."
    );

    let stale: Vec<&String> = in_list.iter().filter(|n| !in_database.contains(n)).collect();
    assert!(
        stale.is_empty(),
        "src/catalog/internal_relations.rs lists relations no migration creates: {stale:?}"
    );
}
```

- [ ] **Step 2: Run the drift test**

Run: `cargo test --test information_schema_namespace_test internal_relation_list_matches_migrated_database`
Expected: PASS. If it fails listing relations, add exactly those names to the correct list in `src/catalog/internal_relations.rs` — `information_schema_*` names go in `INTERNAL_INFORMATION_SCHEMA_RELATIONS`, everything else in `INTERNAL_PG_CATALOG_RELATIONS`.

- [ ] **Step 3: Update CLAUDE.md**

In the `### Current Migrations (v1-v28)` section, change the heading to `(v1-v29)` and append:

```markdown
- v29: Namespace-aware information_schema.tables/.columns served from SQLite views
```

- [ ] **Step 4: Run the full Rust suite**

Run: `cargo test`
Expected: PASS. Pay attention to `orm_constraint_discovery_test` and `permission_functions_test`, which query `information_schema` and were not touched directly.

- [ ] **Step 5: Run the SQLAlchemy suite**

Run: `./tests/python/run_sqlalchemy_tests.sh`
Expected: 8/8 pass. This is the strongest signal that ORM introspection still works after the handler deletions.

If the script needs a driver argument or a virtualenv that is not present, note that in the commit message rather than skipping silently.

- [ ] **Step 6: Manual verification against the issue's reproduction**

```bash
cargo build
rm -f /tmp/t88.sqlite*
./target/debug/pgsqlite --database /tmp/t88.sqlite --port 5599 &
sleep 3
psql "host=127.0.0.1 port=5599 user=postgres dbname=main gssencmode=disable" \
  -c "CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT);"
psql "host=127.0.0.1 port=5599 user=postgres dbname=main gssencmode=disable" \
  -tA -c "SELECT table_schema, table_name FROM information_schema.tables ORDER BY 1,2;"
psql "host=127.0.0.1 port=5599 user=postgres dbname=main gssencmode=disable" -c "\dt"
kill %1
```

Expected: `customers` is the only row with `table_schema = public`; internal relations report `pg_catalog` or `information_schema`; the list is sorted; `\dt` still shows only `customers`.

- [ ] **Step 7: Pre-commit checklist**

Run: `cargo check && cargo clippy && cargo build && cargo test`
Expected: no errors, no new warnings, all tests pass.

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "test: guard the internal-relation list against migration drift (#88)

A future migration that adds a catalog relation without updating
internal_relations.rs would silently leak it into information_schema.tables
as a user table. Fail in CI instead.

Closes #88."
```

---

## Post-implementation

- File the two follow-ups the spec names but does not track yet: view columns in `information_schema.columns` / `pg_attribute`, and the umbrella audit of the six remaining `information_schema` Rust handlers (`key_column_usage`, `table_constraints`, `referential_constraints`, `routines`, `views`, `schemata`) for the same `ORDER BY` / aggregate / predicate defects.
- [#102](https://github.com/erans/pgsqlite/issues/102) is already filed and now unblocked: both its changes depend on `__pgsqlite_relnamespace`, which Task 1 delivers.
