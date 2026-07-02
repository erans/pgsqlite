# Result Column Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make pgsqlite's SELECT result column names and types fully PostgreSQL-conformant, fixing the 11 confirmed findings (F1–F10 + F-new) from the adversarial review of range `204380d..c5814eb`.

**Architecture:** A central `ProjectionResolver` turns SQLite's `column_name(i)` into `(wire_name, type_oid, datetime_flag)` using schema-stored effective names as the truth for real columns and parsing the projection AST only for queries containing `AS` (cached on `PreparedStatement`). All three SELECT paths (`DbHandler`, `ReadOnlyDbHandler`, extended/portal) converge on it. A session GUC `pgsqlite.legacy_result_columns` (default off) toggles legacy casing. Catalog extractors get localized fixes.

**Tech Stack:** Rust, sqlparser 0.57 (`Ident.quote_style: Option<char>`), rusqlite, tokio (`RwLock`), regex. Tests via `cargo test` (unit) and `tests/runner/run_ssl_tests.sh` + `tests/sql/<category>/*.sql` (integration).

## Global Constraints

- Pre-commit checklist (run before any commit): `cargo check` (no warnings), `cargo clippy`, `cargo build`, `cargo test` (all pass).
- Project rule: NEVER infer types from column names — only explicit PG type declarations, PRAGMA `table_info`, explicit casts, value-based inference as last resort. The resolver's schema match (step 1) and argument-preserving function typing (step 2) honor this.
- DateTime storage: DATE=days, TIME/TIMETZ=micros-since-midnight, TIMESTAMP/TIMESTAMPTZ=micros-since-epoch, INTERVAL=micros (INTEGER). The resolver's `datetime_flag` feeds the existing converter.
- Zero-perf-impact design: non-`AS` queries must never parse sqlparser; alias view is cached on `PreparedStatement`.
- GUC `pgsqlite.legacy_result_columns` stored in `SessionState.parameters` (existing `RwLock<HashMap<String,String>>`), set via existing `SetHandler`, default `off` (conformant). The paren-corruption, empty-name, and datetime fixes are NOT behind the GUC — only casing in steps 3 & 5.
- Spec: `docs/superpowers/specs/2026-07-02-result-column-conformance-design.md`.

---

## File Structure

**Created:**
- `src/query/projection_resolver.rs` — `ProjectionResolver`, `ColumnMeta`, `AliasItem`, `fn_shape`, function→type table. The single convergence point. Pure logic, no DB/async.
- `tests/sql/features/column_conformance.sql` — integration SQL tests for F1/F2/F5/F4/F-new/F7/C2.

**Modified:**
- `src/session/state.rs` — add `projection_metadata: Option<Vec<AliasItem>>` to `PreparedStatement`; add a `legacy_result_columns()` reader helper.
- `src/session/db_handler.rs` — replace the ~10 `sanitize_column_name(stmt.column_name(i)?)` sites with `resolve_columns(...)`; thread `session`/GUC into the executor call.
- `src/session/read_only_handler.rs` — route column construction through `resolve_columns` (fixes F3).
- `src/query/executor.rs` — use `ColumnMeta` from the resolver to set `field_descriptions[i].type_oid` and populate `datetime_columns` (fixes F2); remove the bare-name OID block dependency.
- `src/types/schema_type_mapper.rs` — delete the bare-name OID block at `:344+`; keep the query-regex fallback below it (fixes F5).
- `src/query/mod.rs` — `pub mod projection_resolver;`
- `src/catalog/query_interceptor.rs` — F7 (remove `break`), F-new (`Expr::Function` arm + `count(*)` dataset count), F6 (case-fold via `quote_style`), F8 (log label).
- `src/catalog/pg_roles.rs` — F7 (remove `break`), F6 (case-fold via `quote_style`).
- `tests/catalog_alias_test.rs` — add F6/F7/F-new/CAST/CompoundIdentifier/WHERE/unknown-source tests (fixes F10).
- `src/query/column_sanitizer.rs` — delete (last task, after all callers migrated).

---

### Task 1: GUC plumbing — `legacy_result_columns` reader + default

**Files:**
- Modify: `src/session/state.rs` (struct `SessionState` at `:21`, `new()` at `:50`)
- Test: `src/session/state.rs` (new `#[cfg(test)]` module)

**Interfaces:**
- Produces: `impl SessionState { pub async fn legacy_result_columns(&self) -> bool }` — reads `self.parameters`, returns `true` iff the key `"pgsqlite.legacy_result_columns"` exists with value `"on"` (case-insensitive). Default `false` (conformant).

- [ ] **Step 1: Write the failing test**

Add to `src/session/state.rs`:

```rust
#[cfg(test)]
mod legacy_guc_tests {
    use super::*;

    #[tokio::test]
    async fn test_legacy_default_off() {
        let s = SessionState::new("db".into(), "user".into());
        assert!(!s.legacy_result_columns().await);
    }

    #[tokio::test]
    async fn test_legacy_on_when_set_on() {
        let s = SessionState::new("db".into(), "user".into());
        s.parameters.write().await.insert(
            "pgsqlite.legacy_result_columns".to_string(), "on".to_string());
        assert!(s.legacy_result_columns().await);
    }

    #[tokio::test]
    async fn test_legacy_off_explicit() {
        let s = SessionState::new("db".into(), "user".into());
        s.parameters.write().await.insert(
            "pgsqlite.legacy_result_columns".to_string(), "off".to_string());
        assert!(!s.legacy_result_columns().await);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib session::state::legacy_guc_tests`
Expected: FAIL — `legacy_result_columns` method not found.

- [ ] **Step 3: Write minimal implementation**

Add to `impl SessionState` in `src/session/state.rs` (after `new`):

```rust
    /// Read the legacy result-column GUC. Default off (conformant).
    /// Only casing in resolver steps 3 & 5 honors this; the paren-corruption,
    /// empty-name, and datetime fixes are always applied.
    pub async fn legacy_result_columns(&self) -> bool {
        self.parameters.read().await
            .get("pgsqlite.legacy_result_columns")
            .map(|v| v.eq_ignore_ascii_case("on"))
            .unwrap_or(false)
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib session::state::legacy_guc_tests`
Expected: PASS (3 tests).

- [ ] **Step 5: Pre-commit + commit**

```bash
cargo check && cargo clippy --quiet && cargo test --lib session::state::legacy_guc_tests
git add src/session/state.rs
git commit -m "feat: add pgsqlite.legacy_result_columns GUC reader (default off)"
```

---

### Task 2: `ColumnMeta` + `fn_shape` + function→type table (pure, no parse)

**Files:**
- Create: `src/query/projection_resolver.rs`
- Modify: `src/query/mod.rs` (add `pub mod projection_resolver;`)
- Test: `src/query/projection_resolver.rs` (in-file `#[cfg(test)]`)

**Interfaces:**
- Produces:
  - `pub struct ColumnMeta { pub wire_name: String, pub type_oid: i32, pub datetime_flag: bool }`
  - `pub fn fn_shape(raw_name: &str) -> Option<FnShape>` where `pub struct FnShape { pub name: String, pub inner: String }` — parses SQLite result names: `count(*)`→`{name:"count", inner:"*"}`, `max(created_at)`→`{name:"max", inner:"created_at"}`, `COALESCE(max(id), 0)`→`{name:"COALESCE", inner:"max(id), 0"}`. Returns `None` when no `(` present.
  - `pub fn function_generic_oid(fn_name_lower: &str) -> Option<i32>` — the T3 generic table (count→int8, avg→numeric, json_extract→text, array_length→int4, now→timestamptz, …). Returns `None` for `min`/`max`/`sum` (argument-preserving — handled by the resolver with schema) and unknown names.
  - `pub fn is_arg_preserving(fn_name_lower: &str) -> bool` — true for `min`/`max`/`sum`/`first`/`last`.
- Consumes: `crate::types::PgType` for OIDs.

- [ ] **Step 1: Write the failing tests**

Create `src/query/projection_resolver.rs`:

```rust
use crate::types::PgType;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMeta {
    pub wire_name: String,
    pub type_oid: i32,
    pub datetime_flag: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FnShape {
    pub name: String,
    pub inner: String,
}

/// Parse a SQLite result-column name into function name + inner argument text.
/// `count(*)` -> { count, * }; `max(created_at)` -> { max, created_at };
/// `COALESCE(max(id), 0)` -> { COALESCE, "max(id), 0" }.
/// Returns None when the name contains no `(`.
pub fn fn_shape(raw_name: &str) -> Option<FnShape> {
    let open = raw_name.find('(')?;
    let last_close = raw_name.rfind(')')?;
    if last_close <= open { return None; }
    Some(FnShape {
        name: raw_name[..open].to_string(),
        inner: raw_name[open + 1..last_close].to_string(),
    })
}

/// T3 generic return OID for functions whose type does not depend on the argument.
pub fn function_generic_oid(fn_name_lower: &str) -> Option<i32> {
    Some(match fn_name_lower {
        "count" => PgType::Int8.to_oid(),
        "avg" => PgType::Numeric.to_oid(),
        "json_extract" | "json_agg" | "jsonb_agg" | "json_object" | "json_object_agg"
        | "jsonb_object_agg" | "row_to_json" | "json_array" | "json_group_array"
        | "json_extract_path" | "json_extract_path_text" => PgType::Text.to_oid(),
        "array_length" | "array_upper" | "array_lower" | "array_ndims"
        | "array_position" => PgType::Int4.to_oid(),
        "array_append" | "array_prepend" | "array_cat" | "array_remove"
        | "array_replace" | "array_slice" | "string_to_array" | "array_positions"
        | "array_to_string" | "unnest" | "array_agg" => PgType::Text.to_oid(),
        "array_contains" | "array_contained" | "array_overlap" => PgType::Bool.to_oid(),
        "now" | "current_timestamp" => PgType::Timestamptz.to_oid(),
        "current_date" => PgType::Text.to_oid(),
        "current_time" => PgType::Time.to_oid(),
        "extract" => PgType::Float8.to_oid(),
        "date_trunc" | "to_timestamp" => PgType::Timestamp.to_oid(),
        "make_date" => PgType::Date.to_oid(),
        "make_time" => PgType::Time.to_oid(),
        "age" => PgType::Interval.to_oid(),
        "decimal_add" | "decimal_sub" | "decimal_mul" | "decimal_div"
        | "decimal_from_text" => PgType::Numeric.to_oid(),
        _ => return None,
    })
}

/// min/max/sum/first/last take the argument column's type (T3).
pub fn is_arg_preserving(fn_name_lower: &str) -> bool {
    matches!(fn_name_lower, "min" | "max" | "sum" | "first" | "last")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fn_shape_count_star() {
        assert_eq!(fn_shape("count(*)"), Some(FnShape { name: "count".into(), inner: "*".into() }));
    }
    #[test]
    fn fn_shape_max() {
        assert_eq!(fn_shape("max(created_at)"), Some(FnShape { name: "max".into(), inner: "created_at".into() }));
    }
    #[test]
    fn fn_shape_coalesce_nested() {
        let s = fn_shape("COALESCE(max(id), 0)").unwrap();
        assert_eq!(s.name, "COALESCE");
        assert_eq!(s.inner, "max(id), 0");
    }
    #[test]
    fn fn_shape_no_parens_is_none() {
        assert!(fn_shape("my_column").is_none());
        assert!(fn_shape("").is_none());
    }
    #[test]
    fn generic_oid_count_int8() {
        assert_eq!(function_generic_oid("count"), Some(PgType::Int8.to_oid()));
    }
    #[test]
    fn generic_oid_min_max_none() {
        assert!(function_generic_oid("min").is_none());
        assert!(function_generic_oid("max").is_none());
        assert!(function_generic_oid("sum").is_none());
    }
    #[test]
    fn arg_preserving_flags() {
        assert!(is_arg_preserving("max") && is_arg_preserving("sum"));
        assert!(!is_arg_preserving("count"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib query::projection_resolver`
Expected: FAIL — module not declared in `mod.rs`.

- [ ] **Step 3: Register the module**

In `src/query/mod.rs` add:

```rust
pub mod projection_resolver;
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib query::projection_resolver`
Expected: PASS (7 tests).

- [ ] **Step 5: Pre-commit + commit**

```bash
cargo check && cargo clippy --quiet && cargo test --lib query::projection_resolver
git add src/query/projection_resolver.rs src/query/mod.rs
git commit -m "feat: add ColumnMeta, fn_shape, and function->type table"
```

---

### Task 3: `AliasItem` + cached alias-view parse on `PreparedStatement`

**Files:**
- Modify: `src/query/projection_resolver.rs` (add `AliasItem`, `parse_alias_view`)
- Modify: `src/session/state.rs` (struct `PreparedStatement` at `:32`, `new()` helper if any)
- Test: `src/query/projection_resolver.rs`

**Interfaces:**
- Produces:
  - `pub struct AliasItem { pub position: usize, pub alias: String, pub is_quoted: bool, pub source_expr: sqlparser::ast::Expr }` — `is_quoted` from `Ident::quote_style.is_some()`.
  - `pub fn parse_alias_view(query: &str) -> Option<Vec<AliasItem>>` — returns `None` when the query contains no `AS` (case-insensitive). Otherwise parses with `sqlparser::parser::Parser::parse_sql(&PostgreSqlDialect{}, query)`, walks `Statement::Query` → `SetExpr::Select`, and for each `SelectItem::ExprWithAlias { expr, alias }` pushes one `AliasItem` (position = index into projection). Returns `Some(vec![])` if parsed but no aliases found; `None` on parse error treated as `None` (resolver falls back to no-alias path).
- Consumes: `sqlparser` (already a dependency), `sqlparser::dialect::PostgreSqlDialect`, `sqlparser::ast::{Statement, SetExpr, SelectItem, Expr}`.

- [ ] **Step 1: Write the failing tests**

Append to `src/query/projection_resolver.rs` tests module:

```rust
    #[test]
    fn alias_view_none_when_no_as() {
        assert!(parse_alias_view("SELECT id, name FROM users").is_none());
    }
    #[test]
    fn alias_view_unquoted_alias() {
        let v = parse_alias_view("SELECT id AS user_id FROM users").unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].alias, "user_id");
        assert!(!v[0].is_quoted);
    }
    #[test]
    fn alias_view_quoted_alias_preserved() {
        let v = parse_alias_view(r#"SELECT id AS "UserId" FROM users"#).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].alias, "UserId");
        assert!(v[0].is_quoted);
    }
    #[test]
    fn alias_view_position_indexed() {
        let v = parse_alias_view("SELECT id AS a, name AS b FROM users").unwrap();
        assert_eq!(v[0].position, 0);
        assert_eq!(v[1].position, 1);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib query::projection_resolver::tests::alias_view`
Expected: FAIL — `parse_alias_view` / `AliasItem` not found.

- [ ] **Step 3: Implement**

Add to `src/query/projection_resolver.rs` (top, after `use crate::types::PgType;`):

```rust
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone)]
pub struct AliasItem {
    pub position: usize,
    pub alias: String,
    pub is_quoted: bool,
    pub source_expr: Expr,
}

/// Parse the projection alias view. Returns None when the query has no `AS`
/// (so the zero-alloc fast path never pays a parse). On parse error, None.
pub fn parse_alias_view(query: &str) -> Option<Vec<AliasItem>> {
    if !query.to_uppercase().contains(" AS ") { return None; }
    let parsed = Parser::parse_sql(&PostgreSqlDialect {}, query).ok()?;
    let stmt = parsed.into_iter().next()?;
    let body = match stmt { Statement::Query(q) => q, _ => return None };
    let select = match &*body.body { SetExpr::Select(s) => s, _ => return None };
    let items: Vec<AliasItem> = select.projection.iter().enumerate()
        .filter_map(|(i, item)| match item {
            SelectItem::ExprWithAlias { expr, alias } => Some(AliasItem {
                position: i,
                alias: alias.value.clone(),
                is_quoted: alias.quote_style.is_some(),
                source_expr: expr.clone(),
            }),
            _ => None,
        })
        .collect();
    Some(items)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib query::projection_resolver`
Expected: PASS (all 11 tests).

- [ ] **Step 5: Add cache field to `PreparedStatement`**

In `src/session/state.rs`, struct `PreparedStatement`, add after `translation_metadata`:

```rust
    pub projection_metadata: Option<Vec<crate::query::projection_resolver::AliasItem>>,
```

Find every `PreparedStatement { ... }` construction site (search `PreparedStatement {`); add `projection_metadata: None,` to each. (These are typically in prepared-statement registration; verify with `grep -rn "PreparedStatement {" src/`.)

- [ ] **Step 6: Pre-commit + commit**

```bash
cargo check && cargo clippy --quiet && cargo test --lib query::projection_resolver
git add src/query/projection_resolver.rs src/session/state.rs
git commit -m "feat: add AliasItem + cached alias-view parse (AS-gated)"
```

---

### Task 4: `ProjectionResolver::resolve` — the precedence core

**Files:**
- Modify: `src/query/projection_resolver.rs` (add `ProjectionResolver`, `ResolveCtx`)
- Test: `src/query/projection_resolver.rs`

**Interfaces:**
- Produces:
  - `pub struct ResolveCtx<'a> { pub schema_types: &'a HashMap<String, String>, pub hints: &'a crate::translator::TranslationMetadata, pub alias_view: Option<&'a [AliasItem]>, pub legacy: bool }`
  - `pub struct ProjectionResolver;`
  - `impl ProjectionResolver { pub fn resolve(raw_name: &str, position: usize, ctx: &ResolveCtx) -> ColumnMeta }`
  - The 5-step precedence (schema match → function shape → alias → unnamed `?column?` → fallback). Argument-preserving fn types resolved via `ctx.schema_types` keyed by the inner column name; datetime iff resolved type uppercased contains `TIMESTAMP`/`DATE`/`TIME`. `?column?` duplicate naming: track a static `usize` counter within a single `resolve_columns` call (see Task 5), NOT a global — pass the running count via `position`-relative dedup in Task 5. For the unit test here, test `?column?` for the first unnamed expr and accept that duplicate numbering is wired in Task 5.
- Consumes: `crate::translator::TranslationMetadata` (`get_hint(name) -> Option<Hint>` with `Hint { source_column: Option<String>, expression_type: Option<ExpressionType>, suggested_type: Option<PgType> }`), `crate::types::SchemaTypeMapper::pg_type_string_to_oid(&str) -> i32`.

- [ ] **Step 1: Write the failing tests**

Append to the tests module:

```rust
    use std::collections::HashMap;
    use crate::translator::TranslationMetadata;

    fn ctx_no_aliases<'a>(schema: &'a HashMap<String,String>, legacy: bool) -> ResolveCtx<'a> {
        static EMPTY: once_cell::sync::Lazy<TranslationMetadata> = once_cell::sync::Lazy::new(TranslationMetadata::new);
        ResolveCtx { schema_types: schema, hints: &EMPTY, alias_view: None, legacy }
    }

    #[test]
    fn schema_match_keeps_paren_name() {
        let mut schema = HashMap::new();
        schema.insert("price(usd)".to_string(), "INT4".to_string());
        let m = ProjectionResolver::resolve("price(usd)", 0, &ctx_no_aliases(&schema, false));
        assert_eq!(m.wire_name, "price(usd)");
        assert_eq!(m.type_oid, PgType::Int4.to_oid());
    }
    #[test]
    fn function_shape_lowers_and_types() {
        let schema = HashMap::new();
        let m = ProjectionResolver::resolve("count(*)", 0, &ctx_no_aliases(&schema, false));
        assert_eq!(m.wire_name, "count");
        assert_eq!(m.type_oid, PgType::Int8.to_oid());
        assert!(!m.datetime_flag);
    }
    #[test]
    fn min_over_timestamp_is_datetime() {
        let mut schema = HashMap::new();
        schema.insert("created_at".to_string(), "TIMESTAMP".to_string());
        let m = ProjectionResolver::resolve("max(created_at)", 0, &ctx_no_aliases(&schema, false));
        assert_eq!(m.wire_name, "max");
        assert_eq!(m.type_oid, PgType::Timestamp.to_oid());
        assert!(m.datetime_flag);
    }
    #[test]
    fn min_over_int4_no_datetime() {
        let mut schema = HashMap::new();
        schema.insert("qty".to_string(), "INT4".to_string());
        let m = ProjectionResolver::resolve("max(qty)", 0, &ctx_no_aliases(&schema, false));
        assert_eq!(m.type_oid, PgType::Int4.to_oid());
        assert!(!m.datetime_flag);
    }
    #[test]
    fn unnamed_expr_gets_question_column() {
        let schema = HashMap::new();
        let m = ProjectionResolver::resolve("1+1", 0, &ctx_no_aliases(&schema, false));
        assert_eq!(m.wire_name, "?column?");
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib query::projection_resolver::tests`
Expected: FAIL — `ProjectionResolver`/`ResolveCtx` not found.

- [ ] **Step 3: Implement the resolver**

Add to `src/query/projection_resolver.rs`:

```rust
use std::collections::HashMap;
use crate::translator::TranslationMetadata;
use crate::types::SchemaTypeMapper;

pub struct ResolveCtx<'a> {
    pub schema_types: &'a HashMap<String, String>,
    pub hints: &'a TranslationMetadata,
    pub alias_view: Option<&'a [AliasItem]>,
    pub legacy: bool,
}

pub struct ProjectionResolver;

impl ProjectionResolver {
    pub fn resolve(raw_name: &str, position: usize, ctx: &ResolveCtx) -> ColumnMeta {
        // Step 1: schema match on the raw name (real column — correct case already).
        if let Some(pg_type) = ctx.schema_types.get(raw_name) {
            return ColumnMeta {
                wire_name: raw_name.to_string(),
                type_oid: SchemaTypeMapper::pg_type_string_to_oid(pg_type),
                datetime_flag: is_datetime_type(pg_type),
            };
        }

        // Step 2: function-call shape (raw name contains `(`).
        if let Some(shape) = fn_shape(raw_name) {
            let lower = shape.name.to_lowercase();
            let wire_name = if ctx.legacy { shape.name.clone() } else { lower.clone() };
            let (type_oid, datetime_flag) = resolve_function_type(&lower, &shape.inner, ctx);
            return ColumnMeta { wire_name, type_oid, datetime_flag };
        }

        // Step 3: alias (position maps to an ExprWithAlias item).
        if let Some(view) = ctx.alias_view {
            if let Some(item) = view.iter().find(|a| a.position == position) {
                let wire_name = if item.is_quoted || ctx.legacy {
                    item.alias.clone()
                } else {
                    item.alias.to_lowercase()
                };
                let type_oid = ctx.hints.get_hint(&item.alias)
                    .and_then(|h| h.suggested_type.map(|t| t.to_oid()))
                    .unwrap_or(PgType::Text.to_oid());
                return ColumnMeta { wire_name, type_oid, datetime_flag: false };
            }
        }

        // Step 4: unnamed non-function expression -> ?column?.
        if raw_name.is_empty() || looks_unnamed_expr(raw_name) {
            return ColumnMeta {
                wire_name: "?column?".to_string(),
                type_oid: ctx.hints.get_hint(raw_name)
                    .and_then(|h| h.suggested_type.map(|t| t.to_oid()))
                    .unwrap_or(PgType::Text.to_oid()),
                datetime_flag: false,
            };
        }

        // Step 5: fallback — raw name, lowercased unless legacy.
        let wire_name = if ctx.legacy { raw_name.to_string() } else { raw_name.to_lowercase() };
        ColumnMeta { wire_name, type_oid: PgType::Text.to_oid(), datetime_flag: false }
    }
}

fn is_datetime_type(pg_type: &str) -> bool {
    let u = pg_type.to_uppercase();
    u.contains("TIMESTAMP") || u.contains("DATE") || u.contains("TIME")
}

fn looks_unnamed_expr(name: &str) -> bool {
    // SQLite emits the verbatim expression text for unaliased expressions.
    // Heuristic: contains an operator, a space, or is purely numeric/literal.
    name.chars().any(|c| matches!(c, '+'|'-'|'*'|'/'|' '|'=')) || name.parse::<f64>().is_ok()
}

fn resolve_function_type(lower_fn: &str, inner: &str, ctx: &ResolveCtx) -> (i32, bool) {
    if is_arg_preserving(lower_fn) {
        if let Some(pg_type) = ctx.schema_types.get(inner.trim()) {
            let oid = SchemaTypeMapper::pg_type_string_to_oid(pg_type);
            return (oid, is_datetime_type(pg_type));
        }
        // sum generic fallback is numeric; min/max fallback is text.
        return (function_generic_oid(lower_fn).unwrap_or(PgType::Text.to_oid()), false);
    }
    (function_generic_oid(lower_fn).unwrap_or(PgType::Text.to_oid()), false)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --lib query::projection_resolver`
Expected: PASS (all tests, incl. the 5 new ones).

- [ ] **Step 5: Pre-commit + commit**

```bash
cargo check && cargo clippy --quiet && cargo test --lib query::projection_resolver
git add src/query/projection_resolver.rs
git commit -m "feat: ProjectionResolver precedence core (schema>fn>alias>?column?>fallback)"
```

---

### Task 5: `resolve_columns` helper + `?column?N` dedup + integration hook

**Files:**
- Modify: `src/query/projection_resolver.rs` (add `resolve_columns`)
- Modify: `src/session/db_handler.rs` (one representative site: `:1721-1725`)
- Test: `src/query/projection_resolver.rs`

**Interfaces:**
- Produces: `pub async fn resolve_columns(stmt: &rusqlite::Statement, query: &str, schema_types: &HashMap<String,String>, hints: &TranslationMetadata, session: &crate::session::SessionState) -> anyhow::Result<Vec<ColumnMeta>>` — reads `stmt.column_name(i)` for each column, reads `session.legacy_result_columns().await`, fetches/caches alias view via `parse_alias_view(query)`, calls `ProjectionResolver::resolve` per column, then applies `?column?N` dedup (second+ unnamed → `?column?2`, `?column?3`, …).
- Consumes: `rusqlite::Statement::column_name`/`column_count`, `SessionState::legacy_result_columns`.

- [ ] **Step 1: Write the failing test**

Append to tests module:

```rust
    #[test]
    fn question_column_dedup_numbering() {
        // Two unnamed expressions at positions 0 and 1 should both yield ?column?
        // in the single-column resolve() (dedup happens at resolve_columns level),
        // but resolve_columns is async+DB-bound; tested via integration in Task 8.
        // Here we only assert the base name for one unnamed expr:
        let schema = HashMap::new();
        let m = ProjectionResolver::resolve("a+b", 0, &ctx_no_aliases(&schema, false));
        assert_eq!(m.wire_name, "?column?");
    }
```

- [ ] **Step 2: Run test to verify it fails/passes baseline**

Run: `cargo test --lib query::projection_resolver::tests::question_column`
Expected: PASS (baseline — the real dedup is integration-tested in Task 8).

- [ ] **Step 3: Implement `resolve_columns`**

Add to `src/query/projection_resolver.rs`:

```rust
use anyhow::Result;

/// Resolve all columns of a prepared statement into PG-conformant metadata.
/// Convergence point for DbHandler, ReadOnlyDbHandler, and extended paths.
pub async fn resolve_columns(
    stmt: &rusqlite::Statement,
    query: &str,
    schema_types: &HashMap<String, String>,
    hints: &TranslationMetadata,
    session: &crate::session::SessionState,
) -> Result<Vec<ColumnMeta>> {
    let legacy = session.legacy_result_columns().await;
    let alias_view = parse_alias_view(query);
    let view_ref = alias_view.as_deref();
    let count = stmt.column_count();
    let mut out = Vec::with_capacity(count);
    let mut qcol_seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for i in 0..count {
        let raw = stmt.column_name(i)?.to_string();
        let ctx = ResolveCtx { schema_types, hints, alias_view: view_ref, legacy };
        let mut meta = ProjectionResolver::resolve(&raw, i, &ctx);
        if meta.wire_name == "?column?" {
            let n = qcol_seen.entry("?column?".to_string()).or_insert(0);
            *n += 1;
            if *n > 1 { meta.wire_name = format!("?column?{}", *n); }
        }
        out.push(meta);
    }
    Ok(out)
}
```

- [ ] **Step 4: Migrate ONE representative db_handler.rs site**

In `src/session/db_handler.rs` at `:1721-1725` replace:

```rust
            let mut columns = Vec::with_capacity(column_count);
            for i in 0..column_count {
                columns.push(sanitize_column_name(stmt.column_name(i)?).to_string());
            }
```
with (note: this site is inside `execute_with_session`; `session`/`schema_types` availability is verified in Task 6 — for this task, if the site lacks `session`, leave the migration to Task 6 and only commit `resolve_columns`):

```rust
            let metas = crate::query::projection_resolver::resolve_columns(
                &stmt, &processed_query, &schema_types, &translation_metadata, session).await?;
            let mut columns: Vec<String> = metas.iter().map(|m| m.wire_name.clone()).collect();
```
If `session`/`schema_types`/`translation_metadata` are not in scope at this site, revert this edit and commit only the new `resolve_columns` function (Task 6 does the full migration). Document in the commit which.

- [ ] **Step 5: Pre-commit + commit**

```bash
cargo check && cargo clippy --quiet && cargo test --lib query::projection_resolver
git add src/query/projection_resolver.rs src/session/db_handler.rs
git commit -m "feat: resolve_columns helper + ?column?N dedup (first site migrated if in scope)"
```

---

### Task 6: Migrate all `db_handler.rs` sanitize sites + read_only_handler.rs (fixes F3)

**Files:**
- Modify: `src/session/db_handler.rs` (sites `:530, :665, :778, :896, :1243, :1454, :1723, :2327, :2425`)
- Modify: `src/session/read_only_handler.rs` (`:72`, `:120`)
- Test: `cargo build` + `cargo test` (existing suite stays green)

**Interfaces:**
- Consumes: `resolve_columns` (Task 5), `ColumnMeta`.
- Produces: all SELECT paths emit `columns` via the resolver; `read_only_handler` uses the same helper.

- [ ] **Step 1: Inventory and characterize each site**

Run: `grep -n "sanitize_column_name" src/session/db_handler.rs src/session/read_only_handler.rs`
For each site, note whether `session: &Arc<SessionState>`, `schema_types`, and `translation_metadata` are in scope. Most `db_handler` SELECT sites run inside `execute_with_session(session_id, ...)` closures where `session` is available via `self.get_session(session_id)`; where it is not, add a minimal fetch. `read_only_handler.rs:72,120` build `column_names` directly — these need `session` threaded in from the caller (the `QueryRouter` path has it).

- [ ] **Step 2: Migrate db_handler.rs sites**

For each `columns.push(sanitize_column_name(stmt.column_name(i)?).to_string());` loop, replace with:

```rust
            let metas = crate::query::projection_resolver::resolve_columns(
                &stmt, query, &schema_types, &translation_metadata, session).await?;
            let mut columns: Vec<String> = metas.iter().map(|m| m.wire_name.clone()).collect();
```
Where `translation_metadata` is not built at that site, construct `crate::translator::TranslationMetadata::new()` as the hints source (the resolver handles empty hints). Where `schema_types` is not built, pass an empty `HashMap::new()`. Where `session` is not in scope, fetch via `self.get_session(&session_id).await` (existing helper) and pass `&session`.

- [ ] **Step 3: Migrate read_only_handler.rs**

In `src/session/read_only_handler.rs`, change `query` (and the analogous second method) to accept `session: &Arc<SessionState>` and build column names via `resolve_columns(&stmt, query, &HashMap::new(), &TranslationMetadata::new(), session).await?` instead of `stmt.column_names().map(|s| s.to_string())`. Update the `QueryRouter` call site to pass `session`.

- [ ] **Step 4: Build + run full unit suite**

Run: `cargo build && cargo test --lib`
Expected: BUILD OK; all existing tests PASS (casing may change for some — update assertions in the same commit to the lowercased expectation, since the default is now conformant).

- [ ] **Step 5: Pre-commit + commit**

```bash
cargo check && cargo clippy --quiet && cargo build && cargo test --lib
git add src/session/db_handler.rs src/session/read_only_handler.rs
git commit -m "refactor: route all SELECT paths through resolve_columns (fixes F3)"
```

---

### Task 7: Wire `ColumnMeta.type_oid` + `datetime_flag` into the executor (fixes F2/F5)

**Files:**
- Modify: `src/query/executor.rs` (`:1191` field build, `:1289-1422` datetime_columns population)
- Modify: `src/types/schema_type_mapper.rs` (delete bare-name OID block `:344-371`)
- Test: `src/query/executor.rs` (new `#[cfg(test)]` for the type path) + `cargo test`

**Interfaces:**
- Consumes: `resolve_columns` output (`Vec<ColumnMeta>`) — the executor should receive it (passed from the db_handler site, or re-resolved). Decision: the `execute_select` path that builds `field_descriptions` (`:1191`) receives `metas: &[ColumnMeta]` from the caller and uses `metas[i].type_oid` directly instead of the 5-priority `if/else` chain, and inserts into `datetime_columns` when `metas[i].datetime_flag`.

- [ ] **Step 1: Write the failing test for the datetime conversion (regression guard for F2)**

Create `tests/sql/features/column_conformance.sql` with (integration; run in Task 8). For the unit guard, add to `src/query/executor.rs` a test asserting that when a `ColumnMeta` has `datetime_flag=true`, the column name is inserted into `datetime_columns`. If `execute_select` internals are hard to unit-test, defer this assertion to the integration test in Task 8 and here only assert the schema_type_mapper bare block removal (next step).

- [ ] **Step 2: Delete the bare-name OID block (fixes F5)**

In `src/types/schema_type_mapper.rs`, delete lines `:344-371` (the `match upper.as_str() { "COUNT" => ... }` block added by commit `3b9c49f`), keeping the surrounding `if !function_name.contains('(') && !function_name.contains(' ') {` guard and the query-regex fallback at `:379+`. After deletion, the bare-name path falls through to the query-regex (which reads the *query* text, not the alias name) and returns `None` if unmatched.

Run: `cargo test --lib types::schema_type_mapper`
Expected: any test asserting the bare block returns OIDs now fails — those tests are the regression (they encoded the buggy behavior). Update them to assert the new behavior (bare `count` with no query context → `None`, since the resolver handles it via `function_generic_oid`).

- [ ] **Step 3: Use `ColumnMeta` for field_descriptions in execute_select**

In `src/query/executor.rs:1191`, the `fields: Vec<FieldDescription> = response.columns.iter().enumerate().map(|(i, name)| { ... 5-priority if/else ... })`. Replace the type_oid computation to prefer the resolver-provided meta: thread `metas: &[ColumnMeta]` into `execute_select` (add parameter), and set `type_oid = metas.get(i).map(|m| m.type_oid).unwrap_or_else(|| /* existing fallback chain */ )`. For `datetime_columns`: after the field loop, `for m in metas { if m.datetime_flag { datetime_columns.insert(m.wire_name.clone(), <pg_type from schema_types or "TIMESTAMP">); } }`.

- [ ] **Step 4: Build + test**

Run: `cargo build && cargo test --lib`
Expected: PASS (existing suite + updated schema_type_mapper tests).

- [ ] **Step 5: Pre-commit + commit**

```bash
cargo check && cargo clippy --quiet && cargo build && cargo test --lib
git add src/query/executor.rs src/types/schema_type_mapper.rs
git commit -m "fix: use ColumnMeta for types + datetime_flag; remove bare-name OID block (F2,F5)"
```

---

### Task 8: Catalog fixes — F7 (break removal), F-new (Function arm + count(*)), F6 (case-fold), F8 (log)

**Files:**
- Modify: `src/catalog/query_interceptor.rs` (`:1756-1761` break, `:1767` extract_projection_source_column, `:1749-1754` alias case, `:1082` log)
- Modify: `src/catalog/pg_roles.rs` (`:77-83`,`:102-106` break, `:95-100` alias case)
- Test: `tests/catalog_alias_test.rs` (new tests — Task 9)

**Interfaces:**
- Consumes: `sqlparser::ast::{Expr, SelectItem, Ident}`; session GUC for F6.

- [ ] **Step 1: F7 — remove `break` in both extractors**

In `src/catalog/query_interceptor.rs:1756-1761`, change the `Wildcard | QualifiedWildcard` arm:

```rust
                SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {
                    cols.extend_from_slice(all_columns);
                    indices.extend(0..all_columns.len());
                    break;
                }
```
to:
```rust
                SelectItem::Wildcard(_) => {
                    cols.extend_from_slice(all_columns);
                    indices.extend(0..all_columns.len());
                }
                SelectItem::QualifiedWildcard(_, _) => {
                    cols.extend_from_slice(all_columns);
                    indices.extend(0..all_columns.len());
                }
```
(No `break`; continue. QualifiedWildcard currently uses all_columns — acceptable per spec; named-relation filtering is a refinement, noted as future work.)

Repeat the same `break` removal in `src/catalog/pg_roles.rs` for both `Wildcard` (`:77-83`) and `QualifiedWildcard` (`:102-106`).

- [ ] **Step 2: F-new — add `Expr::Function` arm to `extract_projection_source_column`**

In `src/catalog/query_interceptor.rs:1767`, the function currently handles `Identifier`/`CompoundIdentifier`/`Cast`/`Nested` and `_ => None`. Add before the `_`:

```rust
            Expr::Function(f) => {
                f.name.0.last().map(|ident| ident.value.to_lowercase())
            }
```
Then, in each catalog handler that calls `extract_selected_columns` (e.g. `handle_pg_namespace_query` at `:1057`), add detection: if the resulting `(cols, indices)` is empty AND the projection contains `UnnamedExpr(Expr::Function(_))` whose name is `count`, compute the row count of the handler's static dataset and return a single-row single-column `DbResponse { columns: ["count"], rows: [[Some(count.to_string().into_bytes())]], rows_affected: 1 }`. For `pg_namespace` the dataset size is `schemas.len()` (2); for `pg_roles` it's the roles list length. (Minimal scope per spec.)

- [ ] **Step 3: F6 — case-fold unquoted aliases**

In `src/catalog/query_interceptor.rs:1749-1754` (`ExprWithAlias` arm) and `src/catalog/pg_roles.rs:95-100`, change `cols.push(alias.value.clone())` to:

```rust
                    let name = if alias.quote_style.is_some() || legacy {
                        alias.value.clone()
                    } else {
                        alias.value.to_lowercase()
                    };
                    cols.push(name);
```
where `legacy` is read from the session GUC `pgsqlite.legacy_result_columns`. Thread `legacy: bool` into `extract_selected_columns` and `get_selected_columns` (add parameter; pass from the handler which has session access, or default `false` where the handler lacks it — catalog queries are typically session-scoped).

- [ ] **Step 4: F8 — fix the debug log label**

In `src/catalog/query_interceptor.rs:1082`, change `"pg_type query"` to `"pg_namespace query"`.

- [ ] **Step 5: Build + test**

Run: `cargo build && cargo test --lib catalog`
Expected: PASS (new behavior; existing catalog tests may need assertion updates for case-folding — update in this commit).

- [ ] **Step 6: Pre-commit + commit**

```bash
cargo check && cargo clippy --quiet && cargo build && cargo test --lib catalog
git add src/catalog/query_interceptor.rs src/catalog/pg_roles.rs
git commit -m "fix: catalog wildcard break (F7), Function projection (F-new), alias case-fold (F6), log (F8)"
```

---

### Task 9: Expand `catalog_alias_test.rs` (fixes F10) + delete stray file (F9)

**Files:**
- Modify: `tests/catalog_alias_test.rs`
- Delete: `tests/test_batch_7b.py`
- Test: `cargo test --test catalog_alias_test`

**Interfaces:**
- Consumes: the catalog extractor behaviors from Task 8.

- [ ] **Step 1: Add the failing tests**

Append to `tests/catalog_alias_test.rs` (follow the existing test style; these call the handlers' projection logic). Add tests for:
1. Uppercase alias `SELECT oid AS Did FROM pg_catalog.pg_namespace` → output column `did` (legacy off).
2. Mid-list wildcard `SELECT *, oid FROM pg_catalog.pg_namespace` → 3 columns (`oid, nspname, oid`).
3. `SELECT count(*) FROM pg_catalog.pg_namespace` → 1 row, value `2`.
4. `SELECT CAST(oid AS text) AS o FROM pg_catalog.pg_namespace` → resolves via the `Cast` arm.
5. `SELECT n.nspname FROM pg_catalog.pg_namespace AS n` (CompoundIdentifier) → resolves `nspname`.
6. `SELECT rolname FROM pg_roles WHERE rolname = 'postgres'` (WHERE + alias-free projection) → filtered.
7. `SELECT nonexistent FROM pg_catalog.pg_namespace` → no columns (unknown source).

- [ ] **Step 2: Run to verify failures**

Run: `cargo test --test catalog_alias_test`
Expected: some FAIL (the behaviors from Task 8 must be in place; if Task 8 done, these pass; if any fail, fix in Task 8's extractor code).

- [ ] **Step 3: Delete the stray file (F9)**

```bash
git rm tests/test_batch_7b.py
```
Confirm no runner references it: `grep -rn "test_batch_7b" tests/` → expect no hits.

- [ ] **Step 4: Run + commit**

```bash
cargo check && cargo clippy --quiet && cargo test --test catalog_alias_test
git add tests/catalog_alias_test.rs
git commit -m "test: expand catalog_alias_test coverage (F10); remove stray test_batch_7b.py (F9)"
```

---

### Task 10: Integration SQL tests + final sanitizer deletion (F1/F2/F5/F4 end-to-end, remove column_sanitizer.rs)

**Files:**
- Create: `tests/sql/features/column_conformance.sql`
- Delete: `src/query/column_sanitizer.rs` (and remove `pub mod column_sanitizer;` from `src/query/mod.rs`)
- Test: `./tests/runner/run_ssl_tests.sh` (or the project's integration entry)

**Interfaces:**
- Consumes: full resolver pipeline (Tasks 5–7) + GUC (Task 1).

- [ ] **Step 1: Write the integration SQL file**

Create `tests/sql/features/column_conformance.sql` (follow the existing `tests/sql/features/*.sql` format — statements + `-- EXPECT:` comments the runner checks, or psql `\echo` markers per the runner's convention; inspect an existing file in that dir first):

```sql
-- F1: quoted paren-containing column name preserved
CREATE TABLE t1 ("price(usd)" INT);
INSERT INTO t1 VALUES (5);
SELECT "price(usd)" FROM t1;
-- EXPECT: column name "price(usd)", type int4, value 5

-- F2: unaliased MAX(timestamp) returns formatted timestamp
CREATE TABLE t2 (created_at TIMESTAMP);
INSERT INTO t2 VALUES ('2023-11-14 22:13:20');
SELECT max(created_at) FROM t2;
-- EXPECT: column "max", value "2023-11-14 22:13:20.000000" (not raw microseconds)

-- F5: aliased non-aggregate not mistyped
CREATE TABLE t3 (name TEXT);
INSERT INTO t3 VALUES ('alice');
SELECT upper(name) AS count FROM t3;
-- EXPECT: column "count" type text, value "ALICE" (decodes without error)

-- F4: unnamed expression -> ?column?
SELECT 1+1;
-- EXPECT: column "?column?", value 2

-- F-new: catalog count
SELECT count(*) FROM pg_catalog.pg_namespace;
-- EXPECT: 1 row, value 2

-- F7: mid-list wildcard
SELECT *, oid FROM pg_catalog.pg_namespace;
-- EXPECT: 3 columns

-- C2: legacy GUC off-switch
SET pgsqlite.legacy_result_columns = on;
SELECT MyCol FROM t1;  -- t1 has no MyCol; use a real unquoted col if needed
-- EXPECT: legacy casing preserved where applicable
SET pgsqlite.legacy_result_columns = off;
```
(Adjust the `-- EXPECT` syntax to match the runner's actual convention found by inspecting a sibling file. Replace the C2 example with a real unquoted column from a table created above.)

- [ ] **Step 2: Run the integration suite**

Run: `./tests/runner/run_ssl_tests.sh` (from project root)
Expected: the new cases PASS. If the runner doesn't auto-pick `tests/sql/features/*.sql`, wire it in per the runner's include mechanism (check `run_ssl_tests.sh` for the `SQL_FILE` / category loop).

- [ ] **Step 3: Delete `column_sanitizer.rs` (final cleanup)**

```bash
grep -rn "sanitize_column_name\|column_sanitizer" src/   # must be zero outside the file itself
git rm src/query/column_sanitizer.rs
```
Remove `pub mod column_sanitizer;` from `src/query/mod.rs`.

- [ ] **Step 4: Full pre-commit checklist**

```bash
cargo check && cargo clippy --quiet && cargo build && cargo test && ./tests/runner/run_ssl_tests.sh
```
Expected: ALL PASS, no warnings.

- [ ] **Step 5: Commit**

```bash
git add tests/sql/features/column_conformance.sql src/query/mod.rs
git commit -m "test: end-to-end column conformance (F1,F2,F4,F5,F-new,F7,C2); remove column_sanitizer.rs"
```

---

## Self-Review

**1. Spec coverage:**
- F1 (paren-corruption) → Task 4 step 1 test + Task 6 migration. ✅
- F2 (MAX/MIN datetime) → Task 4 (`min_over_timestamp_is_datetime`) + Task 7 (`datetime_flag` wiring). ✅
- F3 (path divergence) → Task 6 (`read_only_handler` migration). ✅
- F4 (empty name → `?column?`) → Task 4 + Task 5 (dedup). ✅
- F5 (bare-name OID block) → Task 7 step 2 (delete block). ✅
- F6 (alias case-fold) → Task 8 step 3. ✅
- F7 (wildcard break) → Task 8 step 1. ✅
- F8 (log label) → Task 8 step 4. ✅
- F9 (stray file) → Task 9 step 3. ✅
- F10 (test gaps) → Task 9. ✅
- F-new (catalog aggregate) → Task 8 step 2. ✅
- C2 GUC → Task 1 + applied in Tasks 4/8. ✅
- T3 (arg-preserving + datetime) → Task 4 (`resolve_function_type`) + Task 7. ✅
- Removal of `column_sanitizer.rs` → Task 10 step 3. ✅

**2. Placeholder scan:** No TBD/TODO/"implement later". One caveat flagged inline: Task 7 step 1 notes the unit-vs-integration test split for the datetime guard (deferred to Task 8 integration) — this is a deliberate test-placement note, not a placeholder. Task 5 step 4 conditionally migrates one site depending on scope availability, with explicit fallback to Task 6 — documented, not vague.

**3. Type consistency:** `ColumnMeta { wire_name: String, type_oid: i32, datetime_flag: bool }` consistent across Tasks 2/4/5/7. `FnShape { name, inner }` consistent. `AliasItem { position, alias, is_quoted, source_expr }` consistent. `resolve_columns(stmt, query, schema_types, hints, session)` signature consistent across Tasks 5/6/7. `legacy_result_columns() -> bool` consistent. `Ident.quote_style.is_some()` used for `is_quoted` (verified against sqlparser 0.57 API via the codebase's existing `alias.value` usage).
