use std::collections::HashMap;
use anyhow::Result;
use crate::types::{PgType, SchemaTypeMapper};
use crate::translator::TranslationMetadata;
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
    if !query.split_whitespace().any(|w| w.eq_ignore_ascii_case("AS")) { return None; }
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
            let name = shape.name.trim();
            let lower = name.to_lowercase();
            let wire_name = lower.clone();
            let (type_oid, datetime_flag) = resolve_function_type(&lower, &shape.inner, ctx);
            return ColumnMeta { wire_name, type_oid, datetime_flag };
        }

        // Step 3: alias (position maps to an ExprWithAlias item).
        if let Some(item) = ctx.alias_view.and_then(|view| view.iter().find(|a| a.position == position)) {
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

/// Resolve all columns of a prepared statement into PG-conformant metadata.
/// Convergence point for DbHandler, ReadOnlyDbHandler, and extended paths.
pub async fn resolve_columns(
    stmt: &rusqlite::Statement<'_>,
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
    for i in 0..count {
        let raw = stmt.column_name(i)?.to_string();
        let ctx = ResolveCtx { schema_types, hints, alias_view: view_ref, legacy };
        let meta = ProjectionResolver::resolve(&raw, i, &ctx);
        out.push(meta);
    }
    dedup_question_columns(&mut out);
    Ok(out)
}

/// Resolve a list of column-name strings (from DbResponse.columns) into PG-conformant
/// metadata. Used by execute_select, which only has the column-name strings, not the stmt.
/// Same precedence and ?column?N dedup as [`resolve_columns`].
pub async fn resolve_columns_from_names(
    names: &[String],
    query: &str,
    schema_types: &HashMap<String, String>,
    hints: &TranslationMetadata,
    session: &crate::session::SessionState,
) -> Result<Vec<ColumnMeta>> {
    let legacy = session.legacy_result_columns().await;
    let alias_view = parse_alias_view(query);
    let view_ref = alias_view.as_deref();
    let mut out = Vec::with_capacity(names.len());
    for (i, raw) in names.iter().enumerate() {
        let ctx = ResolveCtx { schema_types, hints, alias_view: view_ref, legacy };
        let meta = ProjectionResolver::resolve(raw, i, &ctx);
        out.push(meta);
    }
    dedup_question_columns(&mut out);
    Ok(out)
}

/// Apply PostgreSQL's `?column?N` dedup to consecutive unnamed-expression columns.
/// First unnamed stays `?column?`; the second becomes `?column?2`, third `?column?3`, etc.
fn dedup_question_columns(metas: &mut [ColumnMeta]) {
    let mut count: usize = 0;
    for meta in metas.iter_mut() {
        if meta.wire_name == "?column?" {
            count += 1;
            if count > 1 {
                meta.wire_name = format!("?column?{}", count);
            }
        }
    }
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
    #[test]
    fn alias_view_as_with_tab_newline_whitespace() {
        // Hardened AS-gate must match `AS` separated by tab/newline, not just spaces.
        let v = parse_alias_view("SELECT id\tAS\nuser_id FROM users").unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].alias, "user_id");
        assert!(!v[0].is_quoted);
    }

    use std::collections::HashMap;
    use crate::translator::TranslationMetadata;

    fn ctx_no_aliases<'a>(schema: &'a HashMap<String,String>, legacy: bool) -> ResolveCtx<'a> {
        static EMPTY: once_cell::sync::Lazy<TranslationMetadata> = once_cell::sync::Lazy::new(TranslationMetadata::new);
        ResolveCtx { schema_types: schema, hints: &EMPTY, alias_view: None, legacy }
    }

    fn ctx_with_aliases<'a>(schema: &'a HashMap<String,String>, alias_view: &'a [AliasItem], legacy: bool) -> ResolveCtx<'a> {
        static EMPTY: once_cell::sync::Lazy<TranslationMetadata> = once_cell::sync::Lazy::new(TranslationMetadata::new);
        ResolveCtx { schema_types: schema, hints: &EMPTY, alias_view: Some(alias_view), legacy }
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

    #[test]
    fn step2_function_call_ignores_legacy() {
        // Guard: legacy affects only steps 3 & 5. A COUNT(*) projection must
        // always emit conformant wire-name `count` even when legacy=true.
        let schema = HashMap::new();
        let m = ProjectionResolver::resolve("COUNT(*)", 0, &ctx_no_aliases(&schema, true));
        assert_eq!(m.wire_name, "count", "legacy must not leak into step 2 casing");
        assert_eq!(m.type_oid, PgType::Int8.to_oid());
    }

    #[test]
    fn step3_unquoted_alias_legacy_preserves_case() {
        // Step 3 honors legacy: an unquoted alias is preserved as-written when
        // legacy=true, and lowercased when legacy=false.
        let schema = HashMap::new();
        let items = vec![AliasItem {
            position: 0,
            alias: "MyAlias".to_string(),
            is_quoted: false,
            source_expr: Expr::Identifier(sqlparser::ast::Ident::new("id")),
        }];
        let legacy_m = ProjectionResolver::resolve("id", 0, &ctx_with_aliases(&schema, &items, true));
        assert_eq!(legacy_m.wire_name, "MyAlias", "legacy preserves unquoted alias as-written");
        let conformant_m = ProjectionResolver::resolve("id", 0, &ctx_with_aliases(&schema, &items, false));
        assert_eq!(conformant_m.wire_name, "myalias", "conformant lowercases unquoted alias");
    }

    #[test]
    fn step5_fallback_legacy_preserves_case() {
        // Step 5 honors legacy: fallback casing preserves the raw name when
        // legacy=true and lowercases it when legacy=false.
        let schema = HashMap::new();
        let legacy_m = ProjectionResolver::resolve("MyCol", 0, &ctx_no_aliases(&schema, true));
        assert_eq!(legacy_m.wire_name, "MyCol", "legacy preserves fallback name as-written");
        let conformant_m = ProjectionResolver::resolve("MyCol", 0, &ctx_no_aliases(&schema, false));
        assert_eq!(conformant_m.wire_name, "mycol", "conformant lowercases fallback name");
    }

    #[test]
    fn array_agg_returns_text_oid() {
        // MUST-FIX #3: array_agg returns Text (JSON array storage).
        let schema = HashMap::new();
        let m = ProjectionResolver::resolve("array_agg(x)", 0, &ctx_no_aliases(&schema, false));
        assert_eq!(m.wire_name, "array_agg");
        assert_eq!(m.type_oid, PgType::Text.to_oid());
    }

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

    #[tokio::test]
    async fn resolve_columns_from_names_dedup_numbering() {
        // Unit test for the shared ?column?N dedup: two unnamed expressions
        // -> first is ?column?, second is ?column?2.
        let schema = HashMap::new();
        let hints = TranslationMetadata::new();
        let session = crate::session::SessionState::new("db".into(), "user".into());
        let names = vec!["a+b".to_string(), "c+d".to_string()];
        let metas = resolve_columns_from_names(
            &names, "SELECT a+b, c+d FROM t", &schema, &hints, &session,
        )
        .await
        .unwrap();
        assert_eq!(metas.len(), 2);
        assert_eq!(metas[0].wire_name, "?column?");
        assert_eq!(metas[1].wire_name, "?column?2");
    }
}
