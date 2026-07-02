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
