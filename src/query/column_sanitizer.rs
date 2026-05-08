pub fn sanitize_column_name(name: &str) -> &str {
    match name.find('(') {
        Some(pos) => &name[..pos],
        None => name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_function() {
        assert_eq!(sanitize_column_name("version()"), "version");
    }

    #[test]
    fn test_count_star() {
        assert_eq!(sanitize_column_name("count(*)"), "count");
    }

    #[test]
    fn test_max_with_arg() {
        assert_eq!(sanitize_column_name("max(id)"), "max");
    }

    #[test]
    fn test_coalesce_nested() {
        assert_eq!(sanitize_column_name("COALESCE(max(id), 0)"), "COALESCE");
    }

    #[test]
    fn test_current_timestamp() {
        assert_eq!(sanitize_column_name("current_timestamp"), "current_timestamp");
    }

    #[test]
    fn test_plain_column() {
        assert_eq!(sanitize_column_name("my_column"), "my_column");
    }

    #[test]
    fn test_empty_string() {
        assert_eq!(sanitize_column_name(""), "");
    }

    #[test]
    fn test_just_parentheses() {
        assert_eq!(sanitize_column_name("()"), "");
    }

    #[test]
    fn test_open_paren_only() {
        assert_eq!(sanitize_column_name("("), "");
    }

    #[test]
    fn test_multiple_parens() {
        assert_eq!(sanitize_column_name("func(arg1, arg2)"), "func");
    }

    #[test]
    fn test_nested_parens() {
        assert_eq!(sanitize_column_name("outer(inner())"), "outer");
    }
}