use crate::PgSqliteError;
use crate::translator::{TranslationMetadata, ColumnTypeHint, ExpressionType};
use crate::types::PgType;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::debug;

/// Regex patterns for unnest function calls
static UNNEST_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bunnest\s*\(\s*([^)]+)\s*\)").unwrap()
});


static UNNEST_FROM_CLAUSE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bFROM\s+unnest\s*\(\s*([^)]+)\s*\)(?:\s+(?:AS\s+)?(\w+))?").unwrap()
});

static UNNEST_WITH_ORDINALITY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bFROM\s+unnest\s*\(\s*([^)]+)\s*\)\s+WITH\s+ORDINALITY(?:\s+(?:AS\s+)?(\w+))?").unwrap()
});

// === PATCH v27: DBeaver queries table indexes with
//   JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n) ON true
// The existing regexes only match `FROM unnest(...)`, so this form reached
// SQLite verbatim and failed with "near ( : syntax error". Translate it to a
// json_each() subquery whose column names come from the AS k(attnum, n) list,
// so query-body references to k.attnum / k.n need no rewriting.
static JOIN_LATERAL_UNNEST_ORDINALITY_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)((?:LEFT\s+)?JOIN)\s+LATERAL\s+unnest\s*\(\s*([^)]+?)\s*\)\s+WITH\s+ORDINALITY(?:\s+(?:AS\s+)?(\w+))?\s*\(\s*([^)]*)\s*\)").unwrap()
});

/// Translates PostgreSQL unnest() function calls to SQLite json_each() equivalents
pub struct UnnestTranslator;

impl UnnestTranslator {
    /// Check if SQL contains unnest function calls
    pub fn contains_unnest(sql: &str) -> bool {
        // Fast path: check for unnest / generate_series before any expensive ops
        if !sql.contains("unnest") && !sql.contains("UNNEST")
            && !sql.contains("generate_series") && !sql.contains("GENERATE_SERIES")
        {
            return false;
        }
        
        // Only do lowercase conversion if unnest is present
        let sql_lower = sql.to_lowercase();
        // === PATCH v28: also catch the DBeaver index-query variant that
        // uses ARRAY(SELECT ... FROM generate_series(...)) instead of
        // JOIN LATERAL unnest(...) WITH ORDINALITY.
        sql_lower.contains("unnest(") || sql_lower.contains("generate_series(")
    }
    
    /// Translate unnest() function calls to json_each() equivalents
    pub fn translate_unnest(sql: &str) -> Result<String, PgSqliteError> {
        if !Self::contains_unnest(sql) {
            return Ok(sql.to_string());
        }
        
        let mut result = sql.to_string();
        
        // Handle different patterns:
        // 1. FROM unnest(array) WITH ORDINALITY AS alias
        // 2. FROM unnest(array) AS alias
        // 3. unnest(array) in SELECT clause
        
        // === PATCH v28: DBeaver index-columns variant (ARRAY + generate_series)
        result = Self::translate_pg_array_generate_series(&result)?;
        result = Self::translate_join_lateral_with_ordinality(&result)?;
        result = Self::translate_from_clause_with_ordinality(&result)?;
        result = Self::translate_from_clause(&result)?;
        result = Self::translate_select_clause(&result)?;
        
        Ok(result)
    }
    
    /// Translate unnest with metadata
    pub fn translate_with_metadata(sql: &str) -> Result<(String, TranslationMetadata), PgSqliteError> {
        if !Self::contains_unnest(sql) {
            return Ok((sql.to_string(), TranslationMetadata::new()));
        }
        
        let mut result = sql.to_string();
        let mut metadata = TranslationMetadata::new();
        
        // Translate unnest calls
        result = Self::translate_join_lateral_with_ordinality(&result)?;
        result = Self::translate_from_clause_with_ordinality(&result)?;
        result = Self::translate_from_clause(&result)?;
        result = Self::translate_select_clause(&result)?;
        
        // Extract metadata for aliased unnest functions
        Self::extract_unnest_metadata(&result, &mut metadata);
        
        Ok((result, metadata))
    }
    
    /// Translate FROM unnest(array) AS alias to FROM json_each(array) AS alias
    fn translate_from_clause(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Collect replacements to avoid borrowing issues
        let mut replacements = Vec::new();
        for captures in UNNEST_FROM_CLAUSE_REGEX.captures_iter(&result) {
            let array_expr = captures[1].trim();
            let alias = captures.get(2).map(|m| m.as_str()).unwrap_or("unnest_table");
            
            // Convert unnest(array) to json_each(array) with proper column selection
            let replacement = format!("FROM json_each({array_expr}) AS {alias}");
            
            replacements.push((captures[0].to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated FROM unnest: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Translate FROM unnest(array) WITH ORDINALITY AS alias to CTE with ROW_NUMBER
    fn translate_from_clause_with_ordinality(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Collect replacements to avoid borrowing issues
        let mut replacements = Vec::new();
        for captures in UNNEST_WITH_ORDINALITY_REGEX.captures_iter(&result) {
            let array_expr = captures[1].trim();
            let alias = captures.get(2).map(|m| m.as_str()).unwrap_or("unnest_table");
            
            // Convert unnest(array) WITH ORDINALITY to a CTE that includes row numbers
            // PostgreSQL's WITH ORDINALITY returns (value, ordinality) columns
            let replacement = format!(
                "FROM (SELECT value, (key + 1) AS ordinality FROM json_each({array_expr})) AS {alias}"
            );
            
            replacements.push((captures[0].to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated FROM unnest WITH ORDINALITY: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Translate JOIN LATERAL unnest(x) WITH ORDINALITY AS k(c1, c2) ON true
    ///
    /// SQLite has no LATERAL, so a json_each() subquery cannot reference the
    /// outer row's column (e.g. ix.indkey) -> "no such column". The only safe
    /// translation is a zero-row subquery: `JOIN (SELECT NULL, NULL WHERE 0)`
    /// keeps the original `AS k(attnum, n) ON true` (column names come from the
    /// alias list), an INNER zero-row join yields zero rows, the SELECT list is
    /// never evaluated (no UDF calls), and the DBeaver index tab shows empty
    /// instead of erroring / poisoning the transaction.
    fn translate_join_lateral_with_ordinality(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        let mut replacements = Vec::new();
        for captures in JOIN_LATERAL_UNNEST_ORDINALITY_REGEX.captures_iter(&result) {
            let original = captures[0].to_string();
            let alias = captures.get(3).map(|m| m.as_str()).unwrap_or("unnest_table");
            let col_list = captures.get(4).map(|m| m.as_str()).unwrap_or("value, ordinality");
            let cols: Vec<String> = col_list.split(',').map(|s| s.trim().to_string()).collect();
            let value_col = cols.first().map(|s| s.as_str()).unwrap_or("value");
            let ordinal_col = cols.get(1).map(|s| s.as_str()).unwrap_or("ordinality");
            // Force INNER JOIN so a LEFT JOIN variant also yields zero rows and
            // never evaluates the SELECT list with NULL lateral columns. Column
            // names are defined INSIDE the subquery (SQLite < 3.35 rejects the
            // AS k(attnum, n) column-alias-list on a FROM subquery), so the
            // query-body k.attnum / k.n references resolve via AS k.
            let replacement = format!(
                "JOIN (SELECT NULL AS {value_col}, NULL AS {ordinal_col} WHERE 0) AS {alias}"
            );
            replacements.push((original, replacement));
        }
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated JOIN LATERAL unnest WITH ORDINALITY: {} -> {}", original, replacement);
        }
        Ok(result)
    }

    /// Translate `ARRAY( SELECT ... FROM generate_series(...) ... )` to NULL.
    ///
    /// DBeaver's other index-columns variant wraps the lateral expansion in a
    /// PostgreSQL ARRAY(...) subquery over generate_series(). SQLite has neither
    /// ARRAY() nor the generate_series table function, so the whole ARRAY(...)
    /// subquery is replaced with NULL via bracket matching. The rest of the query
    /// (pg_index/pg_class/pg_namespace/pg_am views + UDFs) then executes fine and
    /// returns index rows with a NULL columns cell.
    fn translate_pg_array_generate_series(sql: &str) -> Result<String, PgSqliteError> {
        let lower = sql.to_lowercase();
        if !lower.contains("array(") || !lower.contains("generate_series(") {
            return Ok(sql.to_string());
        }
        let mut result = sql.to_string();
        let mut replacements = Vec::new();
        let mut search_from = 0usize;
        loop {
            let rel = result[search_from..].to_lowercase().find("array(");
            let Some(rel) = rel else { break };
            let start = search_from + rel;
            // Bracket match: "array(" is 6 chars; start+6 points past the '(',
            // so depth starts at 1 for ARRAY('s own '(' and the FIRST ')' that
            // brings it back to 0 closes the ARRAY(...) subquery.
            let mut depth = 1i32;
            let mut i = start + 6;
            let bytes = result.as_bytes();
            while i < bytes.len() {
                match bytes[i] {
                    b'(' => depth += 1,
                    b')' => {
                        depth -= 1;
                        if depth == 0 {
                            break;
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            if i >= bytes.len() {
                // Unbalanced; leave as-is rather than mangling the query.
                break;
            }
            let original = result[start..=i].to_string();
            replacements.push((original, "NULL".to_string()));
            search_from = i + 1;
        }
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated ARRAY(generate_series): {} -> {}", original, replacement);
        }
        Ok(result)
    }

    /// Translate unnest() calls in SELECT clause to subqueries with json_each
    fn translate_select_clause(sql: &str) -> Result<String, PgSqliteError> {
        let mut result = sql.to_string();
        
        // Handle unnest() in SELECT clause - this is more complex
        // For now, we'll provide a basic translation
        let mut replacements = Vec::new();
        for captures in UNNEST_REGEX.captures_iter(&result) {
            let array_expr = captures[1].trim();
            
            // This is a simplified translation that works for basic cases
            // More complex cases might need different handling
            let replacement = format!("(SELECT value FROM json_each({array_expr}))");
            
            replacements.push((captures[0].to_string(), replacement));
        }
        
        // Apply replacements
        for (original, replacement) in replacements {
            result = result.replace(&original, &replacement);
            debug!("Translated SELECT unnest: {} -> {}", original, replacement);
        }
        
        Ok(result)
    }
    
    /// Extract metadata for aliased unnest functions
    fn extract_unnest_metadata(sql: &str, metadata: &mut TranslationMetadata) {
        // Look for aliased unnest functions (now converted to json_each)
        let alias_regex = Regex::new(r"(?i)json_each\s*\([^)]+\)\s+(?:AS\s+)?(\w+)").unwrap();
        
        for captures in alias_regex.captures_iter(sql) {
            let alias = captures[1].to_string();
            debug!("Found json_each (unnest) alias: {}", alias);
            
            metadata.add_hint(alias, ColumnTypeHint {
                source_column: None,
                suggested_type: Some(PgType::Text), // json_each returns text values
                datetime_subtype: None,
                is_expression: true,
                expression_type: Some(ExpressionType::Other),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_unnest_from_clause() {
        let sql = "SELECT value FROM unnest(ARRAY[1,2,3]) AS t";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each"));
        assert!(!result.contains("unnest"));
    }
    
    #[test]
    fn test_unnest_from_clause_with_alias() {
        let sql = "SELECT t.value FROM unnest('[1,2,3]'::json) AS t";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each('[1,2,3]'::json) AS t"));
    }
    
    #[test]
    fn test_unnest_select_clause() {
        let sql = "SELECT unnest(tags) FROM articles";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("(SELECT value FROM json_each(tags))"));
    }
    
    #[test]
    fn test_no_unnest() {
        let sql = "SELECT name FROM users";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert_eq!(result, "SELECT name FROM users");
    }
    
    #[test]
    fn test_contains_unnest() {
        assert!(UnnestTranslator::contains_unnest("SELECT unnest(array) FROM table"));
        assert!(UnnestTranslator::contains_unnest("FROM unnest(array) AS t"));
        assert!(!UnnestTranslator::contains_unnest("SELECT name FROM users"));
        
        // Test the specific query from the integration test
        assert!(UnnestTranslator::contains_unnest("SELECT value FROM unnest('[\"first\", \"second\", \"third\"]') AS t"));
    }
    
    #[test]
    fn test_unnest_with_metadata() {
        let sql = "SELECT value FROM unnest('[1,2,3]') AS expanded";
        let (result, _metadata) = UnnestTranslator::translate_with_metadata(sql).unwrap();
        assert!(result.contains("json_each"));
        // The metadata should contain hints for the alias if it's a table alias
    }
    
    #[test]
    fn test_unnest_with_ordinality() {
        let sql = "SELECT value, ordinality FROM unnest('[1,2,3]') WITH ORDINALITY AS t";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each"));
        assert!(result.contains("ordinality"));
        assert!(result.contains("(key + 1)"));
        assert!(!result.contains("WITH ORDINALITY"));
    }
    
    #[test]
    fn test_unnest_with_ordinality_no_alias() {
        let sql = "SELECT value, ordinality FROM unnest('[1,2,3]') WITH ORDINALITY";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each"));
        assert!(result.contains("AS unnest_table"));
        assert!(result.contains("ordinality"));
    }
    
    #[test]
    fn test_unnest_with_ordinality_complex() {
        let sql = "SELECT t.value, t.ordinality FROM unnest(ARRAY['a','b','c']) WITH ORDINALITY AS t WHERE t.ordinality > 1";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each"));
        assert!(result.contains("(key + 1) AS ordinality"));
        assert!(!result.contains("WITH ORDINALITY"));
    }
    
    #[test]
    fn test_unnest_with_ordinality_uppercase() {
        let sql = "SELECT value, ordinality FROM UNNEST('[\"first\", \"second\", \"third\"]') WITH ORDINALITY AS t ORDER BY ordinality";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each"));
        assert!(result.contains("(key + 1) AS ordinality"));
        assert!(!result.contains("WITH ORDINALITY"));
    }
    
    #[test]
    fn test_integration_test_query() {
        let sql = "SELECT value FROM unnest('[\"first\", \"second\", \"third\"]') AS t";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(result.contains("json_each"));
        assert!(!result.contains("unnest"));
    }

    // === PATCH v27: DBeaver index query (JOIN LATERAL unnest WITH ORDINALITY)
    #[test]
    fn v27_dbeaver_join_lateral_unnest() {
        let sql = "SELECT i.relname AS index_name, array_agg(COALESCE(a.attname, pg_get_indexdef(ix.indexrelid, CAST(k.n AS INTEGER), true)) ORDER BY k.n) AS columns, ix.indisunique AS is_unique, ix.indisprimary AS is_primary, pg_get_expr(ix.indpred, ix.indrelid) AS filter_expr, am.amname AS index_type, ix.indnkeyatts AS nkeyatts, ix.indkey AS indkey, obj_description(i.oid, 'pg_class') AS index_comment FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_class i ON i.oid = ix.indexrelid JOIN pg_namespace n ON n.oid = t.relnamespace JOIN pg_am am ON am.oid = i.relam JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(attnum, n) ON true LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum AND k.attnum > 0 WHERE n.nspname = 'public' AND t.relname = 'bath_records' GROUP BY i.relname, i.oid, ix.indisunique, ix.indisprimary, ix.indpred, ix.indrelid, am.amname, ix.indnkeyatts, ix.indkey ORDER BY i.relname";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(!result.contains("unnest"), "unnest 必须被翻译: {}", result);
        assert!(result.contains("(SELECT NULL AS attnum, NULL AS n WHERE 0)"), "0 行子查询: {}", result);
        assert!(result.contains(") AS k ON true"), "别名保留: {}", result);
        assert!(result.contains("k.attnum"), "k.attnum 引用保留: {}", result);
        assert!(result.contains("k.n"), "k.n 引用保留: {}", result);
    }

    #[test]
    fn v27_left_join_lateral_unnest() {
        let sql = "SELECT * FROM pg_index ix LEFT JOIN LATERAL unnest(ix.indkey) WITH ORDINALITY AS k(v, ord) ON true";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(!result.contains("unnest"));
        assert!(result.contains("(SELECT NULL AS v, NULL AS ord WHERE 0)"));
        assert!(result.contains(") AS k ON true"));
        assert!(!result.contains("LEFT JOIN"), "LEFT 强制改 INNER 避免 NULL 列求值");
    }

    // === PATCH v28: DBeaver index-columns variant (ARRAY + generate_series)
    #[test]
    fn v28_array_generate_series_replaced_with_null() {
        let sql = "SELECT i.relname AS index_name, ARRAY( SELECT COALESCE(a.attname, pg_get_indexdef(ix.indexrelid, pos.n, true)) FROM generate_series(1, array_length(string_to_array(ix.indkey::text, ' '), 1)) AS pos(n) LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = (string_to_array(ix.indkey::text, ' '))[pos.n]::int2 AND a.attnum > 0 ORDER BY pos.n ) AS columns, ix.indisunique AS is_unique, ix.indisprimary AS is_primary, pg_get_expr(ix.indpred, ix.indrelid) AS filter_expr, am.amname AS index_type, NULL::smallint AS nkeyatts, ix.indkey AS indkey, obj_description(i.oid, 'pg_class') AS index_comment FROM pg_index ix JOIN pg_class t ON t.oid = ix.indrelid JOIN pg_class i ON i.oid = ix.indexrelid JOIN pg_namespace n ON n.oid = t.relnamespace JOIN pg_am am ON am.oid = i.relam WHERE n.nspname = 'public' AND t.relname = 'bath_records' ORDER BY i.relname";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert!(!result.contains("generate_series"), "generate_series 必须消失: {}", result);
        assert!(!result.contains("ARRAY("), "ARRAY( 必须消失: {}", result);
        assert!(result.contains("NULL AS columns") || result.contains(", NULL AS columns"), "columns 列应为 NULL: {}", result);
        assert!(result.contains("ix.indkey AS indkey"), "indkey 保留: {}", result);
        assert!(result.contains("obj_description"), "obj_description 保留: {}", result);
    }

    #[test]
    fn v28_generate_series_without_array_is_left_alone() {
        let sql = "SELECT * FROM some_table WHERE x = 1";
        let result = UnnestTranslator::translate_unnest(sql).unwrap();
        assert_eq!(result, sql);
    }
}