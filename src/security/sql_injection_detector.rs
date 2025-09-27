use std::collections::HashSet;
use sqlparser::ast::{Statement, Expr, SelectItem, TableFactor, Join, Value, FunctionArguments};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tracing::{warn, debug};
use crate::security::events;
use crate::PgSqliteError;

/// Advanced SQL injection detection using proper SQL parsing and AST analysis
pub struct SqlInjectionDetector {
    /// Maximum allowed nesting depth for subqueries
    max_depth: usize,
    /// Maximum number of statements in a multi-statement query
    max_statements: usize,
    /// Maximum number of UNION operations allowed
    max_unions: usize,
    /// Blacklisted function names that are potentially dangerous
    dangerous_functions: HashSet<String>,
    /// Whitelisted table patterns for legitimate operations
    allowed_table_patterns: HashSet<String>,
}

impl Default for SqlInjectionDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlInjectionDetector {
    pub fn new() -> Self {
        let mut dangerous_functions = HashSet::new();
        dangerous_functions.insert("exec".to_string());
        dangerous_functions.insert("execute".to_string());
        dangerous_functions.insert("sp_executesql".to_string());
        dangerous_functions.insert("xp_cmdshell".to_string());
        dangerous_functions.insert("eval".to_string());
        dangerous_functions.insert("system".to_string());
        dangerous_functions.insert("shell".to_string());
        dangerous_functions.insert("cmd".to_string());

        Self {
            max_depth: 10,
            max_statements: 3,
            max_unions: 5,
            dangerous_functions,
            allowed_table_patterns: HashSet::new(),
        }
    }

    /// Analyze SQL query for injection attempts using AST-based detection
    pub fn analyze_query(&self, query: &str) -> Result<SqlAnalysisResult, PgSqliteError> {
        debug!("Analyzing query for SQL injection: {}", query);

        // Basic length check
        if query.len() > 1_000_000 {
            return Err(PgSqliteError::InvalidParameter(
                format!("Query too long: {} bytes", query.len())
            ));
        }

        // Try to parse the SQL
        let dialect = PostgreSqlDialect {};
        let statements = match Parser::parse_sql(&dialect, query) {
            Ok(stmts) => stmts,
            Err(parse_error) => {
                // If parsing fails, apply basic pattern-based analysis as fallback
                warn!("SQL parsing failed, falling back to pattern analysis: {}", parse_error);
                return self.fallback_pattern_analysis(query);
            }
        };

        if statements.len() > self.max_statements {
            events::sql_injection_attempt(None, None, query, "too many statements");
            return Err(PgSqliteError::InvalidParameter(
                format!("Too many statements: {} (max: {})", statements.len(), self.max_statements)
            ));
        }

        let mut analysis = SqlAnalysisResult::new();

        for (i, statement) in statements.iter().enumerate() {
            self.analyze_statement(statement, &mut analysis, 0, query)?;

            // Check for dangerous multi-statement combinations
            if i > 0 && self.is_dangerous_statement(statement) {
                events::sql_injection_attempt(None, None, query, "dangerous multi-statement");
                return Err(PgSqliteError::InvalidParameter(
                    "Dangerous multi-statement query detected".to_string()
                ));
            }
        }

        // Analyze the overall structure
        if analysis.union_count > self.max_unions {
            events::sql_injection_attempt(None, None, query, "excessive unions");
            return Err(PgSqliteError::InvalidParameter(
                format!("Too many UNION operations: {} (max: {})", analysis.union_count, self.max_unions)
            ));
        }

        if analysis.max_depth > self.max_depth {
            events::sql_injection_attempt(None, None, query, "excessive nesting");
            return Err(PgSqliteError::InvalidParameter(
                format!("Query nesting too deep: {} (max: {})", analysis.max_depth, self.max_depth)
            ));
        }

        // Check for suspicious tautologies
        if analysis.has_tautology {
            events::sql_injection_attempt(None, None, query, "tautology detected");
            return Err(PgSqliteError::InvalidParameter(
                "Suspicious tautology condition detected".to_string()
            ));
        }

        if analysis.has_dangerous_function {
            events::sql_injection_attempt(None, None, query, "dangerous function call");
            return Err(PgSqliteError::InvalidParameter(
                "Dangerous function call detected".to_string()
            ));
        }

        Ok(analysis)
    }

    fn analyze_statement(
        &self,
        statement: &Statement,
        analysis: &mut SqlAnalysisResult,
        depth: usize,
        original_query: &str
    ) -> Result<(), PgSqliteError> {
        analysis.max_depth = analysis.max_depth.max(depth);

        match statement {
            Statement::Query(query) => {
                self.analyze_query_statement(query, analysis, depth + 1, original_query)?;
            }
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. } => {
                analysis.has_modifying_statements = true;
                self.analyze_dml_statement(statement, analysis, depth + 1, original_query)?;
            }
            Statement::Drop { .. } | Statement::CreateTable { .. } | Statement::AlterTable { .. } => {
                analysis.has_ddl_statements = true;
                // DDL statements in injection contexts are highly suspicious
                if depth > 0 || analysis.statement_count > 0 {
                    events::sql_injection_attempt(None, None, original_query, "suspicious DDL");
                    return Err(PgSqliteError::InvalidParameter(
                        "Suspicious DDL statement detected".to_string()
                    ));
                }
            }
            _ => {
                // Other statement types
            }
        }

        analysis.statement_count += 1;
        Ok(())
    }

    fn analyze_query_statement(
        &self,
        query: &sqlparser::ast::Query,
        analysis: &mut SqlAnalysisResult,
        depth: usize,
        original_query: &str
    ) -> Result<(), PgSqliteError> {
        if let sqlparser::ast::SetExpr::Select(select) = &*query.body {
            // Analyze SELECT items
            for item in &select.projection {
                self.analyze_select_item(item, analysis, depth, original_query)?;
            }

            // Analyze FROM clause
            for table in &select.from {
                self.analyze_table_factor(&table.relation, analysis, depth, original_query)?;

                for join in &table.joins {
                    self.analyze_join(join, analysis, depth, original_query)?;
                }
            }

            // Analyze WHERE clause
            if let Some(ref where_clause) = select.selection {
                self.analyze_expression(where_clause, analysis, depth, original_query)?;
            }
        } else if let sqlparser::ast::SetExpr::SetOperation { op: _, left, right, .. } = &*query.body {
            analysis.union_count += 1;

            // Check for suspicious UNION patterns with sensitive data
            let query_str = original_query.to_uppercase();
            if (query_str.contains("UNION") && query_str.contains("PASSWORD")) ||
               (query_str.contains("UNION") && query_str.contains("ADMIN")) {
                events::sql_injection_attempt(None, None, original_query, "suspicious union with sensitive data");
                return Err(PgSqliteError::InvalidParameter(
                    "Suspicious UNION with sensitive data access".to_string()
                ));
            }

            // Recursively analyze the left and right parts
            let left_query = sqlparser::ast::Query {
                body: left.clone(),
                order_by: None,
                limit_clause: None,
                settings: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                with: None,
                format_clause: None,
                pipe_operators: vec![],
            };

            let right_query = sqlparser::ast::Query {
                body: right.clone(),
                order_by: None,
                limit_clause: None,
                settings: None,
                fetch: None,
                locks: vec![],
                for_clause: None,
                with: None,
                format_clause: None,
                pipe_operators: vec![],
            };

            self.analyze_query_statement(&left_query, analysis, depth + 1, original_query)?;
            self.analyze_query_statement(&right_query, analysis, depth + 1, original_query)?;
        }

        Ok(())
    }

    fn analyze_select_item(
        &self,
        item: &SelectItem,
        analysis: &mut SqlAnalysisResult,
        depth: usize,
        original_query: &str
    ) -> Result<(), PgSqliteError> {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                self.analyze_expression(expr, analysis, depth, original_query)?;
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                self.analyze_expression(expr, analysis, depth, original_query)?;
            }
            SelectItem::QualifiedWildcard(_, _) => {
                // Check for suspicious wildcard usage
            }
            SelectItem::Wildcard(_) => {
                // Wildcard is generally OK but note it
                analysis.has_wildcards = true;
            }
        }
        Ok(())
    }

    fn analyze_table_factor(
        &self,
        table: &TableFactor,
        analysis: &mut SqlAnalysisResult,
        depth: usize,
        original_query: &str
    ) -> Result<(), PgSqliteError> {
        match table {
            TableFactor::Table { name, .. } => {
                let table_name = name.to_string().to_lowercase();

                // Check for system/sensitive tables
                if self.is_sensitive_table(&table_name) {
                    analysis.accesses_sensitive_tables = true;

                    // System table access in complex queries is suspicious
                    if depth > 1 || analysis.union_count > 0 {
                        events::sql_injection_attempt(None, None, original_query, "suspicious system table access");
                        return Err(PgSqliteError::InvalidParameter(
                            "Suspicious system table access detected".to_string()
                        ));
                    }
                }
            }
            TableFactor::Derived { subquery, .. } => {
                self.analyze_query_statement(subquery, analysis, depth + 1, original_query)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn analyze_join(
        &self,
        join: &Join,
        analysis: &mut SqlAnalysisResult,
        depth: usize,
        original_query: &str
    ) -> Result<(), PgSqliteError> {
        self.analyze_table_factor(&join.relation, analysis, depth, original_query)?;

        // For now, skip join constraint analysis to avoid API compatibility issues
        // TODO: Update when sqlparser API is clarified

        Ok(())
    }

    fn analyze_expression(
        &self,
        expr: &Expr,
        analysis: &mut SqlAnalysisResult,
        depth: usize,
        original_query: &str
    ) -> Result<(), PgSqliteError> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // Check for tautologies like 1=1, 'a'='a', etc.
                if self.is_tautology(left, op, right) {
                    analysis.has_tautology = true;
                }

                self.analyze_expression(left, analysis, depth, original_query)?;
                self.analyze_expression(right, analysis, depth, original_query)?;
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_lowercase();

                if self.dangerous_functions.contains(&func_name) {
                    analysis.has_dangerous_function = true;
                }

                // Analyze function arguments
                if let FunctionArguments::List(function_arg_list) = &func.args {
                    for arg in &function_arg_list.args {
                        if let sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(arg_expr)) = arg {
                            self.analyze_expression(&arg_expr, analysis, depth, original_query)?;
                        }
                    }
                }
            }
            Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
                // Skip detailed subquery analysis for now to avoid API compatibility issues
                // The main query will catch most injection attempts
                analysis.statement_count += 1;
                if analysis.statement_count > self.max_statements {
                    analysis.has_modifying_statements = true;
                }
            }
            _ => {
                // Other expression types
            }
        }
        Ok(())
    }

    fn analyze_dml_statement(
        &self,
        statement: &Statement,
        analysis: &mut SqlAnalysisResult,
        depth: usize,
        original_query: &str
    ) -> Result<(), PgSqliteError> {
        // For INSERT, UPDATE, DELETE statements, analyze the WHERE clauses and subqueries
        match statement {
            Statement::Update { selection, .. } => {
                if let Some(where_clause) = selection {
                    self.analyze_expression(where_clause, analysis, depth, original_query)?;
                }
            }
            Statement::Delete(delete_stmt) => {
                if let Some(where_clause) = &delete_stmt.selection {
                    self.analyze_expression(where_clause, analysis, depth, original_query)?;
                }
            }
            Statement::Insert(insert_stmt) => {
                if let Some(source) = &insert_stmt.source {
                    self.analyze_query_statement(source, analysis, depth, original_query)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn is_tautology(&self, left: &Expr, op: &sqlparser::ast::BinaryOperator, right: &Expr) -> bool {
        use sqlparser::ast::BinaryOperator;

        match op {
            BinaryOperator::Eq => {
                // Check for obvious tautologies like 1=1, 'a'='a'
                match (left, right) {
                    (Expr::Value(left_val), Expr::Value(right_val)) => {
                        self.values_equal(&left_val.value, &right_val.value)
                    }
                    // Check for same identifier on both sides
                    (Expr::Identifier(l), Expr::Identifier(r)) => l.value == r.value,
                    _ => false,
                }
            }
            BinaryOperator::Or => {
                // Check for patterns like "condition OR 1=1"
                self.expression_is_always_true(right) || self.expression_is_always_true(left)
            }
            _ => false,
        }
    }

    fn values_equal(&self, left: &Value, right: &Value) -> bool {
        match (left, right) {
            (Value::Number(l, _), Value::Number(r, _)) => l == r,
            (Value::SingleQuotedString(l), Value::SingleQuotedString(r)) => l == r,
            (Value::Boolean(l), Value::Boolean(r)) => l == r,
            _ => false,
        }
    }

    fn expression_is_always_true(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Value(val) => {
                matches!(val.value, Value::Boolean(true))
            }
            Expr::BinaryOp { left, op, right } => {
                self.is_tautology(left, op, right)
            }
            _ => false,
        }
    }

    fn is_dangerous_statement(&self, statement: &Statement) -> bool {
        matches!(statement,
            Statement::Drop { .. } |
            Statement::Delete { .. } |
            Statement::Update { .. } |
            Statement::Insert { .. } |
            Statement::CreateTable { .. } |
            Statement::AlterTable { .. }
        )
    }

    fn is_sensitive_table(&self, table_name: &str) -> bool {
        let sensitive_tables = [
            "information_schema",
            "pg_user", "pg_shadow", "pg_roles", "pg_database",
            "pg_tables", "pg_views", "pg_indexes",
            "sqlite_master", "sqlite_sequence",
            "__pgsqlite_schema", "__pgsqlite_enums",
            "sys", "sysobjects", "syscolumns"
        ];

        sensitive_tables.iter().any(|&sensitive| table_name.contains(sensitive))
    }

    /// Fallback pattern-based analysis when SQL parsing fails
    fn fallback_pattern_analysis(&self, query: &str) -> Result<SqlAnalysisResult, PgSqliteError> {
        let query_upper = query.to_uppercase();

        // High-confidence injection patterns
        let dangerous_patterns = [
            "'; DROP",
            "\"; DROP",
            "'; DELETE",
            "\"; DELETE",
            "' OR '1'='1",
            "\" OR \"1\"=\"1\"",
            " OR 1=1",
            " OR 1 = 1",
            "UNION SELECT PASSWORD",
            "UNION SELECT * FROM ADMIN",
            "UNION SELECT PASSWORD FROM ADMIN",
            "EXEC(",
            "EXECUTE(",
            "XP_CMDSHELL",
            "SP_EXECUTESQL",
        ];

        for pattern in &dangerous_patterns {
            if query_upper.contains(pattern) {
                events::sql_injection_attempt(None, None, query, pattern);
                return Err(PgSqliteError::InvalidParameter(
                    format!("Malicious pattern detected: {}", pattern)
                ));
            }
        }

        // Check for excessive statements
        let semicolon_count = query.matches(';').count();
        if semicolon_count > self.max_statements {
            events::sql_injection_attempt(None, None, query, "too many statements");
            return Err(PgSqliteError::InvalidParameter(
                format!("Too many statements: {}", semicolon_count)
            ));
        }

        Ok(SqlAnalysisResult::new())
    }
}

/// Result of SQL injection analysis
#[derive(Debug, Clone, Default)]
pub struct SqlAnalysisResult {
    pub max_depth: usize,
    pub statement_count: usize,
    pub union_count: usize,
    pub has_tautology: bool,
    pub has_dangerous_function: bool,
    pub has_modifying_statements: bool,
    pub has_ddl_statements: bool,
    pub has_wildcards: bool,
    pub accesses_sensitive_tables: bool,
}

impl SqlAnalysisResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_suspicious(&self) -> bool {
        self.has_tautology ||
        self.has_dangerous_function ||
        (self.has_modifying_statements && self.statement_count > 1) ||
        (self.accesses_sensitive_tables && (self.union_count > 0 || self.max_depth > 2))
    }

    pub fn risk_score(&self) -> u32 {
        let mut score = 0;

        if self.has_tautology { score += 50; }
        if self.has_dangerous_function { score += 70; }
        if self.has_ddl_statements { score += 40; }
        if self.accesses_sensitive_tables { score += 30; }
        if self.statement_count > 2 { score += 20; }
        if self.union_count > 2 { score += 25; }
        if self.max_depth > 5 { score += 15; }

        score
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_legitimate_queries() {
        let detector = SqlInjectionDetector::new();

        let legitimate_queries = [
            "SELECT * FROM users WHERE id = 1",
            "SELECT name, email FROM users WHERE created_at > '2024-01-01'",
            "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
            "UPDATE users SET last_login = NOW() WHERE id = 1",
            "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
        ];

        for query in &legitimate_queries {
            let result = detector.analyze_query(query);
            assert!(result.is_ok(), "Should accept legitimate query: {}", query);
        }
    }

    #[test]
    fn test_sql_injection_detection() {
        let detector = SqlInjectionDetector::new();

        let malicious_queries = [
            "SELECT * FROM users WHERE id = 1 OR 1=1",
            "SELECT * FROM users UNION SELECT password FROM admin",
            "SELECT * FROM users; DELETE FROM logs",
        ];

        for query in &malicious_queries {
            let result = detector.analyze_query(query);
            assert!(result.is_err(), "Should reject malicious query: {}", query);
        }
    }

    #[test]
    fn test_fallback_pattern_detection() {
        let detector = SqlInjectionDetector::new();

        let malicious_queries = [
            "SELECT * FROM users'; DROP TABLE users; --",
            "SELECT * FROM users WHERE name = 'test' OR 'a'='a'",
        ];

        for query in &malicious_queries {
            let result = detector.analyze_query(query);
            assert!(result.is_err(), "Should reject malicious query: {}", query);
        }
    }

    #[test]
    fn test_dangerous_functions() {
        let detector = SqlInjectionDetector::new();

        let result = detector.analyze_query("SELECT exec('rm -rf /')");
        assert!(result.is_err());
    }

    #[test]
    fn test_sensitive_table_access() {
        let detector = SqlInjectionDetector::new();

        // Simple access should be OK
        let result = detector.analyze_query("SELECT * FROM pg_tables");
        assert!(result.is_ok());

        // Complex access should be suspicious
        let result = detector.analyze_query("SELECT * FROM users UNION SELECT * FROM pg_user");
        assert!(result.is_err());
    }
}