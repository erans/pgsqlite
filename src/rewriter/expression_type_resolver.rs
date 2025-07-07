use sqlparser::ast::{
    Expr, BinaryOperator, UnaryOperator, Value, Function, FunctionArg, FunctionArgExpr, 
    FunctionArguments, Query, TableFactor, Join, SetExpr, ValueWithSpan, Cte, SelectItem,
    SelectItemQualifiedWildcardKind, ObjectNamePart
};
use std::collections::HashMap;
use rusqlite::Connection;
use crate::types::{PgType, SchemaTypeMapper};

/// Context for resolving types within a query
#[derive(Debug, Clone, Default)]
pub struct QueryContext {
    /// Maps table aliases to actual table names
    pub table_aliases: HashMap<String, String>,
    /// Current table being queried (for unqualified columns)
    pub default_table: Option<String>,
    /// Maps CTE names to their column types
    pub cte_columns: HashMap<String, Vec<(String, PgType)>>,
    /// Maps derived table aliases to their column types
    pub derived_table_columns: HashMap<String, Vec<(String, PgType)>>,
}

impl QueryContext {
    /// Find the table name for an unqualified column
    pub fn find_table_for_column(&self, _column: &str) -> Option<String> {
        // For now, return the default table if available
        // In a more sophisticated implementation, we would search all tables
        // to find which one contains this column
        self.default_table.clone()
    }
}

/// Resolves expression types by analyzing the query and schema
pub struct ExpressionTypeResolver<'a> {
    conn: &'a Connection,
    /// Cache of table.column -> PgType mappings
    type_cache: HashMap<String, PgType>,
}

impl<'a> ExpressionTypeResolver<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            conn,
            type_cache: HashMap::new(),
        }
    }
    
    /// Get the connection
    pub fn conn(&self) -> &Connection {
        self.conn
    }
    
    /// Build query context from a parsed query
    pub fn build_context(&mut self, query: &Query) -> QueryContext {
        let mut context = QueryContext::default();
        
        // First, process CTEs if any
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                self.process_cte(cte, &mut context);
            }
        }
        
        // Extract table information from query body
        match &*query.body {
            SetExpr::Select(select) => {
                for table in &select.from {
                    self.process_table_with_joins(&table.relation, &table.joins, &mut context);
                }
            }
            _ => {}
        }
        
        context
    }
    
    /// Process a CTE and extract its column types
    fn process_cte(&mut self, cte: &Cte, context: &mut QueryContext) {
        let cte_name = cte.alias.name.value.clone();
        let mut column_types = Vec::new();
        
        // Analyze the CTE query to determine column types
        match &*cte.query.body {
            SetExpr::Select(select) => {
                // Build a temporary context for the CTE
                let mut cte_context = QueryContext::default();
                
                // Process tables in the CTE
                for table in &select.from {
                    self.process_table_with_joins(&table.relation, &table.joins, &mut cte_context);
                }
                
                // Analyze projection to get column types
                for (idx, item) in select.projection.iter().enumerate() {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            let expr_type = self.resolve_expr_type(expr, &cte_context);
                            let col_name = if !cte.alias.columns.is_empty() {
                                cte.alias.columns.get(idx).map(|c| c.name.value.clone()).unwrap_or_else(|| format!("column{}", idx))
                            } else {
                                // Try to extract name from expression
                                self.extract_column_name(expr).unwrap_or_else(|| format!("column{}", idx))
                            };
                            column_types.push((col_name, expr_type));
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let expr_type = self.resolve_expr_type(expr, &cte_context);
                            column_types.push((alias.value.clone(), expr_type));
                        }
                        SelectItem::Wildcard(_) => {
                            // For wildcards, we need to get all columns from the referenced tables
                            if let Some(table) = &cte_context.default_table {
                                if let Ok(cols) = self.get_table_columns(table) {
                                    column_types.extend(cols);
                                }
                            }
                        }
                        SelectItem::QualifiedWildcard(name, _) => {
                            let table_name = match name {
                                SelectItemQualifiedWildcardKind::ObjectName(obj_name) => {
                                    obj_name.0.last().map(|p| match p {
                                        ObjectNamePart::Identifier(i) => i.value.clone(),
                                    }).unwrap_or_default()
                                }
                                SelectItemQualifiedWildcardKind::Expr(_) => String::new(),
                            };
                            let actual_table = cte_context.table_aliases.get(&table_name)
                                .cloned()
                                .unwrap_or(table_name);
                            if let Ok(cols) = self.get_table_columns(&actual_table) {
                                column_types.extend(cols);
                            }
                        }
                    }
                }
            }
            _ => {} // Handle other SetExpr variants if needed
        }
        
        context.cte_columns.insert(cte_name, column_types);
    }
    
    /// Process a table and its joins to build context
    pub fn process_table_with_joins(&mut self, table: &TableFactor, joins: &[Join], context: &mut QueryContext) {
        match table {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                
                if let Some(alias) = alias {
                    let alias_name = alias.name.value.clone();
                    
                    // Check if this table name refers to a CTE
                    if let Some(cte_columns) = context.cte_columns.get(&table_name) {
                        // Map the CTE columns to the alias
                        context.derived_table_columns.insert(alias_name.clone(), cte_columns.clone());
                        context.table_aliases.insert(alias_name, table_name.clone());
                    } else {
                        // Regular table
                        context.table_aliases.insert(alias_name, table_name.clone());
                    }
                } else {
                    // Check if this is a CTE reference without alias
                    if context.cte_columns.contains_key(&table_name) {
                        // Use the CTE name directly
                        context.default_table = Some(table_name);
                    } else if context.default_table.is_none() {
                        context.default_table = Some(table_name);
                    }
                }
            }
            TableFactor::Derived { subquery, alias, .. } => {
                // Handle subqueries by analyzing their projection
                if let Some(alias) = alias {
                    let alias_name = alias.name.value.clone();
                    let mut column_types = Vec::new();
                    
                    // Build context for subquery
                    let subquery_context = self.build_context(subquery);
                    
                    // Analyze subquery projection
                    match &*subquery.body {
                        SetExpr::Select(select) => {
                            for (idx, item) in select.projection.iter().enumerate() {
                                match item {
                                    SelectItem::UnnamedExpr(expr) => {
                                        let expr_type = self.resolve_expr_type(expr, &subquery_context);
                                        let col_name = if !alias.columns.is_empty() {
                                            alias.columns.get(idx).map(|c| c.name.value.clone()).unwrap_or_else(|| format!("column{}", idx))
                                        } else {
                                            self.extract_column_name(expr).unwrap_or_else(|| format!("column{}", idx))
                                        };
                                        column_types.push((col_name, expr_type));
                                    }
                                    SelectItem::ExprWithAlias { expr, alias: col_alias } => {
                                        let expr_type = self.resolve_expr_type(expr, &subquery_context);
                                        column_types.push((col_alias.value.clone(), expr_type));
                                    }
                                    _ => {} // Handle other select items
                                }
                            }
                        }
                        _ => {}
                    }
                    
                    context.derived_table_columns.insert(alias_name.clone(), column_types);
                    context.table_aliases.insert(alias_name.clone(), "__derived__".to_string());
                    
                    // If this is the only table in the FROM clause, set it as default
                    if context.default_table.is_none() {
                        context.default_table = Some(alias_name);
                    }
                }
            }
            _ => {}
        }
        
        // Process joins
        for join in joins {
            self.process_table_with_joins(&join.relation, &[], context);
        }
    }
    
    /// Extract column name from an expression
    pub fn extract_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(parts) => parts.last().map(|i| i.value.clone()),
            Expr::Function(func) => {
                // For functions, use the function name as a hint
                Some(func.name.to_string().to_lowercase())
            }
            _ => None,
        }
    }
    
    /// Get all columns from a table
    fn get_table_columns(&mut self, table_name: &str) -> Result<Vec<(String, PgType)>, String> {
        let mut columns = Vec::new();
        
        // Query SQLite's pragma to get column information
        let query = format!("PRAGMA table_info({})", table_name);
        if let Ok(mut stmt) = self.conn.prepare(&query) {
            if let Ok(rows) = stmt.query_map([], |row| {
                let col_name: String = row.get(1)?;
                Ok(col_name)
            }) {
                for row in rows {
                    if let Ok(col_name) = row {
                        if let Some(type_oid) = SchemaTypeMapper::get_type_from_schema(self.conn, table_name, &col_name) {
                            let pg_type = PgType::from_oid(type_oid).unwrap_or(PgType::Text);
                            columns.push((col_name, pg_type));
                        } else {
                            columns.push((col_name, PgType::Text));
                        }
                    }
                }
            }
        }
        
        Ok(columns)
    }
    
    /// Resolve the type of an expression
    pub fn resolve_expr_type(&mut self, expr: &Expr, context: &QueryContext) -> PgType {
        match expr {
            Expr::Identifier(ident) => {
                self.resolve_column_type(None, &ident.value, context)
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() >= 2 {
                    let table = &parts[parts.len() - 2].value;
                    let column = &parts[parts.len() - 1].value;
                    self.resolve_column_type(Some(table), column, context)
                } else {
                    PgType::Text
                }
            }
            Expr::Value(ValueWithSpan { value, .. }) => self.resolve_value_type(value),
            Expr::BinaryOp { left, op, right } => {
                let left_type = self.resolve_expr_type(left, context);
                let right_type = self.resolve_expr_type(right, context);
                self.infer_binary_op_type(left_type, op.clone(), right_type)
            }
            Expr::UnaryOp { op, expr } => {
                let expr_type = self.resolve_expr_type(expr, context);
                self.infer_unary_op_type(op.clone(), expr_type)
            }
            Expr::Function(func) => {
                self.resolve_function_type(func, context)
            }
            Expr::Cast { data_type, .. } => {
                // Convert SQL data type to PgType
                self.sql_type_to_pg_type(&data_type.to_string())
            }
            Expr::Case { else_result, .. } => {
                // For CASE expressions, use the type of else_result or Text
                if let Some(else_expr) = else_result {
                    self.resolve_expr_type(else_expr, context)
                } else {
                    PgType::Text
                }
            }
            Expr::Nested(expr) => self.resolve_expr_type(expr, context),
            Expr::Subquery(subquery) => {
                // For scalar subqueries, analyze the projection
                let subquery_context = self.build_context(subquery);
                match &*subquery.body {
                    SetExpr::Select(select) => {
                        if let Some(first_item) = select.projection.first() {
                            match first_item {
                                SelectItem::UnnamedExpr(expr) => {
                                    self.resolve_expr_type(expr, &subquery_context)
                                }
                                SelectItem::ExprWithAlias { expr, .. } => {
                                    self.resolve_expr_type(expr, &subquery_context)
                                }
                                _ => PgType::Text,
                            }
                        } else {
                            PgType::Text
                        }
                    }
                    _ => PgType::Text,
                }
            }
            _ => PgType::Text, // Default for unknown expressions
        }
    }
    
    /// Resolve column type from schema
    fn resolve_column_type(&mut self, table: Option<&str>, column: &str, context: &QueryContext) -> PgType {
        // Determine actual table name
        let table_name = if let Some(t) = table {
            // Check if it's an alias
            context.table_aliases.get(t)
                .cloned()
                .unwrap_or_else(|| t.to_string())
        } else {
            // Use default table
            context.default_table.clone().unwrap_or_default()
        };
        
        // Check if this is a derived table or CTE
        if table_name == "__derived__" {
            // Look up in derived table columns
            if let Some(alias) = table {
                if let Some(columns) = context.derived_table_columns.get(alias) {
                    for (col_name, col_type) in columns {
                        if col_name == column {
                            return *col_type;
                        }
                    }
                }
            }
            return PgType::Text;
        }
        
        // Also check if the table_name itself is a derived table (when column is unqualified)
        if table.is_none() && context.derived_table_columns.contains_key(&table_name) {
            if let Some(columns) = context.derived_table_columns.get(&table_name) {
                for (col_name, col_type) in columns {
                    if col_name == column {
                        return *col_type;
                    }
                }
            }
        }
        
        // Check if this is a CTE
        if let Some(cte_columns) = context.cte_columns.get(&table_name) {
            for (col_name, col_type) in cte_columns {
                if col_name == column {
                    return *col_type;
                }
            }
            return PgType::Text;
        }
        
        // Check cache first
        let cache_key = format!("{}.{}", table_name, column);
        if let Some(&pg_type) = self.type_cache.get(&cache_key) {
            return pg_type;
        }
        
        // Look up in schema
        if let Some(type_oid) = SchemaTypeMapper::get_type_from_schema(self.conn, &table_name, column) {
            let pg_type = PgType::from_oid(type_oid).unwrap_or(PgType::Text);
            self.type_cache.insert(cache_key, pg_type);
            pg_type
        } else {
            PgType::Text // Default if not found
        }
    }
    
    /// Resolve type of a literal value
    fn resolve_value_type(&self, val: &Value) -> PgType {
        match val {
            Value::Number(n, _) => {
                if n.contains('.') {
                    PgType::Numeric
                } else if let Ok(i) = n.parse::<i64>() {
                    if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                        PgType::Int4
                    } else {
                        PgType::Int8
                    }
                } else {
                    PgType::Numeric
                }
            }
            Value::Boolean(_) => PgType::Bool,
            Value::Null => PgType::Text,
            _ => PgType::Text,
        }
    }
    
    /// Infer result type of binary operation
    fn infer_binary_op_type(&self, left: PgType, op: BinaryOperator, right: PgType) -> PgType {
        use BinaryOperator::*;
        
        match op {
            // Arithmetic operations
            Plus | Minus | Multiply | Divide | Modulo => {
                // If either operand is numeric, result is numeric
                if left == PgType::Numeric || right == PgType::Numeric {
                    PgType::Numeric
                } else if left == PgType::Float8 || right == PgType::Float8 ||
                          left == PgType::Float4 || right == PgType::Float4 {
                    PgType::Float8
                } else if left == PgType::Int8 || right == PgType::Int8 {
                    PgType::Int8
                } else if left == PgType::Int4 || right == PgType::Int4 ||
                          left == PgType::Int2 || right == PgType::Int2 {
                    PgType::Int4
                } else {
                    PgType::Text
                }
            }
            // Comparison operations return boolean
            Eq | NotEq | Lt | LtEq | Gt | GtEq => PgType::Bool,
            // Logical operations
            And | Or | Xor => PgType::Bool,
            // String operations
            StringConcat => PgType::Text,
            _ => PgType::Text,
        }
    }
    
    /// Infer result type of unary operation
    fn infer_unary_op_type(&self, op: UnaryOperator, expr_type: PgType) -> PgType {
        use UnaryOperator::*;
        
        match op {
            Not => PgType::Bool,
            Plus | Minus => expr_type, // Preserves numeric type
            _ => PgType::Text,
        }
    }
    
    /// Resolve function return type
    fn resolve_function_type(&mut self, func: &Function, context: &QueryContext) -> PgType {
        let func_name = func.name.to_string().to_uppercase();
            
        match func_name.as_str() {
            // Aggregate functions
            "COUNT" => PgType::Int8,
            "SUM" => {
                // Type depends on argument
                if let FunctionArguments::List(list) = &func.args {
                    if !list.args.is_empty() {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &list.args[0] {
                            let arg_type = self.resolve_expr_type(expr, context);
                            if arg_type == PgType::Numeric {
                                PgType::Numeric
                            } else {
                                PgType::Float8
                            }
                        } else {
                            PgType::Float8
                        }
                    } else {
                        PgType::Float8
                    }
                } else {
                    PgType::Float8
                }
            }
            "AVG" => PgType::Numeric, // Always returns numeric for precision
            "MAX" | "MIN" => {
                // Returns same type as argument
                if let FunctionArguments::List(list) = &func.args {
                    if !list.args.is_empty() {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &list.args[0] {
                            self.resolve_expr_type(expr, context)
                        } else {
                            PgType::Text
                        }
                    } else {
                        PgType::Text
                    }
                } else {
                    PgType::Text
                }
            }
            // Math functions
            "ABS" | "CEIL" | "FLOOR" | "ROUND" => {
                if let FunctionArguments::List(list) = &func.args {
                    if !list.args.is_empty() {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &list.args[0] {
                            self.resolve_expr_type(expr, context)
                        } else {
                            PgType::Float8
                        }
                    } else {
                        PgType::Float8
                    }
                } else {
                    PgType::Float8
                }
            }
            // String functions
            "LENGTH" | "CHAR_LENGTH" => PgType::Int4,
            "LOWER" | "UPPER" | "TRIM" | "SUBSTR" => PgType::Text,
            // Our decimal functions
            "DECIMAL_ADD" | "DECIMAL_SUB" | "DECIMAL_MUL" | "DECIMAL_DIV" => PgType::Numeric,
            "DECIMAL_FROM_TEXT" => PgType::Numeric,
            "DECIMAL_TO_TEXT" => PgType::Text,
            // Date/Time functions (SQLite built-ins)
            "DATE" => PgType::Date,
            "TIME" => PgType::Time,
            "DATETIME" => PgType::Timestamp,
            "JULIANDAY" => PgType::Float8,
            "STRFTIME" => PgType::Text,
            // Our datetime functions
            "NOW" | "CURRENT_TIMESTAMP" => PgType::Timestamptz,
            "EXTRACT" => PgType::Float8,
            "DATE_TRUNC" => PgType::Timestamp,
            "AGE" => PgType::Interval,
            _ => PgType::Text, // Default for unknown functions
        }
    }
    
    /// Convert SQL type string to PgType
    fn sql_type_to_pg_type(&self, type_str: &str) -> PgType {
        let upper = type_str.to_uppercase();
        match upper.as_str() {
            s if s.contains("INT") => PgType::Int4,
            s if s.contains("NUMERIC") || s.contains("DECIMAL") => PgType::Numeric,
            s if s.contains("FLOAT") || s.contains("REAL") || s.contains("DOUBLE") => PgType::Float8,
            s if s.contains("BOOL") => PgType::Bool,
            s if s.contains("TEXT") || s.contains("VARCHAR") || s.contains("CHAR") => PgType::Text,
            _ => PgType::Text,
        }
    }
    
    /// Check if an expression involves decimal type
    pub fn involves_decimal(&mut self, expr: &Expr, context: &QueryContext) -> bool {
        match expr {
            Expr::BinaryOp { left, right, .. } => {
                self.involves_decimal(left, context) || self.involves_decimal(right, context)
            }
            Expr::UnaryOp { expr, .. } => self.involves_decimal(expr, context),
            Expr::Function(func) => {
                if let FunctionArguments::List(list) = &func.args {
                    list.args.iter().any(|arg| {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.involves_decimal(e, context)
                        } else {
                            false
                        }
                    })
                } else {
                    false
                }
            }
            Expr::Subquery(subquery) => {
                // Check if subquery result involves decimal
                let subquery_context = self.build_context(subquery);
                match &*subquery.body {
                    SetExpr::Select(select) => {
                        select.projection.iter().any(|item| {
                            match item {
                                SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                                    let t = self.resolve_expr_type(e, &subquery_context);
                                    t == PgType::Numeric || t == PgType::Float4 || t == PgType::Float8
                                }
                                _ => false,
                            }
                        })
                    }
                    _ => false,
                }
            }
            _ => {
                let t = self.resolve_expr_type(expr, context);
                t == PgType::Numeric || t == PgType::Float4 || t == PgType::Float8
            }
        }
    }
}