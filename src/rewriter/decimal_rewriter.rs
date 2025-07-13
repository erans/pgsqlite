use sqlparser::ast::{
    Expr, BinaryOperator, Function, FunctionArg, FunctionArgExpr, FunctionArguments, FunctionArgumentList,
    ObjectName, ObjectNamePart, Ident, SelectItem, Query, SetExpr, Statement, DataType, Cte, TableFactor,
    GroupByExpr, OrderBy, OrderByKind
};
use rusqlite::Connection;
use crate::types::PgType;
use super::expression_type_resolver::{ExpressionTypeResolver, QueryContext};

/// Rewrites queries to use decimal functions for NUMERIC operations
pub struct DecimalQueryRewriter<'a> {
    resolver: ExpressionTypeResolver<'a>,
}

impl<'a> DecimalQueryRewriter<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            resolver: ExpressionTypeResolver::new(conn),
        }
    }
    
    /// Check if any table in the query has decimal columns
    fn query_has_decimal_columns(&self, query: &Query) -> bool {
        let tables = self.extract_table_names_from_query(query);
        self.any_table_has_decimal_columns(&tables)
    }
    
    /// Extract all table names from a query
    fn extract_table_names_from_query(&self, query: &Query) -> Vec<String> {
        let mut tables = Vec::new();
        
        // Extract from CTEs
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                tables.extend(self.extract_table_names_from_query(&cte.query));
            }
        }
        
        // Extract from query body
        self.extract_table_names_from_set_expr(&query.body, &mut tables);
        
        tables
    }
    
    /// Extract table names from a SetExpr
    fn extract_table_names_from_set_expr(&self, set_expr: &SetExpr, tables: &mut Vec<String>) {
        match set_expr {
            SetExpr::Select(select) => {
                for table_with_joins in &select.from {
                    self.extract_table_names_from_table_factor(&table_with_joins.relation, tables);
                    for join in &table_with_joins.joins {
                        self.extract_table_names_from_table_factor(&join.relation, tables);
                    }
                }
            }
            SetExpr::Query(query) => {
                tables.extend(self.extract_table_names_from_query(query));
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.extract_table_names_from_set_expr(left, tables);
                self.extract_table_names_from_set_expr(right, tables);
            }
            _ => {}
        }
    }
    
    /// Extract table names from a TableFactor
    fn extract_table_names_from_table_factor(&self, table_factor: &TableFactor, tables: &mut Vec<String>) {
        match table_factor {
            TableFactor::Table { name, .. } => {
                tables.push(name.to_string());
            }
            TableFactor::Derived { subquery, .. } => {
                tables.extend(self.extract_table_names_from_query(subquery));
            }
            _ => {}
        }
    }
    
    /// Check if any of the given tables has decimal columns
    fn any_table_has_decimal_columns(&self, tables: &[String]) -> bool {
        let conn = self.resolver.conn();
        
        for table in tables {
            // Check if this table has any decimal columns in the schema
            let query = "SELECT 1 FROM __pgsqlite_schema 
                         WHERE table_name = ?1 
                         AND sqlite_type = 'DECIMAL' 
                         LIMIT 1";
            
            // Check using exists query
            match conn.query_row(query, [table], |_| Ok(())) {
                Ok(_) => return true,
                Err(_) => continue,
            }
        }
        
        false
    }
    
    /// Rewrite a statement to use decimal functions
    pub fn rewrite_statement(&mut self, stmt: &mut Statement) -> Result<(), String> {
        match stmt {
            Statement::Query(query) => {
                // Always rewrite queries - the optimization is applied at the expression level
                self.rewrite_query(query)
            }
            Statement::Insert(insert) => {
                if let Some(source) = &mut insert.source {
                    // Check if the target table has decimal columns
                    let table_name = match &insert.table {
                        sqlparser::ast::TableObject::TableName(name) => name.to_string(),
                        _ => return Ok(()),
                    };
                    let tables = self.extract_table_names_from_query(source);
                    let mut all_tables = vec![table_name];
                    all_tables.extend(tables);
                    
                    if !self.any_table_has_decimal_columns(&all_tables) {
                        return Ok(()); // Skip rewriting
                    }
                    self.rewrite_query(source)
                } else {
                    Ok(())
                }
            }
            Statement::Update { table, selection, assignments, .. } => {
                // Check if the table has decimal columns
                if let sqlparser::ast::TableFactor::Table { name, .. } = &table.relation {
                    let table_name = name.to_string();
                    if !self.any_table_has_decimal_columns(&[table_name.clone()]) {
                        return Ok(()); // Skip rewriting
                    }
                    
                    // Create context with table name
                    let mut context = QueryContext::default();
                    context.default_table = Some(table_name);
                    
                    // Rewrite WHERE clause
                    if let Some(expr) = selection {
                        self.rewrite_expression(expr, &context)?;
                    }
                    
                    // Rewrite assignment expressions
                    // For UPDATE assignments, we don't want to wrap simple numeric literals
                    // because rust_decimal can't handle very large numbers (>28 digits)
                    for assignment in assignments {
                        self.rewrite_update_assignment(&mut assignment.value, &context)?;
                    }
                }
                Ok(())
            }
            Statement::Delete(delete) => {
                // Extract all tables
                let mut tables = Vec::new();
                
                // Add tables from FROM clause if present
                match &delete.from {
                    sqlparser::ast::FromTable::WithFromKeyword(table_list) => {
                        for table_with_joins in table_list {
                            self.extract_table_names_from_table_factor(&table_with_joins.relation, &mut tables);
                            for join in &table_with_joins.joins {
                                self.extract_table_names_from_table_factor(&join.relation, &mut tables);
                            }
                        }
                    }
                    sqlparser::ast::FromTable::WithoutKeyword(table_list) => {
                        for table_with_joins in table_list {
                            self.extract_table_names_from_table_factor(&table_with_joins.relation, &mut tables);
                            for join in &table_with_joins.joins {
                                self.extract_table_names_from_table_factor(&join.relation, &mut tables);
                            }
                        }
                    }
                }
                
                // If no FROM clause, use the table names from delete.tables
                if tables.is_empty() {
                    tables.extend(delete.tables.iter().map(|t| t.to_string()));
                }
                
                if !self.any_table_has_decimal_columns(&tables) {
                    return Ok(()); // Skip rewriting
                }
                
                // Create context with table name
                let mut context = QueryContext::default();
                if let Some(table_name) = delete.tables.first() {
                    context.default_table = Some(table_name.to_string());
                } else if !tables.is_empty() {
                    context.default_table = Some(tables[0].clone());
                }
                
                if let Some(expr) = &mut delete.selection {
                    self.rewrite_expression_with_optimization(expr, &context, true)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    
    /// Rewrite a query to use decimal functions
    pub fn rewrite_query(&mut self, query: &mut Query) -> Result<(), String> {
        self.rewrite_query_with_context(query, None)
    }
    
    /// Rewrite a query to use decimal functions with optional outer context
    fn rewrite_query_with_context(&mut self, query: &mut Query, outer_context: Option<&QueryContext>) -> Result<(), String> {
        // Build context first to understand CTEs
        let mut context = self.resolver.build_context(query);
        
        // Merge outer context if provided (for correlated subqueries)
        if let Some(outer_ctx) = outer_context {
            // Merge table aliases from outer context
            for (alias, table) in &outer_ctx.table_aliases {
                if !context.table_aliases.contains_key(alias) {
                    context.table_aliases.insert(alias.clone(), table.clone());
                }
            }
            // Merge CTE columns from outer context
            for (cte_name, columns) in &outer_ctx.cte_columns {
                if !context.cte_columns.contains_key(cte_name) {
                    context.cte_columns.insert(cte_name.clone(), columns.clone());
                }
            }
            // Merge derived table columns from outer context
            for (table_name, columns) in &outer_ctx.derived_table_columns {
                if !context.derived_table_columns.contains_key(table_name) {
                    context.derived_table_columns.insert(table_name.clone(), columns.clone());
                }
            }
        }
        
        // Now rewrite CTEs with the full context available
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                self.rewrite_cte_with_context(cte, &context)?;
            }
        }
        
        // Rewrite body
        self.rewrite_set_expr(&mut query.body, &context)?;
        
        // Rewrite ORDER BY
        if let Some(order_by) = &mut query.order_by {
            self.rewrite_order_by(order_by, &context)?;
        }
        
        Ok(())
    }
    
    /// Rewrite a CTE with context (for recursive CTEs that need to reference themselves)
    fn rewrite_cte_with_context(&mut self, cte: &mut Cte, outer_context: &QueryContext) -> Result<(), String> {
        // For recursive CTEs, we need to analyze the initial query to understand the column types
        // then provide that information to the recursive part
        
        // First, build a context that includes outer context
        let mut cte_context = outer_context.clone();
        
        // If this is a recursive CTE, analyze the first (non-recursive) part to determine column types
        if let SetExpr::SetOperation { left, .. } = &*cte.query.body {
            if let SetExpr::Select(base_select) = &**left {
                // Build context for the base query
                let mut base_context = QueryContext::default();
                for table in &base_select.from {
                    self.resolver.process_table_with_joins(&table.relation, &table.joins, &mut base_context);
                }
                
                // Analyze the projection to determine CTE column types
                let mut cte_column_types = Vec::new();
                for (idx, item) in base_select.projection.iter().enumerate() {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            let expr_type = self.resolver.resolve_expr_type(expr, &base_context);
                            let col_name = if !cte.alias.columns.is_empty() {
                                cte.alias.columns.get(idx).map(|c| c.name.value.clone()).unwrap_or_else(|| format!("column{}", idx))
                            } else {
                                self.resolver.extract_column_name(expr).unwrap_or_else(|| format!("column{}", idx))
                            };
                            cte_column_types.push((col_name, expr_type));
                        }
                        SelectItem::ExprWithAlias { expr, alias } => {
                            let expr_type = self.resolver.resolve_expr_type(expr, &base_context);
                            cte_column_types.push((alias.value.clone(), expr_type));
                        }
                        _ => {}
                    }
                }
                
                // Add the CTE's own columns to the context so the recursive part can reference them
                cte_context.cte_columns.insert(cte.alias.name.value.clone(), cte_column_types.clone());
            }
        }
        
        self.rewrite_query_with_context(&mut cte.query, Some(&cte_context))
    }
    
    /// Rewrite a SetExpr
    fn rewrite_set_expr(&mut self, set_expr: &mut Box<SetExpr>, context: &QueryContext) -> Result<(), String> {
        match &mut **set_expr {
            SetExpr::Select(select) => {
                // Check if this SELECT involves any decimal columns
                let mut current_tables = Vec::new();
                for table_with_joins in &select.from {
                    self.extract_table_names_from_table_factor(&table_with_joins.relation, &mut current_tables);
                    for join in &table_with_joins.joins {
                        self.extract_table_names_from_table_factor(&join.relation, &mut current_tables);
                    }
                }
                
                let has_decimal_columns = self.any_table_has_decimal_columns(&current_tables) ||
                    // Also check if any derived tables or CTEs have decimal columns
                    context.derived_table_columns.values().any(|cols| {
                        cols.iter().any(|(_, pg_type)| {
                            *pg_type == PgType::Numeric || *pg_type == PgType::Float4 || *pg_type == PgType::Float8
                        })
                    }) ||
                    context.cte_columns.values().any(|cols| {
                        cols.iter().any(|(_, pg_type)| {
                            *pg_type == PgType::Numeric || *pg_type == PgType::Float4 || *pg_type == PgType::Float8
                        })
                    });
                
                // Build a proper context for this SELECT
                let mut local_context = context.clone();
                
                // Process FROM clause to establish table aliases
                for table_with_joins in &select.from {
                    self.resolver.process_table_with_joins(&table_with_joins.relation, &table_with_joins.joins, &mut local_context);
                }
                
                // Update default table if there's exactly one table in FROM clause
                if current_tables.len() == 1 {
                    local_context.default_table = Some(current_tables[0].clone());
                }
                
                
                // First rewrite any subqueries in FROM clause (always)
                for table_with_joins in &mut select.from {
                    self.rewrite_table_factor(&mut table_with_joins.relation)?;
                    // Also rewrite joins
                    for join in &mut table_with_joins.joins {
                        self.rewrite_table_factor(&mut join.relation)?;
                    }
                }
                
                // Rewrite projection if needed
                for item in &mut select.projection {
                    match item {
                        SelectItem::UnnamedExpr(expr) => {
                            self.rewrite_expression_with_optimization(expr, &local_context, has_decimal_columns)?;
                        }
                        SelectItem::ExprWithAlias { expr, .. } => {
                            self.rewrite_expression_with_optimization(expr, &local_context, has_decimal_columns)?;
                        }
                        _ => {}
                    }
                }
                
                // Rewrite WHERE clause
                if let Some(expr) = &mut select.selection {
                    self.rewrite_expression_with_optimization(expr, &local_context, has_decimal_columns)?;
                }
                
                // Rewrite GROUP BY if needed
                if has_decimal_columns {
                    self.rewrite_group_by(&mut select.group_by, &local_context)?;
                }
                
                // Rewrite HAVING if needed
                if let Some(expr) = &mut select.having {
                    self.rewrite_expression_with_optimization(expr, &local_context, has_decimal_columns)?;
                }
            }
            SetExpr::Query(query) => {
                self.rewrite_query(query)?;
            }
            SetExpr::SetOperation { left, right, .. } => {
                self.rewrite_set_expr(left, context)?;
                self.rewrite_set_expr(right, context)?;
            }
            _ => {}
        }
        
        Ok(())
    }
    
    /// Rewrite an expression with optimization check
    fn rewrite_expression_with_optimization(&mut self, expr: &mut Expr, context: &QueryContext, has_decimal_columns: bool) -> Result<(), String> {
        match expr {
            // Always rewrite subqueries regardless of current context
            Expr::Subquery(subquery) => {
                if self.query_has_decimal_columns(subquery) {
                    self.rewrite_query_with_context(subquery, Some(context))?;
                }
            }
            Expr::Exists { subquery, .. } => {
                if self.query_has_decimal_columns(subquery) {
                    self.rewrite_query_with_context(subquery, Some(context))?;
                }
            }
            Expr::InSubquery { expr: inner_expr, subquery, .. } => {
                // Rewrite the inner expression if current context has decimals
                if has_decimal_columns {
                    self.rewrite_expression_with_optimization(inner_expr, context, has_decimal_columns)?;
                }
                // Always check subquery for decimals
                let temp_query = Query {
                    with: None,
                    body: subquery.clone(),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                    locks: vec![],
                };
                if self.query_has_decimal_columns(&temp_query) {
                    let subquery_context = self.resolver.build_context(&temp_query);
                    self.rewrite_set_expr(subquery, &subquery_context)?;
                }
            }
            // Other expressions only rewrite if current context has decimal columns
            _ => {
                if has_decimal_columns {
                    self.rewrite_expression(expr, context)?;
                }
            }
        }
        Ok(())
    }

    /// Rewrite an expression to use decimal functions
    fn rewrite_expression(&mut self, expr: &mut Expr, context: &QueryContext) -> Result<(), String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                // First rewrite children
                self.rewrite_expression(left, context)?;
                self.rewrite_expression(right, context)?;
                
                // Check if either operand is decimal
                let left_type = self.resolver.resolve_expr_type(left, context);
                let right_type = self.resolver.resolve_expr_type(right, context);
                
                if left_type == PgType::Numeric || right_type == PgType::Numeric ||
                   left_type == PgType::Float4 || right_type == PgType::Float4 ||
                   left_type == PgType::Float8 || right_type == PgType::Float8 {
                    // Rewrite to decimal function
                    let left_clone = left.as_ref().clone();
                    let right_clone = right.as_ref().clone();
                    let op_clone = op.clone();
                    *expr = self.create_decimal_function_expr(op_clone, left_clone, right_clone, left_type, right_type)?;
                }
            }
            Expr::Function(func) => {
                // Check if we need to rewrite this function before processing arguments
                let is_aggregate = self.is_aggregate_function(&func.name);
                let is_math = self.is_math_function(&func.name);
                let mut has_decimal_arg = false;
                
                // Check arguments for decimal involvement
                if let FunctionArguments::List(list) = &func.args {
                    if !list.args.is_empty() {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg_expr)) = &list.args[0] {
                            has_decimal_arg = self.resolver.involves_decimal(arg_expr, context);
                        }
                    }
                }
                
                // Rewrite function arguments
                if let FunctionArguments::List(list) = &mut func.args {
                    for arg in &mut list.args {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = arg {
                            self.rewrite_expression(e, context)?;
                        }
                    }
                }
                
                // Apply function rewrites if needed
                if is_aggregate && has_decimal_arg {
                    self.rewrite_aggregate_to_decimal(func, context)?;
                } else if is_math && has_decimal_arg {
                    self.rewrite_math_function_to_decimal(func)?;
                }
            }
            Expr::Nested(inner) => {
                self.rewrite_expression(inner, context)?;
            }
            Expr::InList { expr, list, .. } => {
                self.rewrite_expression(expr, context)?;
                for item in list {
                    self.rewrite_expression(item, context)?;
                }
            }
            Expr::Between { expr, low, high, .. } => {
                self.rewrite_expression(expr, context)?;
                self.rewrite_expression(low, context)?;
                self.rewrite_expression(high, context)?;
            }
            Expr::Subquery(subquery) => {
                self.rewrite_query_with_context(subquery, Some(context))?;
            }
            Expr::Exists { subquery, .. } => {
                self.rewrite_query_with_context(subquery, Some(context))?;
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.rewrite_expression(expr, context)?;
                // For IN subqueries, we need to create a new Query wrapper
                let temp_query = Query {
                    with: None,
                    body: subquery.clone(),
                    order_by: None,
                    limit_clause: None,
                    fetch: None,
                    for_clause: None,
                    settings: None,
                    format_clause: None,
                    pipe_operators: vec![],
                    locks: vec![],
                };
                let subquery_context = self.resolver.build_context(&temp_query);
                self.rewrite_set_expr(subquery, &subquery_context)?;
            }
            Expr::Cast { expr, data_type, .. } => {
                // Don't rewrite the inner expression if we're casting to TEXT
                // This prevents decimal values from being wrapped when casting to text
                match data_type {
                    DataType::Text | DataType::Varchar(_) | DataType::Char(_) => {
                        // For text casts, we don't want decimal wrapping
                        // The value should be passed as-is to allow proper formatting
                    }
                    _ => {
                        // For other casts, recursively rewrite the inner expression
                        self.rewrite_expression(expr, context)?;
                    }
                }
            }
            // Skip Case for now due to API changes
            _ => {}
        }
        
        Ok(())
    }
    
    /// Create decimal function expression from binary operation
    fn create_decimal_function_expr(
        &self,
        op: BinaryOperator,
        left: Expr,
        right: Expr,
        left_type: PgType,
        right_type: PgType,
    ) -> Result<Expr, String> {
        use BinaryOperator::*;
        
        let func_name = match op {
            Plus => "decimal_add",
            Minus => "decimal_sub",
            Multiply => "decimal_mul",
            Divide => "decimal_div",
            Eq => "decimal_eq",
            Lt => "decimal_lt",
            Gt => "decimal_gt",
            _ => return Ok(Expr::BinaryOp { 
                left: Box::new(left), 
                op, 
                right: Box::new(right) 
            }), // Other operators not supported - return unchanged
        };
        
        // Wrap non-decimal operands in decimal_from_text
        let wrapped_left = if left_type != PgType::Numeric && 
                              left_type != PgType::Float4 && 
                              left_type != PgType::Float8 {
            self.wrap_in_decimal_from_text(left)
        } else {
            left
        };
        
        let wrapped_right = if right_type != PgType::Numeric && 
                               right_type != PgType::Float4 && 
                               right_type != PgType::Float8 {
            self.wrap_in_decimal_from_text(right)
        } else {
            right
        };
        
        // Create function call
        let func = Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(func_name))]),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(wrapped_left)),
                    FunctionArg::Unnamed(FunctionArgExpr::Expr(wrapped_right)),
                ],
                clauses: vec![],
            }),
            over: None,
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            filter: None,
            null_treatment: None,
            within_group: vec![],
        };
        
        Ok(Expr::Function(func))
    }
    
    /// Wrap expression in decimal_from_text function
    fn wrap_in_decimal_from_text(&self, expr: Expr) -> Expr {
        // First cast to text if needed
        let text_expr = match &expr {
            Expr::Value(val) => {
                match &val.value {
                    sqlparser::ast::Value::Number(_, _) => {
                        Expr::Cast {
                            expr: Box::new(expr),
                            data_type: DataType::Text,
                            format: None,
                            kind: sqlparser::ast::CastKind::Cast,
                        }
                    }
                    _ => expr,
                }
            }
            _ => expr,
        };
        
        Expr::Function(Function {
            name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new("decimal_from_text"))]),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(text_expr))],
                clauses: vec![],
            }),
            over: None,
            uses_odbc_syntax: false,
            parameters: FunctionArguments::None,
            filter: None,
            null_treatment: None,
            within_group: vec![],
        })
    }
    
    /// Rewrite a table factor (including derived tables)
    fn rewrite_table_factor(&mut self, table_factor: &mut TableFactor) -> Result<(), String> {
        match table_factor {
            TableFactor::Derived { subquery, .. } => {
                // Rewrite the subquery
                self.rewrite_query(subquery)?;
            }
            _ => {} // Other table factors don't need rewriting
        }
        Ok(())
    }
    
    /// Rewrite GROUP BY clause
    fn rewrite_group_by(&mut self, group_by: &mut GroupByExpr, context: &QueryContext) -> Result<(), String> {
        match group_by {
            GroupByExpr::Expressions(exprs, _modifiers) => {
                for expr in exprs {
                    self.rewrite_expression(expr, context)?;
                }
            }
            GroupByExpr::All(_) => {
                // GROUP BY ALL doesn't need rewriting
            }
        }
        Ok(())
    }
    
    /// Rewrite UPDATE assignment expression
    /// This is special because we don't want to wrap simple numeric literals
    /// to avoid rust_decimal panics on very large numbers
    fn rewrite_update_assignment(&mut self, expr: &mut Expr, context: &QueryContext) -> Result<(), String> {
        match expr {
            // For simple numeric values, don't wrap them
            Expr::Value(val) => {
                match &val.value {
                    sqlparser::ast::Value::Number(_, _) => {
                        // Don't wrap simple numeric literals in UPDATE assignments
                        // This allows storing very large NUMERIC values that exceed rust_decimal's capacity
                        Ok(())
                    }
                    _ => Ok(())
                }
            }
            // For other expressions, use the normal rewriting logic
            _ => self.rewrite_expression(expr, context)
        }
    }
    
    /// Rewrite ORDER BY clause
    fn rewrite_order_by(&mut self, order_by: &mut OrderBy, context: &QueryContext) -> Result<(), String> {
        match &mut order_by.kind {
            OrderByKind::Expressions(order_exprs) => {
                for order_expr in order_exprs {
                    // First rewrite the expression normally
                    self.rewrite_expression(&mut order_expr.expr, context)?;
                    
                    // If the expression is a simple column reference to a decimal type,
                    // we need to ensure it sorts correctly
                    if let Expr::Identifier(_) | Expr::CompoundIdentifier(_) = &order_expr.expr {
                        let expr_type = self.resolver.resolve_expr_type(&order_expr.expr, context);
                        if expr_type == PgType::Numeric || expr_type == PgType::Float4 || expr_type == PgType::Float8 {
                            // Wrap in CAST to REAL for proper numeric ordering
                            order_expr.expr = Expr::Cast {
                                expr: Box::new(order_expr.expr.clone()),
                                data_type: DataType::Real,
                                format: None,
                                kind: sqlparser::ast::CastKind::Cast,
                            };
                        }
                    }
                }
            }
            OrderByKind::All(_) => {
                // ORDER BY ALL doesn't need rewriting
            }
        }
        Ok(())
    }
    
    /// Check if function is an aggregate
    fn is_aggregate_function(&self, name: &ObjectName) -> bool {
        let func_name = name.to_string().to_uppercase();
        matches!(func_name.as_str(), "SUM" | "AVG" | "MIN" | "MAX" | "COUNT")
    }
    
    /// Check if function is a math function that needs decimal handling
    fn is_math_function(&self, name: &ObjectName) -> bool {
        let func_name = name.to_string().to_uppercase();
        matches!(func_name.as_str(), "ROUND" | "ABS")
    }
    
    /// Rewrite math function to use decimal equivalent
    fn rewrite_math_function_to_decimal(&self, func: &mut Function) -> Result<(), String> {
        let func_name = func.name.to_string().to_uppercase();
        
        match func_name.as_str() {
            "ROUND" => {
                func.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("decimal_round"))]);
            }
            "ABS" => {
                func.name = ObjectName(vec![ObjectNamePart::Identifier(Ident::new("decimal_abs"))]);
            }
            _ => return Err(format!("Unsupported math function: {}", func_name)),
        }
        
        Ok(())
    }
    
    /// Check if a column needs decimal wrapping based on its storage type
    fn column_needs_decimal_wrapping(&mut self, expr: &Expr, context: &QueryContext) -> bool {
        match expr {
            Expr::Identifier(ident) => {
                self.check_column_decimal_storage(None, &ident.value, context)
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() >= 2 {
                    let table = &parts[parts.len() - 2].value;
                    let column = &parts[parts.len() - 1].value;
                    self.check_column_decimal_storage(Some(table), column, context)
                } else {
                    false
                }
            }
            _ => false,
        }
    }
    
    /// Check if a column is stored as DECIMAL in SQLite
    fn check_column_decimal_storage(&mut self, table: Option<&str>, column: &str, context: &QueryContext) -> bool {
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
        
        // Check if this is a derived table or CTE (these don't have SQLite storage info)
        if table_name == "__derived__" || context.cte_columns.contains_key(&table_name) || 
           (table.is_none() && context.derived_table_columns.contains_key(&table_name)) {
            // For derived tables/CTEs, check if the column type suggests decimal storage
            let pg_type = self.resolver.resolve_expr_type(&if table.is_some() {
                Expr::CompoundIdentifier(vec![
                    sqlparser::ast::Ident::new(table.unwrap()),
                    sqlparser::ast::Ident::new(column)
                ])
            } else {
                Expr::Identifier(sqlparser::ast::Ident::new(column))
            }, context);
            return pg_type == PgType::Numeric; // Only NUMERIC types need wrapping from derived tables
        }
        
        // Query SQLite schema to check storage type
        let query = "SELECT sqlite_type FROM __pgsqlite_schema 
                     WHERE table_name = ?1 AND column_name = ?2 AND sqlite_type = 'DECIMAL'";
        
        match self.resolver.conn().query_row(query, [&table_name, column], |_| Ok(())) {
            Ok(_) => {
                // Found DECIMAL storage, but need to check if it's a NUMERIC type specifically
                // (vs FLOAT types which should not be wrapped)
                let pg_type_query = "SELECT pg_type FROM __pgsqlite_schema 
                                     WHERE table_name = ?1 AND column_name = ?2";
                if let Ok(pg_type_str) = self.resolver.conn().query_row(pg_type_query, [&table_name, column], |row| {
                    Ok(row.get::<_, String>(0)?)
                }) {
                    // Only wrap NUMERIC types, not FLOAT types
                    pg_type_str == "NUMERIC"
                } else {
                    false
                }
            }
            Err(_) => false,
        }
    }
    
    /// Rewrite aggregate function to use decimal version
    fn rewrite_aggregate_to_decimal(&mut self, func: &mut Function, context: &QueryContext) -> Result<(), String> {
        let func_name = func.name.to_string().to_uppercase();
            
        // For now, we'll use SQLite's built-in aggregates with proper wrapping
        // In a real implementation, we would register custom aggregate functions
        match func_name.as_str() {
            "SUM" | "AVG" | "MIN" | "MAX" => {
                // Ensure argument is wrapped in decimal conversion if needed
                if let FunctionArguments::List(list) = &mut func.args {
                    if !list.args.is_empty() {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(arg)) = &mut list.args[0] {
                            // Check if argument is already a decimal function
                            let is_decimal_func = matches!(
                                arg, 
                                Expr::Function(f) if f.name.to_string().to_uppercase().starts_with("DECIMAL_")
                            );
                            
                            // Check if this column is stored as DECIMAL in SQLite (needs wrapping)
                            let needs_decimal_wrapping = self.column_needs_decimal_wrapping(arg, context);
                            
                            // Wrap if not already a decimal function AND needs decimal wrapping
                            if !is_decimal_func && needs_decimal_wrapping {
                                *arg = self.wrap_in_decimal_from_text(arg.clone());
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        
        Ok(())
    }
}