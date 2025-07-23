use std::collections::HashMap;
use crate::types::{PgType, SchemaTypeMapper};
use crate::translator::DateTimeSubtype;

/// Information about a column's source and type
#[derive(Debug, Clone)]
pub struct ColumnSource {
    pub table_name: Option<String>,
    pub column_name: String,
    pub pg_type: PgType,
    pub datetime_subtype: Option<DateTimeSubtype>,
}

/// Type information for an expression
#[derive(Debug, Clone)]
pub struct ExpressionTypeInfo {
    pub base_type: PgType,
    pub datetime_subtype: Option<DateTimeSubtype>,
    pub transformation: TransformationType,
}

/// Types of transformations that can be applied to expressions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransformationType {
    None,
    DateTimeArithmetic,
    DateTimeFunction,
    TypeCast,
    Aggregate,
    Other,
}

/// Context for resolving types in a query
#[derive(Debug, Clone)]
pub struct TypeResolutionContext {
    /// Table schemas from __pgsqlite_schema
    pub schemas: HashMap<String, TableSchema>,
    /// Column aliases and their source columns
    pub column_aliases: HashMap<String, ColumnSource>,
    /// Expression types for complex expressions
    pub expression_types: HashMap<String, ExpressionTypeInfo>,
}

/// Schema information for a table
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub columns: HashMap<String, ColumnSchema>,
}

/// Schema information for a column
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    pub pg_type: PgType,
    pub sqlite_type: String,
    pub datetime_format: Option<String>,
}

impl Default for TypeResolutionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl TypeResolutionContext {
    /// Create a new empty context
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            column_aliases: HashMap::new(),
            expression_types: HashMap::new(),
        }
    }
    
    /// Load schemas from the schema type mapper
    pub fn load_schemas(&mut self, _schema_mapper: &SchemaTypeMapper) {
        // This will be implemented to load from __pgsqlite_schema
        // For now, just create empty structure
    }
    
    /// Add a column alias
    pub fn add_alias(&mut self, alias: String, source: ColumnSource) {
        self.column_aliases.insert(alias, source);
    }
    
    /// Add an expression type
    pub fn add_expression(&mut self, name: String, type_info: ExpressionTypeInfo) {
        self.expression_types.insert(name, type_info);
    }
    
    /// Resolve the type of a column or expression
    pub fn resolve_type(&self, name: &str) -> Option<(PgType, Option<DateTimeSubtype>)> {
        // First check if it's an expression
        if let Some(expr_info) = self.expression_types.get(name) {
            return Some((expr_info.base_type, expr_info.datetime_subtype));
        }
        
        // Then check if it's an alias
        if let Some(source) = self.column_aliases.get(name) {
            return Some((source.pg_type, source.datetime_subtype));
        }
        
        // Finally check schemas directly
        for schema in self.schemas.values() {
            if let Some(col_schema) = schema.columns.get(name) {
                let datetime_subtype = Self::infer_datetime_subtype(&col_schema.pg_type);
                return Some((col_schema.pg_type, datetime_subtype));
            }
        }
        
        None
    }
    
    /// Infer datetime subtype from PgType
    fn infer_datetime_subtype(pg_type: &PgType) -> Option<DateTimeSubtype> {
        match pg_type {
            PgType::Date => Some(DateTimeSubtype::Date),
            PgType::Time => Some(DateTimeSubtype::Time),
            PgType::Timetz => Some(DateTimeSubtype::TimeTz),
            PgType::Timestamp => Some(DateTimeSubtype::Timestamp),
            PgType::Timestamptz => Some(DateTimeSubtype::TimestampTz),
            PgType::Interval => Some(DateTimeSubtype::Interval),
            _ => None,
        }
    }
}

/// Type propagation rules for expressions
pub struct TypePropagator;

impl TypePropagator {
    /// Determine the result type of a binary operation
    pub fn binary_op_type(
        left: (PgType, Option<DateTimeSubtype>),
        op: &str,
        right: (PgType, Option<DateTimeSubtype>),
    ) -> (PgType, Option<DateTimeSubtype>) {
        match (left, op, right) {
            // Timestamp + interval = timestamp
            ((PgType::Timestamp, dt), "+", (PgType::Interval, _)) |
            ((PgType::Timestamptz, dt), "+", (PgType::Interval, _)) => {
                (left.0, dt)
            }
            // Timestamp - timestamp = interval
            ((PgType::Timestamp, _), "-", (PgType::Timestamp, _)) |
            ((PgType::Timestamptz, _), "-", (PgType::Timestamptz, _)) => {
                (PgType::Interval, Some(DateTimeSubtype::Interval))
            }
            // Date + integer = date
            ((PgType::Date, dt), "+", (PgType::Int4 | PgType::Int8, _)) => {
                (PgType::Date, dt)
            }
            // Time + interval = time
            ((PgType::Time, dt), "+", (PgType::Interval, _)) => {
                (PgType::Time, dt)
            }
            // Default: left type wins
            _ => left,
        }
    }
    
    /// Determine the result type of a function call
    pub fn function_type(name: &str, args: &[(PgType, Option<DateTimeSubtype>)]) -> (PgType, Option<DateTimeSubtype>) {
        match name.to_lowercase().as_str() {
            "now" | "current_timestamp" => (PgType::Timestamptz, Some(DateTimeSubtype::TimestampTz)),
            "current_date" => (PgType::Date, Some(DateTimeSubtype::Date)),
            "current_time" => (PgType::Timetz, Some(DateTimeSubtype::TimeTz)),
            "age" => (PgType::Interval, Some(DateTimeSubtype::Interval)),
            "extract" | "date_part" => (PgType::Float8, None),
            "date_trunc" => {
                // date_trunc preserves the input timestamp type
                if args.len() >= 2 {
                    args[1]
                } else {
                    (PgType::Timestamp, Some(DateTimeSubtype::Timestamp))
                }
            }
            _ => (PgType::Text, None), // Default for unknown functions
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_type_propagation() {
        // Test timestamp + interval
        let result = TypePropagator::binary_op_type(
            (PgType::Timestamp, Some(DateTimeSubtype::Timestamp)),
            "+",
            (PgType::Interval, Some(DateTimeSubtype::Interval)),
        );
        assert_eq!(result.0, PgType::Timestamp);
        assert_eq!(result.1, Some(DateTimeSubtype::Timestamp));
        
        // Test timestamp - timestamp
        let result = TypePropagator::binary_op_type(
            (PgType::Timestamptz, Some(DateTimeSubtype::TimestampTz)),
            "-",
            (PgType::Timestamptz, Some(DateTimeSubtype::TimestampTz)),
        );
        assert_eq!(result.0, PgType::Interval);
        assert_eq!(result.1, Some(DateTimeSubtype::Interval));
    }
    
    #[test]
    fn test_function_types() {
        let result = TypePropagator::function_type("now", &[]);
        assert_eq!(result.0, PgType::Timestamptz);
        assert_eq!(result.1, Some(DateTimeSubtype::TimestampTz));
        
        let result = TypePropagator::function_type("extract", &[
            (PgType::Text, None),
            (PgType::Timestamp, Some(DateTimeSubtype::Timestamp)),
        ]);
        assert_eq!(result.0, PgType::Float8);
        assert_eq!(result.1, None);
    }
}