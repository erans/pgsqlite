use std::collections::HashMap;
use crate::types::PgType;

/// Subtype information for datetime types stored as INTEGER
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateTimeSubtype {
    /// DATE: days since epoch
    Date,
    /// TIME: microseconds since midnight
    Time,
    /// TIMETZ: microseconds since midnight UTC
    TimeTz,
    /// TIMESTAMP: microseconds since epoch
    Timestamp,
    /// TIMESTAMPTZ: microseconds since epoch UTC
    TimestampTz,
    /// INTERVAL: microseconds
    Interval,
}

/// Metadata about query translations to help with type inference
#[derive(Debug, Clone, Default)]
pub struct TranslationMetadata {
    /// Map of result column name -> type hint information
    pub column_mappings: HashMap<String, ColumnTypeHint>,
}

/// Type hint information for a column after translation
#[derive(Debug, Clone)]
pub struct ColumnTypeHint {
    /// The original source column name (if known)
    pub source_column: Option<String>,
    /// The suggested PostgreSQL type for this column
    pub suggested_type: Option<PgType>,
    /// For datetime types stored as INTEGER, the specific subtype
    pub datetime_subtype: Option<DateTimeSubtype>,
    /// Whether this is an expression (not a simple column reference)
    pub is_expression: bool,
    /// The type of expression (if applicable)
    pub expression_type: Option<ExpressionType>,
}

/// Types of expressions that can affect type inference
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpressionType {
    /// Arithmetic operation on a float column (e.g., float_col + integer)
    ArithmeticOnFloat,
    /// DateTime-related expression (e.g., date functions, AT TIME ZONE)
    DateTimeExpression,
    /// String concatenation
    StringConcatenation,
    /// Type cast expression
    TypeCast,
    /// Other/unknown expression type
    Other,
}

impl TranslationMetadata {
    /// Create a new empty metadata instance
    pub fn new() -> Self {
        Self {
            column_mappings: HashMap::new(),
        }
    }
    
    /// Add a type hint for a column
    pub fn add_hint(&mut self, column_name: String, hint: ColumnTypeHint) {
        self.column_mappings.insert(column_name, hint);
    }
    
    /// Get type hint for a column (if any)
    pub fn get_hint(&self, column_name: &str) -> Option<&ColumnTypeHint> {
        self.column_mappings.get(column_name)
    }
    
    /// Merge another metadata instance into this one
    pub fn merge(&mut self, other: TranslationMetadata) {
        self.column_mappings.extend(other.column_mappings);
    }
}

impl ColumnTypeHint {
    /// Create a simple column reference hint
    pub fn simple_column(source: String, pg_type: PgType) -> Self {
        Self {
            source_column: Some(source),
            suggested_type: Some(pg_type),
            datetime_subtype: None,
            is_expression: false,
            expression_type: None,
        }
    }
    
    /// Create an expression hint
    pub fn expression(source: Option<String>, pg_type: PgType, expr_type: ExpressionType) -> Self {
        Self {
            source_column: source,
            suggested_type: Some(pg_type),
            datetime_subtype: None,
            is_expression: true,
            expression_type: Some(expr_type),
        }
    }
    
    /// Create a hint for datetime expressions
    pub fn datetime_expression(source: Option<String>, datetime_subtype: Option<DateTimeSubtype>) -> Self {
        let pg_type = match datetime_subtype {
            Some(DateTimeSubtype::Date) => PgType::Date,
            Some(DateTimeSubtype::Time) => PgType::Time,
            Some(DateTimeSubtype::TimeTz) => PgType::Timetz,
            Some(DateTimeSubtype::Timestamp) => PgType::Timestamp,
            Some(DateTimeSubtype::TimestampTz) => PgType::Timestamptz,
            Some(DateTimeSubtype::Interval) => PgType::Interval,
            None => PgType::Timestamptz, // Default to timestamptz for unknown datetime
        };
        Self {
            source_column: source,
            suggested_type: Some(pg_type),
            datetime_subtype,
            is_expression: true,
            expression_type: Some(ExpressionType::DateTimeExpression),
        }
    }
    
    /// Create a hint for arithmetic on float
    pub fn arithmetic_on_float(source: String) -> Self {
        Self {
            source_column: Some(source),
            suggested_type: Some(PgType::Float8), // float + int = float in PostgreSQL
            datetime_subtype: None,
            is_expression: true,
            expression_type: Some(ExpressionType::ArithmeticOnFloat),
        }
    }
    
    /// Create a hint for arithmetic on datetime
    pub fn datetime_arithmetic(source: String, pg_type: PgType, datetime_subtype: DateTimeSubtype) -> Self {
        Self {
            source_column: Some(source),
            suggested_type: Some(pg_type),
            datetime_subtype: Some(datetime_subtype),
            is_expression: true,
            expression_type: Some(ExpressionType::DateTimeExpression),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_metadata_creation() {
        let mut metadata = TranslationMetadata::new();
        let hint = ColumnTypeHint::simple_column("ts".to_string(), PgType::Float8);
        metadata.add_hint("timestamp".to_string(), hint);
        
        assert!(metadata.get_hint("timestamp").is_some());
        assert!(metadata.get_hint("nonexistent").is_none());
    }
    
    #[test]
    fn test_expression_hints() {
        let hint = ColumnTypeHint::datetime_expression(Some("created_at".to_string()), Some(DateTimeSubtype::Timestamp));
        assert!(hint.is_expression);
        assert_eq!(hint.expression_type, Some(ExpressionType::DateTimeExpression));
        assert_eq!(hint.suggested_type, Some(PgType::Timestamp));
    }
    
    #[test]
    fn test_metadata_merge() {
        let mut metadata1 = TranslationMetadata::new();
        metadata1.add_hint("col1".to_string(), ColumnTypeHint::simple_column("a".to_string(), PgType::Int4));
        
        let mut metadata2 = TranslationMetadata::new();
        metadata2.add_hint("col2".to_string(), ColumnTypeHint::simple_column("b".to_string(), PgType::Text));
        
        metadata1.merge(metadata2);
        assert!(metadata1.get_hint("col1").is_some());
        assert!(metadata1.get_hint("col2").is_some());
    }
}