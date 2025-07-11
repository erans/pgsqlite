use crate::protocol::messages::ErrorResponse;
use std::fmt;

/// PostgreSQL error types
#[derive(Debug)]
pub enum PgError {
    /// 22001: String data right truncation
    StringDataRightTruncation {
        type_name: String,
        column_name: String,
        actual_length: i32,
        max_length: i32,
    },
    /// 22003: Numeric value out of range
    NumericValueOutOfRange {
        type_name: String,
        column_name: String,
        value: String,
    },
    /// 23505: Unique constraint violation
    UniqueViolation {
        constraint_name: String,
        detail: String,
    },
    /// 23503: Foreign key violation
    ForeignKeyViolation {
        constraint_name: String,
        detail: String,
    },
    /// 42601: Syntax error
    SyntaxError {
        message: String,
        position: Option<i32>,
    },
    /// Generic error
    Generic {
        code: String,
        message: String,
    },
}

impl PgError {
    /// Convert to ErrorResponse for protocol
    pub fn to_error_response(&self) -> ErrorResponse {
        match self {
            PgError::StringDataRightTruncation { type_name, column_name, actual_length, max_length } => {
                ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: "22001".to_string(),
                    message: format!("value too long for type {}", type_name),
                    detail: Some(format!(
                        "Failing row contains ({}) with {} characters, maximum is {}.",
                        column_name, actual_length, max_length
                    )),
                    hint: None,
                    position: None,
                    internal_position: None,
                    internal_query: None,
                    where_: None,
                    schema: None,
                    table: None,
                    column: Some(column_name.clone()),
                    datatype: Some(type_name.clone()),
                    constraint: None,
                    file: None,
                    line: None,
                    routine: None,
                }
            }
            PgError::NumericValueOutOfRange { type_name, column_name, value: _ } => {
                ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: "22003".to_string(),
                    message: format!("numeric field overflow"),
                    detail: Some({
                        // Parse numeric(p,s) to extract precision and scale
                        let params = type_name.split('(').nth(1).unwrap_or("").trim_end_matches(')');
                        let parts: Vec<&str> = params.split(',').collect();
                        let precision = parts.get(0).unwrap_or(&"").trim();
                        let scale = parts.get(1).unwrap_or(&"0").trim();
                        format!(
                            "A field with precision {}, scale {} must round to an absolute value less than 10^({}-{}) = 10^{}.",
                            precision, scale, precision, scale,
                            precision.parse::<i32>().unwrap_or(0) - scale.parse::<i32>().unwrap_or(0)
                        )
                    }),
                    hint: None,
                    position: None,
                    internal_position: None,
                    internal_query: None,
                    where_: None,
                    schema: None,
                    table: None,
                    column: Some(column_name.clone()),
                    datatype: Some(type_name.clone()),
                    constraint: None,
                    file: None,
                    line: None,
                    routine: None,
                }
            }
            PgError::UniqueViolation { constraint_name, detail } => {
                ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: "23505".to_string(),
                    message: format!("duplicate key value violates unique constraint \"{}\"", constraint_name),
                    detail: Some(detail.clone()),
                    hint: None,
                    position: None,
                    internal_position: None,
                    internal_query: None,
                    where_: None,
                    schema: None,
                    table: None,
                    column: None,
                    datatype: None,
                    constraint: Some(constraint_name.clone()),
                    file: None,
                    line: None,
                    routine: None,
                }
            }
            PgError::ForeignKeyViolation { constraint_name, detail } => {
                ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: "23503".to_string(),
                    message: format!("insert or update on table violates foreign key constraint \"{}\"", constraint_name),
                    detail: Some(detail.clone()),
                    hint: None,
                    position: None,
                    internal_position: None,
                    internal_query: None,
                    where_: None,
                    schema: None,
                    table: None,
                    column: None,
                    datatype: None,
                    constraint: Some(constraint_name.clone()),
                    file: None,
                    line: None,
                    routine: None,
                }
            }
            PgError::SyntaxError { message, position } => {
                ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: "42601".to_string(),
                    message: message.clone(),
                    detail: None,
                    hint: None,
                    position: *position,
                    internal_position: None,
                    internal_query: None,
                    where_: None,
                    schema: None,
                    table: None,
                    column: None,
                    datatype: None,
                    constraint: None,
                    file: None,
                    line: None,
                    routine: None,
                }
            }
            PgError::Generic { code, message } => {
                ErrorResponse {
                    severity: "ERROR".to_string(),
                    code: code.clone(),
                    message: message.clone(),
                    detail: None,
                    hint: None,
                    position: None,
                    internal_position: None,
                    internal_query: None,
                    where_: None,
                    schema: None,
                    table: None,
                    column: None,
                    datatype: None,
                    constraint: None,
                    file: None,
                    line: None,
                    routine: None,
                }
            }
        }
    }
}

impl fmt::Display for PgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PgError::StringDataRightTruncation { type_name, column_name, actual_length, max_length } => {
                write!(f, "value too long for type {} in column {} ({} characters, maximum is {})", 
                       type_name, column_name, actual_length, max_length)
            }
            PgError::NumericValueOutOfRange { type_name, column_name, value } => {
                write!(f, "numeric field overflow for column {} (type: {}, value: {})", 
                       column_name, type_name, value)
            }
            PgError::UniqueViolation { constraint_name, detail } => {
                write!(f, "duplicate key value violates unique constraint \"{}\": {}", 
                       constraint_name, detail)
            }
            PgError::ForeignKeyViolation { constraint_name, detail } => {
                write!(f, "foreign key constraint \"{}\" violation: {}", 
                       constraint_name, detail)
            }
            PgError::SyntaxError { message, position } => {
                if let Some(pos) = position {
                    write!(f, "syntax error at position {}: {}", pos, message)
                } else {
                    write!(f, "syntax error: {}", message)
                }
            }
            PgError::Generic { code, message } => {
                write!(f, "error {}: {}", code, message)
            }
        }
    }
}

impl std::error::Error for PgError {}

/// Convert SQLite errors to PostgreSQL errors
pub fn sqlite_error_to_pg(err: &rusqlite::Error, _query: &str) -> ErrorResponse {
    match err {
        rusqlite::Error::SqliteFailure(sqlite_err, msg) => {
            use rusqlite::ErrorCode;
            match sqlite_err.code {
                ErrorCode::ConstraintViolation => {
                    // Try to extract constraint details from message
                    if let Some(msg) = msg {
                        if msg.contains("UNIQUE constraint failed") {
                            return ErrorResponse {
                                severity: "ERROR".to_string(),
                                code: "23505".to_string(),
                                message: "duplicate key value violates unique constraint".to_string(),
                                detail: Some(msg.clone()),
                                hint: None,
                                position: None,
                                internal_position: None,
                                internal_query: None,
                                where_: None,
                                schema: None,
                                table: None,
                                column: None,
                                datatype: None,
                                constraint: None,
                                file: None,
                                line: None,
                                routine: None,
                            };
                        } else if msg.contains("FOREIGN KEY constraint failed") {
                            return ErrorResponse {
                                severity: "ERROR".to_string(),
                                code: "23503".to_string(),
                                message: "foreign key constraint violation".to_string(),
                                detail: Some(msg.clone()),
                                hint: None,
                                position: None,
                                internal_position: None,
                                internal_query: None,
                                where_: None,
                                schema: None,
                                table: None,
                                column: None,
                                datatype: None,
                                constraint: None,
                                file: None,
                                line: None,
                                routine: None,
                            };
                        } else if msg.contains("numeric field overflow") {
                            return ErrorResponse {
                                severity: "ERROR".to_string(),
                                code: "22003".to_string(),
                                message: "numeric field overflow".to_string(),
                                detail: Some("A field with precision and scale constraints must round to an absolute value less than 10^p - 1.".to_string()),
                                hint: None,
                                position: None,
                                internal_position: None,
                                internal_query: None,
                                where_: None,
                                schema: None,
                                table: None,
                                column: None,
                                datatype: None,
                                constraint: None,
                                file: None,
                                line: None,
                                routine: None,
                            };
                        }
                    }
                    ErrorResponse::new(
                        "ERROR".to_string(),
                        "23000".to_string(),
                        "constraint violation".to_string(),
                    )
                }
                _ => ErrorResponse::new(
                    "ERROR".to_string(),
                    "XX000".to_string(),
                    format!("SQLite error: {}", err),
                ),
            }
        }
        _ => ErrorResponse::new(
            "ERROR".to_string(),
            "XX000".to_string(),
            format!("Database error: {}", err),
        ),
    }
}