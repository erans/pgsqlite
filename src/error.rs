use crate::protocol::messages::ErrorResponse;

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