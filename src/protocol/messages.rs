use std::collections::HashMap;

#[derive(Debug, Clone)]
pub enum FrontendMessage {
    SslRequest,
    StartupMessage(StartupMessage),
    Query(String),
    Parse {
        name: String,
        query: String,
        param_types: Vec<i32>,
    },
    Bind {
        portal: String,
        statement: String,
        formats: Vec<i16>,
        values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    },
    Execute {
        portal: String,
        max_rows: i32,
    },
    Sync,
    Terminate,
    Close {
        typ: u8, // 'S' for statement, 'P' for portal
        name: String,
    },
    Describe {
        typ: u8, // 'S' for statement, 'P' for portal
        name: String,
    },
    Flush,
}

#[derive(Debug, Clone)]
pub struct StartupMessage {
    pub protocol_version: i32,
    pub parameters: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum BackendMessage {
    Authentication(AuthenticationMessage),
    ParameterStatus { name: String, value: String },
    BackendKeyData { process_id: i32, secret_key: i32 },
    ReadyForQuery { status: TransactionStatus },
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<Vec<u8>>>),
    CommandComplete { tag: String },
    EmptyQueryResponse,
    ErrorResponse(Box<ErrorResponse>),
    NoticeResponse(NoticeResponse),
    ParseComplete,
    BindComplete,
    CloseComplete,
    PortalSuspended,
    NoData,
    ParameterDescription(Vec<i32>),
}

#[derive(Debug, Clone)]
pub enum AuthenticationMessage {
    Ok,
    CleartextPassword,
    MD5Password { salt: [u8; 4] },
    // Add more authentication types as needed
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TransactionStatus {
    Idle,
    InTransaction,
    InFailedTransaction,
}

impl TransactionStatus {
    pub fn as_byte(&self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::InFailedTransaction => b'E',
        }
    }
}

#[derive(Debug, Clone)]
pub struct FieldDescription {
    pub name: String,
    pub table_oid: i32,
    pub column_id: i16,
    pub type_oid: i32,
    pub type_size: i16,
    pub type_modifier: i32,
    pub format: i16, // 0 = text, 1 = binary
}

#[derive(Debug, Clone)]
pub struct ErrorResponse {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<i32>,
    pub internal_position: Option<i32>,
    pub internal_query: Option<String>,
    pub where_: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
    pub column: Option<String>,
    pub datatype: Option<String>,
    pub constraint: Option<String>,
    pub file: Option<String>,
    pub line: Option<i32>,
    pub routine: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NoticeResponse {
    pub severity: String,
    pub code: String,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<i32>,
    pub where_: Option<String>,
}

impl ErrorResponse {
    pub fn new(severity: String, code: String, message: String) -> Self {
        ErrorResponse {
            severity,
            code,
            message,
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