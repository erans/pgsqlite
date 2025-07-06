use crate::session::db_handler::{DbHandler, DbResponse};
use crate::metadata::EnumMetadata;
use sqlparser::ast::{Select, SelectItem, Expr};
use tracing::{debug, info};

/// Handler for pg_enum catalog queries
pub struct PgEnumHandler;

impl PgEnumHandler {
    /// Handle queries to pg_enum table
    pub async fn handle_query(select: &Select, db: &DbHandler) -> Result<DbResponse, String> {
        info!("Handling pg_enum query");
        debug!("pg_enum query selection: {:?}", select.selection);
        
        // pg_enum columns:
        // oid          - OID of the enum value
        // enumtypid    - OID of the enum type this value belongs to
        // enumsortorder - Sort position of this value within its enum type
        // enumlabel    - Textual label for this enum value
        
        let mut columns = vec![];
        
        // Determine which columns are being selected
        for item in select.projection.iter() {
            match item {
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    let col_name = parts.last().unwrap().value.to_lowercase();
                    columns.push(col_name);
                }
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    let col_name = ident.value.to_lowercase();
                    columns.push(col_name);
                }
                SelectItem::Wildcard(_) => {
                    // Return all columns
                    columns = vec!["oid".to_string(), "enumtypid".to_string(), 
                                  "enumsortorder".to_string(), "enumlabel".to_string()];
                    break;
                }
                _ => {}
            }
        }
        
        // If no specific columns, default to all
        if columns.is_empty() {
            columns = vec!["oid".to_string(), "enumtypid".to_string(), 
                          "enumsortorder".to_string(), "enumlabel".to_string()];
        }
        
        // Check for WHERE clause filtering
        let mut filter_type_oid = None;
        let mut filter_value_oid = None;
        
        if let Some(selection) = &select.selection {
            Self::extract_filters(selection, &mut filter_type_oid, &mut filter_value_oid);
        }
        
        // Get connection for metadata queries
        let conn = db.get_mut_connection()
            .map_err(|e| format!("Failed to get connection: {}", e))?;
        
        let mut rows = Vec::new();
        
        debug!("pg_enum filters - type_oid: {:?}, value_oid: {:?}", filter_type_oid, filter_value_oid);
        
        if let Some(type_oid) = filter_type_oid {
            // Filter by enum type
            debug!("Filtering pg_enum by enumtypid = {}", type_oid);
            if let Ok(values) = EnumMetadata::get_enum_values(&conn, type_oid) {
                debug!("Found {} values for enum type {}", values.len(), type_oid);
                for value in values {
                    let row = Self::build_row(&columns, &value);
                    rows.push(row);
                }
            }
        } else if let Some(value_oid) = filter_value_oid {
            // Filter by specific value OID
            debug!("Filtering pg_enum by oid = {}", value_oid);
            if let Ok(Some(value)) = EnumMetadata::get_enum_value_by_oid(&conn, value_oid) {
                let row = Self::build_row(&columns, &value);
                rows.push(row);
            }
        } else {
            // Return all enum values from all types
            debug!("Returning all pg_enum entries");
            if let Ok(enum_types) = EnumMetadata::get_all_enum_types(&conn) {
                for enum_type in enum_types {
                    if let Ok(values) = EnumMetadata::get_enum_values(&conn, enum_type.type_oid) {
                        for value in values {
                            let row = Self::build_row(&columns, &value);
                            rows.push(row);
                        }
                    }
                }
            }
        }
        
        let rows_affected = rows.len();
        
        Ok(DbResponse {
            columns: columns.clone(),
            rows,
            rows_affected,
        })
    }
    
    /// Build a row for the selected columns
    fn build_row(columns: &[String], value: &crate::metadata::EnumValue) -> Vec<Option<Vec<u8>>> {
        let mut row = Vec::new();
        
        for col in columns {
            let cell = match col.as_str() {
                "oid" => Some(value.value_oid.to_string().into_bytes()),
                "enumtypid" => Some(value.type_oid.to_string().into_bytes()),
                "enumsortorder" => Some(value.sort_order.to_string().into_bytes()),
                "enumlabel" => Some(value.label.clone().into_bytes()),
                _ => None,
            };
            row.push(cell);
        }
        
        row
    }
    
    /// Extract filter conditions from WHERE clause
    fn extract_filters(expr: &Expr, filter_type_oid: &mut Option<i32>, filter_value_oid: &mut Option<i32>) {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                if matches!(op, sqlparser::ast::BinaryOperator::Eq) {
                    let column_name = match left.as_ref() {
                        Expr::CompoundIdentifier(parts) => {
                            parts.last().map(|p| p.value.to_lowercase())
                        }
                        Expr::Identifier(ident) => Some(ident.value.to_lowercase()),
                        _ => None,
                    };
                    
                    if let Some(col) = column_name {
                        match col.as_str() {
                            "enumtypid" => {
                                match right.as_ref() {
                                    Expr::Value(sqlparser::ast::ValueWithSpan { 
                                        value: sqlparser::ast::Value::Number(n, _), .. 
                                    }) => {
                                        *filter_type_oid = n.parse().ok();
                                        debug!("Extracted numeric enumtypid filter: {:?}", filter_type_oid);
                                    }
                                    Expr::Value(sqlparser::ast::ValueWithSpan { 
                                        value: sqlparser::ast::Value::SingleQuotedString(s), .. 
                                    }) => {
                                        // Handle quoted numeric strings (from parameter substitution)
                                        *filter_type_oid = s.parse().ok();
                                        debug!("Extracted string enumtypid filter: {:?}", filter_type_oid);
                                    }
                                    Expr::Value(sqlparser::ast::ValueWithSpan { 
                                        value: sqlparser::ast::Value::Placeholder(_), .. 
                                    }) => {
                                        // For placeholders, we can't filter at this stage
                                        // The actual filtering will happen when the query is executed
                                        debug!("Placeholder detected for enumtypid filter");
                                    }
                                    _ => {
                                        debug!("Unknown expression type for enumtypid: {:?}", right);
                                    }
                                }
                            }
                            "oid" => {
                                match right.as_ref() {
                                    Expr::Value(sqlparser::ast::ValueWithSpan { 
                                        value: sqlparser::ast::Value::Number(n, _), .. 
                                    }) => {
                                        *filter_value_oid = n.parse().ok();
                                    }
                                    Expr::Value(sqlparser::ast::ValueWithSpan { 
                                        value: sqlparser::ast::Value::SingleQuotedString(s), .. 
                                    }) => {
                                        // Handle quoted numeric strings
                                        *filter_value_oid = s.parse().ok();
                                    }
                                    _ => {}
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            Expr::InList { expr, list, negated: false } => {
                // Handle IN clauses
                if let Expr::Identifier(ident) = expr.as_ref() {
                    if ident.value.to_lowercase() == "enumtypid" && list.len() == 1 {
                        if let Expr::Value(sqlparser::ast::ValueWithSpan { 
                            value: sqlparser::ast::Value::Number(n, _), .. 
                        }) = &list[0] {
                            *filter_type_oid = n.parse().ok();
                        }
                    }
                }
            }
            _ => {}
        }
    }
}