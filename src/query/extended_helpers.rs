use sqlparser::ast::{Query, TableFactor, SetExpr};

pub fn extract_tables_from_query(query: &Query, tables: &mut Vec<String>) {
    // Extract from WITH clause if present
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            extract_tables_from_query(&cte.query, tables);
        }
    }
    
    // Extract from main query body
    match &query.body.as_ref() {
        SetExpr::Select(select) => {
            for table in &select.from {
                extract_tables_from_table_factor(&table.relation, tables);
                for join in &table.joins {
                    extract_tables_from_table_factor(&join.relation, tables);
                }
            }
        }
        SetExpr::Query(q) => extract_tables_from_query(q, tables),
        SetExpr::SetOperation { left, right, .. } => {
            if let SetExpr::Select(select) = left.as_ref() {
                for table in &select.from {
                    extract_tables_from_table_factor(&table.relation, tables);
                }
            }
            if let SetExpr::Select(select) = right.as_ref() {
                for table in &select.from {
                    extract_tables_from_table_factor(&table.relation, tables);
                }
            }
        }
        _ => {}
    }
}

pub fn extract_tables_from_table_factor(factor: &TableFactor, tables: &mut Vec<String>) {
    match factor {
        TableFactor::Table { name, .. } => {
            tables.push(name.to_string());
        }
        TableFactor::Derived { subquery, .. } => {
            extract_tables_from_query(subquery, tables);
        }
        TableFactor::TableFunction { .. } => {}
        _ => {}
    }
}