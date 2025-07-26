use std::collections::HashMap;
use regex::Regex;
use once_cell::sync::Lazy;
use tracing::info;

// Regex to extract all tables from a JOIN query
static JOIN_TABLES_PATTERN: Lazy<Regex> = Lazy::new(|| {
    // Match FROM/JOIN followed by table name
    // Stop before ON/WHERE/AS keywords
    Regex::new(r"(?i)(?:FROM|JOIN)\s+([^\s,()]+)").unwrap()
});

// Regex to extract column with table prefix (e.g., order_items.unit_price)
static COLUMN_WITH_TABLE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)(\w+)\.(\w+)").unwrap()
});

/// Extract all table names from a query with JOINs
pub fn extract_all_tables_from_query(query: &str) -> Vec<(String, Option<String>)> {
    let mut tables = Vec::new();
    
    for cap in JOIN_TABLES_PATTERN.captures_iter(query) {
        let table_name = cap[1].trim_matches('"').trim_matches('\'').to_string();
        
        // Skip if it's a subquery or function
        if !table_name.starts_with('(') && !table_name.contains('(') {
            // For now, we don't capture aliases from this regex
            tables.push((table_name, None));
        }
    }
    
    tables
}

/// Extract the table name for a specific column from the query
pub fn extract_table_for_column(query: &str, column_name: &str) -> Option<String> {
    // First check if the column name already includes the table prefix
    if let Some(caps) = COLUMN_WITH_TABLE_PATTERN.captures(column_name) {
        return Some(caps[1].to_string());
    }
    
    // Look for explicit table.column pattern in the query
    let pattern = format!(r"(?i)(\w+)\.{}\b", regex::escape(column_name));
    if let Ok(re) = Regex::new(&pattern) {
        if let Some(caps) = re.captures(query) {
            return Some(caps[1].to_string());
        }
    }
    
    // If not found, check aliases
    let alias_pattern = format!(r"(?i)(\w+)\.(\w+)\s+AS\s+{}\b", regex::escape(column_name));
    if let Ok(re) = Regex::new(&alias_pattern) {
        if let Some(caps) = re.captures(query) {
            return Some(caps[1].to_string());
        }
    }
    
    None
}

/// Build a mapping of column names to their source tables for a JOIN query
pub fn build_column_to_table_mapping(query: &str) -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    
    // Extract all table.column patterns
    let column_pattern = Regex::new(r"(?i)(\w+)\.(\w+)(?:\s+AS\s+(\w+))?").unwrap();
    
    for cap in column_pattern.captures_iter(query) {
        let table = cap[1].to_string();
        let column = cap[2].to_string();
        
        // Add mapping for the original column name
        mapping.insert(column.clone(), table.clone());
        
        // If there's an alias, add mapping for that too
        if let Some(alias) = cap.get(3) {
            let alias_str = alias.as_str();
            mapping.insert(alias_str.to_string(), table.clone());
            info!("JOIN mapping: alias '{}' -> table '{}'", alias_str, table);
            
            // Also add a special mapping for SQLAlchemy patterns like "order_items_unit_price"
            // where the alias is "table_column" format
            if alias_str == format!("{}_{}", table, column) {
                // This helps the type inference find the right table for the alias
                mapping.insert(alias_str.to_string(), table.clone());
                info!("JOIN mapping: SQLAlchemy pattern detected '{}' for {}.{}", alias_str, table, column);
            }
        }
        
        // Also add table.column format
        mapping.insert(format!("{}.{}", table, column), table.clone());
    }
    
    info!("JOIN mapping: Built {} mappings from query", mapping.len());
    
    mapping
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_extract_all_tables() {
        // Initialize logger for tests
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .try_init();
            
        let query = "SELECT * FROM orders JOIN users ON orders.customer_id = users.id JOIN order_items ON orders.id = order_items.order_id";
        let tables = extract_all_tables_from_query(query);
        
        // Debug print
        for (i, (table, alias)) in tables.iter().enumerate() {
            println!("Table {}: name='{}', alias={:?}", i, table, alias);
        }
        
        assert_eq!(tables.len(), 3);
        assert_eq!(tables[0].0, "orders");
        assert_eq!(tables[1].0, "users");
        assert_eq!(tables[2].0, "order_items");
    }
    
    #[test]
    fn test_column_to_table_mapping() {
        // Initialize logger for tests
        let _ = tracing_subscriber::fmt()
            .with_test_writer()
            .try_init();
            
        let query = "SELECT orders.id AS orders_id, users.username AS users_username, order_items.unit_price AS order_items_unit_price FROM orders JOIN users ON orders.customer_id = users.id JOIN order_items ON orders.id = order_items.order_id";
        let mapping = build_column_to_table_mapping(query);
        
        assert_eq!(mapping.get("orders_id"), Some(&"orders".to_string()));
        assert_eq!(mapping.get("users_username"), Some(&"users".to_string()));
        assert_eq!(mapping.get("order_items_unit_price"), Some(&"order_items".to_string()));
        assert_eq!(mapping.get("unit_price"), Some(&"order_items".to_string()));
    }
}