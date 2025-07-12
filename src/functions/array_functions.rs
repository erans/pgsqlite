use rusqlite::{Connection, Result, functions::FunctionFlags};
use serde_json::{Value as JsonValue, json};

/// Register array-related functions in SQLite
pub fn register_array_functions(conn: &Connection) -> Result<()> {
    // Basic array information functions
    register_array_length(conn)?;
    register_array_upper(conn)?;
    register_array_lower(conn)?;
    register_array_ndims(conn)?;
    
    // Array manipulation functions
    register_array_append(conn)?;
    register_array_prepend(conn)?;
    register_array_cat(conn)?;
    register_array_remove(conn)?;
    register_array_replace(conn)?;
    
    // Array operator functions
    register_array_contains(conn)?;
    register_array_contained(conn)?;
    register_array_overlap(conn)?;
    
    // Array utility functions
    register_array_slice(conn)?;
    register_array_position(conn)?;
    register_array_positions(conn)?;
    
    // Array aggregate function
    register_array_agg(conn)?;
    
    Ok(())
}

/// array_length(array, dimension) - Get length of array in specified dimension
fn register_array_length(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_length",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let dimension: i32 = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(arr)) => {
                    if dimension == 1 {
                        Ok(Some(arr.len() as i32))
                    } else {
                        // For higher dimensions, check first element
                        if let Some(JsonValue::Array(inner)) = arr.first() {
                            if dimension == 2 {
                                Ok(Some(inner.len() as i32))
                            } else {
                                Ok(None) // Higher dimensions not yet supported
                            }
                        } else {
                            Ok(None)
                        }
                    }
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_upper(array, dimension) - Get upper bound of array dimension
fn register_array_upper(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_upper",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let dimension: i32 = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(arr)) => {
                    if dimension == 1 {
                        Ok(Some(arr.len() as i32))
                    } else if dimension == 2 {
                        // For 2D arrays, check first element
                        if let Some(JsonValue::Array(inner)) = arr.first() {
                            Ok(Some(inner.len() as i32))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_lower(array, dimension) - Get lower bound (always 1 for PostgreSQL arrays)
fn register_array_lower(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_lower",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let dimension: i32 = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(arr)) => {
                    if arr.is_empty() {
                        Ok(None)
                    } else if dimension == 1 {
                        Ok(Some(1))
                    } else if dimension == 2 {
                        // Check if it's actually a 2D array
                        if let Some(JsonValue::Array(_)) = arr.first() {
                            Ok(Some(1))
                        } else {
                            Ok(None)
                        }
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_ndims(array) - Get number of dimensions
fn register_array_ndims(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_ndims",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(json) => Ok(Some(count_dimensions(&json))),
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_append(array, element) - Append element to array
fn register_array_append(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_append",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let element: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(mut arr)) => {
                    // Parse element as JSON if possible, otherwise as string
                    let elem_value = serde_json::from_str::<JsonValue>(&element)
                        .unwrap_or_else(|_| JsonValue::String(element));
                    arr.push(elem_value);
                    Ok(serde_json::to_string(&arr).ok())
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_prepend(element, array) - Prepend element to array
fn register_array_prepend(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_prepend",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let element: String = ctx.get(0)?;
            let array_json: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(mut arr)) => {
                    // Parse element as JSON if possible, otherwise as string
                    let elem_value = serde_json::from_str::<JsonValue>(&element)
                        .unwrap_or_else(|_| JsonValue::String(element));
                    arr.insert(0, elem_value);
                    Ok(serde_json::to_string(&arr).ok())
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_cat(array1, array2) - Concatenate two arrays
fn register_array_cat(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_cat",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array1_json: String = ctx.get(0)?;
            let array2_json: String = ctx.get(1)?;
            
            match (
                serde_json::from_str::<JsonValue>(&array1_json),
                serde_json::from_str::<JsonValue>(&array2_json),
            ) {
                (Ok(JsonValue::Array(mut arr1)), Ok(JsonValue::Array(arr2))) => {
                    arr1.extend(arr2);
                    Ok(serde_json::to_string(&arr1).ok())
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_remove(array, element) - Remove all occurrences of element
fn register_array_remove(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_remove",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let element: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(arr)) => {
                    let elem_value = serde_json::from_str::<JsonValue>(&element)
                        .unwrap_or_else(|_| JsonValue::String(element.clone()));
                    
                    let filtered: Vec<JsonValue> = arr.into_iter()
                        .filter(|v| v != &elem_value)
                        .collect();
                    
                    Ok(serde_json::to_string(&filtered).ok())
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_replace(array, old, new) - Replace all occurrences
fn register_array_replace(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_replace",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let old_element: String = ctx.get(1)?;
            let new_element: String = ctx.get(2)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(arr)) => {
                    let old_value = serde_json::from_str::<JsonValue>(&old_element)
                        .unwrap_or_else(|_| JsonValue::String(old_element));
                    let new_value = serde_json::from_str::<JsonValue>(&new_element)
                        .unwrap_or_else(|_| JsonValue::String(new_element));
                    
                    let replaced: Vec<JsonValue> = arr.into_iter()
                        .map(|v| if v == old_value { new_value.clone() } else { v })
                        .collect();
                    
                    Ok(serde_json::to_string(&replaced).ok())
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_contains(array1, array2) - Check if array1 contains all elements of array2
fn register_array_contains(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_contains",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array1_json: String = ctx.get(0)?;
            let array2_json: String = ctx.get(1)?;
            
            match (
                serde_json::from_str::<JsonValue>(&array1_json),
                serde_json::from_str::<JsonValue>(&array2_json),
            ) {
                (Ok(JsonValue::Array(arr1)), Ok(JsonValue::Array(arr2))) => {
                    // Check if all elements of arr2 are in arr1
                    let contains_all = arr2.iter().all(|elem| arr1.contains(elem));
                    Ok(contains_all)
                }
                _ => Ok(false),
            }
        },
    )?;
    
    Ok(())
}

/// array_contained(array1, array2) - Check if all elements of array1 are in array2
fn register_array_contained(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_contained",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array1_json: String = ctx.get(0)?;
            let array2_json: String = ctx.get(1)?;
            
            match (
                serde_json::from_str::<JsonValue>(&array1_json),
                serde_json::from_str::<JsonValue>(&array2_json),
            ) {
                (Ok(JsonValue::Array(arr1)), Ok(JsonValue::Array(arr2))) => {
                    // Check if all elements of arr1 are in arr2
                    let contained_all = arr1.iter().all(|elem| arr2.contains(elem));
                    Ok(contained_all)
                }
                _ => Ok(false),
            }
        },
    )?;
    
    Ok(())
}

/// array_overlap(array1, array2) - Check if arrays have common elements
fn register_array_overlap(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_overlap",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array1_json: String = ctx.get(0)?;
            let array2_json: String = ctx.get(1)?;
            
            match (
                serde_json::from_str::<JsonValue>(&array1_json),
                serde_json::from_str::<JsonValue>(&array2_json),
            ) {
                (Ok(JsonValue::Array(arr1)), Ok(JsonValue::Array(arr2))) => {
                    // Check if any element of arr1 is in arr2
                    let has_overlap = arr1.iter().any(|elem| arr2.contains(elem));
                    Ok(has_overlap)
                }
                _ => Ok(false),
            }
        },
    )?;
    
    Ok(())
}

/// array_slice(array, start, end) - Extract slice from array (1-based indexing)
fn register_array_slice(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_slice",
        3,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let start: i32 = ctx.get(1)?;
            let end: i32 = ctx.get(2)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(arr)) => {
                    // Convert 1-based PostgreSQL indices to 0-based
                    let start_idx = (start - 1).max(0) as usize;
                    let end_idx = end.min(arr.len() as i32) as usize;
                    
                    if start_idx < arr.len() && start_idx < end_idx {
                        let slice: Vec<JsonValue> = arr[start_idx..end_idx].to_vec();
                        Ok(serde_json::to_string(&slice).ok())
                    } else {
                        Ok(Some("[]".to_string()))
                    }
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_position(array, element) - Find position of element (1-based)
fn register_array_position(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_position",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let element: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(arr)) => {
                    let elem_value = serde_json::from_str::<JsonValue>(&element)
                        .unwrap_or_else(|_| JsonValue::String(element));
                    
                    // Find first occurrence (1-based index)
                    for (i, val) in arr.iter().enumerate() {
                        if val == &elem_value {
                            return Ok(Some((i + 1) as i32));
                        }
                    }
                    Ok(None)
                }
                _ => Ok(None),
            }
        },
    )?;
    
    Ok(())
}

/// array_positions(array, element) - Find all positions of element
fn register_array_positions(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "array_positions",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let array_json: String = ctx.get(0)?;
            let element: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&array_json) {
                Ok(JsonValue::Array(arr)) => {
                    let elem_value = serde_json::from_str::<JsonValue>(&element)
                        .unwrap_or_else(|_| JsonValue::String(element));
                    
                    // Find all occurrences (1-based indices)
                    let positions: Vec<i32> = arr.iter()
                        .enumerate()
                        .filter_map(|(i, val)| {
                            if val == &elem_value {
                                Some((i + 1) as i32)
                            } else {
                                None
                            }
                        })
                        .collect();
                    
                    Ok(serde_json::to_string(&positions).ok())
                }
                _ => Ok(Some("[]".to_string())),
            }
        },
    )?;
    
    Ok(())
}

/// array_agg aggregate function
fn register_array_agg(conn: &Connection) -> Result<()> {
    use rusqlite::functions::Aggregate;
    
    #[derive(Default)]
    struct ArrayAgg;
    
    impl Aggregate<Vec<JsonValue>, Option<String>> for ArrayAgg {
        fn init(&self, _: &mut rusqlite::functions::Context<'_>) -> Result<Vec<JsonValue>> {
            Ok(Vec::new())
        }
        
        fn step(&self, ctx: &mut rusqlite::functions::Context<'_>, agg: &mut Vec<JsonValue>) -> Result<()> {
            let value = ctx.get_raw(0);
            
            let json_value = match value {
                rusqlite::types::ValueRef::Null => JsonValue::Null,
                rusqlite::types::ValueRef::Integer(i) => json!(i),
                rusqlite::types::ValueRef::Real(f) => json!(f),
                rusqlite::types::ValueRef::Text(s) => {
                    // Try to parse as JSON first, otherwise use as string
                    let text = std::str::from_utf8(s).unwrap_or("");
                    serde_json::from_str(text)
                        .unwrap_or_else(|_| JsonValue::String(text.to_string()))
                }
                rusqlite::types::ValueRef::Blob(b) => {
                    JsonValue::String(format!("\\x{}", hex::encode(b)))
                }
            };
            
            agg.push(json_value);
            Ok(())
        }
        
        fn finalize(&self, _: &mut rusqlite::functions::Context<'_>, agg: Option<Vec<JsonValue>>) -> Result<Option<String>> {
            Ok(agg.map(|values| serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_string())))
        }
    }
    
    conn.create_aggregate_function(
        "array_agg",
        1,
        FunctionFlags::SQLITE_UTF8,
        ArrayAgg,
    )?;
    
    Ok(())
}

/// Helper function to count array dimensions
fn count_dimensions(value: &JsonValue) -> i32 {
    match value {
        JsonValue::Array(arr) => {
            if arr.is_empty() {
                1
            } else {
                1 + arr.iter()
                    .filter_map(|v| {
                        if matches!(v, JsonValue::Array(_)) {
                            Some(count_dimensions(v) - 1)
                        } else {
                            None
                        }
                    })
                    .max()
                    .unwrap_or(0)
            }
        }
        _ => 0,
    }
}



#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_array_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_array_functions(&conn).unwrap();
        
        // Test array_length
        let len: i32 = conn.query_row(
            "SELECT array_length('[1,2,3,4,5]', 1)",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(len, 5);
        
        // Test array_append
        let result: String = conn.query_row(
            "SELECT array_append('[1,2,3]', '4')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(result, "[1,2,3,4]");
        
        // Test array_contains
        let contains: bool = conn.query_row(
            "SELECT array_contains('[1,2,3,4,5]', '[2,3]')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!(contains);
        
        // Test array_overlap
        let overlap: bool = conn.query_row(
            "SELECT array_overlap('[1,2,3]', '[3,4,5]')",
            [],
            |row| row.get(0)
        ).unwrap();
        assert!(overlap);
    }
}