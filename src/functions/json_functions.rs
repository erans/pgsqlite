use rusqlite::{Connection, Result, functions::FunctionFlags, types::ValueRef};
use serde_json::Value as JsonValue;

/// Register JSON/JSONB-related functions in SQLite
pub fn register_json_functions(conn: &Connection) -> Result<()> {
    // json_valid(text) - Validate JSON (SQLite built-in, but we override for consistency)
    conn.create_scalar_function(
        "json_valid",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value: String = ctx.get(0)?;
            Ok(serde_json::from_str::<JsonValue>(&value).is_ok())
        },
    )?;
    
    // jsonb_typeof(jsonb) - Get JSON value type
    conn.create_scalar_function(
        "jsonb_typeof",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        json_typeof,
    )?;
    
    // json_typeof(json) - Alias for jsonb_typeof
    conn.create_scalar_function(
        "json_typeof",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        json_typeof,
    )?;
    
    // jsonb_array_length(jsonb) - Get array length
    conn.create_scalar_function(
        "jsonb_array_length",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value: String = ctx.get(0)?;
            match serde_json::from_str::<JsonValue>(&value) {
                Ok(JsonValue::Array(arr)) => Ok(Some(arr.len() as i64)),
                Ok(_) => Ok(None),
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // json_array_length(json) - Alias
    conn.create_scalar_function(
        "json_array_length",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value: String = ctx.get(0)?;
            match serde_json::from_str::<JsonValue>(&value) {
                Ok(JsonValue::Array(arr)) => Ok(Some(arr.len() as i64)),
                Ok(_) => Ok(None),
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // jsonb_object_keys(jsonb) - Get object keys (returns them as comma-separated for now)
    conn.create_scalar_function(
        "jsonb_object_keys",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let value: String = ctx.get(0)?;
            match serde_json::from_str::<JsonValue>(&value) {
                Ok(JsonValue::Object(obj)) => {
                    let keys: Vec<String> = obj.keys().cloned().collect();
                    Ok(Some(keys.join(",")))
                }
                Ok(_) => Ok(None),
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // to_json(anyelement) - Convert to JSON
    conn.create_scalar_function(
        "to_json",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            // Try to get as string first
            if let Ok(s) = ctx.get::<String>(0) {
                Ok(serde_json::to_string(&s).unwrap())
            } else {
                match ctx.get_raw(0) {
                    ValueRef::Null => Ok("null".to_string()),
                    ValueRef::Integer(i) => Ok(i.to_string()),
                    ValueRef::Real(f) => Ok(f.to_string()),
                    ValueRef::Text(s) => Ok(serde_json::to_string(&s).unwrap()),
                    ValueRef::Blob(b) => {
                        // Convert blob to hex string for JSON
                        let hex = b.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
                        Ok(serde_json::to_string(&hex).unwrap())
                    },
                }
            }
        },
    )?;
    
    // to_jsonb(anyelement) - Alias for to_json
    conn.create_scalar_function(
        "to_jsonb",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            // Try to get as string first
            if let Ok(s) = ctx.get::<String>(0) {
                Ok(serde_json::to_string(&s).unwrap())
            } else {
                match ctx.get_raw(0) {
                    ValueRef::Null => Ok("null".to_string()),
                    ValueRef::Integer(i) => Ok(i.to_string()),
                    ValueRef::Real(f) => Ok(f.to_string()),
                    ValueRef::Text(s) => Ok(serde_json::to_string(&s).unwrap()),
                    ValueRef::Blob(b) => {
                        // Convert blob to hex string for JSON
                        let hex = b.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
                        Ok(serde_json::to_string(&hex).unwrap())
                    },
                }
            }
        },
    )?;
    
    // json_build_object(variadic) - Build JSON object from key-value pairs
    // For simplicity, we'll implement a 2-argument version
    conn.create_scalar_function(
        "json_build_object",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let key: String = ctx.get(0)?;
            let value: String = match ctx.get_raw(1) {
                ValueRef::Null => "null".to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(f) => f.to_string(),
                ValueRef::Text(s) => serde_json::to_string(&s).unwrap(),
                ValueRef::Blob(b) => {
                    // Convert blob to hex string for JSON  
                    let hex = b.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
                    serde_json::to_string(&hex).unwrap()
                },
            };
            
            Ok(format!("{{\"{key}\": {value}}}"))
        },
    )?;
    
    // json_extract_scalar(json, path) - Extract scalar value from JSON path
    conn.create_scalar_function(
        "json_extract_scalar",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(json) => {
                    let result = extract_json_path(&json, &path);
                    match result {
                        Some(JsonValue::String(s)) => Ok(Some(s)),
                        Some(JsonValue::Number(n)) => Ok(Some(n.to_string())),
                        Some(JsonValue::Bool(b)) => Ok(Some(b.to_string())),
                        Some(JsonValue::Null) => Ok(None),
                        _ => Ok(None),
                    }
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // Don't override SQLite's built-in json_extract
    // SQLite already has a json_extract function that works correctly
    // Our implementation was interfering with it
    
    // jsonb_contains(jsonb, jsonb) - Check if first JSON contains second
    conn.create_scalar_function(
        "jsonb_contains",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json1: String = ctx.get(0)?;
            let json2: String = ctx.get(1)?;
            
            match (serde_json::from_str::<JsonValue>(&json1), serde_json::from_str::<JsonValue>(&json2)) {
                (Ok(container), Ok(contained)) => Ok(json_contains(&container, &contained)),
                _ => Ok(false),
            }
        },
    )?;
    
    // jsonb_contained(jsonb, jsonb) - Check if first JSON is contained in second
    conn.create_scalar_function(
        "jsonb_contained",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json1: String = ctx.get(0)?;
            let json2: String = ctx.get(1)?;
            
            match (serde_json::from_str::<JsonValue>(&json1), serde_json::from_str::<JsonValue>(&json2)) {
                (Ok(contained), Ok(container)) => Ok(json_contains(&container, &contained)),
                _ => Ok(false),
            }
        },
    )?;
    
    // json_array_elements(json) - Extract array elements as rows (returns as comma-separated for now)
    conn.create_scalar_function(
        "json_array_elements",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Array(arr)) => {
                    let elements: Vec<String> = arr.iter()
                        .map(|v| serde_json::to_string(v).unwrap_or_default())
                        .collect();
                    Ok(Some(elements.join(",")))
                }
                Ok(_) => Ok(None),
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // jsonb_array_elements(jsonb) - Alias for json_array_elements
    conn.create_scalar_function(
        "jsonb_array_elements",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Array(arr)) => {
                    let elements: Vec<String> = arr.iter()
                        .map(|v| serde_json::to_string(v).unwrap_or_default())
                        .collect();
                    Ok(Some(elements.join(",")))
                }
                Ok(_) => Ok(None),
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // json_array_elements_text(json) - Extract array elements as text
    conn.create_scalar_function(
        "json_array_elements_text",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Array(arr)) => {
                    let elements: Vec<String> = arr.iter()
                        .map(|v| match v {
                            JsonValue::String(s) => s.clone(),
                            _ => v.to_string().trim_matches('"').to_string(),
                        })
                        .collect();
                    Ok(Some(elements.join(",")))
                }
                Ok(_) => Ok(None),
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // json_strip_nulls(json) - Remove all null values from JSON
    conn.create_scalar_function(
        "json_strip_nulls",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(json) => {
                    let stripped = strip_nulls(&json);
                    Ok(serde_json::to_string(&stripped).ok())
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // jsonb_strip_nulls(jsonb) - Alias for json_strip_nulls
    conn.create_scalar_function(
        "jsonb_strip_nulls",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(json) => {
                    let stripped = strip_nulls(&json);
                    Ok(serde_json::to_string(&stripped).ok())
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // jsonb_set(jsonb, text[], jsonb, boolean) - Set value at path
    // For simplicity, implement a 3-arg version without create_missing flag
    conn.create_scalar_function(
        "jsonb_set",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path_str: String = ctx.get(1)?;
            let new_value_str: String = ctx.get(2)?;
            
            match (serde_json::from_str::<JsonValue>(&json_str), 
                   serde_json::from_str::<JsonValue>(&new_value_str)) {
                (Ok(mut json), Ok(new_value)) => {
                    // Parse path - expecting format like '{key1,key2}'
                    let path = parse_json_path(&path_str);
                    set_json_value(&mut json, &path, new_value);
                    Ok(serde_json::to_string(&json).ok())
                }
                _ => Ok(Some(json_str)),
            }
        },
    )?;
    
    // json_extract_path(json, variadic text) - Extract value at path
    // For simplicity, implement a 2-arg version
    conn.create_scalar_function(
        "json_extract_path",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(json) => {
                    let result = extract_json_path(&json, &path);
                    Ok(result.map(|v| serde_json::to_string(&v).unwrap_or_default()))
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // json_extract_path_text(json, variadic text) - Extract value at path as text
    conn.create_scalar_function(
        "json_extract_path_text",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(json) => {
                    let result = extract_json_path(&json, &path);
                    Ok(result.map(|v| match v {
                        JsonValue::String(s) => s,
                        JsonValue::Null => "null".to_string(),
                        JsonValue::Bool(b) => b.to_string(),
                        JsonValue::Number(n) => n.to_string(),
                        _ => serde_json::to_string(&v).unwrap_or_default(),
                    }))
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // Custom functions for JSON operators to avoid $ character issues
    
    // pgsqlite_json_get_text(json, key) - Extract key as text (->> operator with string key)
    conn.create_scalar_function(
        "pgsqlite_json_get_text",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Handle both string and direct input for JSON
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(None),
                ValueRef::Blob(_) => return Ok(None),
            };
            
            let key: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Object(map)) => {
                    match map.get(&key) {
                        Some(JsonValue::String(s)) => Ok(Some(s.clone())),
                        Some(JsonValue::Null) => Ok(None),
                        Some(v) => Ok(Some(match v {
                            JsonValue::Bool(b) => b.to_string(),
                            JsonValue::Number(n) => n.to_string(),
                            _ => serde_json::to_string(v).unwrap_or_default(),
                        })),
                        None => Ok(None),
                    }
                }
                _ => Ok(None),
            }
        },
    )?;
    
    // pgsqlite_json_get_json(json, key) - Extract key as JSON (-> operator with string key)
    conn.create_scalar_function(
        "pgsqlite_json_get_json",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Handle both string and direct input for JSON
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(None),
                ValueRef::Blob(_) => return Ok(None),
            };
            
            let key: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Object(map)) => {
                    match map.get(&key) {
                        Some(value) => Ok(Some(serde_json::to_string(value).unwrap_or_default())),
                        None => Ok(None),
                    }
                }
                Ok(JsonValue::Array(_)) => {
                    // If it's an array and we're using a string key, return null
                    Ok(None)
                }
                _ => Ok(None),
            }
        },
    )?;
    
    // pgsqlite_json_get_array_text(json, index) - Extract array element as text (->> operator with integer index)
    conn.create_scalar_function(
        "pgsqlite_json_get_array_text",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Handle both string and direct input for JSON
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(None),
                ValueRef::Blob(_) => return Ok(None),
            };
            
            let index: i64 = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Array(arr)) => {
                    if let Some(value) = arr.get(index as usize) {
                        match value {
                            JsonValue::String(s) => Ok(Some(s.clone())),
                            JsonValue::Null => Ok(None),
                            JsonValue::Bool(b) => Ok(Some(b.to_string())),
                            JsonValue::Number(n) => Ok(Some(n.to_string())),
                            _ => Ok(Some(serde_json::to_string(value).unwrap_or_default())),
                        }
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            }
        },
    )?;
    
    // pgsqlite_json_get_array_json(json, index) - Extract array element as JSON (-> operator with integer index)
    conn.create_scalar_function(
        "pgsqlite_json_get_array_json",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Handle both string and direct input for JSON
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(None),
                ValueRef::Blob(_) => return Ok(None),
            };
            
            let index: i64 = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Array(arr)) => {
                    if let Some(value) = arr.get(index as usize) {
                        Ok(Some(serde_json::to_string(value).unwrap_or_default()))
                    } else {
                        Ok(None)
                    }
                }
                _ => Ok(None),
            }
        },
    )?;
    
    // pgsqlite_json_path_text(json, path) - Extract path as text (#>> operator)
    conn.create_scalar_function(
        "pgsqlite_json_path_text",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Handle both string and direct input for JSON
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(None),
                ValueRef::Blob(_) => return Ok(None),
            };
            
            let path_str: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(json) => {
                    let path_parts: Vec<&str> = path_str.split(',').collect();
                    let result = extract_json_path_by_parts(&json, &path_parts);
                    Ok(result.map(|v| match v {
                        JsonValue::String(s) => s,
                        JsonValue::Null => "null".to_string(),
                        JsonValue::Bool(b) => b.to_string(),
                        JsonValue::Number(n) => n.to_string(),
                        _ => serde_json::to_string(&v).unwrap_or_default(),
                    }))
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // pgsqlite_json_path_json(json, path) - Extract path as JSON (#> operator)
    conn.create_scalar_function(
        "pgsqlite_json_path_json",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // Handle both string and direct input for JSON
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(None),
                ValueRef::Blob(_) => return Ok(None),
            };
            
            let path_str: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(json) => {
                    let path_parts: Vec<&str> = path_str.split(',').collect();
                    let result = extract_json_path_by_parts(&json, &path_parts);
                    Ok(result.map(|v| serde_json::to_string(&v).unwrap_or_default()))
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // pgsqlite_json_has_key(json, key) - Check if JSON object has key (? operator)
    conn.create_scalar_function(
        "pgsqlite_json_has_key",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(false),
                ValueRef::Blob(_) => return Ok(false),
            };
            
            let key: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Object(map)) => Ok(map.contains_key(&key)),
                _ => Ok(false),
            }
        },
    )?;
    
    // pgsqlite_json_has_any_key(json, keys) - Check if JSON object has any of the keys (?| operator)
    conn.create_scalar_function(
        "pgsqlite_json_has_any_key",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(false),
                ValueRef::Blob(_) => return Ok(false),
            };
            
            let keys_str: String = ctx.get(1)?;
            let keys: Vec<&str> = keys_str.split(',').map(|s| s.trim()).collect();
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Object(map)) => {
                    Ok(keys.iter().any(|key| map.contains_key(*key)))
                }
                _ => Ok(false),
            }
        },
    )?;
    
    // pgsqlite_json_has_all_keys(json, keys) - Check if JSON object has all of the keys (?& operator)
    conn.create_scalar_function(
        "pgsqlite_json_has_all_keys",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str = match ctx.get_raw(0) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                ValueRef::Real(r) => r.to_string(),
                ValueRef::Null => return Ok(false),
                ValueRef::Blob(_) => return Ok(false),
            };
            
            let keys_str: String = ctx.get(1)?;
            let keys: Vec<&str> = keys_str.split(',').map(|s| s.trim()).collect();
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Object(map)) => {
                    Ok(keys.iter().all(|key| map.contains_key(*key)))
                }
                _ => Ok(false),
            }
        },
    )?;
    
    // jsonb_insert(target, path, new_value, insert_after) - Insert value at path
    // For simplicity, implement a 3-arg version without insert_after flag (defaults to false)
    conn.create_scalar_function(
        "jsonb_insert",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path_str: String = ctx.get(1)?;
            let new_value_str: String = ctx.get(2)?;
            
            match (serde_json::from_str::<JsonValue>(&json_str), 
                   serde_json::from_str::<JsonValue>(&new_value_str)) {
                (Ok(mut json), Ok(new_value)) => {
                    // Parse path - expecting format like '{key1,key2}' or '{key1,0}' for array index
                    let path = parse_json_path(&path_str);
                    if insert_json_value(&mut json, &path, new_value, false) {
                        Ok(serde_json::to_string(&json).ok())
                    } else {
                        Ok(Some(json_str)) // Return original if insertion failed
                    }
                }
                _ => Ok(Some(json_str)), // Return original if parsing failed
            }
        },
    )?;
    
    // jsonb_insert(target, path, new_value, insert_after) - 4-arg version with insert_after flag
    conn.create_scalar_function(
        "jsonb_insert",
        4,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path_str: String = ctx.get(1)?;
            let new_value_str: String = ctx.get(2)?;
            let insert_after: bool = ctx.get(3)?;
            
            match (serde_json::from_str::<JsonValue>(&json_str), 
                   serde_json::from_str::<JsonValue>(&new_value_str)) {
                (Ok(mut json), Ok(new_value)) => {
                    // Parse path - expecting format like '{key1,key2}' or '{key1,0}' for array index
                    let path = parse_json_path(&path_str);
                    if insert_json_value(&mut json, &path, new_value, insert_after) {
                        Ok(serde_json::to_string(&json).ok())
                    } else {
                        Ok(Some(json_str)) // Return original if insertion failed
                    }
                }
                _ => Ok(Some(json_str)), // Return original if parsing failed
            }
        },
    )?;
    
    // jsonb_delete(target, path) - Delete value at path
    conn.create_scalar_function(
        "jsonb_delete",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path_str: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(mut json) => {
                    // Parse path - expecting format like '{key1,key2}' or '{key1,0}' for array index
                    let path = parse_json_path(&path_str);
                    if delete_json_value(&mut json, &path) {
                        Ok(serde_json::to_string(&json).ok())
                    } else {
                        Ok(Some(json_str)) // Return original if deletion failed
                    }
                }
                _ => Ok(Some(json_str)), // Return original if parsing failed
            }
        },
    )?;
    
    // jsonb_delete_path(target, path) - Delete value at path (alias for jsonb_delete)
    conn.create_scalar_function(
        "jsonb_delete_path",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            let path_str: String = ctx.get(1)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(mut json) => {
                    // Parse path - expecting format like '{key1,key2}' or '{key1,0}' for array index
                    let path = parse_json_path(&path_str);
                    if delete_json_value(&mut json, &path) {
                        Ok(serde_json::to_string(&json).ok())
                    } else {
                        Ok(Some(json_str)) // Return original if deletion failed
                    }
                }
                _ => Ok(Some(json_str)), // Return original if parsing failed
            }
        },
    )?;
    
    // jsonb_pretty(jsonb) - Pretty-print JSON
    conn.create_scalar_function(
        "jsonb_pretty",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(json) => {
                    // Pretty print with 2-space indentation
                    match serde_json::to_string_pretty(&json) {
                        Ok(pretty) => Ok(Some(pretty)),
                        Err(_) => Ok(Some(json_str)), // Return original if pretty-print fails
                    }
                }
                Err(_) => Ok(Some(json_str)), // Return original if not valid JSON
            }
        },
    )?;
    
    // json_each_value(json_text, key) - Get a value from json_each with proper boolean conversion
    conn.create_scalar_function(
        "json_each_value",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_text: String = ctx.get(0)?;
            // Key can be either a string or an integer (for arrays)
            let key = match ctx.get_raw(1) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                _ => return Ok(None),
            };
            
            // Parse the JSON and extract the value for the key
            match serde_json::from_str::<JsonValue>(&json_text) {
                Ok(json) => {
                    let value = match &json {
                        JsonValue::Object(obj) => obj.get(&key),
                        JsonValue::Array(arr) => {
                            if let Ok(idx) = key.parse::<usize>() {
                                arr.get(idx)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    
                    match value {
                        Some(JsonValue::Bool(true)) => Ok(Some("true".to_string())),
                        Some(JsonValue::Bool(false)) => Ok(Some("false".to_string())),
                        Some(JsonValue::String(s)) => Ok(Some(s.clone())),
                        Some(JsonValue::Number(n)) => Ok(Some(n.to_string())),
                        Some(JsonValue::Null) => Ok(Some("null".to_string())),
                        Some(val) => Ok(Some(val.to_string())),
                        None => Ok(None),
                    }
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // json_each_text_value(json_text, key) - Get a value from json_each as text (including arrays/objects as JSON strings)
    conn.create_scalar_function(
        "json_each_text_value",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_text: String = ctx.get(0)?;
            // Key can be either a string or an integer (for arrays)
            let key = match ctx.get_raw(1) {
                ValueRef::Text(s) => String::from_utf8_lossy(s).to_string(),
                ValueRef::Integer(i) => i.to_string(),
                _ => return Ok(None),
            };
            
            // Parse the JSON and extract the value for the key
            match serde_json::from_str::<JsonValue>(&json_text) {
                Ok(json) => {
                    let value = match &json {
                        JsonValue::Object(obj) => obj.get(&key),
                        JsonValue::Array(arr) => {
                            if let Ok(idx) = key.parse::<usize>() {
                                arr.get(idx)
                            } else {
                                None
                            }
                        }
                        _ => None,
                    };
                    
                    match value {
                        Some(JsonValue::Bool(b)) => Ok(Some(b.to_string())),
                        Some(JsonValue::String(s)) => Ok(Some(s.clone())),
                        Some(JsonValue::Number(n)) => Ok(Some(n.to_string())),
                        Some(JsonValue::Null) => Ok(Some("null".to_string())),
                        Some(JsonValue::Array(_)) | Some(JsonValue::Object(_)) => {
                            // For arrays and objects, return as JSON string
                            Ok(Some(serde_json::to_string(value.unwrap()).unwrap_or_default()))
                        }
                        None => Ok(None),
                    }
                }
                Err(_) => Ok(None),
            }
        },
    )?;
    
    // JSON aggregation functions
    register_json_agg(conn)?;
    register_jsonb_agg(conn)?;
    register_json_object_agg(conn)?;
    register_jsonb_object_agg(conn)?;
    
    // Row conversion functions
    register_row_to_json(conn)?;
    
    // Record conversion functions
    register_json_populate_record(conn)?;
    register_json_to_record(conn)?;
    
    Ok(())
}

/// json_agg(expression) - Aggregate values into a JSON array
fn register_json_agg(conn: &Connection) -> Result<()> {
    use rusqlite::functions::Aggregate;
    
    #[derive(Default)]
    struct JsonAgg;
    
    impl Aggregate<Vec<JsonValue>, Option<String>> for JsonAgg {
        fn init(&self, _: &mut rusqlite::functions::Context<'_>) -> Result<Vec<JsonValue>> {
            Ok(Vec::new())
        }
        
        fn step(&self, ctx: &mut rusqlite::functions::Context<'_>, agg: &mut Vec<JsonValue>) -> Result<()> {
            let value = ctx.get_raw(0);
            
            let json_value = match value {
                rusqlite::types::ValueRef::Null => JsonValue::Null,
                rusqlite::types::ValueRef::Integer(i) => JsonValue::Number(serde_json::Number::from(i)),
                rusqlite::types::ValueRef::Real(f) => {
                    if let Some(num) = serde_json::Number::from_f64(f) {
                        JsonValue::Number(num)
                    } else {
                        JsonValue::Null
                    }
                }
                rusqlite::types::ValueRef::Text(s) => {
                    let text = std::str::from_utf8(s).unwrap_or("");
                    // Try to parse as JSON first, if it fails treat as string
                    serde_json::from_str(text)
                        .unwrap_or_else(|_| JsonValue::String(text.to_string()))
                }
                rusqlite::types::ValueRef::Blob(b) => {
                    // Convert blob to hex string
                    JsonValue::String(format!("\\x{}", hex::encode(b)))
                }
            };
            
            agg.push(json_value);
            Ok(())
        }
        
        fn finalize(&self, _: &mut rusqlite::functions::Context<'_>, agg: Option<Vec<JsonValue>>) -> Result<Option<String>> {
            match agg {
                Some(values) => Ok(Some(serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_string()))),
                None => Ok(Some("[]".to_string())), // Return empty array for no rows
            }
        }
    }
    
    conn.create_aggregate_function(
        "json_agg",
        1,
        FunctionFlags::SQLITE_UTF8,
        JsonAgg,
    )?;
    
    Ok(())
}

/// jsonb_agg(expression) - Aggregate values into a JSONB array (alias for json_agg)
fn register_jsonb_agg(conn: &Connection) -> Result<()> {
    use rusqlite::functions::Aggregate;
    
    #[derive(Default)]
    struct JsonbAgg;
    
    impl Aggregate<Vec<JsonValue>, Option<String>> for JsonbAgg {
        fn init(&self, _: &mut rusqlite::functions::Context<'_>) -> Result<Vec<JsonValue>> {
            Ok(Vec::new())
        }
        
        fn step(&self, ctx: &mut rusqlite::functions::Context<'_>, agg: &mut Vec<JsonValue>) -> Result<()> {
            let value = ctx.get_raw(0);
            
            let json_value = match value {
                rusqlite::types::ValueRef::Null => JsonValue::Null,
                rusqlite::types::ValueRef::Integer(i) => JsonValue::Number(serde_json::Number::from(i)),
                rusqlite::types::ValueRef::Real(f) => {
                    if let Some(num) = serde_json::Number::from_f64(f) {
                        JsonValue::Number(num)
                    } else {
                        JsonValue::Null
                    }
                }
                rusqlite::types::ValueRef::Text(s) => {
                    let text = std::str::from_utf8(s).unwrap_or("");
                    // Try to parse as JSON first, if it fails treat as string
                    serde_json::from_str(text)
                        .unwrap_or_else(|_| JsonValue::String(text.to_string()))
                }
                rusqlite::types::ValueRef::Blob(b) => {
                    // Convert blob to hex string
                    JsonValue::String(format!("\\x{}", hex::encode(b)))
                }
            };
            
            agg.push(json_value);
            Ok(())
        }
        
        fn finalize(&self, _: &mut rusqlite::functions::Context<'_>, agg: Option<Vec<JsonValue>>) -> Result<Option<String>> {
            match agg {
                Some(values) => Ok(Some(serde_json::to_string(&values).unwrap_or_else(|_| "[]".to_string()))),
                None => Ok(Some("[]".to_string())), // Return empty array for no rows
            }
        }
    }
    
    conn.create_aggregate_function(
        "jsonb_agg",
        1,
        FunctionFlags::SQLITE_UTF8,
        JsonbAgg,
    )?;
    
    Ok(())
}

/// json_object_agg(key, value) - Aggregate key-value pairs into a JSON object
fn register_json_object_agg(conn: &Connection) -> Result<()> {
    use rusqlite::functions::Aggregate;
    use std::collections::HashMap;
    
    #[derive(Default)]
    struct JsonObjectAgg;
    
    impl Aggregate<HashMap<String, JsonValue>, Option<String>> for JsonObjectAgg {
        fn init(&self, _: &mut rusqlite::functions::Context<'_>) -> Result<HashMap<String, JsonValue>> {
            Ok(HashMap::new())
        }
        
        fn step(&self, ctx: &mut rusqlite::functions::Context<'_>, agg: &mut HashMap<String, JsonValue>) -> Result<()> {
            // Get the key (first argument)
            let key_value = ctx.get_raw(0);
            let key = match key_value {
                rusqlite::types::ValueRef::Text(s) => std::str::from_utf8(s).unwrap_or("").to_string(),
                rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                rusqlite::types::ValueRef::Real(f) => f.to_string(),
                rusqlite::types::ValueRef::Null => "null".to_string(),
                rusqlite::types::ValueRef::Blob(_) => return Ok(()), // Skip blob keys
            };
            
            // Get the value (second argument)
            let value_raw = ctx.get_raw(1);
            let json_value = match value_raw {
                rusqlite::types::ValueRef::Null => JsonValue::Null,
                rusqlite::types::ValueRef::Integer(i) => JsonValue::Number(serde_json::Number::from(i)),
                rusqlite::types::ValueRef::Real(f) => {
                    if let Some(num) = serde_json::Number::from_f64(f) {
                        JsonValue::Number(num)
                    } else {
                        JsonValue::Null
                    }
                }
                rusqlite::types::ValueRef::Text(s) => {
                    let text = std::str::from_utf8(s).unwrap_or("");
                    // For json_object_agg, treat text as literal strings (not JSON)
                    JsonValue::String(text.to_string())
                }
                rusqlite::types::ValueRef::Blob(b) => {
                    // Convert blob to hex string
                    JsonValue::String(format!("\\x{}", hex::encode(b)))
                }
            };
            
            agg.insert(key, json_value);
            Ok(())
        }
        
        fn finalize(&self, _: &mut rusqlite::functions::Context<'_>, agg: Option<HashMap<String, JsonValue>>) -> Result<Option<String>> {
            match agg {
                Some(map) => {
                    let json_map: serde_json::Map<String, JsonValue> = map.into_iter().collect();
                    let json_object = JsonValue::Object(json_map);
                    Ok(Some(serde_json::to_string(&json_object).unwrap_or_else(|_| "{}".to_string())))
                }
                None => Ok(Some("{}".to_string())), // Return empty object for no rows
            }
        }
    }
    
    conn.create_aggregate_function(
        "json_object_agg",
        2,
        FunctionFlags::SQLITE_UTF8,
        JsonObjectAgg,
    )?;
    
    Ok(())
}

/// jsonb_object_agg(key, value) - Aggregate key-value pairs into a JSON object
fn register_jsonb_object_agg(conn: &Connection) -> Result<()> {
    use rusqlite::functions::Aggregate;
    use std::collections::HashMap;
    
    #[derive(Default)]
    struct JsonbObjectAgg;
    
    impl Aggregate<HashMap<String, JsonValue>, Option<String>> for JsonbObjectAgg {
        fn init(&self, _: &mut rusqlite::functions::Context<'_>) -> Result<HashMap<String, JsonValue>> {
            Ok(HashMap::new())
        }
        
        fn step(&self, ctx: &mut rusqlite::functions::Context<'_>, agg: &mut HashMap<String, JsonValue>) -> Result<()> {
            // Get the key (first argument)
            let key_value = ctx.get_raw(0);
            let key = match key_value {
                rusqlite::types::ValueRef::Text(s) => std::str::from_utf8(s).unwrap_or("").to_string(),
                rusqlite::types::ValueRef::Integer(i) => i.to_string(),
                rusqlite::types::ValueRef::Real(f) => f.to_string(),
                rusqlite::types::ValueRef::Null => "null".to_string(),
                rusqlite::types::ValueRef::Blob(_) => return Ok(()), // Skip blob keys
            };
            
            // Get the value (second argument)
            let value_raw = ctx.get_raw(1);
            let json_value = match value_raw {
                rusqlite::types::ValueRef::Null => JsonValue::Null,
                rusqlite::types::ValueRef::Integer(i) => JsonValue::Number(serde_json::Number::from(i)),
                rusqlite::types::ValueRef::Real(f) => {
                    if let Some(num) = serde_json::Number::from_f64(f) {
                        JsonValue::Number(num)
                    } else {
                        JsonValue::Null
                    }
                }
                rusqlite::types::ValueRef::Text(s) => {
                    let text = std::str::from_utf8(s).unwrap_or("");
                    // For jsonb_object_agg, try to parse as JSON first, if it fails treat as string
                    serde_json::from_str(text)
                        .unwrap_or_else(|_| JsonValue::String(text.to_string()))
                }
                rusqlite::types::ValueRef::Blob(b) => {
                    // Convert blob to hex string
                    JsonValue::String(format!("\\x{}", hex::encode(b)))
                }
            };
            
            agg.insert(key, json_value);
            Ok(())
        }
        
        fn finalize(&self, _: &mut rusqlite::functions::Context<'_>, agg: Option<HashMap<String, JsonValue>>) -> Result<Option<String>> {
            match agg {
                Some(map) => {
                    let json_map: serde_json::Map<String, JsonValue> = map.into_iter().collect();
                    let json_object = JsonValue::Object(json_map);
                    Ok(Some(serde_json::to_string(&json_object).unwrap_or_else(|_| "{}".to_string())))
                }
                None => Ok(Some("{}".to_string())), // Return empty object for no rows
            }
        }
    }
    
    conn.create_aggregate_function(
        "jsonb_object_agg",
        2,
        FunctionFlags::SQLITE_UTF8,
        JsonbObjectAgg,
    )?;
    
    Ok(())
}

/// row_to_json(record [, pretty_bool]) - Convert row to JSON object
fn register_row_to_json(conn: &Connection) -> Result<()> {
    // This function will need to be implemented as a query translator
    // rather than a simple SQLite function, because PostgreSQL's row_to_json
    // works with composite types and subqueries.
    
    // For now, implement a basic version that handles simple JSON conversion
    // The real implementation would need to be in the query translator layer
    
    // Single parameter version: row_to_json(record)
    conn.create_scalar_function(
        "row_to_json",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let input = ctx.get_raw(0);
            convert_value_to_json(input, false)
        },
    )?;
    
    // Two parameter version: row_to_json(record, pretty_bool)
    conn.create_scalar_function(
        "row_to_json",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let input = ctx.get_raw(0);
            let pretty: bool = ctx.get(1)?;
            convert_value_to_json(input, pretty)
        },
    )?;
    
    Ok(())
}

/// Convert a SQLite value to JSON format
fn convert_value_to_json(value: rusqlite::types::ValueRef, pretty: bool) -> Result<Option<String>> {
    use rusqlite::types::ValueRef;
    
    let json_value = match value {
        ValueRef::Null => JsonValue::Null,
        ValueRef::Integer(i) => JsonValue::Number(serde_json::Number::from(i)),
        ValueRef::Real(f) => {
            if let Some(num) = serde_json::Number::from_f64(f) {
                JsonValue::Number(num)
            } else {
                JsonValue::Null
            }
        }
        ValueRef::Text(s) => {
            let text = std::str::from_utf8(s).unwrap_or("");
            // Try to parse as JSON first
            if let Ok(parsed) = serde_json::from_str::<JsonValue>(text) {
                parsed
            } else {
                JsonValue::String(text.to_string())
            }
        }
        ValueRef::Blob(b) => {
            // Convert blob to hex string
            JsonValue::String(format!("\\x{}", hex::encode(b)))
        }
    };
    
    Ok(Some(if pretty {
        serde_json::to_string_pretty(&json_value).unwrap_or_else(|_| "null".to_string())
    } else {
        serde_json::to_string(&json_value).unwrap_or_else(|_| "null".to_string())
    }))
}

/// Parse PostgreSQL array path format '{key1,key2}' into Vec<String>
fn parse_json_path(path_str: &str) -> Vec<String> {
    let trimmed = path_str.trim();
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        let inner = &trimmed[1..trimmed.len()-1];
        inner.split(',').map(|s| s.trim().to_string()).collect()
    } else {
        vec![trimmed.to_string()]
    }
}

/// Set value at path in JSON
fn set_json_value(json: &mut JsonValue, path: &[String], new_value: JsonValue) {
    if path.is_empty() {
        *json = new_value;
        return;
    }
    
    // Navigate to the parent of the target
    let (parent_path, last_key) = path.split_at(path.len() - 1);
    let last_key = &last_key[0];
    
    let mut current = json;
    for key in parent_path {
        match current {
            JsonValue::Object(map) => {
                current = map.entry(key.clone()).or_insert(JsonValue::Object(serde_json::Map::new()));
            }
            JsonValue::Array(arr) => {
                if let Ok(index) = key.parse::<usize>() {
                    if index < arr.len() {
                        current = &mut arr[index];
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
            _ => return,
        }
    }
    
    // Set the value at the last key
    match current {
        JsonValue::Object(map) => {
            map.insert(last_key.clone(), new_value);
        }
        JsonValue::Array(arr) => {
            if let Ok(index) = last_key.parse::<usize>()
                && index < arr.len() {
                    arr[index] = new_value;
                }
        }
        _ => {},
    }
}

/// Delete value at path in JSON
/// For objects: removes the key-value pair
/// For arrays: removes the element at specified index
/// Returns true if deletion was successful
fn delete_json_value(json: &mut JsonValue, path: &[String]) -> bool {
    if path.is_empty() {
        return false; // Cannot delete root
    }
    
    // Navigate to the parent container and delete at the specified location
    let (parent_path, last_key) = path.split_at(path.len() - 1);
    let last_key = &last_key[0];
    
    let mut current = json;
    for key in parent_path {
        match current {
            JsonValue::Object(map) => {
                // Check if the key exists before navigating
                if let Some(value) = map.get_mut(key) {
                    current = value;
                } else {
                    return false; // Key doesn't exist, cannot delete
                }
            }
            JsonValue::Array(arr) => {
                if let Ok(index) = key.parse::<usize>() {
                    if index < arr.len() {
                        current = &mut arr[index];
                    } else {
                        return false; // Index out of bounds
                    }
                } else {
                    return false; // Invalid array index
                }
            }
            _ => return false, // Cannot navigate further
        }
    }
    
    // Delete the value at the last key
    match current {
        JsonValue::Object(map) => {
            // For objects, remove the key-value pair
            map.remove(last_key).is_some()
        }
        JsonValue::Array(arr) => {
            // For arrays, remove the element at the specified index
            if let Ok(index) = last_key.parse::<usize>() {
                if index < arr.len() {
                    arr.remove(index);
                    true
                } else {
                    false // Index out of bounds
                }
            } else {
                false // Invalid array index
            }
        }
        _ => false, // Cannot delete from non-container types
    }
}

/// Insert value at path in JSON
/// For objects: inserts a new key-value pair
/// For arrays: inserts value at specified index (insert_after determines before/after)
/// Returns true if insertion was successful
fn insert_json_value(json: &mut JsonValue, path: &[String], new_value: JsonValue, insert_after: bool) -> bool {
    if path.is_empty() {
        return false; // Cannot insert at root
    }
    
    // Navigate to the parent container and insert at the specified location
    let (parent_path, last_key) = path.split_at(path.len() - 1);
    let last_key = &last_key[0];
    
    let mut current = json;
    for key in parent_path {
        match current {
            JsonValue::Object(map) => {
                // Check if the key exists before navigating
                if let Some(value) = map.get_mut(key) {
                    current = value;
                } else {
                    return false; // Key doesn't exist, cannot insert
                }
            }
            JsonValue::Array(arr) => {
                if let Ok(index) = key.parse::<usize>() {
                    if index < arr.len() {
                        current = &mut arr[index];
                    } else {
                        return false; // Index out of bounds
                    }
                } else {
                    return false; // Invalid array index
                }
            }
            _ => return false, // Cannot navigate further
        }
    }
    
    // Insert the value at the last key
    match current {
        JsonValue::Object(map) => {
            // For objects, insert new key-value pair (only if key doesn't exist)
            if !map.contains_key(last_key) {
                map.insert(last_key.clone(), new_value);
                true
            } else {
                false // Key already exists
            }
        }
        JsonValue::Array(arr) => {
            // For arrays, insert at the specified index
            if let Ok(index) = last_key.parse::<usize>() {
                let insert_index = if insert_after {
                    index + 1
                } else {
                    index
                };
                
                if insert_index <= arr.len() {
                    arr.insert(insert_index, new_value);
                    true
                } else {
                    false // Index out of bounds
                }
            } else {
                false // Invalid array index
            }
        }
        _ => false, // Cannot insert into non-container types
    }
}

/// Get the type of a JSON value
fn json_typeof(ctx: &rusqlite::functions::Context) -> Result<Option<String>> {
    let value: String = ctx.get(0)?;
    match serde_json::from_str::<JsonValue>(&value) {
        Ok(JsonValue::Null) => Ok(Some("null".to_string())),
        Ok(JsonValue::Bool(_)) => Ok(Some("boolean".to_string())),
        Ok(JsonValue::Number(_)) => Ok(Some("number".to_string())),
        Ok(JsonValue::String(_)) => Ok(Some("string".to_string())),
        Ok(JsonValue::Array(_)) => Ok(Some("array".to_string())),
        Ok(JsonValue::Object(_)) => Ok(Some("object".to_string())),
        Err(_) => Ok(None),
    }
}

/// Extract value from JSON using array of path parts
fn extract_json_path_by_parts(json: &JsonValue, path_parts: &[&str]) -> Option<JsonValue> {
    let mut current = json;
    
    for part in path_parts {
        let part = part.trim();
        match current {
            JsonValue::Object(map) => {
                current = map.get(part)?;
            }
            JsonValue::Array(arr) => {
                let index: usize = part.parse().ok()?;
                current = arr.get(index)?;
            }
            _ => return None,
        }
    }
    
    Some(current.clone())
}

/// Extract value from JSON using simple path notation
fn extract_json_path(json: &JsonValue, path: &str) -> Option<JsonValue> {
    // Handle root path '$'
    if path == "$" {
        return Some(json.clone());
    }
    
    // Handle paths starting with '$.'
    let path = if let Some(stripped) = path.strip_prefix("$.") {
        stripped
    } else if path.starts_with("$[") {
        &path[1..]
    } else {
        path
    };
    
    // Handle array index at root level
    if path.starts_with("[") && path.ends_with("]") {
        if let JsonValue::Array(arr) = json {
            let index_str = &path[1..path.len()-1];
            if let Ok(index) = index_str.parse::<usize>() {
                return arr.get(index).cloned();
            }
        }
        return None;
    }
    
    let parts: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();
    let mut current = json;
    
    for part in parts {
        if part.starts_with("[") && part.ends_with("]") {
            // Array index notation
            if let JsonValue::Array(arr) = current {
                let index_str = &part[1..part.len()-1];
                if let Ok(index) = index_str.parse::<usize>() {
                    current = arr.get(index)?;
                } else {
                    return None;
                }
            } else {
                return None;
            }
        } else {
            match current {
                JsonValue::Object(map) => {
                    current = map.get(part)?;
                }
                JsonValue::Array(arr) => {
                    let index: usize = part.parse().ok()?;
                    current = arr.get(index)?;
                }
                _ => return None,
            }
        }
    }
    
    Some(current.clone())
}

/// Remove null values from JSON
fn strip_nulls(json: &JsonValue) -> JsonValue {
    match json {
        JsonValue::Object(map) => {
            let mut new_map = serde_json::Map::new();
            for (key, value) in map {
                if !value.is_null() {
                    new_map.insert(key.clone(), strip_nulls(value));
                }
            }
            JsonValue::Object(new_map)
        }
        JsonValue::Array(arr) => {
            JsonValue::Array(arr.iter().map(strip_nulls).collect())
        }
        _ => json.clone(),
    }
}

/// Check if container JSON contains the contained JSON
fn json_contains(container: &JsonValue, contained: &JsonValue) -> bool {
    match (container, contained) {
        (JsonValue::Object(cont_map), JsonValue::Object(item_map)) => {
            // All keys in item must exist in container with same values
            item_map.iter().all(|(key, value)| {
                cont_map.get(key).is_some_and(|v| json_contains(v, value))
            })
        }
        (JsonValue::Array(cont_arr), JsonValue::Array(item_arr)) => {
            // All items in item_arr must be contained in cont_arr
            item_arr.iter().all(|item| {
                cont_arr.iter().any(|cont_item| json_contains(cont_item, item))
            })
        }
        (JsonValue::Array(cont_arr), item) => {
            // Check if array contains the single item
            cont_arr.iter().any(|cont_item| json_contains(cont_item, item))
        }
        _ => container == contained,
    }
}

/// Register json_populate_record function
fn register_json_populate_record(conn: &Connection) -> Result<()> {
    // json_populate_record is complex in PostgreSQL as it returns a record type
    // For pgsqlite, we'll implement a simplified version that works with table-valued functions
    // The full implementation would require significant changes to support PostgreSQL's RECORD type
    
    // For now, we implement a basic version that can extract values from JSON
    // This is a placeholder implementation - full RECORD type support would need more infrastructure
    conn.create_scalar_function(
        "json_populate_record",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            // This is a simplified implementation
            // In a full implementation, this would need to:
            // 1. Parse the base record structure
            // 2. Extract matching fields from JSON
            // 3. Return a properly formatted record
            
            let _base_record: String = ctx.get(0).unwrap_or_default();
            let json_str: String = ctx.get(1)?;
            
            // For now, just return the JSON as a validation
            // A full implementation would require significant infrastructure changes
            Ok(format!("json_populate_record: base={_base_record}, json={json_str}"))
        },
    )?;
    
    Ok(())
}

/// Register json_to_record function  
fn register_json_to_record(conn: &Connection) -> Result<()> {
    // json_to_record is complex in PostgreSQL as it returns a dynamic record type
    // For pgsqlite, we'll implement a simplified version
    
    conn.create_scalar_function(
        "json_to_record",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let json_str: String = ctx.get(0)?;
            
            // Parse JSON and validate it's an object
            match serde_json::from_str::<JsonValue>(&json_str) {
                Ok(JsonValue::Object(obj)) => {
                    // For now, return a representation of the record
                    // A full implementation would require PostgreSQL RECORD type support
                    let mut result = String::new();
                    result.push('(');
                    
                    let mut first = true;
                    for (key, value) in obj.iter() {
                        if !first {
                            result.push(',');
                        }
                        first = false;
                        
                        // Format the value appropriately
                        match value {
                            JsonValue::String(s) => result.push_str(&format!("{key}:{s}")),
                            JsonValue::Number(n) => result.push_str(&format!("{key}:{n}")),
                            JsonValue::Bool(b) => result.push_str(&format!("{key}:{b}")),
                            JsonValue::Null => result.push_str(&format!("{key}:null")),
                            _ => result.push_str(&format!("{key}:{value}")),
                        }
                    }
                    
                    result.push(')');
                    Ok(result)
                }
                Ok(_) => {
                    // Not an object
                    Ok("json_to_record: input must be a JSON object".to_string())
                }
                Err(_) => {
                    // Invalid JSON
                    Ok("json_to_record: invalid JSON".to_string())
                }
            }
        },
    )?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    
    #[test]
    fn test_json_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        // Test json_valid
        let valid: bool = conn.query_row("SELECT json_valid(?)", ["{\"key\": \"value\"}"], |row| row.get(0)).unwrap();
        assert!(valid);
        
        let invalid: bool = conn.query_row("SELECT json_valid(?)", ["{invalid}"], |row| row.get(0)).unwrap();
        assert!(!invalid);
        
        // Test json_typeof
        let typ: Option<String> = conn.query_row("SELECT json_typeof(?)", ["[1,2,3]"], |row| row.get(0)).unwrap();
        assert_eq!(typ, Some("array".to_string()));
        
        let typ: Option<String> = conn.query_row("SELECT json_typeof(?)", ["{\"a\": 1}"], |row| row.get(0)).unwrap();
        assert_eq!(typ, Some("object".to_string()));
        
        // Test json_array_length
        let len: i64 = conn.query_row("SELECT json_array_length(?)", ["[1,2,3,4,5]"], |row| row.get(0)).unwrap();
        assert_eq!(len, 5);
        
        // Test json_extract_scalar
        let value: Option<String> = conn.query_row(
            "SELECT json_extract_scalar(?, ?)", 
            ["{\"name\": \"John\", \"age\": 30}", "name"],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(value, Some("John".to_string()));
        
        // Test jsonb_contains
        let contains: bool = conn.query_row(
            "SELECT jsonb_contains(?, ?)",
            ["{\"a\": 1, \"b\": 2}", "{\"a\": 1}"],
            |row| row.get(0)
        ).unwrap();
        assert!(contains);
        
        let not_contains: bool = conn.query_row(
            "SELECT jsonb_contains(?, ?)",
            ["{\"a\": 1, \"b\": 2}", "{\"c\": 3}"],
            |row| row.get(0)
        ).unwrap();
        assert!(!not_contains);
    }
    
    #[test]
    fn test_custom_json_path_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        let test_json = r#"{"name": "John", "age": 30, "items": ["item1", "item2"], "address": {"city": "NYC", "zip": "10001"}}"#;
        
        // Test pgsqlite_json_get_text (string key)
        let name: Option<String> = conn.query_row(
            "SELECT pgsqlite_json_get_text(?, ?)",
            [test_json, "name"],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(name, Some("John".to_string()));
        
        // Test pgsqlite_json_get_json (string key)
        let address: Option<String> = conn.query_row(
            "SELECT pgsqlite_json_get_json(?, ?)",
            [test_json, "address"],
            |row| row.get(0)
        ).unwrap();
        assert!(address.is_some());
        assert!(address.unwrap().contains("NYC"));
        
        // Test pgsqlite_json_get_array_text (array index)
        let first_item: Option<String> = conn.query_row(
            "SELECT pgsqlite_json_get_array_text(?, ?)",
            (r#"["item1", "item2", "item3"]"#, 0i64),
            |row| row.get(0)
        ).unwrap();
        assert_eq!(first_item, Some("item1".to_string()));
        
        // Test pgsqlite_json_get_array_json (array index)
        let second_item: Option<String> = conn.query_row(
            "SELECT pgsqlite_json_get_array_json(?, ?)",
            (r#"["item1", {"nested": "value"}, "item3"]"#, 1i64),
            |row| row.get(0)
        ).unwrap();
        assert!(second_item.is_some());
        assert!(second_item.unwrap().contains("nested"));
        
        // Test pgsqlite_json_path_text (path navigation)
        let city: Option<String> = conn.query_row(
            "SELECT pgsqlite_json_path_text(?, ?)",
            [test_json, "address,city"],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(city, Some("NYC".to_string()));
        
        // Test pgsqlite_json_path_json (path navigation)
        let address_json: Option<String> = conn.query_row(
            "SELECT pgsqlite_json_path_json(?, ?)",
            [test_json, "address"],
            |row| row.get(0)
        ).unwrap();
        assert!(address_json.is_some());
        assert!(address_json.unwrap().contains("NYC"));
        
        // Test array access via path
        let nested_json = r#"{"items": [{"name": "first"}, {"name": "second"}]}"#;
        let item_name: Option<String> = conn.query_row(
            "SELECT pgsqlite_json_path_text(?, ?)",
            [nested_json, "items,0,name"],
            |row| row.get(0)
        ).unwrap();
        assert_eq!(item_name, Some("first".to_string()));
    }
    
    #[test]
    fn test_json_existence_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        let test_json = r#"{"name": "John", "age": 30, "address": {"city": "NYC"}, "tags": ["work", "friend"]}"#;
        
        // Test pgsqlite_json_has_key (? operator)
        let has_name: bool = conn.query_row(
            "SELECT pgsqlite_json_has_key(?, ?)",
            [test_json, "name"],
            |row| row.get(0)
        ).unwrap();
        assert!(has_name);
        
        let has_missing: bool = conn.query_row(
            "SELECT pgsqlite_json_has_key(?, ?)",
            [test_json, "missing"],
            |row| row.get(0)
        ).unwrap();
        assert!(!has_missing);
        
        // Test pgsqlite_json_has_any_key (?| operator)
        let has_any: bool = conn.query_row(
            "SELECT pgsqlite_json_has_any_key(?, ?)",
            [test_json, "email,name,phone"],
            |row| row.get(0)
        ).unwrap();
        assert!(has_any); // has 'name'
        
        let has_none: bool = conn.query_row(
            "SELECT pgsqlite_json_has_any_key(?, ?)",
            [test_json, "email,phone,country"],
            |row| row.get(0)
        ).unwrap();
        assert!(!has_none);
        
        // Test pgsqlite_json_has_all_keys (?& operator)
        let has_all: bool = conn.query_row(
            "SELECT pgsqlite_json_has_all_keys(?, ?)",
            [test_json, "name,age"],
            |row| row.get(0)
        ).unwrap();
        assert!(has_all);
        
        let missing_one: bool = conn.query_row(
            "SELECT pgsqlite_json_has_all_keys(?, ?)",
            [test_json, "name,age,email"],
            |row| row.get(0)
        ).unwrap();
        assert!(!missing_one); // missing 'email'
        
        // Test with non-object JSON (should return false)
        let array_json = r#"["item1", "item2"]"#;
        let not_object: bool = conn.query_row(
            "SELECT pgsqlite_json_has_key(?, ?)",
            [array_json, "name"],
            |row| row.get(0)
        ).unwrap();
        assert!(!not_object);
    }
    
    #[test]
    fn test_json_agg_functions() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        // Create test data
        conn.execute_batch(r#"
            CREATE TABLE test_agg (id INTEGER, name TEXT, score INTEGER);
            INSERT INTO test_agg VALUES (1, 'Alice', 95);
            INSERT INTO test_agg VALUES (2, 'Bob', 87);
            INSERT INTO test_agg VALUES (3, 'Charlie', 92);
        "#).unwrap();
        
        // Test json_agg with simple values
        let result: String = conn.query_row(
            "SELECT json_agg(name) FROM test_agg ORDER BY id",
            [],
            |row| row.get(0)
        ).unwrap();
        
        let parsed: JsonValue = serde_json::from_str(&result).unwrap();
        match parsed {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], JsonValue::String("Alice".to_string()));
                assert_eq!(arr[1], JsonValue::String("Bob".to_string()));
                assert_eq!(arr[2], JsonValue::String("Charlie".to_string()));
            }
            _ => panic!("Expected JSON array"),
        }
        
        // Test json_agg with numbers
        let result: String = conn.query_row(
            "SELECT json_agg(score) FROM test_agg ORDER BY id",
            [],
            |row| row.get(0)
        ).unwrap();
        
        let parsed: JsonValue = serde_json::from_str(&result).unwrap();
        match parsed {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], JsonValue::Number(serde_json::Number::from(95)));
                assert_eq!(arr[1], JsonValue::Number(serde_json::Number::from(87)));
                assert_eq!(arr[2], JsonValue::Number(serde_json::Number::from(92)));
            }
            _ => panic!("Expected JSON array"),
        }
        
        // Test jsonb_agg (should behave identically)
        let result: String = conn.query_row(
            "SELECT jsonb_agg(name) FROM test_agg ORDER BY id",
            [],
            |row| row.get(0)
        ).unwrap();
        
        let parsed: JsonValue = serde_json::from_str(&result).unwrap();
        match parsed {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], JsonValue::String("Alice".to_string()));
            }
            _ => panic!("Expected JSON array"),
        }
        
        // Test with NULL values
        conn.execute("INSERT INTO test_agg VALUES (4, NULL, 88)", []).unwrap();
        
        let result: String = conn.query_row(
            "SELECT json_agg(name) FROM test_agg WHERE id >= 4",
            [],
            |row| row.get(0)
        ).unwrap();
        
        let parsed: JsonValue = serde_json::from_str(&result).unwrap();
        match parsed {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 1);
                assert_eq!(arr[0], JsonValue::Null);
            }
            _ => panic!("Expected JSON array"),
        }
        
        // Test empty result
        let result: Option<String> = conn.query_row(
            "SELECT json_agg(name) FROM test_agg WHERE id > 100",
            [],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some("[]".to_string()));
    }
    
    #[test]
    fn test_jsonb_insert_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        // Test inserting into object
        let test_json = r#"{"name": "John", "age": 30}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_insert(?, ?, ?)",
            [test_json, "{email}", "\"john@example.com\""],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Object(map) => {
                assert_eq!(map.get("name"), Some(&JsonValue::String("John".to_string())));
                assert_eq!(map.get("age"), Some(&JsonValue::Number(serde_json::Number::from(30))));
                assert_eq!(map.get("email"), Some(&JsonValue::String("john@example.com".to_string())));
            }
            _ => panic!("Expected JSON object"),
        }
        
        // Test inserting into nested object
        let nested_json = r#"{"user": {"name": "Alice"}, "active": true}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_insert(?, ?, ?)",
            [nested_json, "{user,email}", "\"alice@example.com\""],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Object(map) => {
                if let Some(JsonValue::Object(user_map)) = map.get("user") {
                    assert_eq!(user_map.get("name"), Some(&JsonValue::String("Alice".to_string())));
                    assert_eq!(user_map.get("email"), Some(&JsonValue::String("alice@example.com".to_string())));
                } else {
                    panic!("Expected nested user object");
                }
            }
            _ => panic!("Expected JSON object"),
        }
        
        // Test inserting into array (before index)
        let array_json = r#"["apple", "banana", "cherry"]"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_insert(?, ?, ?)",
            [array_json, "{1}", "\"orange\""],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[0], JsonValue::String("apple".to_string()));
                assert_eq!(arr[1], JsonValue::String("orange".to_string()));
                assert_eq!(arr[2], JsonValue::String("banana".to_string()));
                assert_eq!(arr[3], JsonValue::String("cherry".to_string()));
            }
            _ => panic!("Expected JSON array"),
        }
        
        // Test inserting into array (after index) using 4-arg version
        let array_json = r#"["apple", "banana", "cherry"]"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_insert(?, ?, ?, ?)",
            (array_json, "{1}", "\"orange\"", true),
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 4);
                assert_eq!(arr[0], JsonValue::String("apple".to_string()));
                assert_eq!(arr[1], JsonValue::String("banana".to_string()));
                assert_eq!(arr[2], JsonValue::String("orange".to_string()));
                assert_eq!(arr[3], JsonValue::String("cherry".to_string()));
            }
            _ => panic!("Expected JSON array"),
        }
        
        // Test inserting with key that already exists (should fail)
        let test_json = r#"{"name": "John", "age": 30}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_insert(?, ?, ?)",
            [test_json, "{name}", "\"Jane\""],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Object(map) => {
                // Should still be "John" since key already exists
                assert_eq!(map.get("name"), Some(&JsonValue::String("John".to_string())));
                assert_eq!(map.len(), 2); // No new key added
            }
            _ => panic!("Expected JSON object"),
        }
        
        // Test inserting at array end
        let array_json = r#"["apple", "banana"]"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_insert(?, ?, ?)",
            [array_json, "{2}", "\"cherry\""],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], JsonValue::String("apple".to_string()));
                assert_eq!(arr[1], JsonValue::String("banana".to_string()));
                assert_eq!(arr[2], JsonValue::String("cherry".to_string()));
            }
            _ => panic!("Expected JSON array"),
        }
        
        // Test inserting with invalid path (should return original)
        let test_json = r#"{"name": "John"}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_insert(?, ?, ?)",
            [test_json, "{invalid,path,structure}", "\"value\""],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some(test_json.to_string()));
    }
    
    #[test]
    fn test_jsonb_delete_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        // Test deleting from object
        let test_json = r#"{"name": "John", "age": 30, "email": "john@example.com"}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_delete(?, ?)",
            [test_json, "{email}"],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Object(map) => {
                assert_eq!(map.get("name"), Some(&JsonValue::String("John".to_string())));
                assert_eq!(map.get("age"), Some(&JsonValue::Number(serde_json::Number::from(30))));
                assert_eq!(map.get("email"), None); // Should be deleted
                assert_eq!(map.len(), 2); // Only 2 keys remaining
            }
            _ => panic!("Expected JSON object"),
        }
        
        // Test deleting from nested object
        let nested_json = r#"{"user": {"name": "Alice", "email": "alice@example.com"}, "active": true}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_delete(?, ?)",
            [nested_json, "{user,email}"],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Object(map) => {
                if let Some(JsonValue::Object(user_map)) = map.get("user") {
                    assert_eq!(user_map.get("name"), Some(&JsonValue::String("Alice".to_string())));
                    assert_eq!(user_map.get("email"), None); // Should be deleted
                    assert_eq!(user_map.len(), 1); // Only name remaining
                } else {
                    panic!("Expected nested user object");
                }
            }
            _ => panic!("Expected JSON object"),
        }
        
        // Test deleting from array
        let array_json = r#"["apple", "banana", "cherry", "date"]"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_delete(?, ?)",
            [array_json, "{1}"],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Array(arr) => {
                assert_eq!(arr.len(), 3);
                assert_eq!(arr[0], JsonValue::String("apple".to_string()));
                assert_eq!(arr[1], JsonValue::String("cherry".to_string())); // banana was deleted
                assert_eq!(arr[2], JsonValue::String("date".to_string()));
            }
            _ => panic!("Expected JSON array"),
        }
        
        // Test deleting non-existent key (should return original)
        let test_json = r#"{"name": "John", "age": 30}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_delete(?, ?)",
            [test_json, "{email}"],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some(test_json.to_string()));
        
        // Test deleting with invalid path (should return original)
        let test_json = r#"{"name": "John"}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_delete(?, ?)",
            [test_json, "{invalid,path,structure}"],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some(test_json.to_string()));
        
        // Test deleting array element out of bounds (should return original)
        let array_json = r#"["apple", "banana"]"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_delete(?, ?)",
            [array_json, "{5}"],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some(array_json.to_string()));
        
        // Test jsonb_delete_path (should behave identically to jsonb_delete)
        let test_json = r#"{"name": "John", "age": 30, "email": "john@example.com"}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_delete_path(?, ?)",
            [test_json, "{age}"],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let parsed: JsonValue = serde_json::from_str(&result.unwrap()).unwrap();
        match parsed {
            JsonValue::Object(map) => {
                assert_eq!(map.get("name"), Some(&JsonValue::String("John".to_string())));
                assert_eq!(map.get("age"), None); // Should be deleted
                assert_eq!(map.get("email"), Some(&JsonValue::String("john@example.com".to_string())));
                assert_eq!(map.len(), 2); // Only 2 keys remaining
            }
            _ => panic!("Expected JSON object"),
        }
    }
    
    #[test]
    fn test_jsonb_pretty_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        // Test pretty-printing a simple object
        let test_json = r#"{"name":"John","age":30,"active":true}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [test_json],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let pretty = result.unwrap();
        assert!(pretty.contains("{\n"));
        assert!(pretty.contains("  \"name\": \"John\""));
        assert!(pretty.contains("  \"age\": 30"));
        assert!(pretty.contains("  \"active\": true"));
        assert!(pretty.contains("\n}"));
        
        // Test pretty-printing nested objects
        let nested_json = r#"{"user":{"name":"Alice","email":"alice@example.com"},"items":[1,2,3]}"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [nested_json],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let pretty = result.unwrap();
        assert!(pretty.contains("  \"user\": {"));
        assert!(pretty.contains("    \"name\": \"Alice\""));
        assert!(pretty.contains("    \"email\": \"alice@example.com\""));
        assert!(pretty.contains("  \"items\": ["));
        assert!(pretty.contains("    1,"));
        assert!(pretty.contains("    2,"));
        assert!(pretty.contains("    3"));
        
        // Test pretty-printing array
        let array_json = r#"[{"id":1,"name":"Item 1"},{"id":2,"name":"Item 2"}]"#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [array_json],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let pretty = result.unwrap();
        assert!(pretty.contains("[\n"));
        assert!(pretty.contains("  {\n"));
        assert!(pretty.contains("    \"id\": 1,"));
        assert!(pretty.contains("    \"name\": \"Item 1\""));
        assert!(pretty.contains("  },"));
        assert!(pretty.contains("    \"id\": 2,"));
        assert!(pretty.contains("    \"name\": \"Item 2\""));
        assert!(pretty.contains("\n]"));
        
        // Test with simple values
        let simple_json = r#""hello world""#;
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [simple_json],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some("\"hello world\"".to_string()));
        
        // Test with number
        let number_json = "42";
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [number_json],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some("42".to_string()));
        
        // Test with null
        let null_json = "null";
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [null_json],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some("null".to_string()));
        
        // Test with invalid JSON (should return original)
        let invalid_json = "{not valid json}";
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [invalid_json],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some(invalid_json.to_string()));
        
        // Test with empty object
        let empty_obj = "{}";
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [empty_obj],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some("{}".to_string()));
        
        // Test with empty array
        let empty_arr = "[]";
        let result: Option<String> = conn.query_row(
            "SELECT jsonb_pretty(?)",
            [empty_arr],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some("[]".to_string()));
    }
    
    #[test]
    fn test_json_populate_record_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        // Test basic json_populate_record functionality
        let base_record = "null";
        let json_data = r#"{"name": "John", "age": 30}"#;
        let result: Option<String> = conn.query_row(
            "SELECT json_populate_record(?, ?)",
            [base_record, json_data],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let result_str = result.unwrap();
        assert!(result_str.contains("json_populate_record"));
        assert!(result_str.contains(json_data));
        
        // Test with empty base record
        let empty_base = "";
        let result: Option<String> = conn.query_row(
            "SELECT json_populate_record(?, ?)",
            [empty_base, json_data],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        
        // Test with invalid JSON
        let invalid_json = "{not valid json}";
        let result: Option<String> = conn.query_row(
            "SELECT json_populate_record(?, ?)",
            [base_record, invalid_json],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
    }
    
    #[test]
    fn test_json_to_record_function() {
        let conn = Connection::open_in_memory().unwrap();
        register_json_functions(&conn).unwrap();
        
        // Test with simple JSON object
        let json_data = r#"{"name": "Alice", "age": 25, "active": true}"#;
        let result: Option<String> = conn.query_row(
            "SELECT json_to_record(?)",
            [json_data],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let result_str = result.unwrap();
        assert!(result_str.starts_with('('));
        assert!(result_str.ends_with(')'));
        assert!(result_str.contains("name:Alice"));
        assert!(result_str.contains("age:25"));
        assert!(result_str.contains("active:true"));
        
        // Test with object containing different data types
        let complex_json = r#"{"id": 123, "title": "Test", "enabled": false, "data": null}"#;
        let result: Option<String> = conn.query_row(
            "SELECT json_to_record(?)",
            [complex_json],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let result_str = result.unwrap();
        assert!(result_str.contains("id:123"));
        assert!(result_str.contains("title:Test"));
        assert!(result_str.contains("enabled:false"));
        assert!(result_str.contains("data:null"));
        
        // Test with empty object
        let empty_obj = "{}";
        let result: Option<String> = conn.query_row(
            "SELECT json_to_record(?)",
            [empty_obj],
            |row| row.get(0)
        ).unwrap();
        
        assert_eq!(result, Some("()".to_string()));
        
        // Test with array (should return error message)
        let array_json = r#"[{"name": "test"}]"#;
        let result: Option<String> = conn.query_row(
            "SELECT json_to_record(?)",
            [array_json],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let result_str = result.unwrap();
        assert!(result_str.contains("input must be a JSON object"));
        
        // Test with invalid JSON
        let invalid_json = "{not valid json}";
        let result: Option<String> = conn.query_row(
            "SELECT json_to_record(?)",
            [invalid_json],
            |row| row.get(0)
        ).unwrap();
        
        assert!(result.is_some());
        let result_str = result.unwrap();
        assert!(result_str.contains("invalid JSON"));
    }
}