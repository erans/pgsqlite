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
                        let hex = b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>();
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
                        let hex = b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>();
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
                    let hex = b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>();
                    serde_json::to_string(&hex).unwrap()
                },
            };
            
            Ok(format!("{{\"{}\": {}}}", key, value))
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
    
    Ok(())
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
            if let Ok(index) = last_key.parse::<usize>() {
                if index < arr.len() {
                    arr[index] = new_value;
                }
            }
        }
        _ => {},
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
    let path = if path.starts_with("$.") {
        &path[2..]
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
                cont_map.get(key).map_or(false, |v| json_contains(v, value))
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
}