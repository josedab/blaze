//! JSON processing functions.
//!
//! This module provides JSON parsing, extraction, and construction functions
//! for SQL query processing:
//! - JSON_EXTRACT: Extract a value from JSON using a JSON path
//! - JSON_VALUE: Extract a scalar value from JSON as a native type
//! - JSON_OBJECT: Construct a JSON object from key-value pairs
//! - JSON_ARRAY: Construct a JSON array from values
//! - JSON_VALID: Check if a string is valid JSON

use serde_json::{Map, Value};

use crate::error::{BlazeError, Result};

/// Extract a value from a JSON string using a JSON path expression.
///
/// Supports basic JSON path syntax:
/// - `$` - root element
/// - `.key` - object member access
/// - `[n]` - array element access by index
///
/// # Examples
///
/// ```ignore
/// json_extract('{"a": {"b": 1}}', '$.a.b') => 1
/// json_extract('[1, 2, 3]', '$[1]') => 2
/// ```
pub fn json_extract(json_str: &str, path: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let result = extract_path(&value, path)?;

    match result {
        Some(v) => Ok(Some(v.to_string())),
        None => Ok(None),
    }
}

/// Extract a scalar value from JSON as a string.
///
/// Similar to json_extract but returns the unwrapped scalar value
/// (without JSON quotes for strings).
pub fn json_value(json_str: &str, path: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let result = extract_path(&value, path)?;

    match result {
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(Value::Number(n)) => Ok(Some(n.to_string())),
        Some(Value::Bool(b)) => Ok(Some(b.to_string())),
        Some(Value::Null) => Ok(None),
        Some(v) => Ok(Some(v.to_string())),
        None => Ok(None),
    }
}

/// Extract an integer value from JSON.
pub fn json_extract_int(json_str: &str, path: &str) -> Result<Option<i64>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let result = extract_path(&value, path)?;

    match result {
        Some(Value::Number(n)) => Ok(n.as_i64()),
        Some(Value::String(s)) => Ok(s.parse::<i64>().ok()),
        _ => Ok(None),
    }
}

/// Extract a float value from JSON.
pub fn json_extract_float(json_str: &str, path: &str) -> Result<Option<f64>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let result = extract_path(&value, path)?;

    match result {
        Some(Value::Number(n)) => Ok(n.as_f64()),
        Some(Value::String(s)) => Ok(s.parse::<f64>().ok()),
        _ => Ok(None),
    }
}

/// Extract a boolean value from JSON.
pub fn json_extract_bool(json_str: &str, path: &str) -> Result<Option<bool>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let result = extract_path(&value, path)?;

    match result {
        Some(Value::Bool(b)) => Ok(Some(*b)),
        Some(Value::String(s)) => match s.to_lowercase().as_str() {
            "true" | "1" | "yes" => Ok(Some(true)),
            "false" | "0" | "no" => Ok(Some(false)),
            _ => Ok(None),
        },
        _ => Ok(None),
    }
}

/// Construct a JSON object from key-value pairs.
///
/// Takes alternating keys and values and creates a JSON object.
pub fn json_object(pairs: &[(String, Value)]) -> Result<String> {
    let mut map = Map::new();
    for (key, value) in pairs {
        map.insert(key.clone(), value.clone());
    }
    Ok(Value::Object(map).to_string())
}

/// Construct a JSON object from string key-value pairs.
pub fn json_object_from_strings(pairs: &[(String, String)]) -> Result<String> {
    let mut map = Map::new();
    for (key, value) in pairs {
        // Try to parse value as JSON, otherwise treat as string
        let json_value = serde_json::from_str(value).unwrap_or(Value::String(value.clone()));
        map.insert(key.clone(), json_value);
    }
    Ok(Value::Object(map).to_string())
}

/// Construct a JSON array from values.
pub fn json_array(values: &[Value]) -> Result<String> {
    let arr = Value::Array(values.to_vec());
    Ok(arr.to_string())
}

/// Construct a JSON array from string values.
pub fn json_array_from_strings(values: &[String]) -> Result<String> {
    let arr: Vec<Value> = values
        .iter()
        .map(|s| serde_json::from_str(s).unwrap_or(Value::String(s.clone())))
        .collect();
    Ok(Value::Array(arr).to_string())
}

/// Check if a string is valid JSON.
pub fn json_valid(json_str: &str) -> bool {
    serde_json::from_str::<Value>(json_str).is_ok()
}

/// Get the type of a JSON value.
///
/// Returns one of: "object", "array", "string", "number", "boolean", "null"
pub fn json_type(json_str: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let type_str = match &value {
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Null => "null",
    };

    Ok(Some(type_str.to_string()))
}

/// Get the type of a JSON value at a path.
pub fn json_type_at_path(json_str: &str, path: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let result = extract_path(&value, path)?;

    match result {
        Some(v) => {
            let type_str = match &v {
                Value::Object(_) => "object",
                Value::Array(_) => "array",
                Value::String(_) => "string",
                Value::Number(_) => "number",
                Value::Bool(_) => "boolean",
                Value::Null => "null",
            };
            Ok(Some(type_str.to_string()))
        }
        None => Ok(None),
    }
}

/// Get the length of a JSON array or object.
///
/// For arrays, returns the number of elements.
/// For objects, returns the number of keys.
/// For other types, returns None.
pub fn json_length(json_str: &str) -> Result<Option<i64>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    match &value {
        Value::Array(arr) => Ok(Some(arr.len() as i64)),
        Value::Object(obj) => Ok(Some(obj.len() as i64)),
        _ => Ok(None),
    }
}

/// Get the keys of a JSON object as a JSON array.
pub fn json_keys(json_str: &str) -> Result<Option<String>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    match &value {
        Value::Object(obj) => {
            let keys: Vec<Value> = obj.keys().map(|k| Value::String(k.clone())).collect();
            Ok(Some(Value::Array(keys).to_string()))
        }
        _ => Ok(None),
    }
}

/// Check if a JSON object contains a key.
pub fn json_contains_key(json_str: &str, key: &str) -> Result<bool> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    match &value {
        Value::Object(obj) => Ok(obj.contains_key(key)),
        _ => Ok(false),
    }
}

/// Set a value at a JSON path, creating intermediate objects/arrays as needed.
pub fn json_set(json_str: &str, path: &str, new_value: &str) -> Result<String> {
    let mut root: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;
    let set_value: Value =
        serde_json::from_str(new_value).unwrap_or(Value::String(new_value.to_string()));

    set_at_path(&mut root, path, set_value)?;
    Ok(root.to_string())
}

/// Remove a value at a JSON path.
pub fn json_remove(json_str: &str, path: &str) -> Result<String> {
    let mut root: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    remove_at_path(&mut root, path)?;
    Ok(root.to_string())
}

/// Flatten nested JSON into tabular rows (JSON_TABLE equivalent).
///
/// Given a JSON array (or array at a path), produces rows of key-value pairs
/// for each element. Nested objects are flattened with dot-notation keys.
///
/// # Example
/// ```ignore
/// json_table('[{"a":1,"b":{"c":2}},{"a":3,"b":{"c":4}}]', '$')
/// // Returns:
/// // [{"a": "1", "b.c": "2"}, {"a": "3", "b.c": "4"}]
/// ```
pub fn json_table(json_str: &str, path: &str) -> Result<Vec<Vec<(String, String)>>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let target =
        extract_path(&value, path)?.ok_or_else(|| BlazeError::execution("JSON path not found"))?;

    let array = match target {
        Value::Array(arr) => arr,
        other => {
            // Wrap single object in array
            return Ok(vec![flatten_value(other, "")]);
        }
    };

    Ok(array.iter().map(|v| flatten_value(v, "")).collect())
}

/// Extract all matching paths from a JSON document (for path indexing).
pub fn json_extract_all_paths(json_str: &str) -> Result<Vec<(String, String)>> {
    let value: Value = serde_json::from_str(json_str)
        .map_err(|e| BlazeError::execution(format!("Invalid JSON: {}", e)))?;

    let mut paths = Vec::new();
    collect_paths(&value, "$", &mut paths);
    Ok(paths)
}

/// JSON path index for fast lookups across multiple JSON documents.
#[derive(Debug, Clone, Default)]
pub struct JsonPathIndex {
    /// Maps (path -> row_index -> value)
    index: std::collections::HashMap<String, Vec<(usize, String)>>,
}

impl JsonPathIndex {
    pub fn new() -> Self {
        Self {
            index: std::collections::HashMap::new(),
        }
    }

    /// Index a JSON document at the given row index.
    pub fn add_document(&mut self, row_idx: usize, json_str: &str) -> Result<()> {
        let paths = json_extract_all_paths(json_str)?;
        for (path, value) in paths {
            self.index.entry(path).or_default().push((row_idx, value));
        }
        Ok(())
    }

    /// Look up all row indices and values for a given path.
    pub fn lookup(&self, path: &str) -> Option<&[(usize, String)]> {
        self.index.get(path).map(|v| v.as_slice())
    }

    /// Get all indexed paths.
    pub fn paths(&self) -> Vec<&String> {
        self.index.keys().collect()
    }

    /// Number of indexed paths.
    pub fn num_paths(&self) -> usize {
        self.index.len()
    }
}

/// Flatten a JSON value into key-value pairs with dot-notation.
fn flatten_value(value: &Value, prefix: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let full_key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{}.{}", prefix, key)
                };
                match val {
                    Value::Object(_) => {
                        result.extend(flatten_value(val, &full_key));
                    }
                    Value::Array(arr) => {
                        for (i, item) in arr.iter().enumerate() {
                            let arr_key = format!("{}[{}]", full_key, i);
                            if matches!(item, Value::Object(_) | Value::Array(_)) {
                                result.extend(flatten_value(item, &arr_key));
                            } else {
                                result.push((arr_key, value_to_string(item)));
                            }
                        }
                    }
                    _ => {
                        result.push((full_key, value_to_string(val)));
                    }
                }
            }
        }
        _ => {
            let key = if prefix.is_empty() {
                "$".to_string()
            } else {
                prefix.to_string()
            };
            result.push((key, value_to_string(value)));
        }
    }
    result
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Collect all paths from a JSON value.
fn collect_paths(value: &Value, prefix: &str, paths: &mut Vec<(String, String)>) {
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let full_path = format!("{}.{}", prefix, key);
                match val {
                    Value::Object(_) | Value::Array(_) => {
                        collect_paths(val, &full_path, paths);
                    }
                    _ => {
                        paths.push((full_path, value_to_string(val)));
                    }
                }
            }
        }
        Value::Array(arr) => {
            for (i, val) in arr.iter().enumerate() {
                let full_path = format!("{}[{}]", prefix, i);
                match val {
                    Value::Object(_) | Value::Array(_) => {
                        collect_paths(val, &full_path, paths);
                    }
                    _ => {
                        paths.push((full_path, value_to_string(val)));
                    }
                }
            }
        }
        _ => {
            paths.push((prefix.to_string(), value_to_string(value)));
        }
    }
}

/// Set a value at a JSON path (mutable).
fn set_at_path(root: &mut Value, path: &str, new_value: Value) -> Result<()> {
    let path = path.trim();
    if path == "$" {
        *root = new_value;
        return Ok(());
    }
    if !path.starts_with('$') {
        return Err(BlazeError::invalid_argument(
            "JSON path must start with '$'",
        ));
    }

    let segments = parse_path_segments(&path[1..])?;
    if segments.is_empty() {
        *root = new_value;
        return Ok(());
    }

    let mut current = root;
    for (i, segment) in segments.iter().enumerate() {
        let is_last = i == segments.len() - 1;
        match segment {
            PathSegment::Key(key) => {
                if is_last {
                    if let Value::Object(obj) = current {
                        obj.insert(key.clone(), new_value);
                        return Ok(());
                    }
                    return Err(BlazeError::execution("Cannot set key on non-object"));
                }
                if let Value::Object(obj) = current {
                    current = obj.entry(key.clone()).or_insert(Value::Object(Map::new()));
                } else {
                    return Err(BlazeError::execution("Path traversal: expected object"));
                }
            }
            PathSegment::Index(idx) => {
                if is_last {
                    if let Value::Array(arr) = current {
                        if *idx < arr.len() {
                            arr[*idx] = new_value;
                        } else {
                            arr.push(new_value);
                        }
                        return Ok(());
                    }
                    return Err(BlazeError::execution("Cannot set index on non-array"));
                }
                if let Value::Array(arr) = current {
                    if *idx < arr.len() {
                        current = &mut arr[*idx];
                    } else {
                        return Err(BlazeError::execution("Array index out of bounds"));
                    }
                } else {
                    return Err(BlazeError::execution("Path traversal: expected array"));
                }
            }
        }
    }
    Ok(())
}

/// Remove a value at a JSON path.
fn remove_at_path(root: &mut Value, path: &str) -> Result<()> {
    let path = path.trim();
    if !path.starts_with('$') {
        return Err(BlazeError::invalid_argument(
            "JSON path must start with '$'",
        ));
    }

    let segments = parse_path_segments(&path[1..])?;
    if segments.is_empty() {
        return Err(BlazeError::invalid_argument("Cannot remove root"));
    }

    let mut current = root;
    for (i, segment) in segments.iter().enumerate() {
        let is_last = i == segments.len() - 1;
        match segment {
            PathSegment::Key(key) => {
                if is_last {
                    if let Value::Object(obj) = current {
                        obj.remove(key);
                        return Ok(());
                    }
                    return Ok(());
                }
                if let Value::Object(obj) = current {
                    if let Some(val) = obj.get_mut(key) {
                        current = val;
                    } else {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
            }
            PathSegment::Index(idx) => {
                if is_last {
                    if let Value::Array(arr) = current {
                        if *idx < arr.len() {
                            arr.remove(*idx);
                        }
                        return Ok(());
                    }
                    return Ok(());
                }
                if let Value::Array(arr) = current {
                    if *idx < arr.len() {
                        current = &mut arr[*idx];
                    } else {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

enum PathSegment {
    Key(String),
    Index(usize),
}

fn parse_path_segments(path: &str) -> Result<Vec<PathSegment>> {
    let mut segments = Vec::new();
    let mut chars = path.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '.' => {
                let mut key = String::new();
                while let Some(&next) = chars.peek() {
                    if next == '.' || next == '[' {
                        break;
                    }
                    key.push(chars.next().unwrap());
                }
                if !key.is_empty() {
                    segments.push(PathSegment::Key(key));
                }
            }
            '[' => {
                let mut idx_str = String::new();
                for next in chars.by_ref() {
                    if next == ']' {
                        break;
                    }
                    idx_str.push(next);
                }
                let idx: usize = idx_str
                    .trim()
                    .parse()
                    .map_err(|_| BlazeError::invalid_argument("Invalid array index"))?;
                segments.push(PathSegment::Index(idx));
            }
            ' ' | '\t' => continue,
            _ => {
                return Err(BlazeError::invalid_argument(format!(
                    "Unexpected character '{}' in path",
                    c
                )));
            }
        }
    }
    Ok(segments)
}

/// Parse a JSON path and extract the value.
fn extract_path<'a>(value: &'a Value, path: &str) -> Result<Option<&'a Value>> {
    let path = path.trim();

    // Handle empty path or just "$"
    if path.is_empty() || path == "$" {
        return Ok(Some(value));
    }

    // Path must start with $
    if !path.starts_with('$') {
        return Err(BlazeError::invalid_argument(
            "JSON path must start with '$'",
        ));
    }

    let path = &path[1..]; // Remove the leading $
    let mut current = value;

    let mut chars = path.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '.' => {
                // Object member access
                let mut key = String::new();
                while let Some(&next_c) = chars.peek() {
                    if next_c == '.' || next_c == '[' {
                        break;
                    }
                    key.push(chars.next().unwrap());
                }

                if key.is_empty() {
                    return Err(BlazeError::invalid_argument("Empty key in JSON path"));
                }

                match current {
                    Value::Object(obj) => {
                        current = match obj.get(&key) {
                            Some(v) => v,
                            None => return Ok(None),
                        };
                    }
                    _ => return Ok(None),
                }
            }
            '[' => {
                // Array index or quoted key
                let mut index_str = String::new();
                let mut found_close = false;

                for next_c in chars.by_ref() {
                    if next_c == ']' {
                        found_close = true;
                        break;
                    }
                    index_str.push(next_c);
                }

                if !found_close {
                    return Err(BlazeError::invalid_argument(
                        "Unclosed bracket in JSON path",
                    ));
                }

                // Check if it's a quoted string (object key) or numeric index
                let index_str = index_str.trim();
                if (index_str.starts_with('"') && index_str.ends_with('"'))
                    || (index_str.starts_with('\'') && index_str.ends_with('\''))
                {
                    // Quoted key for object access
                    let key = &index_str[1..index_str.len() - 1];
                    match current {
                        Value::Object(obj) => {
                            current = match obj.get(key) {
                                Some(v) => v,
                                None => return Ok(None),
                            };
                        }
                        _ => return Ok(None),
                    }
                } else {
                    // Numeric index for array access
                    let index: usize = index_str.parse().map_err(|_| {
                        BlazeError::invalid_argument("Invalid array index in JSON path")
                    })?;

                    match current {
                        Value::Array(arr) => {
                            current = match arr.get(index) {
                                Some(v) => v,
                                None => return Ok(None),
                            };
                        }
                        _ => return Ok(None),
                    }
                }
            }
            ' ' | '\t' | '\n' | '\r' => {
                // Skip whitespace
                continue;
            }
            _ => {
                return Err(BlazeError::invalid_argument(format!(
                    "Unexpected character '{}' in JSON path",
                    c
                )));
            }
        }
    }

    Ok(Some(current))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_extract_simple() {
        let json = r#"{"name": "John", "age": 30}"#;

        let result = json_extract(json, "$.name").unwrap();
        assert_eq!(result, Some("\"John\"".to_string()));

        let result = json_extract(json, "$.age").unwrap();
        assert_eq!(result, Some("30".to_string()));
    }

    #[test]
    fn test_json_extract_nested() {
        let json = r#"{"person": {"name": "John", "address": {"city": "NYC"}}}"#;

        let result = json_extract(json, "$.person.name").unwrap();
        assert_eq!(result, Some("\"John\"".to_string()));

        let result = json_extract(json, "$.person.address.city").unwrap();
        assert_eq!(result, Some("\"NYC\"".to_string()));
    }

    #[test]
    fn test_json_extract_array() {
        let json = r#"{"items": [1, 2, 3, 4, 5]}"#;

        let result = json_extract(json, "$.items[0]").unwrap();
        assert_eq!(result, Some("1".to_string()));

        let result = json_extract(json, "$.items[2]").unwrap();
        assert_eq!(result, Some("3".to_string()));
    }

    #[test]
    fn test_json_value() {
        let json = r#"{"name": "John", "age": 30, "active": true}"#;

        let result = json_value(json, "$.name").unwrap();
        assert_eq!(result, Some("John".to_string()));

        let result = json_value(json, "$.age").unwrap();
        assert_eq!(result, Some("30".to_string()));

        let result = json_value(json, "$.active").unwrap();
        assert_eq!(result, Some("true".to_string()));
    }

    #[test]
    fn test_json_extract_int() {
        let json = r#"{"count": 42, "str_num": "123"}"#;

        let result = json_extract_int(json, "$.count").unwrap();
        assert_eq!(result, Some(42));

        let result = json_extract_int(json, "$.str_num").unwrap();
        assert_eq!(result, Some(123));
    }

    #[test]
    fn test_json_valid() {
        assert!(json_valid(r#"{"valid": true}"#));
        assert!(json_valid(r#"[1, 2, 3]"#));
        assert!(!json_valid(r#"{"invalid": }"#));
        assert!(!json_valid("not json"));
    }

    #[test]
    fn test_json_type() {
        assert_eq!(json_type(r#"{}"#).unwrap(), Some("object".to_string()));
        assert_eq!(json_type(r#"[]"#).unwrap(), Some("array".to_string()));
        assert_eq!(json_type(r#""hello""#).unwrap(), Some("string".to_string()));
        assert_eq!(json_type(r#"42"#).unwrap(), Some("number".to_string()));
        assert_eq!(json_type(r#"true"#).unwrap(), Some("boolean".to_string()));
        assert_eq!(json_type(r#"null"#).unwrap(), Some("null".to_string()));
    }

    #[test]
    fn test_json_length() {
        assert_eq!(json_length(r#"[1, 2, 3]"#).unwrap(), Some(3));
        assert_eq!(json_length(r#"{"a": 1, "b": 2}"#).unwrap(), Some(2));
        assert_eq!(json_length(r#""string""#).unwrap(), None);
    }

    #[test]
    fn test_json_keys() {
        let result = json_keys(r#"{"a": 1, "b": 2, "c": 3}"#).unwrap().unwrap();
        let keys: Value = serde_json::from_str(&result).unwrap();
        assert!(keys
            .as_array()
            .unwrap()
            .contains(&Value::String("a".to_string())));
        assert!(keys
            .as_array()
            .unwrap()
            .contains(&Value::String("b".to_string())));
        assert!(keys
            .as_array()
            .unwrap()
            .contains(&Value::String("c".to_string())));
    }

    #[test]
    fn test_json_contains_key() {
        let json = r#"{"name": "John", "age": 30}"#;
        assert!(json_contains_key(json, "name").unwrap());
        assert!(json_contains_key(json, "age").unwrap());
        assert!(!json_contains_key(json, "email").unwrap());
    }

    #[test]
    fn test_json_object_from_strings() {
        let pairs = vec![
            ("name".to_string(), "\"John\"".to_string()),
            ("age".to_string(), "30".to_string()),
        ];
        let result = json_object_from_strings(&pairs).unwrap();
        let obj: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(obj["name"], "John");
        assert_eq!(obj["age"], 30);
    }

    #[test]
    fn test_json_array_from_strings() {
        let values = vec!["1".to_string(), "2".to_string(), "\"three\"".to_string()];
        let result = json_array_from_strings(&values).unwrap();
        let arr: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(arr[0], 1);
        assert_eq!(arr[1], 2);
        assert_eq!(arr[2], "three");
    }

    #[test]
    fn test_json_path_missing_key() {
        let json = r#"{"name": "John"}"#;
        let result = json_extract(json, "$.missing").unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_json_path_root() {
        let json = r#"{"name": "John"}"#;
        let result = json_extract(json, "$").unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_json_set() {
        let json = r#"{"name": "John", "age": 30}"#;
        let result = json_set(json, "$.age", "31").unwrap();
        let v: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["age"], 31);
    }

    #[test]
    fn test_json_set_nested() {
        let json = r#"{"person": {"name": "John"}}"#;
        let result = json_set(json, "$.person.name", "\"Jane\"").unwrap();
        let v: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v["person"]["name"], "Jane");
    }

    #[test]
    fn test_json_remove() {
        let json = r#"{"name": "John", "age": 30}"#;
        let result = json_remove(json, "$.age").unwrap();
        let v: Value = serde_json::from_str(&result).unwrap();
        assert_eq!(v.get("age"), None);
        assert_eq!(v["name"], "John");
    }

    #[test]
    fn test_json_table_basic() {
        let json = r#"[{"a": 1, "b": 2}, {"a": 3, "b": 4}]"#;
        let rows = json_table(json, "$").unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows[0].contains(&("a".to_string(), "1".to_string())));
        assert!(rows[1].contains(&("a".to_string(), "3".to_string())));
    }

    #[test]
    fn test_json_table_nested() {
        let json = r#"[{"a": 1, "b": {"c": 2}}]"#;
        let rows = json_table(json, "$").unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].contains(&("a".to_string(), "1".to_string())));
        assert!(rows[0].contains(&("b.c".to_string(), "2".to_string())));
    }

    #[test]
    fn test_json_path_index() {
        let mut idx = JsonPathIndex::new();
        idx.add_document(0, r#"{"name": "Alice", "age": 30}"#)
            .unwrap();
        idx.add_document(1, r#"{"name": "Bob", "age": 25}"#)
            .unwrap();

        let names = idx.lookup("$.name").unwrap();
        assert_eq!(names.len(), 2);
        assert_eq!(names[0], (0, "Alice".to_string()));
        assert_eq!(names[1], (1, "Bob".to_string()));
    }

    #[test]
    fn test_json_extract_all_paths() {
        let json = r#"{"a": 1, "b": {"c": 2}}"#;
        let paths = json_extract_all_paths(json).unwrap();
        assert!(paths.contains(&("$.a".to_string(), "1".to_string())));
        assert!(paths.contains(&("$.b.c".to_string(), "2".to_string())));
    }
}
