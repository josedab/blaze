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

                while let Some(next_c) = chars.next() {
                    if next_c == ']' {
                        found_close = true;
                        break;
                    }
                    index_str.push(next_c);
                }

                if !found_close {
                    return Err(BlazeError::invalid_argument("Unclosed bracket in JSON path"));
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
                    let index: usize = index_str
                        .parse()
                        .map_err(|_| BlazeError::invalid_argument("Invalid array index in JSON path"))?;

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
        assert!(keys.as_array().unwrap().contains(&Value::String("a".to_string())));
        assert!(keys.as_array().unwrap().contains(&Value::String("b".to_string())));
        assert!(keys.as_array().unwrap().contains(&Value::String("c".to_string())));
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
}
