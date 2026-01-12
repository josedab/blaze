//! WASM Serialization
//!
//! This module provides serialization utilities for converting between
//! Arrow data and JavaScript-compatible formats (JSON, Arrow IPC).

use std::io::Cursor;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};

/// JSON serializer for Arrow data.
pub struct JsonSerializer;

impl JsonSerializer {
    /// Serialize record batches to JSON.
    pub fn serialize(batches: &[RecordBatch], pretty: bool) -> Result<String> {
        if batches.is_empty() {
            return Ok("[]".to_string());
        }

        let mut rows: Vec<String> = Vec::new();

        for batch in batches {
            for row_idx in 0..batch.num_rows() {
                let row = Self::serialize_row(batch, row_idx)?;
                rows.push(row);
            }
        }

        if pretty {
            Ok(format!("[\n  {}\n]", rows.join(",\n  ")))
        } else {
            Ok(format!("[{}]", rows.join(",")))
        }
    }

    /// Serialize a single row to JSON.
    fn serialize_row(batch: &RecordBatch, row_idx: usize) -> Result<String> {
        let schema = batch.schema();
        let mut fields: Vec<String> = Vec::with_capacity(batch.num_columns());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let column = batch.column(col_idx);
            let value = Self::serialize_value(column.as_ref(), row_idx)?;
            fields.push(format!("\"{}\":{}", field.name(), value));
        }

        Ok(format!("{{{}}}", fields.join(",")))
    }

    /// Serialize a single value to JSON.
    fn serialize_value(array: &dyn Array, idx: usize) -> Result<String> {
        if array.is_null(idx) {
            return Ok("null".to_string());
        }

        match array.data_type() {
            DataType::Null => Ok("null".to_string()),
            DataType::Boolean => {
                let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::Int8 => {
                let arr = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::Int16 => {
                let arr = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::Int32 => {
                let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::UInt8 => {
                let arr = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::UInt16 => {
                let arr = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::UInt32 => {
                let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::UInt64 => {
                let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(arr.value(idx).to_string())
            }
            DataType::Float32 => {
                let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
                let val = arr.value(idx);
                if val.is_finite() {
                    Ok(val.to_string())
                } else {
                    Ok("null".to_string())
                }
            }
            DataType::Float64 => {
                let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
                let val = arr.value(idx);
                if val.is_finite() {
                    Ok(val.to_string())
                } else {
                    Ok("null".to_string())
                }
            }
            DataType::Utf8 => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(format!("\"{}\"", escape_json_string(arr.value(idx))))
            }
            DataType::LargeUtf8 => {
                let arr = array.as_any().downcast_ref::<arrow::array::LargeStringArray>().unwrap();
                Ok(format!("\"{}\"", escape_json_string(arr.value(idx))))
            }
            _ => Ok(format!("\"<unsupported type: {:?}>\"", array.data_type())),
        }
    }

    /// Serialize record batches to columnar JSON format.
    pub fn serialize_columnar(batches: &[RecordBatch]) -> Result<String> {
        if batches.is_empty() {
            return Ok("{}".to_string());
        }

        let schema = batches[0].schema();
        let mut columns: Vec<String> = Vec::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let mut values: Vec<String> = Vec::new();

            for batch in batches {
                let column = batch.column(col_idx);
                for row_idx in 0..batch.num_rows() {
                    let value = Self::serialize_value(column.as_ref(), row_idx)?;
                    values.push(value);
                }
            }

            columns.push(format!("\"{}\":[{}]", field.name(), values.join(",")));
        }

        Ok(format!("{{{}}}", columns.join(",")))
    }

    /// Deserialize JSON to record batches.
    pub fn deserialize(json: &str) -> Result<Vec<RecordBatch>> {
        // Parse JSON
        let value: serde_json::Value = serde_json::from_str(json)
            .map_err(|e| BlazeError::invalid_argument(format!("Invalid JSON: {}", e)))?;

        match value {
            serde_json::Value::Array(rows) => {
                Self::deserialize_rows(&rows)
            }
            serde_json::Value::Object(_) => {
                // Columnar format
                Self::deserialize_columnar(&value)
            }
            _ => Err(BlazeError::invalid_argument("JSON must be an array or object")),
        }
    }

    /// Deserialize row-oriented JSON.
    fn deserialize_rows(rows: &[serde_json::Value]) -> Result<Vec<RecordBatch>> {
        if rows.is_empty() {
            return Ok(vec![]);
        }

        // Infer schema from first row
        let first_row = rows.first().ok_or_else(|| {
            BlazeError::invalid_argument("Empty JSON array")
        })?;

        let obj = first_row.as_object().ok_or_else(|| {
            BlazeError::invalid_argument("JSON rows must be objects")
        })?;

        let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
        for (key, value) in obj.iter() {
            let data_type = infer_json_type(value);
            fields.push(arrow::datatypes::Field::new(key, data_type, true));
        }

        let schema = Arc::new(Schema::new(fields));

        // Build arrays
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col_name = field.name();
            let col_type = field.data_type();

            let array = build_array_from_json(rows, col_name, col_type)?;
            columns.push(array);
        }

        let batch = RecordBatch::try_new(schema, columns)
            .map_err(|e| BlazeError::execution(format!("Failed to create batch: {}", e)))?;

        Ok(vec![batch])
    }

    /// Deserialize columnar JSON.
    fn deserialize_columnar(value: &serde_json::Value) -> Result<Vec<RecordBatch>> {
        let obj = value.as_object().ok_or_else(|| {
            BlazeError::invalid_argument("Columnar JSON must be an object")
        })?;

        if obj.is_empty() {
            return Ok(vec![]);
        }

        let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for (key, values) in obj.iter() {
            let arr = values.as_array().ok_or_else(|| {
                BlazeError::invalid_argument("Column values must be arrays")
            })?;

            if arr.is_empty() {
                continue;
            }

            let data_type = infer_json_type(&arr[0]);
            fields.push(arrow::datatypes::Field::new(key, data_type.clone(), true));

            let array = build_array_from_json_values(arr, &data_type)?;
            arrays.push(array);
        }

        if fields.is_empty() {
            return Ok(vec![]);
        }

        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays)
            .map_err(|e| BlazeError::execution(format!("Failed to create batch: {}", e)))?;

        Ok(vec![batch])
    }
}

/// Escape special characters in JSON strings.
fn escape_json_string(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c.is_control() => {
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result
}

/// Infer Arrow data type from JSON value.
fn infer_json_type(value: &serde_json::Value) -> DataType {
    match value {
        serde_json::Value::Null => DataType::Null,
        serde_json::Value::Bool(_) => DataType::Boolean,
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                DataType::Int64
            } else {
                DataType::Float64
            }
        }
        serde_json::Value::String(_) => DataType::Utf8,
        serde_json::Value::Array(_) => DataType::Utf8, // Stringify arrays
        serde_json::Value::Object(_) => DataType::Utf8, // Stringify objects
    }
}

/// Build an Arrow array from JSON values.
fn build_array_from_json(
    rows: &[serde_json::Value],
    column_name: &str,
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let values: Vec<Option<bool>> = rows
                .iter()
                .map(|row| row.get(column_name).and_then(|v| v.as_bool()))
                .collect();
            Ok(Arc::new(BooleanArray::from(values)) as ArrayRef)
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = rows
                .iter()
                .map(|row| row.get(column_name).and_then(|v| v.as_i64()))
                .collect();
            Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = rows
                .iter()
                .map(|row| row.get(column_name).and_then(|v| v.as_f64()))
                .collect();
            Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
        }
        DataType::Utf8 => {
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| {
                    row.get(column_name).map(|v| {
                        if let Some(s) = v.as_str() {
                            s.to_string()
                        } else {
                            v.to_string()
                        }
                    })
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)) as ArrayRef)
        }
        _ => {
            // Default to string for unsupported types
            let values: Vec<Option<String>> = rows
                .iter()
                .map(|row| row.get(column_name).map(|v| v.to_string()))
                .collect();
            Ok(Arc::new(StringArray::from(values)) as ArrayRef)
        }
    }
}

/// Build an Arrow array from JSON values array.
fn build_array_from_json_values(
    values: &[serde_json::Value],
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Boolean => {
            let arr: Vec<Option<bool>> = values.iter().map(|v| v.as_bool()).collect();
            Ok(Arc::new(BooleanArray::from(arr)) as ArrayRef)
        }
        DataType::Int64 => {
            let arr: Vec<Option<i64>> = values.iter().map(|v| v.as_i64()).collect();
            Ok(Arc::new(Int64Array::from(arr)) as ArrayRef)
        }
        DataType::Float64 => {
            let arr: Vec<Option<f64>> = values.iter().map(|v| v.as_f64()).collect();
            Ok(Arc::new(Float64Array::from(arr)) as ArrayRef)
        }
        DataType::Utf8 => {
            let arr: Vec<Option<String>> = values
                .iter()
                .map(|v| {
                    if v.is_null() {
                        None
                    } else if let Some(s) = v.as_str() {
                        Some(s.to_string())
                    } else {
                        Some(v.to_string())
                    }
                })
                .collect();
            Ok(Arc::new(StringArray::from(arr)) as ArrayRef)
        }
        _ => {
            let arr: Vec<Option<String>> = values
                .iter()
                .map(|v| if v.is_null() { None } else { Some(v.to_string()) })
                .collect();
            Ok(Arc::new(StringArray::from(arr)) as ArrayRef)
        }
    }
}

/// Arrow IPC serializer.
pub struct ArrowIpcSerializer;

impl ArrowIpcSerializer {
    /// Serialize record batches to Arrow IPC stream format.
    pub fn serialize(batches: &[RecordBatch]) -> Result<Vec<u8>> {
        if batches.is_empty() {
            return Ok(vec![]);
        }

        let schema = batches[0].schema();
        let mut buf = Cursor::new(Vec::new());

        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema)
                .map_err(|e| BlazeError::execution(format!("Failed to create IPC writer: {}", e)))?;

            for batch in batches {
                writer.write(batch)
                    .map_err(|e| BlazeError::execution(format!("Failed to write batch: {}", e)))?;
            }

            writer.finish()
                .map_err(|e| BlazeError::execution(format!("Failed to finish IPC stream: {}", e)))?;
        }

        Ok(buf.into_inner())
    }

    /// Deserialize Arrow IPC stream to record batches.
    pub fn deserialize(data: &[u8]) -> Result<Vec<RecordBatch>> {
        if data.is_empty() {
            return Ok(vec![]);
        }

        let cursor = Cursor::new(data);
        let reader = StreamReader::try_new(cursor, None)
            .map_err(|e| BlazeError::execution(format!("Failed to create IPC reader: {}", e)))?;

        let batches: Vec<RecordBatch> = reader
            .filter_map(|r| r.ok())
            .collect();

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
        ]);

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);
        let active_array = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(active_array),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_json_serialize() {
        let batch = create_test_batch();
        let json = JsonSerializer::serialize(&[batch], false).unwrap();

        assert!(json.contains("\"id\":1"));
        assert!(json.contains("\"name\":\"Alice\""));
        assert!(json.contains("\"active\":true"));
    }

    #[test]
    fn test_json_serialize_pretty() {
        let batch = create_test_batch();
        let json = JsonSerializer::serialize(&[batch], true).unwrap();

        assert!(json.contains('\n'));
        assert!(json.contains("  "));
    }

    #[test]
    fn test_json_serialize_columnar() {
        let batch = create_test_batch();
        let json = JsonSerializer::serialize_columnar(&[batch]).unwrap();

        assert!(json.contains("\"id\":[1,2,3]"));
        assert!(json.contains("\"name\":[\"Alice\",\"Bob\",null]"));
    }

    #[test]
    fn test_json_deserialize() {
        let json = r#"[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]"#;
        let batches = JsonSerializer::deserialize(json).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[0].num_columns(), 2);
    }

    #[test]
    fn test_json_deserialize_columnar() {
        let json = r#"{"id": [1, 2, 3], "name": ["a", "b", "c"]}"#;
        let batches = JsonSerializer::deserialize(json).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn test_arrow_ipc_roundtrip() {
        let batch = create_test_batch();

        let data = ArrowIpcSerializer::serialize(&[batch.clone()]).unwrap();
        assert!(!data.is_empty());

        let restored = ArrowIpcSerializer::deserialize(&data).unwrap();
        assert_eq!(restored.len(), 1);
        assert_eq!(restored[0].num_rows(), batch.num_rows());
        assert_eq!(restored[0].num_columns(), batch.num_columns());
    }

    #[test]
    fn test_escape_json_string() {
        assert_eq!(escape_json_string("hello"), "hello");
        assert_eq!(escape_json_string("he\"llo"), "he\\\"llo");
        assert_eq!(escape_json_string("he\\llo"), "he\\\\llo");
        assert_eq!(escape_json_string("line1\nline2"), "line1\\nline2");
    }

    #[test]
    fn test_infer_json_type() {
        assert_eq!(
            infer_json_type(&serde_json::json!(null)),
            DataType::Null
        );
        assert_eq!(
            infer_json_type(&serde_json::json!(true)),
            DataType::Boolean
        );
        assert_eq!(
            infer_json_type(&serde_json::json!(42)),
            DataType::Int64
        );
        assert_eq!(
            infer_json_type(&serde_json::json!(3.14)),
            DataType::Float64
        );
        assert_eq!(
            infer_json_type(&serde_json::json!("hello")),
            DataType::Utf8
        );
    }

    #[test]
    fn test_empty_json() {
        let json = "[]";
        let batches = JsonSerializer::deserialize(json).unwrap();
        assert!(batches.is_empty());

        let json = JsonSerializer::serialize(&[], false).unwrap();
        assert_eq!(json, "[]");
    }
}
