//! Schema-on-read with automatic type inference for CSV, JSON, and Parquet files.
//!
//! Provides sample-based type detection that infers the best column types from
//! actual data values, supporting integers, floats, booleans, dates, timestamps,
//! and strings with configurable detection parameters.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};

use crate::error::{BlazeError, Result};

/// Configuration for type inference behavior.
#[derive(Debug, Clone)]
pub struct InferenceConfig {
    /// Maximum number of rows to sample for inference
    pub sample_rows: usize,
    /// Whether to detect boolean values (true/false, yes/no, 1/0)
    pub detect_booleans: bool,
    /// Whether to detect date values (YYYY-MM-DD and common formats)
    pub detect_dates: bool,
    /// Whether to detect timestamp values
    pub detect_timestamps: bool,
    /// Custom null value strings (default: empty string, "null", "NULL", "NA", "N/A")
    pub null_values: Vec<String>,
    /// CSV delimiter override (None = auto-detect)
    pub delimiter: Option<u8>,
}

impl Default for InferenceConfig {
    fn default() -> Self {
        Self {
            sample_rows: 1000,
            detect_booleans: true,
            detect_dates: true,
            detect_timestamps: true,
            null_values: vec![
                String::new(),
                "null".to_string(),
                "NULL".to_string(),
                "NA".to_string(),
                "N/A".to_string(),
                "n/a".to_string(),
                "None".to_string(),
                "none".to_string(),
            ],
            delimiter: None,
        }
    }
}

/// Candidate types tracked during inference, ordered from most to least specific.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CandidateType {
    Boolean,
    Int64,
    Float64,
    Date32,
    Timestamp,
    Utf8,
}

impl CandidateType {
    fn to_arrow_type(self) -> DataType {
        match self {
            CandidateType::Boolean => DataType::Boolean,
            CandidateType::Int64 => DataType::Int64,
            CandidateType::Float64 => DataType::Float64,
            CandidateType::Date32 => DataType::Date32,
            CandidateType::Timestamp => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            CandidateType::Utf8 => DataType::Utf8,
        }
    }
}

/// Per-column inference state.
#[derive(Debug)]
struct ColumnInference {
    name: String,
    non_null_count: usize,
    null_count: usize,
    type_counts: HashMap<CandidateType, usize>,
}

impl ColumnInference {
    fn new(name: String) -> Self {
        Self {
            name,
            non_null_count: 0,
            null_count: 0,
            type_counts: HashMap::new(),
        }
    }

    fn record_null(&mut self) {
        self.null_count += 1;
    }

    fn record_value(&mut self, candidate: CandidateType) {
        self.non_null_count += 1;
        *self.type_counts.entry(candidate).or_insert(0) += 1;
    }

    /// Determine the best type for this column based on observed values.
    /// Uses majority voting with type promotion.
    fn resolve_type(&self) -> DataType {
        if self.non_null_count == 0 {
            return DataType::Utf8;
        }

        // Check in order of specificity
        let total = self.non_null_count;
        let threshold = (total as f64 * 0.95).ceil() as usize;

        // Boolean: all values must be boolean
        if let Some(&count) = self.type_counts.get(&CandidateType::Boolean) {
            if count >= threshold {
                return CandidateType::Boolean.to_arrow_type();
            }
        }

        // Int64: all numeric values must be integer
        if let Some(&count) = self.type_counts.get(&CandidateType::Int64) {
            if count >= threshold {
                return CandidateType::Int64.to_arrow_type();
            }
        }

        // Float64: integers can promote to float
        let int_count = self.type_counts.get(&CandidateType::Int64).copied().unwrap_or(0);
        let float_count = self.type_counts.get(&CandidateType::Float64).copied().unwrap_or(0);
        if int_count + float_count >= threshold && float_count > 0 {
            return CandidateType::Float64.to_arrow_type();
        }

        // Date32
        if let Some(&count) = self.type_counts.get(&CandidateType::Date32) {
            if count >= threshold {
                return CandidateType::Date32.to_arrow_type();
            }
        }

        // Timestamp
        if let Some(&count) = self.type_counts.get(&CandidateType::Timestamp) {
            let date_count = self.type_counts.get(&CandidateType::Date32).copied().unwrap_or(0);
            if count + date_count >= threshold && count > 0 {
                return CandidateType::Timestamp.to_arrow_type();
            }
        }

        CandidateType::Utf8.to_arrow_type()
    }

    fn is_nullable(&self) -> bool {
        self.null_count > 0
    }
}

/// Infer the type of a single string value.
fn classify_value(value: &str, config: &InferenceConfig) -> CandidateType {
    // Boolean detection
    if config.detect_booleans {
        let lower = value.to_lowercase();
        if matches!(
            lower.as_str(),
            "true" | "false" | "yes" | "no" | "t" | "f" | "y" | "n"
        ) {
            return CandidateType::Boolean;
        }
    }

    // Integer detection
    if value.parse::<i64>().is_ok() {
        return CandidateType::Int64;
    }

    // Float detection
    if value.parse::<f64>().is_ok() {
        return CandidateType::Float64;
    }

    // Timestamp detection (before date, since timestamps are more specific)
    if config.detect_timestamps && is_timestamp(value) {
        return CandidateType::Timestamp;
    }

    // Date detection
    if config.detect_dates && is_date(value) {
        return CandidateType::Date32;
    }

    CandidateType::Utf8
}

fn is_date(value: &str) -> bool {
    let trimmed = value.trim();
    // YYYY-MM-DD
    if let Some((y, rest)) = trimmed.split_once('-') {
        if y.len() == 4 && y.chars().all(|c| c.is_ascii_digit()) {
            if let Some((m, d)) = rest.split_once('-') {
                if m.len() <= 2
                    && d.len() <= 2
                    && m.chars().all(|c| c.is_ascii_digit())
                    && d.chars().all(|c| c.is_ascii_digit())
                {
                    let month: u32 = m.parse().unwrap_or(0);
                    let day: u32 = d.parse().unwrap_or(0);
                    return (1..=12).contains(&month) && (1..=31).contains(&day);
                }
            }
        }
    }
    // MM/DD/YYYY
    if let Some((m, rest)) = trimmed.split_once('/') {
        if let Some((d, y)) = rest.split_once('/') {
            if y.len() == 4
                && m.len() <= 2
                && d.len() <= 2
                && y.chars().all(|c| c.is_ascii_digit())
                && m.chars().all(|c| c.is_ascii_digit())
                && d.chars().all(|c| c.is_ascii_digit())
            {
                return true;
            }
        }
    }
    false
}

fn is_timestamp(value: &str) -> bool {
    let trimmed = value.trim();
    // ISO 8601: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS
    if trimmed.len() >= 19 {
        let has_t = trimmed.chars().nth(10) == Some('T') || trimmed.chars().nth(10) == Some(' ');
        if has_t && is_date(&trimmed[..10]) {
            let time_part = &trimmed[11..];
            // Check HH:MM:SS pattern
            if time_part.len() >= 8
                && time_part.chars().nth(2) == Some(':')
                && time_part.chars().nth(5) == Some(':')
            {
                return true;
            }
        }
    }
    false
}

/// Auto-detect the CSV delimiter from the first line.
fn detect_delimiter(first_line: &str) -> u8 {
    let candidates = [b',', b'\t', b';', b'|'];
    let mut best = b',';
    let mut best_count = 0;

    for &delim in &candidates {
        let count = first_line
            .bytes()
            .filter(|&b| b == delim)
            .count();
        if count > best_count {
            best_count = count;
            best = delim;
        }
    }
    best
}

/// Infer schema from a CSV file using sample-based type detection.
pub fn infer_csv_schema(
    path: impl AsRef<Path>,
    config: &InferenceConfig,
) -> Result<(Arc<ArrowSchema>, u8)> {
    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();

    // Read header
    let header_line = lines
        .next()
        .ok_or_else(|| BlazeError::invalid_argument("Empty CSV file"))??;

    let delimiter = config.delimiter.unwrap_or_else(|| detect_delimiter(&header_line));
    let delim_char = delimiter as char;

    let headers: Vec<String> = header_line
        .split(delim_char)
        .map(|s| s.trim().trim_matches('"').to_string())
        .collect();

    let _num_columns = headers.len();
    let mut columns: Vec<ColumnInference> = headers
        .into_iter()
        .map(ColumnInference::new)
        .collect();

    // Sample rows for type inference
    let mut rows_sampled = 0;
    for line_result in lines {
        if rows_sampled >= config.sample_rows {
            break;
        }
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }

        let values: Vec<&str> = line.split(delim_char).collect();
        for (i, col) in columns.iter_mut().enumerate() {
            let value = values.get(i).map(|s| s.trim().trim_matches('"')).unwrap_or("");
            if value.is_empty() || config.null_values.contains(&value.to_string()) {
                col.record_null();
            } else {
                let candidate = classify_value(value, config);
                col.record_value(candidate);
            }
        }
        rows_sampled += 1;
    }

    // Build Arrow schema from inferred types
    let fields: Vec<Field> = columns
        .iter()
        .map(|col| Field::new(&col.name, col.resolve_type(), col.is_nullable()))
        .collect();

    Ok((Arc::new(ArrowSchema::new(fields)), delimiter))
}

/// Infer schema from a JSON file (newline-delimited JSON).
pub fn infer_json_schema(
    path: impl AsRef<Path>,
    config: &InferenceConfig,
) -> Result<Arc<ArrowSchema>> {
    let file = File::open(path.as_ref())?;
    let reader = BufReader::new(file);

    let mut columns: HashMap<String, ColumnInference> = HashMap::new();
    let mut column_order: Vec<String> = Vec::new();
    let mut rows_sampled = 0;

    for line_result in reader.lines() {
        if rows_sampled >= config.sample_rows {
            break;
        }
        let line = line_result?;
        if line.trim().is_empty() {
            continue;
        }

        let parsed: serde_json::Value = serde_json::from_str(&line).map_err(|e| {
            BlazeError::invalid_argument(format!("Invalid JSON on line {}: {}", rows_sampled + 1, e))
        })?;

        if let serde_json::Value::Object(obj) = parsed {
            for (key, value) in obj {
                if !columns.contains_key(&key) {
                    column_order.push(key.clone());
                    columns.insert(key.clone(), ColumnInference::new(key.clone()));
                }
                let col = columns.get_mut(&key).unwrap();

                match &value {
                    serde_json::Value::Null => col.record_null(),
                    serde_json::Value::Bool(_) => col.record_value(CandidateType::Boolean),
                    serde_json::Value::Number(n) => {
                        if n.is_i64() {
                            col.record_value(CandidateType::Int64);
                        } else {
                            col.record_value(CandidateType::Float64);
                        }
                    }
                    serde_json::Value::String(s) => {
                        if config.null_values.contains(s) {
                            col.record_null();
                        } else {
                            let candidate = classify_value(s, config);
                            col.record_value(candidate);
                        }
                    }
                    // Arrays and objects stored as JSON strings
                    _ => col.record_value(CandidateType::Utf8),
                }
            }
        }
        rows_sampled += 1;
    }

    let fields: Vec<Field> = column_order
        .iter()
        .filter_map(|name| {
            columns
                .get(name)
                .map(|col| Field::new(&col.name, col.resolve_type(), col.is_nullable()))
        })
        .collect();

    Ok(Arc::new(ArrowSchema::new(fields)))
}

/// Merge two Arrow schemas, keeping all columns from both.
/// When a column exists in both schemas, the wider type is used.
pub fn merge_schemas(left: &ArrowSchema, right: &ArrowSchema) -> ArrowSchema {
    let mut fields: Vec<Field> = Vec::new();
    let mut seen: HashMap<String, usize> = HashMap::new();

    // Add all fields from left
    for field in left.fields() {
        seen.insert(field.name().clone(), fields.len());
        fields.push(field.as_ref().clone());
    }

    // Merge fields from right
    for field in right.fields() {
        if let Some(&idx) = seen.get(field.name()) {
            let existing = &fields[idx];
            let merged_type = widen_type(existing.data_type(), field.data_type());
            let nullable = existing.is_nullable() || field.is_nullable();
            fields[idx] = Field::new(existing.name(), merged_type, nullable);
        } else {
            // New column from right schema — nullable since left doesn't have it
            fields.push(Field::new(field.name(), field.data_type().clone(), true));
        }
    }

    ArrowSchema::new(fields)
}

/// Return the wider of two types for schema merging.
fn widen_type(left: &DataType, right: &DataType) -> DataType {
    if left == right {
        return left.clone();
    }
    match (left, right) {
        // Int -> Float promotion
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
            DataType::Float64
        }
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => DataType::Int64,
        (DataType::Int32, DataType::Float64) | (DataType::Float64, DataType::Int32) => {
            DataType::Float64
        }
        // Date -> Timestamp promotion
        (DataType::Date32, DataType::Timestamp(u, tz))
        | (DataType::Timestamp(u, tz), DataType::Date32) => {
            DataType::Timestamp(*u, tz.clone())
        }
        // Fallback to Utf8
        _ => DataType::Utf8,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_classify_integer() {
        let config = InferenceConfig::default();
        assert_eq!(classify_value("42", &config), CandidateType::Int64);
        assert_eq!(classify_value("-100", &config), CandidateType::Int64);
        assert_eq!(classify_value("0", &config), CandidateType::Int64);
    }

    #[test]
    fn test_classify_float() {
        let config = InferenceConfig::default();
        assert_eq!(classify_value("3.14", &config), CandidateType::Float64);
        assert_eq!(classify_value("-0.5", &config), CandidateType::Float64);
        assert_eq!(classify_value("1e10", &config), CandidateType::Float64);
    }

    #[test]
    fn test_classify_boolean() {
        let config = InferenceConfig::default();
        assert_eq!(classify_value("true", &config), CandidateType::Boolean);
        assert_eq!(classify_value("False", &config), CandidateType::Boolean);
        assert_eq!(classify_value("yes", &config), CandidateType::Boolean);
        assert_eq!(classify_value("no", &config), CandidateType::Boolean);
    }

    #[test]
    fn test_classify_date() {
        let config = InferenceConfig::default();
        assert_eq!(classify_value("2024-01-15", &config), CandidateType::Date32);
        assert_eq!(classify_value("01/15/2024", &config), CandidateType::Date32);
    }

    #[test]
    fn test_classify_timestamp() {
        let config = InferenceConfig::default();
        assert_eq!(
            classify_value("2024-01-15T10:30:00", &config),
            CandidateType::Timestamp
        );
        assert_eq!(
            classify_value("2024-01-15 10:30:00", &config),
            CandidateType::Timestamp
        );
    }

    #[test]
    fn test_classify_string() {
        let config = InferenceConfig::default();
        assert_eq!(classify_value("hello world", &config), CandidateType::Utf8);
        assert_eq!(classify_value("abc123", &config), CandidateType::Utf8);
    }

    #[test]
    fn test_detect_delimiter() {
        assert_eq!(detect_delimiter("a,b,c"), b',');
        assert_eq!(detect_delimiter("a\tb\tc"), b'\t');
        assert_eq!(detect_delimiter("a;b;c"), b';');
        assert_eq!(detect_delimiter("a|b|c"), b'|');
    }

    #[test]
    fn test_infer_csv_schema_integers() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "id,count").unwrap();
        writeln!(file, "1,100").unwrap();
        writeln!(file, "2,200").unwrap();
        writeln!(file, "3,300").unwrap();
        file.flush().unwrap();

        let config = InferenceConfig::default();
        let (schema, delim) = infer_csv_schema(file.path(), &config).unwrap();

        assert_eq!(delim, b',');
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn test_infer_csv_schema_mixed_numeric() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "id,score").unwrap();
        writeln!(file, "1,95.5").unwrap();
        writeln!(file, "2,87").unwrap();
        writeln!(file, "3,92.3").unwrap();
        file.flush().unwrap();

        let config = InferenceConfig::default();
        let (schema, _) = infer_csv_schema(file.path(), &config).unwrap();

        assert_eq!(schema.field(0).data_type(), &DataType::Int64);
        // Mix of int and float → Float64
        assert_eq!(schema.field(1).data_type(), &DataType::Float64);
    }

    #[test]
    fn test_infer_csv_schema_dates() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "name,birthday").unwrap();
        writeln!(file, "Alice,2024-01-15").unwrap();
        writeln!(file, "Bob,2024-06-20").unwrap();
        file.flush().unwrap();

        let config = InferenceConfig::default();
        let (schema, _) = infer_csv_schema(file.path(), &config).unwrap();

        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Date32);
    }

    #[test]
    fn test_infer_csv_schema_with_nulls() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "id,value").unwrap();
        writeln!(file, "1,100").unwrap();
        writeln!(file, "2,").unwrap();
        writeln!(file, "3,NULL").unwrap();
        file.flush().unwrap();

        let config = InferenceConfig::default();
        let (schema, _) = infer_csv_schema(file.path(), &config).unwrap();

        assert_eq!(schema.field(1).data_type(), &DataType::Int64);
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn test_infer_csv_tsv_delimiter() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "id\tname\tvalue").unwrap();
        writeln!(file, "1\tAlice\t100").unwrap();
        writeln!(file, "2\tBob\t200").unwrap();
        file.flush().unwrap();

        let config = InferenceConfig::default();
        let (schema, delim) = infer_csv_schema(file.path(), &config).unwrap();

        assert_eq!(delim, b'\t');
        assert_eq!(schema.fields().len(), 3);
    }

    #[test]
    fn test_infer_json_schema() {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, r#"{{"id": 1, "name": "Alice", "active": true}}"#).unwrap();
        writeln!(file, r#"{{"id": 2, "name": "Bob", "active": false}}"#).unwrap();
        writeln!(file, r#"{{"id": 3, "name": "Charlie", "score": 95.5}}"#).unwrap();
        file.flush().unwrap();

        let config = InferenceConfig::default();
        let schema = infer_json_schema(file.path(), &config).unwrap();

        assert_eq!(
            schema.field_with_name("id").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            schema.field_with_name("name").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn test_merge_schemas() {
        let left = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);
        let right = ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("score", DataType::Float64, true),
        ]);

        let merged = merge_schemas(&left, &right);
        assert_eq!(merged.fields().len(), 3);
        assert_eq!(merged.field(0).data_type(), &DataType::Int64);
        assert_eq!(merged.field(1).data_type(), &DataType::Utf8);
        assert_eq!(merged.field(2).data_type(), &DataType::Float64);
        // "score" should be nullable since left doesn't have it
        assert!(merged.field(2).is_nullable());
    }

    #[test]
    fn test_widen_type() {
        assert_eq!(widen_type(&DataType::Int64, &DataType::Float64), DataType::Float64);
        assert_eq!(widen_type(&DataType::Int32, &DataType::Int64), DataType::Int64);
        assert_eq!(widen_type(&DataType::Int64, &DataType::Int64), DataType::Int64);
        assert_eq!(widen_type(&DataType::Utf8, &DataType::Int64), DataType::Utf8);
    }

    #[test]
    fn test_column_inference_resolve_type() {
        let mut col = ColumnInference::new("test".to_string());
        col.record_value(CandidateType::Int64);
        col.record_value(CandidateType::Int64);
        col.record_value(CandidateType::Int64);
        assert_eq!(col.resolve_type(), DataType::Int64);

        let mut col2 = ColumnInference::new("mixed".to_string());
        col2.record_value(CandidateType::Int64);
        col2.record_value(CandidateType::Float64);
        col2.record_value(CandidateType::Int64);
        assert_eq!(col2.resolve_type(), DataType::Float64);
    }
}
