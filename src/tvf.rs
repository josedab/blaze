//! Table-Valued Functions (TVFs) for generating multi-row results.
//!
//! Provides built-in table functions like `generate_series`, `unnest`,
//! `json_each`, and a registry for user-defined table functions.

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

use arrow::array::{
    ArrayRef, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};

/// A table-valued function that produces zero or more rows from its arguments.
pub struct TableFunction {
    /// Function name (used in SQL)
    pub name: String,
    /// Output schema (column names and types)
    pub output_schema: Arc<ArrowSchema>,
    /// The function implementation: takes scalar arguments, returns RecordBatches
    pub func: Arc<dyn Fn(&[ScalarArg]) -> Result<Vec<RecordBatch>> + Send + Sync>,
}

impl TableFunction {
    pub fn new(
        name: impl Into<String>,
        output_schema: Arc<ArrowSchema>,
        func: impl Fn(&[ScalarArg]) -> Result<Vec<RecordBatch>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            name: name.into(),
            output_schema,
            func: Arc::new(func),
        }
    }

    pub fn execute(&self, args: &[ScalarArg]) -> Result<Vec<RecordBatch>> {
        (self.func)(args)
    }
}

impl fmt::Debug for TableFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableFunction")
            .field("name", &self.name)
            .field("output_schema", &self.output_schema)
            .finish()
    }
}

/// Scalar argument to a table function.
#[derive(Debug, Clone)]
pub enum ScalarArg {
    Int64(i64),
    Float64(f64),
    Utf8(String),
    Null,
    /// An array of values (for UNNEST)
    Array(Vec<ScalarArg>),
}

impl ScalarArg {
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ScalarArg::Int64(v) => Some(*v),
            ScalarArg::Float64(v) => Some(*v as i64),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ScalarArg::Float64(v) => Some(*v),
            ScalarArg::Int64(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ScalarArg::Utf8(v) => Some(v),
            _ => None,
        }
    }
}

/// Registry for table-valued functions.
pub struct TvfRegistry {
    functions: RwLock<HashMap<String, Arc<TableFunction>>>,
}

impl Default for TvfRegistry {
    fn default() -> Self {
        let registry = Self {
            functions: RwLock::new(HashMap::new()),
        };
        registry.register_builtins();
        registry
    }
}

impl TvfRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a table-valued function.
    pub fn register(&self, tvf: TableFunction) -> Result<()> {
        let name = tvf.name.to_uppercase();
        let mut funcs = self.functions.write().map_err(|e| {
            BlazeError::internal(format!("Failed to acquire TVF registry lock: {}", e))
        })?;
        funcs.insert(name, Arc::new(tvf));
        Ok(())
    }

    /// Get a table function by name.
    pub fn get(&self, name: &str) -> Option<Arc<TableFunction>> {
        let funcs = self.functions.read().ok()?;
        funcs.get(&name.to_uppercase()).cloned()
    }

    /// Check if a table function exists.
    pub fn has(&self, name: &str) -> bool {
        self.functions
            .read()
            .ok()
            .map(|f| f.contains_key(&name.to_uppercase()))
            .unwrap_or(false)
    }

    /// List all registered table function names.
    pub fn list(&self) -> Vec<String> {
        self.functions
            .read()
            .ok()
            .map(|f| f.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn register_builtins(&self) {
        let _ = self.register(generate_series_tvf());
        let _ = self.register(unnest_int_tvf());
        let _ = self.register(unnest_str_tvf());
        let _ = self.register(json_each_tvf());
        let _ = self.register(regexp_matches_tvf());
    }
}

impl fmt::Debug for TvfRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TvfRegistry")
            .field("functions", &self.list())
            .finish()
    }
}

// ── Built-in Table Functions ────────────────────────────────────────────

/// `GENERATE_SERIES(start, stop[, step])` - Generate a series of integers.
pub fn generate_series_tvf() -> TableFunction {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));

    TableFunction::new("generate_series", schema, |args| {
        if args.len() < 2 || args.len() > 3 {
            return Err(BlazeError::execution(
                "GENERATE_SERIES requires 2 or 3 arguments: (start, stop[, step])",
            ));
        }

        let start = args[0]
            .as_i64()
            .ok_or_else(|| BlazeError::execution("GENERATE_SERIES: start must be an integer"))?;
        let stop = args[1]
            .as_i64()
            .ok_or_else(|| BlazeError::execution("GENERATE_SERIES: stop must be an integer"))?;
        let step = if args.len() == 3 {
            args[2]
                .as_i64()
                .ok_or_else(|| BlazeError::execution("GENERATE_SERIES: step must be an integer"))?
        } else if start <= stop {
            1
        } else {
            -1
        };

        if step == 0 {
            return Err(BlazeError::execution(
                "GENERATE_SERIES: step must not be zero",
            ));
        }

        let mut values = Vec::new();
        let mut current = start;

        if step > 0 {
            while current <= stop {
                values.push(current);
                current = current.saturating_add(step);
            }
        } else {
            while current >= stop {
                values.push(current);
                current = current.saturating_add(step);
            }
        }

        // Limit to 10 million rows
        if values.len() > 10_000_000 {
            return Err(BlazeError::execution(
                "GENERATE_SERIES: result exceeds 10 million rows",
            ));
        }

        let array = Arc::new(Int64Array::from(values)) as ArrayRef;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array])?;
        Ok(vec![batch])
    })
}

/// `UNNEST(array)` for integer arrays - Expand an array into rows.
pub fn unnest_int_tvf() -> TableFunction {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));

    TableFunction::new("unnest", schema, |args| {
        if args.len() != 1 {
            return Err(BlazeError::execution(
                "UNNEST requires exactly 1 argument: (array)",
            ));
        }

        match &args[0] {
            ScalarArg::Array(elements) => {
                let values: Vec<Option<i64>> = elements
                    .iter()
                    .map(|e| match e {
                        ScalarArg::Int64(v) => Some(*v),
                        ScalarArg::Null => None,
                        _ => None,
                    })
                    .collect();

                let array = Arc::new(Int64Array::from(values)) as ArrayRef;
                let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    true,
                )]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                Ok(vec![batch])
            }
            _ => Err(BlazeError::execution("UNNEST: argument must be an array")),
        }
    })
}

/// `UNNEST_STR(array)` for string arrays.
pub fn unnest_str_tvf() -> TableFunction {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "value",
        DataType::Utf8,
        true,
    )]));

    TableFunction::new("unnest_str", schema, |args| {
        if args.len() != 1 {
            return Err(BlazeError::execution(
                "UNNEST_STR requires exactly 1 argument: (array)",
            ));
        }

        match &args[0] {
            ScalarArg::Array(elements) => {
                let values: Vec<Option<&str>> = elements
                    .iter()
                    .map(|e| match e {
                        ScalarArg::Utf8(v) => Some(v.as_str()),
                        ScalarArg::Null => None,
                        _ => None,
                    })
                    .collect();

                let array = Arc::new(StringArray::from(values)) as ArrayRef;
                let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                    "value",
                    DataType::Utf8,
                    true,
                )]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                Ok(vec![batch])
            }
            _ => Err(BlazeError::execution(
                "UNNEST_STR: argument must be a string array",
            )),
        }
    })
}

/// `JSON_EACH(json_string)` - Expand a JSON object into key/value rows.
pub fn json_each_tvf() -> TableFunction {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
    ]));

    TableFunction::new("json_each", schema, |args| {
        if args.len() != 1 {
            return Err(BlazeError::execution(
                "JSON_EACH requires exactly 1 argument: (json_string)",
            ));
        }

        let json_str = args[0]
            .as_str()
            .ok_or_else(|| BlazeError::execution("JSON_EACH: argument must be a string"))?;

        let parsed: serde_json::Value = serde_json::from_str(json_str).map_err(|e| {
            BlazeError::execution(format!("JSON_EACH: invalid JSON: {}", e))
        })?;

        match parsed {
            serde_json::Value::Object(obj) => {
                let mut keys = Vec::new();
                let mut values = Vec::new();

                for (k, v) in obj {
                    keys.push(k);
                    values.push(match v {
                        serde_json::Value::Null => None,
                        other => Some(other.to_string()),
                    });
                }

                let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
                let value_array = Arc::new(StringArray::from(values)) as ArrayRef;
                let schema = Arc::new(ArrowSchema::new(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]));
                let batch = RecordBatch::try_new(schema, vec![key_array, value_array])?;
                Ok(vec![batch])
            }
            serde_json::Value::Array(arr) => {
                let mut keys = Vec::new();
                let mut values = Vec::new();

                for (i, v) in arr.into_iter().enumerate() {
                    keys.push(i.to_string());
                    values.push(match v {
                        serde_json::Value::Null => None,
                        other => Some(other.to_string()),
                    });
                }

                let key_array = Arc::new(StringArray::from(keys)) as ArrayRef;
                let value_array = Arc::new(StringArray::from(values)) as ArrayRef;
                let schema = Arc::new(ArrowSchema::new(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]));
                let batch = RecordBatch::try_new(schema, vec![key_array, value_array])?;
                Ok(vec![batch])
            }
            _ => Err(BlazeError::execution(
                "JSON_EACH: argument must be a JSON object or array",
            )),
        }
    })
}

/// `REGEXP_MATCHES(text, pattern)` - Find all regex matches and return as rows.
pub fn regexp_matches_tvf() -> TableFunction {
    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("match_index", DataType::Int64, false),
        Field::new("match_value", DataType::Utf8, false),
        Field::new("start_pos", DataType::Int64, false),
        Field::new("end_pos", DataType::Int64, false),
    ]));

    TableFunction::new("regexp_matches", schema, |args| {
        if args.len() != 2 {
            return Err(BlazeError::execution(
                "REGEXP_MATCHES requires 2 arguments: (text, pattern)",
            ));
        }

        let text = args[0]
            .as_str()
            .ok_or_else(|| BlazeError::execution("REGEXP_MATCHES: text must be a string"))?;
        let pattern = args[1]
            .as_str()
            .ok_or_else(|| BlazeError::execution("REGEXP_MATCHES: pattern must be a string"))?;

        let re = regex::Regex::new(pattern).map_err(|e| {
            BlazeError::execution(format!("REGEXP_MATCHES: invalid regex: {}", e))
        })?;

        let mut indices = Vec::new();
        let mut match_values = Vec::new();
        let mut starts = Vec::new();
        let mut ends = Vec::new();

        for (i, m) in re.find_iter(text).enumerate() {
            indices.push(i as i64);
            match_values.push(m.as_str().to_string());
            starts.push(m.start() as i64);
            ends.push(m.end() as i64);
        }

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("match_index", DataType::Int64, false),
            Field::new("match_value", DataType::Utf8, false),
            Field::new("start_pos", DataType::Int64, false),
            Field::new("end_pos", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(indices)) as ArrayRef,
                Arc::new(StringArray::from(match_values)) as ArrayRef,
                Arc::new(Int64Array::from(starts)) as ArrayRef,
                Arc::new(Int64Array::from(ends)) as ArrayRef,
            ],
        )?;
        Ok(vec![batch])
    })
}

/// `GENERATE_SERIES_FLOAT(start, stop, step)` - Generate a series of floats.
pub fn generate_series_float_tvf() -> TableFunction {
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));

    TableFunction::new("generate_series_float", schema, |args| {
        if args.len() != 3 {
            return Err(BlazeError::execution(
                "GENERATE_SERIES_FLOAT requires 3 arguments: (start, stop, step)",
            ));
        }

        let start = args[0].as_f64().ok_or_else(|| {
            BlazeError::execution("GENERATE_SERIES_FLOAT: start must be numeric")
        })?;
        let stop = args[1].as_f64().ok_or_else(|| {
            BlazeError::execution("GENERATE_SERIES_FLOAT: stop must be numeric")
        })?;
        let step = args[2].as_f64().ok_or_else(|| {
            BlazeError::execution("GENERATE_SERIES_FLOAT: step must be numeric")
        })?;

        if step == 0.0 {
            return Err(BlazeError::execution(
                "GENERATE_SERIES_FLOAT: step must not be zero",
            ));
        }

        let mut values = Vec::new();
        let mut current = start;

        if step > 0.0 {
            while current <= stop + f64::EPSILON {
                values.push(current);
                current += step;
                if values.len() > 10_000_000 {
                    return Err(BlazeError::execution(
                        "GENERATE_SERIES_FLOAT: result exceeds 10 million rows",
                    ));
                }
            }
        } else {
            while current >= stop - f64::EPSILON {
                values.push(current);
                current += step;
                if values.len() > 10_000_000 {
                    return Err(BlazeError::execution(
                        "GENERATE_SERIES_FLOAT: result exceeds 10 million rows",
                    ));
                }
            }
        }

        let array = Arc::new(Float64Array::from(values)) as ArrayRef;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "value",
            DataType::Float64,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array])?;
        Ok(vec![batch])
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_series_ascending() {
        let tvf = generate_series_tvf();
        let args = vec![ScalarArg::Int64(1), ScalarArg::Int64(5)];
        let result = tvf.execute(&args).unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 5);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(4), 5);
    }

    #[test]
    fn test_generate_series_descending() {
        let tvf = generate_series_tvf();
        let args = vec![ScalarArg::Int64(5), ScalarArg::Int64(1), ScalarArg::Int64(-1)];
        let result = tvf.execute(&args).unwrap();

        assert_eq!(result.len(), 1);
        let batch = &result[0];
        assert_eq!(batch.num_rows(), 5);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 5);
        assert_eq!(values.value(4), 1);
    }

    #[test]
    fn test_generate_series_with_step() {
        let tvf = generate_series_tvf();
        let args = vec![
            ScalarArg::Int64(0),
            ScalarArg::Int64(10),
            ScalarArg::Int64(2),
        ];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 6); // 0, 2, 4, 6, 8, 10
    }

    #[test]
    fn test_generate_series_zero_step_error() {
        let tvf = generate_series_tvf();
        let args = vec![
            ScalarArg::Int64(1),
            ScalarArg::Int64(5),
            ScalarArg::Int64(0),
        ];
        assert!(tvf.execute(&args).is_err());
    }

    #[test]
    fn test_generate_series_empty() {
        let tvf = generate_series_tvf();
        let args = vec![
            ScalarArg::Int64(5),
            ScalarArg::Int64(1),
            ScalarArg::Int64(1),
        ];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_unnest_int() {
        let tvf = unnest_int_tvf();
        let args = vec![ScalarArg::Array(vec![
            ScalarArg::Int64(10),
            ScalarArg::Int64(20),
            ScalarArg::Int64(30),
        ])];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert_eq!(values.value(1), 20);
        assert_eq!(values.value(2), 30);
    }

    #[test]
    fn test_unnest_str() {
        let tvf = unnest_str_tvf();
        let args = vec![ScalarArg::Array(vec![
            ScalarArg::Utf8("hello".to_string()),
            ScalarArg::Utf8("world".to_string()),
        ])];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2);
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "hello");
        assert_eq!(values.value(1), "world");
    }

    #[test]
    fn test_json_each_object() {
        let tvf = json_each_tvf();
        let args = vec![ScalarArg::Utf8(
            r#"{"name": "Alice", "age": 30, "active": true}"#.to_string(),
        )];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 2); // key, value
    }

    #[test]
    fn test_json_each_array() {
        let tvf = json_each_tvf();
        let args = vec![ScalarArg::Utf8(r#"[1, 2, 3]"#.to_string())];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 3);
    }

    #[test]
    fn test_json_each_invalid() {
        let tvf = json_each_tvf();
        let args = vec![ScalarArg::Utf8("not json".to_string())];
        assert!(tvf.execute(&args).is_err());
    }

    #[test]
    fn test_regexp_matches() {
        let tvf = regexp_matches_tvf();
        let args = vec![
            ScalarArg::Utf8("Hello 123 World 456".to_string()),
            ScalarArg::Utf8(r"\d+".to_string()),
        ];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 2); // "123" and "456"

        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "123");
        assert_eq!(values.value(1), "456");
    }

    #[test]
    fn test_regexp_matches_no_match() {
        let tvf = regexp_matches_tvf();
        let args = vec![
            ScalarArg::Utf8("hello world".to_string()),
            ScalarArg::Utf8(r"\d+".to_string()),
        ];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_generate_series_float() {
        let tvf = generate_series_float_tvf();
        let args = vec![
            ScalarArg::Float64(0.0),
            ScalarArg::Float64(1.0),
            ScalarArg::Float64(0.25),
        ];
        let result = tvf.execute(&args).unwrap();

        let batch = &result[0];
        assert_eq!(batch.num_rows(), 5); // 0.0, 0.25, 0.5, 0.75, 1.0
    }

    #[test]
    fn test_tvf_registry() {
        let registry = TvfRegistry::new();
        assert!(registry.has("GENERATE_SERIES"));
        assert!(registry.has("generate_series"));
        assert!(registry.has("UNNEST"));
        assert!(registry.has("JSON_EACH"));
        assert!(registry.has("REGEXP_MATCHES"));

        let names = registry.list();
        assert!(names.len() >= 5);
    }

    #[test]
    fn test_tvf_registry_custom() {
        let registry = TvfRegistry::new();

        let custom = TableFunction::new(
            "my_func",
            Arc::new(ArrowSchema::new(vec![Field::new(
                "x",
                DataType::Int64,
                false,
            )])),
            |_args| {
                let array = Arc::new(Int64Array::from(vec![42])) as ArrayRef;
                let schema = Arc::new(ArrowSchema::new(vec![Field::new(
                    "x",
                    DataType::Int64,
                    false,
                )]));
                let batch = RecordBatch::try_new(schema, vec![array])?;
                Ok(vec![batch])
            },
        );

        registry.register(custom).unwrap();
        assert!(registry.has("MY_FUNC"));

        let tvf = registry.get("my_func").unwrap();
        let result = tvf.execute(&[]).unwrap();
        assert_eq!(result[0].num_rows(), 1);
    }
}
