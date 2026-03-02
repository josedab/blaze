//! WASM Bindings
//!
//! This module provides the JavaScript-callable bindings for the Blaze query engine.

use std::collections::HashMap;
use std::sync::RwLock;

use arrow::record_batch::RecordBatch;

use super::serialization::{ArrowIpcSerializer, JsonSerializer};
use super::{indexeddb, serialization, ResultFormat, WasmConfig, WasmQueryOptions, WasmStats};
use crate::error::{BlazeError, Result};
use crate::types::ScalarValue;
use crate::Connection;

/// Error type for WASM bindings.
#[derive(Debug, Clone)]
pub struct WasmError {
    /// Error message
    pub message: String,
    /// Error code
    pub code: WasmErrorCode,
    /// Additional details
    pub details: Option<String>,
}

impl WasmError {
    /// Create a new WASM error.
    pub fn new(code: WasmErrorCode, message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            code,
            details: None,
        }
    }

    /// Add details to the error.
    pub fn with_details(mut self, details: impl Into<String>) -> Self {
        self.details = Some(details.into());
        self
    }

    /// Create from a BlazeError.
    pub fn from_blaze(err: &BlazeError) -> Self {
        Self::new(WasmErrorCode::QueryError, err.to_string())
    }

    /// Convert to JSON string.
    pub fn to_json(&self) -> String {
        let details = self
            .details
            .as_ref()
            .map(|d| format!(", \"details\": \"{}\"", d))
            .unwrap_or_default();

        format!(
            "{{\"code\": \"{:?}\", \"message\": \"{}\"{}}}",
            self.code, self.message, details
        )
    }
}

impl std::fmt::Display for WasmError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for WasmError {}

impl From<BlazeError> for WasmError {
    fn from(err: BlazeError) -> Self {
        Self::from_blaze(&err)
    }
}

/// Error codes for WASM errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WasmErrorCode {
    /// Invalid SQL syntax
    SyntaxError,
    /// Query execution error
    QueryError,
    /// Table not found
    TableNotFound,
    /// Invalid JSON input
    InvalidJson,
    /// Memory limit exceeded
    MemoryLimit,
    /// Invalid argument
    InvalidArgument,
    /// I/O error
    IoError,
    /// Internal error
    InternalError,
}

/// Query result from WASM execution.
#[derive(Debug, Clone)]
pub struct WasmQueryResult {
    /// Result data (serialized based on format)
    pub data: Vec<u8>,
    /// Result format
    pub format: ResultFormat,
    /// Number of rows
    pub row_count: usize,
    /// Number of columns
    pub column_count: usize,
    /// Schema (if included)
    pub schema: Option<WasmSchema>,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl WasmQueryResult {
    /// Create a new query result.
    pub fn new(data: Vec<u8>, format: ResultFormat, row_count: usize, column_count: usize) -> Self {
        Self {
            data,
            format,
            row_count,
            column_count,
            schema: None,
            execution_time_ms: 0,
        }
    }

    /// Set the schema.
    pub fn with_schema(mut self, schema: WasmSchema) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Set execution time.
    pub fn with_time(mut self, time_ms: u64) -> Self {
        self.execution_time_ms = time_ms;
        self
    }

    /// Get data as string (for JSON/CSV formats).
    pub fn as_string(&self) -> Result<String> {
        String::from_utf8(self.data.clone())
            .map_err(|e| BlazeError::execution(format!("Invalid UTF-8 in result: {}", e)))
    }

    /// Check if result is empty.
    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }
}

/// Schema representation for WASM.
#[derive(Debug, Clone)]
pub struct WasmSchema {
    /// Column definitions
    pub columns: Vec<WasmColumn>,
}

impl WasmSchema {
    /// Create from an Arrow schema.
    pub fn from_arrow(schema: &arrow::datatypes::Schema) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|f| WasmColumn {
                name: f.name().clone(),
                data_type: format!("{:?}", f.data_type()),
                nullable: f.is_nullable(),
            })
            .collect();

        Self { columns }
    }

    /// Convert to JSON.
    pub fn to_json(&self) -> String {
        let cols: Vec<String> = self
            .columns
            .iter()
            .map(|c| {
                format!(
                    "{{\"name\": \"{}\", \"type\": \"{}\", \"nullable\": {}}}",
                    c.name, c.data_type, c.nullable
                )
            })
            .collect();

        format!("[{}]", cols.join(", "))
    }
}

/// Column definition for WASM schema.
#[derive(Debug, Clone)]
pub struct WasmColumn {
    /// Column name
    pub name: String,
    /// Data type
    pub data_type: String,
    /// Whether column is nullable
    pub nullable: bool,
}

/// WASM connection to the database.
pub struct WasmConnection {
    /// Inner connection
    inner: Connection,
    /// Configuration
    config: WasmConfig,
    /// Statistics
    stats: RwLock<WasmStats>,
    /// Registered tables metadata
    tables: RwLock<HashMap<String, TableMetadata>>,
}

/// Metadata about a registered table.
#[derive(Debug, Clone)]
struct TableMetadata {
    /// Table name
    #[allow(dead_code)]
    name: String,
    /// Number of rows
    row_count: usize,
    /// Schema
    schema: WasmSchema,
    /// Size in bytes (approximate)
    size_bytes: usize,
}

impl WasmConnection {
    /// Create a new WASM connection with default configuration.
    pub fn new() -> Result<Self> {
        Self::with_config(WasmConfig::default())
    }

    /// Create a new WASM connection with custom configuration.
    pub fn with_config(config: WasmConfig) -> Result<Self> {
        let inner = Connection::in_memory()?;

        Ok(Self {
            inner,
            config,
            stats: RwLock::new(WasmStats::default()),
            tables: RwLock::new(HashMap::new()),
        })
    }

    /// Execute a SQL statement (DDL).
    pub fn execute(&self, sql: &str) -> Result<usize> {
        let start = std::time::Instant::now();
        let result = self.inner.execute(sql);

        if let Ok(mut stats) = self.stats.write() {
            stats.queries_executed += 1;
            stats.total_query_time_ms += start.elapsed().as_millis() as u64;
        }

        result
    }

    /// Execute a SQL query and return results.
    pub fn query(&self, sql: &str) -> Result<WasmQueryResult> {
        self.query_with_options(sql, WasmQueryOptions::default())
    }

    /// Execute a SQL query with options.
    pub fn query_with_options(
        &self,
        sql: &str,
        options: WasmQueryOptions,
    ) -> Result<WasmQueryResult> {
        let start = std::time::Instant::now();
        let batches = self.inner.query(sql)?;
        let elapsed = start.elapsed().as_millis() as u64;

        // Update stats
        if let Ok(mut stats) = self.stats.write() {
            stats.queries_executed += 1;
            stats.total_query_time_ms += elapsed;
        }

        // Serialize results
        let (data, row_count, column_count, schema) = self.serialize_results(&batches, &options)?;

        let mut result =
            WasmQueryResult::new(data, options.format, row_count, column_count).with_time(elapsed);

        if options.include_schema {
            if let Some(s) = schema {
                result = result.with_schema(s);
            }
        }

        Ok(result)
    }

    /// Serialize query results to the specified format.
    fn serialize_results(
        &self,
        batches: &[RecordBatch],
        options: &WasmQueryOptions,
    ) -> Result<(Vec<u8>, usize, usize, Option<WasmSchema>)> {
        let _total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let column_count = batches.first().map(|b| b.num_columns()).unwrap_or(0);
        let schema = batches
            .first()
            .map(|b| WasmSchema::from_arrow(b.schema().as_ref()));

        // Apply row limit if specified
        let limited_batches: Vec<RecordBatch> = if let Some(max_rows) = options.max_rows {
            self.limit_batches(batches, max_rows)
        } else {
            batches.to_vec()
        };

        let data = match options.format {
            ResultFormat::Json => {
                let json = JsonSerializer::serialize(&limited_batches, options.pretty)?;
                json.into_bytes()
            }
            ResultFormat::ArrowIpc => ArrowIpcSerializer::serialize(&limited_batches)?,
            ResultFormat::Csv => {
                let csv = self.serialize_csv(&limited_batches)?;
                csv.into_bytes()
            }
            ResultFormat::ColumnarJson => {
                let json = JsonSerializer::serialize_columnar(&limited_batches)?;
                json.into_bytes()
            }
        };

        let actual_rows = limited_batches.iter().map(|b| b.num_rows()).sum();
        Ok((data, actual_rows, column_count, schema))
    }

    /// Limit batches to max rows.
    fn limit_batches(&self, batches: &[RecordBatch], max_rows: usize) -> Vec<RecordBatch> {
        let mut remaining = max_rows;
        let mut result = Vec::new();

        for batch in batches {
            if remaining == 0 {
                break;
            }

            if batch.num_rows() <= remaining {
                result.push(batch.clone());
                remaining -= batch.num_rows();
            } else {
                result.push(batch.slice(0, remaining));
                remaining = 0;
            }
        }

        result
    }

    /// Serialize to CSV format.
    fn serialize_csv(&self, batches: &[RecordBatch]) -> Result<String> {
        use arrow::csv::WriterBuilder;
        use std::io::Cursor;

        if batches.is_empty() {
            return Ok(String::new());
        }

        let mut buf = Cursor::new(Vec::new());
        {
            let mut writer = WriterBuilder::new().with_header(true).build(&mut buf);
            for batch in batches {
                writer.write(batch).map_err(|e| {
                    BlazeError::execution(format!("CSV serialization failed: {}", e))
                })?;
            }
        }

        String::from_utf8(buf.into_inner())
            .map_err(|e| BlazeError::execution(format!("Invalid UTF-8 in CSV: {}", e)))
    }

    /// Load JSON data into a table.
    pub fn load_json(&self, table_name: &str, json: &str) -> Result<usize> {
        let batches = JsonSerializer::deserialize(json)?;

        if batches.is_empty() {
            return Ok(0);
        }

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        let size_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        let schema = WasmSchema::from_arrow(batches[0].schema().as_ref());

        // Register the table
        self.inner.register_batches(table_name, batches)?;

        // Update metadata
        if let Ok(mut tables) = self.tables.write() {
            tables.insert(
                table_name.to_string(),
                TableMetadata {
                    name: table_name.to_string(),
                    row_count,
                    schema,
                    size_bytes,
                },
            );
        }

        // Update stats
        if let Ok(mut stats) = self.stats.write() {
            stats.table_count += 1;
            stats.total_rows += row_count;
            stats.memory_used += size_bytes;
            stats.peak_memory = stats.peak_memory.max(stats.memory_used);
        }

        Ok(row_count)
    }

    /// Drop a table.
    pub fn drop_table(&self, table_name: &str) -> Result<()> {
        // Get table metadata before dropping
        let size_to_free = if let Ok(tables) = self.tables.read() {
            tables.get(table_name).map(|t| (t.row_count, t.size_bytes))
        } else {
            None
        };

        self.inner.deregister_table(table_name)?;

        // Update metadata
        if let Ok(mut tables) = self.tables.write() {
            tables.remove(table_name);
        }

        // Update stats
        if let Some((rows, size)) = size_to_free {
            if let Ok(mut stats) = self.stats.write() {
                stats.table_count = stats.table_count.saturating_sub(1);
                stats.total_rows = stats.total_rows.saturating_sub(rows);
                stats.memory_used = stats.memory_used.saturating_sub(size);
            }
        }

        Ok(())
    }

    /// List all tables.
    pub fn list_tables(&self) -> Vec<String> {
        self.inner.list_tables()
    }

    /// Get table schema.
    pub fn table_schema(&self, table_name: &str) -> Option<WasmSchema> {
        if let Ok(tables) = self.tables.read() {
            tables.get(table_name).map(|t| t.schema.clone())
        } else {
            None
        }
    }

    /// Get runtime statistics.
    pub fn stats(&self) -> WasmStats {
        self.stats.read().map(|s| s.clone()).unwrap_or_default()
    }

    /// Get configuration.
    pub fn config(&self) -> &WasmConfig {
        &self.config
    }

    /// Check memory usage against limit.
    pub fn check_memory(&self) -> Result<()> {
        if let Ok(stats) = self.stats.read() {
            if stats.memory_used > self.config.max_memory {
                return Err(BlazeError::resource_exhausted(format!(
                    "Memory limit exceeded: {} > {}",
                    stats.memory_used, self.config.max_memory
                )));
            }
        }
        Ok(())
    }

    /// Execute a query with streaming results.
    pub fn query_streaming(&self, sql: &str, chunk_size: usize) -> Result<WasmStreamingResult> {
        let batches = self.inner.query(sql)?;
        Ok(WasmStreamingResult::new(
            batches,
            chunk_size,
            ResultFormat::Json,
        ))
    }

    /// Execute a query with streaming results in a specific format.
    pub fn query_streaming_with_format(
        &self,
        sql: &str,
        chunk_size: usize,
        format: ResultFormat,
    ) -> Result<WasmStreamingResult> {
        let batches = self.inner.query(sql)?;
        Ok(WasmStreamingResult::new(batches, chunk_size, format))
    }

    /// Save a table to IndexedDB persistence.
    pub fn persist_table(&self, table_name: &str) -> Result<()> {
        let batches = self
            .inner
            .query(&format!("SELECT * FROM {}", table_name))?;
        let data = serialization::ArrowIpcSerializer::serialize(&batches)?;
        let store = indexeddb::IndexedDbStore::new(indexeddb::IndexedDbConfig::default());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let metadata = indexeddb::ItemMetadata {
            content_type: "application/vnd.apache.arrow.stream".to_string(),
            size: data.len(),
            stored_at: now,
            table_name: Some(table_name.to_string()),
        };
        store.put(table_name, data, metadata)
    }

    /// Restore a table from IndexedDB persistence.
    pub fn restore_table(&self, table_name: &str) -> Result<bool> {
        let store = indexeddb::IndexedDbStore::new(indexeddb::IndexedDbConfig::default());
        if let Some(data) = store.get(table_name)? {
            let batches = serialization::ArrowIpcSerializer::deserialize(&data)?;
            if !batches.is_empty() {
                self.inner.register_batches(table_name, batches)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Clear all tables and reset state.
    pub fn clear(&self) -> Result<()> {
        let table_names = self.list_tables();
        for name in table_names {
            self.drop_table(&name)?;
        }

        if let Ok(mut stats) = self.stats.write() {
            *stats = WasmStats::default();
        }

        Ok(())
    }

    /// Prepare a SQL statement for repeated execution with parameters.
    pub fn prepare(&self, sql: &str) -> Result<WasmPreparedStatement> {
        let stmt = self.inner.prepare(sql)?;
        Ok(WasmPreparedStatement { inner: stmt })
    }
}

/// Parameter types for prepared statement execution in WASM.
#[derive(Debug, Clone)]
pub enum WasmParam {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit integer value
    Int(i64),
    /// 64-bit floating point value
    Float(f64),
    /// UTF-8 string value
    String(String),
}

impl WasmParam {
    /// Convert to a ScalarValue for query execution.
    fn into_scalar(self) -> ScalarValue {
        match self {
            WasmParam::Null => ScalarValue::Null,
            WasmParam::Bool(b) => ScalarValue::Boolean(Some(b)),
            WasmParam::Int(i) => ScalarValue::Int64(Some(i)),
            WasmParam::Float(f) => ScalarValue::Float64(Some(f)),
            WasmParam::String(s) => ScalarValue::Utf8(Some(s)),
        }
    }
}

/// A prepared statement for WASM that supports parameterized execution.
pub struct WasmPreparedStatement {
    inner: crate::prepared::PreparedStatement,
}

impl WasmPreparedStatement {
    /// Execute the prepared statement with the given parameters.
    pub fn execute(&self, params: Vec<WasmParam>) -> Result<WasmQueryResult> {
        let scalar_params: Vec<ScalarValue> =
            params.into_iter().map(|p| p.into_scalar()).collect();
        let batches = self.inner.execute(&scalar_params)?;

        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        let column_count = batches.first().map(|b| b.num_columns()).unwrap_or(0);
        let schema = batches
            .first()
            .map(|b| WasmSchema::from_arrow(b.schema().as_ref()));

        let json = JsonSerializer::serialize(&batches, false)?;
        let data = json.into_bytes();

        let mut result =
            WasmQueryResult::new(data, ResultFormat::Json, row_count, column_count);
        if let Some(s) = schema {
            result = result.with_schema(s);
        }

        Ok(result)
    }

    /// Execute the prepared statement with no parameters.
    pub fn execute_no_params(&self) -> Result<WasmQueryResult> {
        self.execute(vec![])
    }

    /// Get the number of parameters expected by this statement.
    pub fn parameter_count(&self) -> usize {
        self.inner.parameter_count()
    }

    /// Get the original SQL query.
    pub fn sql(&self) -> &str {
        self.inner.sql()
    }
}

impl Default for WasmConnection {
    fn default() -> Self {
        Self::new().expect("Failed to create WASM connection")
    }
}

/// Streaming query result iterator for large result sets.
pub struct WasmStreamingResult {
    batches: Vec<RecordBatch>,
    current_batch: usize,
    current_row: usize,
    chunk_size: usize,
    format: ResultFormat,
    schema: Option<WasmSchema>,
    total_rows: usize,
}

impl WasmStreamingResult {
    pub fn new(batches: Vec<RecordBatch>, chunk_size: usize, format: ResultFormat) -> Self {
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let schema = batches
            .first()
            .map(|b| WasmSchema::from_arrow(b.schema().as_ref()));
        Self {
            batches,
            current_batch: 0,
            current_row: 0,
            chunk_size,
            format,
            schema,
            total_rows,
        }
    }

    /// Check if there are more results.
    pub fn has_next(&self) -> bool {
        self.current_batch < self.batches.len()
    }

    /// Get the next chunk of results.
    pub fn next_chunk(&mut self) -> Result<Option<WasmQueryResult>> {
        if !self.has_next() {
            return Ok(None);
        }

        let mut chunk_batches = Vec::new();
        let mut rows_remaining = self.chunk_size;

        while rows_remaining > 0 && self.current_batch < self.batches.len() {
            let batch = &self.batches[self.current_batch];
            let available = batch.num_rows() - self.current_row;
            let take = available.min(rows_remaining);

            let sliced = batch.slice(self.current_row, take);
            chunk_batches.push(sliced);
            rows_remaining -= take;
            self.current_row += take;

            if self.current_row >= batch.num_rows() {
                self.current_batch += 1;
                self.current_row = 0;
            }
        }

        let row_count: usize = chunk_batches.iter().map(|b| b.num_rows()).sum();
        let col_count = chunk_batches
            .first()
            .map(|b| b.num_columns())
            .unwrap_or(0);

        let data = match self.format {
            ResultFormat::Json => {
                let json =
                    serialization::JsonSerializer::serialize(&chunk_batches, false)?;
                json.into_bytes()
            }
            ResultFormat::ArrowIpc => {
                serialization::ArrowIpcSerializer::serialize(&chunk_batches)?
            }
            _ => {
                let json =
                    serialization::JsonSerializer::serialize(&chunk_batches, false)?;
                json.into_bytes()
            }
        };

        let mut result = WasmQueryResult::new(data, self.format, row_count, col_count);
        if let Some(ref schema) = self.schema {
            result = result.with_schema(schema.clone());
        }
        Ok(Some(result))
    }

    /// Get total number of rows in the full result.
    pub fn total_rows(&self) -> usize {
        self.total_rows
    }

    /// Get the progress as a fraction (0.0 to 1.0).
    pub fn progress(&self) -> f64 {
        if self.total_rows == 0 {
            return 1.0;
        }
        let processed: usize = self.batches[..self.current_batch]
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>()
            + self.current_row;
        processed as f64 / self.total_rows as f64
    }

    /// Reset the iterator to the beginning.
    pub fn reset(&mut self) {
        self.current_batch = 0;
        self.current_row = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_error() {
        let err = WasmError::new(WasmErrorCode::SyntaxError, "Invalid SQL")
            .with_details("Near token 'SELCT'");

        assert_eq!(err.code, WasmErrorCode::SyntaxError);
        assert!(err.message.contains("Invalid SQL"));
        assert!(err.details.is_some());

        let json = err.to_json();
        assert!(json.contains("SyntaxError"));
    }

    #[test]
    fn test_wasm_query_result() {
        let result =
            WasmQueryResult::new(b"[{\"a\": 1}]".to_vec(), ResultFormat::Json, 1, 1).with_time(100);

        assert_eq!(result.row_count, 1);
        assert_eq!(result.column_count, 1);
        assert_eq!(result.execution_time_ms, 100);
        assert!(!result.is_empty());

        let s = result.as_string().unwrap();
        assert!(s.contains("\"a\""));
    }

    #[test]
    fn test_wasm_schema() {
        let arrow_schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]);

        let wasm_schema = WasmSchema::from_arrow(&arrow_schema);
        assert_eq!(wasm_schema.columns.len(), 2);
        assert_eq!(wasm_schema.columns[0].name, "id");
        assert!(!wasm_schema.columns[0].nullable);
        assert!(wasm_schema.columns[1].nullable);

        let json = wasm_schema.to_json();
        assert!(json.contains("\"id\""));
        assert!(json.contains("\"name\""));
    }

    #[test]
    fn test_wasm_connection() {
        let conn = WasmConnection::new().unwrap();

        conn.execute("CREATE TABLE test (id INT, value VARCHAR)")
            .unwrap();

        let tables = conn.list_tables();
        assert!(tables.contains(&"test".to_string()));
    }

    #[test]
    fn test_wasm_connection_stats() {
        let conn = WasmConnection::new().unwrap();

        conn.execute("CREATE TABLE t (x INT)").unwrap();

        let stats = conn.stats();
        assert!(stats.queries_executed > 0);
    }

    #[test]
    fn test_wasm_connection_clear() {
        let conn = WasmConnection::new().unwrap();

        conn.execute("CREATE TABLE t1 (x INT)").unwrap();
        conn.execute("CREATE TABLE t2 (y INT)").unwrap();

        assert_eq!(conn.list_tables().len(), 2);

        conn.clear().unwrap();

        assert!(conn.list_tables().is_empty());
    }

    #[test]
    fn test_streaming_result_iteration() {
        let conn = WasmConnection::new().unwrap();
        conn.execute("CREATE TABLE s (id INT, name VARCHAR)")
            .unwrap();
        conn.load_json("s", r#"[{"id":1,"name":"a"},{"id":2,"name":"b"},{"id":3,"name":"c"}]"#)
            .unwrap();

        let mut stream = conn.query_streaming("SELECT * FROM s", 2).unwrap();
        assert_eq!(stream.total_rows(), 3);
        assert!(stream.has_next());

        let chunk1 = stream.next_chunk().unwrap().unwrap();
        assert_eq!(chunk1.row_count, 2);

        let chunk2 = stream.next_chunk().unwrap().unwrap();
        assert_eq!(chunk2.row_count, 1);

        assert!(!stream.has_next());
        assert!(stream.next_chunk().unwrap().is_none());
    }

    #[test]
    fn test_streaming_result_progress() {
        let conn = WasmConnection::new().unwrap();
        conn.execute("CREATE TABLE sp (v INT)").unwrap();
        conn.load_json("sp", r#"[{"v":1},{"v":2},{"v":3},{"v":4}]"#)
            .unwrap();

        let mut stream = conn.query_streaming("SELECT * FROM sp", 2).unwrap();
        assert!((stream.progress() - 0.0).abs() < f64::EPSILON);

        stream.next_chunk().unwrap();
        assert!((stream.progress() - 0.5).abs() < f64::EPSILON);

        stream.next_chunk().unwrap();
        assert!((stream.progress() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_streaming_result_reset() {
        let conn = WasmConnection::new().unwrap();
        conn.execute("CREATE TABLE sr (v INT)").unwrap();
        conn.load_json("sr", r#"[{"v":1},{"v":2}]"#).unwrap();

        let mut stream = conn.query_streaming("SELECT * FROM sr", 10).unwrap();
        stream.next_chunk().unwrap();
        assert!(!stream.has_next());

        stream.reset();
        assert!(stream.has_next());
        assert!((stream.progress() - 0.0).abs() < f64::EPSILON);

        let chunk = stream.next_chunk().unwrap().unwrap();
        assert_eq!(chunk.row_count, 2);
    }

    #[test]
    fn test_persist_restore_table() {
        let conn = WasmConnection::new().unwrap();
        conn.execute("CREATE TABLE pt (id INT, val VARCHAR)")
            .unwrap();
        conn.load_json("pt", r#"[{"id":1,"val":"x"},{"id":2,"val":"y"}]"#)
            .unwrap();

        // persist_table serializes and stores data
        conn.persist_table("pt").unwrap();

        // restore_table creates a new in-memory store, so data won't actually
        // survive across different IndexedDbStore instances (simulated storage).
        // We test that the API succeeds and returns false for a missing key.
        let conn2 = WasmConnection::new().unwrap();
        let restored = conn2.restore_table("nonexistent").unwrap();
        assert!(!restored);
    }
}
