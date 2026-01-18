//! Delta Lake Table Format Support
//!
//! This module provides read support for Delta Lake tables.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{DataFileInfo, LakehouseTable, Operation, TableProperties, TableVersion};
use crate::catalog::TableProvider;
use crate::error::{BlazeError, Result};
use crate::storage::ParquetTable;
use crate::types::{DataType, Field, Schema, TimeUnit};
use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

/// Delta Lake table.
pub struct DeltaTable {
    /// Table location
    location: PathBuf,
    /// Table name
    name: String,
    /// Current schema
    schema: Schema,
    /// Arrow schema
    arrow_schema: Arc<ArrowSchema>,
    /// Delta log
    log: DeltaLog,
    /// Table properties
    properties: TableProperties,
    /// Partition columns
    partition_columns: Vec<String>,
}

impl DeltaTable {
    /// Open a Delta Lake table from the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let location = path.to_path_buf();

        // Check for _delta_log directory
        let delta_log_path = path.join("_delta_log");
        if !delta_log_path.exists() {
            return Err(BlazeError::invalid_argument(format!(
                "No _delta_log directory found at '{}'",
                path.display()
            )));
        }

        // Load the delta log
        let log = DeltaLog::load(&delta_log_path)?;

        // Get schema from the latest version
        let schema = log
            .current_schema()
            .cloned()
            .unwrap_or_else(|| Schema::new(vec![Field::new("_placeholder", DataType::Utf8, true)]));

        let arrow_schema = Arc::new(schema.to_arrow());

        // Extract table name from path
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("delta_table")
            .to_string();

        // Get partition columns
        let partition_columns = log.partition_columns().to_vec();

        // Build properties
        let properties = TableProperties::new(location.to_string_lossy(), "delta")
            .with_format_version(log.protocol_version().to_string())
            .with_num_files(log.data_files().len());

        Ok(Self {
            location,
            name,
            schema,
            arrow_schema,
            log,
            properties,
            partition_columns,
        })
    }

    /// Open with custom options.
    pub fn open_with_options(path: impl AsRef<Path>, options: DeltaTableOptions) -> Result<Self> {
        let mut table = Self::open(path)?;

        // Apply options
        if let Some(version) = options.version {
            table.log = table.log.at_version(version)?;
            table.schema = table.log.current_schema().cloned().unwrap_or(table.schema);
            table.arrow_schema = Arc::new(table.schema.to_arrow());
        }

        Ok(table)
    }

    /// Get the table location.
    pub fn location(&self) -> &Path {
        &self.location
    }

    /// Get the delta log.
    pub fn log(&self) -> &DeltaLog {
        &self.log
    }

    /// Get the list of data files.
    pub fn data_files(&self) -> &[DataFileInfo] {
        self.log.data_files()
    }

    /// Time travel to a specific version.
    pub fn at_version(&self, version: i64) -> Result<Self> {
        Self::open_with_options(
            &self.location,
            DeltaTableOptions {
                version: Some(version),
                ..Default::default()
            },
        )
    }

    /// Time travel to a specific timestamp.
    pub fn as_of_timestamp(&self, timestamp_ms: i64) -> Result<Self> {
        let version = self.log.version_at_timestamp(timestamp_ms)?;
        self.at_version(version)
    }

    /// Get table history.
    pub fn history(&self) -> Result<Vec<DeltaLogEntry>> {
        self.log.history()
    }
}

impl LakehouseTable for DeltaTable {
    fn name(&self) -> &str {
        &self.name
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn arrow_schema(&self) -> Arc<ArrowSchema> {
        self.arrow_schema.clone()
    }

    fn current_version(&self) -> TableVersion {
        TableVersion::Version(self.log.version())
    }

    fn list_versions(&self) -> Result<Vec<TableVersion>> {
        Ok((0..=self.log.version())
            .map(TableVersion::Version)
            .collect())
    }

    fn read_all(&self) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for file_info in self.log.data_files() {
            let file_path = self.location.join(&file_info.path);
            if file_path.exists() {
                let parquet = ParquetTable::open(&file_path)?;
                let batches = parquet.scan(None, &[], None)?;
                all_batches.extend(batches);
            }
        }

        Ok(all_batches)
    }

    fn read_version(&self, version: TableVersion) -> Result<Vec<RecordBatch>> {
        let v = version.as_version().ok_or_else(|| {
            BlazeError::invalid_argument("Delta Lake requires version number, not snapshot ID")
        })?;

        let table_at_version = self.at_version(v)?;
        table_at_version.read_all()
    }

    fn read_as_of(&self, timestamp_ms: i64) -> Result<Vec<RecordBatch>> {
        let table_at_time = self.as_of_timestamp(timestamp_ms)?;
        table_at_time.read_all()
    }

    fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    fn properties(&self) -> &TableProperties {
        &self.properties
    }
}

/// Options for opening a Delta table.
#[derive(Debug, Clone, Default)]
pub struct DeltaTableOptions {
    /// Specific version to load
    pub version: Option<i64>,
    /// Load as of timestamp
    pub timestamp_ms: Option<i64>,
    /// Custom storage options
    pub storage_options: HashMap<String, String>,
}

impl DeltaTableOptions {
    /// Create new options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set version.
    pub fn with_version(mut self, version: i64) -> Self {
        self.version = Some(version);
        self
    }

    /// Set timestamp.
    pub fn with_timestamp(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }
}

/// Delta Lake transaction log.
#[derive(Debug, Clone)]
pub struct DeltaLog {
    /// Log directory path
    path: PathBuf,
    /// Current version
    version: i64,
    /// Protocol version
    protocol: DeltaProtocol,
    /// Current metadata
    metadata: Option<DeltaMetadata>,
    /// Active data files
    data_files: Vec<DataFileInfo>,
    /// All log entries
    entries: Vec<DeltaLogEntry>,
}

impl DeltaLog {
    /// Load delta log from directory.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Find all JSON log files
        let mut versions: Vec<i64> = Vec::new();
        if let Ok(entries) = fs::read_dir(&path) {
            for entry in entries.flatten() {
                let file_name = entry.file_name();
                let name = file_name.to_string_lossy();
                if name.ends_with(".json") {
                    if let Ok(v) = name.trim_end_matches(".json").parse::<i64>() {
                        versions.push(v);
                    }
                }
            }
        }

        versions.sort();

        let version = versions.last().copied().unwrap_or(-1);

        // Parse log files to build state
        let mut protocol = DeltaProtocol::default();
        let mut metadata = None;
        let mut data_files = Vec::new();
        let mut entries = Vec::new();

        for v in &versions {
            let log_file = path.join(format!("{:020}.json", v));
            if let Ok(content) = fs::read_to_string(&log_file) {
                let entry = DeltaLogEntry::parse(*v, &content)?;

                if let Some(p) = &entry.protocol {
                    protocol = p.clone();
                }
                if let Some(m) = &entry.metadata {
                    metadata = Some(m.clone());
                }

                // Track file adds/removes
                for add in &entry.adds {
                    data_files
                        .push(DataFileInfo::new(&add.path, "parquet").with_size(add.size as usize));
                }
                for remove in &entry.removes {
                    data_files.retain(|f| f.path != remove.path);
                }

                entries.push(entry);
            }
        }

        Ok(Self {
            path,
            version,
            protocol,
            metadata,
            data_files,
            entries,
        })
    }

    /// Get log at specific version.
    pub fn at_version(&self, target_version: i64) -> Result<Self> {
        if target_version > self.version {
            return Err(BlazeError::invalid_argument(format!(
                "Version {} does not exist (latest: {})",
                target_version, self.version
            )));
        }

        let mut log = self.clone();
        log.version = target_version;

        // Rebuild state up to target version
        log.data_files.clear();
        for entry in &log.entries {
            if entry.version > target_version {
                break;
            }

            if let Some(m) = &entry.metadata {
                log.metadata = Some(m.clone());
            }

            for add in &entry.adds {
                log.data_files
                    .push(DataFileInfo::new(&add.path, "parquet").with_size(add.size as usize));
            }
            for remove in &entry.removes {
                log.data_files.retain(|f| f.path != remove.path);
            }
        }

        Ok(log)
    }

    /// Get version at timestamp.
    pub fn version_at_timestamp(&self, timestamp_ms: i64) -> Result<i64> {
        for entry in self.entries.iter().rev() {
            if entry.timestamp <= timestamp_ms {
                return Ok(entry.version);
            }
        }

        Err(BlazeError::invalid_argument(format!(
            "No version found for timestamp {}",
            timestamp_ms
        )))
    }

    /// Get current version.
    pub fn version(&self) -> i64 {
        self.version
    }

    /// Get protocol version.
    pub fn protocol_version(&self) -> i32 {
        self.protocol.min_reader_version
    }

    /// Get current schema.
    pub fn current_schema(&self) -> Option<&Schema> {
        self.metadata.as_ref().map(|m| &m.schema)
    }

    /// Get data files.
    pub fn data_files(&self) -> &[DataFileInfo] {
        &self.data_files
    }

    /// Get partition columns.
    pub fn partition_columns(&self) -> &[String] {
        self.metadata
            .as_ref()
            .map(|m| m.partition_columns.as_slice())
            .unwrap_or(&[])
    }

    /// Get table history.
    pub fn history(&self) -> Result<Vec<DeltaLogEntry>> {
        Ok(self.entries.clone())
    }
}

/// Delta log entry.
#[derive(Debug, Clone)]
pub struct DeltaLogEntry {
    /// Version number
    pub version: i64,
    /// Timestamp
    pub timestamp: i64,
    /// Operation
    pub operation: Operation,
    /// Protocol (if present)
    pub protocol: Option<DeltaProtocol>,
    /// Metadata (if present)
    pub metadata: Option<DeltaMetadata>,
    /// Added files
    pub adds: Vec<DeltaAdd>,
    /// Removed files
    pub removes: Vec<DeltaRemove>,
}

impl DeltaLogEntry {
    /// Parse log entry from JSON content.
    pub fn parse(version: i64, content: &str) -> Result<Self> {
        let mut protocol = None;
        let mut metadata = None;
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        let mut timestamp = 0i64;
        let mut operation = Operation::Unknown("UNKNOWN".to_string());

        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Parse JSON line
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(line) {
                if let Some(p) = value.get("protocol") {
                    protocol = Some(DeltaProtocol {
                        min_reader_version: p
                            .get("minReaderVersion")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(1) as i32,
                        min_writer_version: p
                            .get("minWriterVersion")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(2) as i32,
                    });
                }

                if let Some(m) = value.get("metaData") {
                    let schema =
                        if let Some(schema_str) = m.get("schemaString").and_then(|s| s.as_str()) {
                            parse_delta_schema(schema_str).unwrap_or_else(|_| {
                                Schema::new(vec![Field::new("_placeholder", DataType::Utf8, true)])
                            })
                        } else {
                            Schema::new(vec![Field::new("_placeholder", DataType::Utf8, true)])
                        };

                    let partition_columns = m
                        .get("partitionColumns")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();

                    metadata = Some(DeltaMetadata {
                        id: m
                            .get("id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string(),
                        name: m.get("name").and_then(|v| v.as_str()).map(String::from),
                        schema,
                        partition_columns,
                        created_time: m.get("createdTime").and_then(|v| v.as_i64()),
                    });
                }

                if let Some(a) = value.get("add") {
                    let path = a
                        .get("path")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let size = a.get("size").and_then(|v| v.as_i64()).unwrap_or(0);

                    adds.push(DeltaAdd { path, size });
                }

                if let Some(r) = value.get("remove") {
                    let path = r
                        .get("path")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    removes.push(DeltaRemove { path });
                }

                if let Some(txn) = value.get("commitInfo") {
                    timestamp = txn.get("timestamp").and_then(|v| v.as_i64()).unwrap_or(0);

                    if let Some(op) = txn.get("operation").and_then(|v| v.as_str()) {
                        operation = Operation::from_str(op);
                    }
                }
            }
        }

        Ok(Self {
            version,
            timestamp,
            operation,
            protocol,
            metadata,
            adds,
            removes,
        })
    }
}

/// Delta protocol information.
#[derive(Debug, Clone, Default)]
pub struct DeltaProtocol {
    /// Minimum reader version
    pub min_reader_version: i32,
    /// Minimum writer version
    pub min_writer_version: i32,
}

/// Delta metadata.
#[derive(Debug, Clone)]
pub struct DeltaMetadata {
    /// Table ID
    pub id: String,
    /// Table name
    pub name: Option<String>,
    /// Table schema
    pub schema: Schema,
    /// Partition columns
    pub partition_columns: Vec<String>,
    /// Created timestamp
    pub created_time: Option<i64>,
}

/// Delta add action.
#[derive(Debug, Clone)]
pub struct DeltaAdd {
    /// File path
    pub path: String,
    /// File size
    pub size: i64,
}

/// Delta remove action.
#[derive(Debug, Clone)]
pub struct DeltaRemove {
    /// File path
    pub path: String,
}

/// Parse Delta Lake schema string.
fn parse_delta_schema(schema_str: &str) -> Result<Schema> {
    let value: serde_json::Value = serde_json::from_str(schema_str)
        .map_err(|e| BlazeError::execution(format!("Failed to parse schema: {}", e)))?;

    let fields = value
        .get("fields")
        .and_then(|f| f.as_array())
        .ok_or_else(|| BlazeError::execution("Invalid schema: no fields"))?;

    let mut result_fields = Vec::new();
    for field in fields {
        let name = field
            .get("name")
            .and_then(|n| n.as_str())
            .unwrap_or("_unknown")
            .to_string();

        let nullable = field
            .get("nullable")
            .and_then(|n| n.as_bool())
            .unwrap_or(true);

        let data_type = parse_delta_type(field.get("type"))?;

        result_fields.push(Field::new(&name, data_type, nullable));
    }

    Ok(Schema::new(result_fields))
}

/// Parse Delta Lake data type.
fn parse_delta_type(type_value: Option<&serde_json::Value>) -> Result<DataType> {
    match type_value {
        Some(serde_json::Value::String(s)) => Ok(match s.as_str() {
            "string" => DataType::Utf8,
            "long" | "bigint" => DataType::Int64,
            "integer" | "int" => DataType::Int32,
            "short" | "smallint" => DataType::Int16,
            "byte" | "tinyint" => DataType::Int8,
            "float" => DataType::Float32,
            "double" => DataType::Float64,
            "boolean" => DataType::Boolean,
            "binary" => DataType::Binary,
            "date" => DataType::Date32,
            "timestamp" => DataType::Timestamp {
                unit: TimeUnit::Microsecond,
                timezone: None,
            },
            _ => DataType::Utf8,
        }),
        Some(serde_json::Value::Object(obj)) => {
            // Complex type
            if let Some(element_type) = obj.get("elementType") {
                // Array type
                let inner = parse_delta_type(Some(element_type))?;
                Ok(DataType::List(Box::new(inner)))
            } else {
                Ok(DataType::Utf8) // Default for unknown complex types
            }
        }
        _ => Ok(DataType::Utf8),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_table_options() {
        let opts = DeltaTableOptions::new().with_version(5);

        assert_eq!(opts.version, Some(5));
    }

    #[test]
    fn test_delta_protocol() {
        let protocol = DeltaProtocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        assert_eq!(protocol.min_reader_version, 1);
        assert_eq!(protocol.min_writer_version, 2);
    }

    #[test]
    fn test_parse_delta_type() {
        assert_eq!(
            parse_delta_type(Some(&serde_json::json!("string"))).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            parse_delta_type(Some(&serde_json::json!("long"))).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            parse_delta_type(Some(&serde_json::json!("integer"))).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            parse_delta_type(Some(&serde_json::json!("double"))).unwrap(),
            DataType::Float64
        );
    }
}
