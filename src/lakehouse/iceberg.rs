//! Apache Iceberg Table Format Support
//!
//! This module provides read support for Apache Iceberg tables.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use super::{DataFileInfo, LakehouseTable, Operation, SnapshotId, TableProperties, TableVersion};
use crate::catalog::TableProvider;
use crate::error::{BlazeError, Result};
use crate::storage::ParquetTable;
use crate::types::{DataType, Field, Schema, TimeUnit};

/// Apache Iceberg table.
pub struct IcebergTable {
    /// Table location
    location: PathBuf,
    /// Table name
    name: String,
    /// Current schema
    schema: Schema,
    /// Arrow schema
    arrow_schema: Arc<ArrowSchema>,
    /// Current snapshot
    current_snapshot: Option<IcebergSnapshot>,
    /// All snapshots
    snapshots: Vec<IcebergSnapshot>,
    /// Table properties
    properties: TableProperties,
    /// Partition spec
    partition_columns: Vec<String>,
    /// Data files
    data_files: Vec<DataFileInfo>,
}

impl IcebergTable {
    /// Open an Iceberg table from the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let location = path.to_path_buf();

        // Check for metadata directory
        let metadata_path = path.join("metadata");
        if !metadata_path.exists() {
            return Err(BlazeError::invalid_argument(format!(
                "No metadata directory found at '{}'",
                path.display()
            )));
        }

        // Find the latest metadata file (version-hint.text or latest metadata.json)
        let metadata = Self::load_metadata(&metadata_path)?;

        // Extract table name from path
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("iceberg_table")
            .to_string();

        // Build schema from metadata
        let schema = metadata.schema.clone();
        let arrow_schema = Arc::new(schema.to_arrow());

        // Build properties
        let properties = TableProperties::new(location.to_string_lossy(), "iceberg")
            .with_format_version(metadata.format_version.to_string())
            .with_num_files(metadata.data_files.len());

        Ok(Self {
            location,
            name,
            schema,
            arrow_schema,
            current_snapshot: metadata.current_snapshot,
            snapshots: metadata.snapshots,
            properties,
            partition_columns: metadata.partition_columns,
            data_files: metadata.data_files,
        })
    }

    /// Open with custom options.
    pub fn open_with_options(path: impl AsRef<Path>, options: IcebergTableOptions) -> Result<Self> {
        let mut table = Self::open(path)?;

        // Apply options
        if let Some(snapshot_id) = options.snapshot_id {
            table.current_snapshot = table
                .snapshots
                .iter()
                .find(|s| s.snapshot_id == snapshot_id)
                .cloned();
        }

        Ok(table)
    }

    /// Load metadata from directory.
    fn load_metadata(metadata_path: &Path) -> Result<IcebergMetadata> {
        // Try to find version-hint.text
        let version_hint_path = metadata_path.join("version-hint.text");
        let latest_version = if version_hint_path.exists() {
            fs::read_to_string(&version_hint_path)
                .ok()
                .and_then(|s| s.trim().parse::<i64>().ok())
        } else {
            None
        };

        // Find metadata files
        let mut metadata_files: Vec<(i64, PathBuf)> = Vec::new();
        if let Ok(entries) = fs::read_dir(metadata_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.starts_with("v") && name.ends_with(".metadata.json") {
                        if let Ok(v) = name
                            .trim_start_matches("v")
                            .trim_end_matches(".metadata.json")
                            .parse::<i64>()
                        {
                            metadata_files.push((v, path));
                        }
                    } else if name.ends_with(".metadata.json") {
                        // Timestamp-based naming
                        metadata_files.push((0, path));
                    }
                }
            }
        }

        metadata_files.sort_by_key(|(v, _)| *v);

        // Load the latest metadata
        let metadata_file = if let Some(hint) = latest_version {
            metadata_files
                .iter()
                .find(|(v, _)| *v == hint)
                .map(|(_, p)| p.clone())
        } else {
            metadata_files.last().map(|(_, p)| p.clone())
        };

        let metadata_file =
            metadata_file.ok_or_else(|| BlazeError::invalid_argument("No metadata files found"))?;

        // Parse metadata JSON
        let content = fs::read_to_string(&metadata_file)
            .map_err(|e| BlazeError::execution(format!("Failed to read metadata: {}", e)))?;

        Self::parse_metadata(&content)
    }

    /// Parse Iceberg metadata JSON.
    fn parse_metadata(content: &str) -> Result<IcebergMetadata> {
        let value: serde_json::Value = serde_json::from_str(content)
            .map_err(|e| BlazeError::execution(format!("Failed to parse metadata: {}", e)))?;

        // Parse format version
        let format_version = value
            .get("format-version")
            .and_then(|v| v.as_i64())
            .unwrap_or(1) as i32;

        // Parse schema
        let schema = if let Some(schemas) = value.get("schemas").and_then(|s| s.as_array()) {
            // Get current schema
            let current_schema_id = value
                .get("current-schema-id")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            let schema_value = schemas
                .iter()
                .find(|s| {
                    s.get("schema-id").and_then(|id| id.as_i64()).unwrap_or(-1) == current_schema_id
                })
                .or_else(|| schemas.last());

            if let Some(sv) = schema_value {
                Self::parse_iceberg_schema(sv)?
            } else {
                Schema::new(vec![Field::new("_placeholder", DataType::Utf8, true)])
            }
        } else if let Some(schema_value) = value.get("schema") {
            Self::parse_iceberg_schema(schema_value)?
        } else {
            Schema::new(vec![Field::new("_placeholder", DataType::Utf8, true)])
        };

        // Parse partition spec
        let partition_columns = if let Some(specs) =
            value.get("partition-specs").and_then(|s| s.as_array())
        {
            specs
                .first()
                .and_then(|s| s.get("fields"))
                .and_then(|f| f.as_array())
                .map(|fields| {
                    fields
                        .iter()
                        .filter_map(|f| f.get("name").and_then(|n| n.as_str()).map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        } else {
            Vec::new()
        };

        // Parse snapshots
        let snapshots = if let Some(snaps) = value.get("snapshots").and_then(|s| s.as_array()) {
            snaps
                .iter()
                .filter_map(|s| Self::parse_snapshot(s).ok())
                .collect()
        } else {
            Vec::new()
        };

        // Get current snapshot
        let current_snapshot_id = value.get("current-snapshot-id").and_then(|v| v.as_i64());
        let current_snapshot = current_snapshot_id
            .and_then(|id| snapshots.iter().find(|s| s.snapshot_id == id).cloned());

        // For now, data files would be read from manifests (simplified here)
        let data_files = Vec::new();

        Ok(IcebergMetadata {
            format_version,
            schema,
            partition_columns,
            snapshots,
            current_snapshot,
            data_files,
        })
    }

    /// Parse Iceberg schema.
    fn parse_iceberg_schema(value: &serde_json::Value) -> Result<Schema> {
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

            let required = field
                .get("required")
                .and_then(|r| r.as_bool())
                .unwrap_or(false);

            let data_type = Self::parse_iceberg_type(field.get("type"))?;

            result_fields.push(Field::new(&name, data_type, !required));
        }

        Ok(Schema::new(result_fields))
    }

    /// Parse Iceberg data type.
    fn parse_iceberg_type(type_value: Option<&serde_json::Value>) -> Result<DataType> {
        match type_value {
            Some(serde_json::Value::String(s)) => {
                Ok(match s.as_str() {
                    "string" => DataType::Utf8,
                    "long" => DataType::Int64,
                    "int" | "integer" => DataType::Int32,
                    "float" => DataType::Float32,
                    "double" => DataType::Float64,
                    "boolean" => DataType::Boolean,
                    "binary" => DataType::Binary,
                    "date" => DataType::Date32,
                    "timestamp" | "timestamptz" => DataType::Timestamp {
                        unit: TimeUnit::Microsecond,
                        timezone: None,
                    },
                    "uuid" => DataType::Utf8,
                    _ if s.starts_with("decimal(") => {
                        // Parse decimal(precision, scale)
                        DataType::Float64 // Simplified
                    }
                    _ if s.starts_with("fixed(") => DataType::Binary,
                    _ => DataType::Utf8,
                })
            }
            Some(serde_json::Value::Object(obj)) => {
                if let Some(list_type) = obj.get("type").and_then(|t| t.as_str()) {
                    if list_type == "list" {
                        let element = obj.get("element-type");
                        let inner = Self::parse_iceberg_type(element)?;
                        return Ok(DataType::List(Box::new(inner)));
                    } else if list_type == "map" {
                        // Maps are complex, simplified to JSON
                        return Ok(DataType::Utf8);
                    } else if list_type == "struct" {
                        // Nested structs are complex
                        return Ok(DataType::Utf8);
                    }
                }
                Ok(DataType::Utf8)
            }
            _ => Ok(DataType::Utf8),
        }
    }

    /// Parse snapshot from JSON.
    fn parse_snapshot(value: &serde_json::Value) -> Result<IcebergSnapshot> {
        let snapshot_id = value
            .get("snapshot-id")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| BlazeError::execution("Missing snapshot-id"))?;

        let timestamp_ms = value
            .get("timestamp-ms")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);

        let manifest_list = value
            .get("manifest-list")
            .and_then(|v| v.as_str())
            .map(String::from);

        let operation = value
            .get("summary")
            .and_then(|s| s.get("operation"))
            .and_then(|o| o.as_str())
            .map(Operation::from_str)
            .unwrap_or(Operation::Unknown("UNKNOWN".to_string()));

        let parent_id = value.get("parent-snapshot-id").and_then(|v| v.as_i64());

        Ok(IcebergSnapshot {
            snapshot_id,
            parent_id,
            timestamp_ms,
            manifest_list,
            operation,
            summary: HashMap::new(),
        })
    }

    /// Get the table location.
    pub fn location(&self) -> &Path {
        &self.location
    }

    /// Get current snapshot.
    pub fn current_snapshot(&self) -> Option<&IcebergSnapshot> {
        self.current_snapshot.as_ref()
    }

    /// Get all snapshots.
    pub fn snapshots(&self) -> &[IcebergSnapshot] {
        &self.snapshots
    }

    /// Time travel to a specific snapshot.
    pub fn at_snapshot(&self, snapshot_id: i64) -> Result<Self> {
        Self::open_with_options(
            &self.location,
            IcebergTableOptions {
                snapshot_id: Some(snapshot_id),
                ..Default::default()
            },
        )
    }

    /// Get table history as snapshots.
    pub fn history(&self) -> Vec<&IcebergSnapshot> {
        self.snapshots.iter().collect()
    }
}

impl LakehouseTable for IcebergTable {
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
        if let Some(snapshot) = &self.current_snapshot {
            TableVersion::Snapshot(SnapshotId(snapshot.snapshot_id))
        } else {
            TableVersion::Snapshot(SnapshotId(0))
        }
    }

    fn list_versions(&self) -> Result<Vec<TableVersion>> {
        Ok(self
            .snapshots
            .iter()
            .map(|s| TableVersion::Snapshot(SnapshotId(s.snapshot_id)))
            .collect())
    }

    fn read_all(&self) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for file_info in &self.data_files {
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
        let snapshot_id = version
            .as_snapshot()
            .ok_or_else(|| BlazeError::invalid_argument("Iceberg requires snapshot ID"))?;

        let table_at_snapshot = self.at_snapshot(snapshot_id.value())?;
        table_at_snapshot.read_all()
    }

    fn read_as_of(&self, timestamp_ms: i64) -> Result<Vec<RecordBatch>> {
        // Find snapshot at or before timestamp
        let snapshot = self
            .snapshots
            .iter()
            .filter(|s| s.timestamp_ms <= timestamp_ms)
            .max_by_key(|s| s.timestamp_ms)
            .ok_or_else(|| {
                BlazeError::invalid_argument(format!(
                    "No snapshot found for timestamp {}",
                    timestamp_ms
                ))
            })?;

        let table_at_snapshot = self.at_snapshot(snapshot.snapshot_id)?;
        table_at_snapshot.read_all()
    }

    fn partition_columns(&self) -> &[String] {
        &self.partition_columns
    }

    fn properties(&self) -> &TableProperties {
        &self.properties
    }
}

/// Options for opening an Iceberg table.
#[derive(Debug, Clone, Default)]
pub struct IcebergTableOptions {
    /// Specific snapshot ID to load
    pub snapshot_id: Option<i64>,
    /// Load as of timestamp
    pub timestamp_ms: Option<i64>,
    /// Custom catalog options
    pub catalog_options: HashMap<String, String>,
}

impl IcebergTableOptions {
    /// Create new options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set snapshot ID.
    pub fn with_snapshot(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Set timestamp.
    pub fn with_timestamp(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_ms = Some(timestamp_ms);
        self
    }
}

/// Iceberg snapshot information.
#[derive(Debug, Clone)]
pub struct IcebergSnapshot {
    /// Snapshot ID
    pub snapshot_id: i64,
    /// Parent snapshot ID
    pub parent_id: Option<i64>,
    /// Timestamp in milliseconds
    pub timestamp_ms: i64,
    /// Manifest list location
    pub manifest_list: Option<String>,
    /// Operation that created this snapshot
    pub operation: Operation,
    /// Summary statistics
    pub summary: HashMap<String, String>,
}

impl IcebergSnapshot {
    /// Create a new snapshot.
    pub fn new(snapshot_id: i64, timestamp_ms: i64, operation: Operation) -> Self {
        Self {
            snapshot_id,
            parent_id: None,
            timestamp_ms,
            manifest_list: None,
            operation,
            summary: HashMap::new(),
        }
    }

    /// Set parent ID.
    pub fn with_parent(mut self, parent_id: i64) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set manifest list.
    pub fn with_manifest_list(mut self, path: impl Into<String>) -> Self {
        self.manifest_list = Some(path.into());
        self
    }
}

/// Internal metadata structure.
struct IcebergMetadata {
    format_version: i32,
    schema: Schema,
    partition_columns: Vec<String>,
    snapshots: Vec<IcebergSnapshot>,
    current_snapshot: Option<IcebergSnapshot>,
    data_files: Vec<DataFileInfo>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_table_options() {
        let opts = IcebergTableOptions::new().with_snapshot(12345);

        assert_eq!(opts.snapshot_id, Some(12345));
    }

    #[test]
    fn test_iceberg_snapshot() {
        let snapshot = IcebergSnapshot::new(1, 1234567890000, Operation::Append)
            .with_parent(0)
            .with_manifest_list("metadata/snap-1.avro");

        assert_eq!(snapshot.snapshot_id, 1);
        assert_eq!(snapshot.parent_id, Some(0));
        assert!(snapshot.manifest_list.is_some());
    }

    #[test]
    fn test_parse_iceberg_type() {
        assert_eq!(
            IcebergTable::parse_iceberg_type(Some(&serde_json::json!("string"))).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            IcebergTable::parse_iceberg_type(Some(&serde_json::json!("long"))).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            IcebergTable::parse_iceberg_type(Some(&serde_json::json!("double"))).unwrap(),
            DataType::Float64
        );
    }
}
