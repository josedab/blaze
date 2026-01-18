//! Lakehouse Table Format Support
//!
//! This module provides support for modern lakehouse table formats including
//! Delta Lake and Apache Iceberg.
//!
//! # Features
//!
//! - **Delta Lake**: Read Delta Lake tables with time travel
//! - **Apache Iceberg**: Read Iceberg tables with snapshot isolation
//! - **Time Travel**: Query historical versions of data
//! - **Schema Evolution**: Handle schema changes gracefully
//! - **Partition Pruning**: Skip irrelevant partitions based on metadata
//!
//! # Example
//!
//! ```rust,ignore
//! use blaze::lakehouse::{DeltaTable, IcebergTable};
//!
//! // Read Delta Lake table
//! let delta = DeltaTable::open("path/to/delta")?;
//! let batches = delta.read_all()?;
//!
//! // Time travel to specific version
//! let batches = delta.read_version(5)?;
//! ```

mod delta;
mod iceberg;
mod snapshot;

pub use delta::{DeltaLog, DeltaTable, DeltaTableOptions};
pub use iceberg::{IcebergSnapshot, IcebergTable, IcebergTableOptions};
pub use snapshot::{Operation, Snapshot, SnapshotId, SnapshotSummary, TableVersion};

use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// Trait for lakehouse table formats.
pub trait LakehouseTable: Send + Sync {
    /// Get the table name.
    fn name(&self) -> &str;

    /// Get the current schema.
    fn schema(&self) -> &Schema;

    /// Get the Arrow schema.
    fn arrow_schema(&self) -> Arc<ArrowSchema>;

    /// Get the current version/snapshot.
    fn current_version(&self) -> TableVersion;

    /// List all available versions.
    fn list_versions(&self) -> Result<Vec<TableVersion>>;

    /// Read all data from the table.
    fn read_all(&self) -> Result<Vec<RecordBatch>>;

    /// Read data as of a specific version.
    fn read_version(&self, version: TableVersion) -> Result<Vec<RecordBatch>>;

    /// Read data as of a specific timestamp.
    fn read_as_of(&self, timestamp_ms: i64) -> Result<Vec<RecordBatch>>;

    /// Get partition columns.
    fn partition_columns(&self) -> &[String];

    /// Get table properties/metadata.
    fn properties(&self) -> &TableProperties;
}

/// Table properties and metadata.
#[derive(Debug, Clone, Default)]
pub struct TableProperties {
    /// Table location
    pub location: String,
    /// Table format name
    pub format: String,
    /// Format version
    pub format_version: String,
    /// Creation time
    pub created_at: Option<i64>,
    /// Last modified time
    pub last_modified: Option<i64>,
    /// Number of files
    pub num_files: usize,
    /// Total size in bytes
    pub total_size: usize,
    /// Custom properties
    pub custom: std::collections::HashMap<String, String>,
}

impl TableProperties {
    /// Create new table properties.
    pub fn new(location: impl Into<String>, format: impl Into<String>) -> Self {
        Self {
            location: location.into(),
            format: format.into(),
            ..Default::default()
        }
    }

    /// Set format version.
    pub fn with_format_version(mut self, version: impl Into<String>) -> Self {
        self.format_version = version.into();
        self
    }

    /// Set number of files.
    pub fn with_num_files(mut self, num_files: usize) -> Self {
        self.num_files = num_files;
        self
    }

    /// Set total size.
    pub fn with_total_size(mut self, total_size: usize) -> Self {
        self.total_size = total_size;
        self
    }

    /// Add a custom property.
    pub fn with_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.custom.insert(key.into(), value.into());
        self
    }
}

/// Partition information.
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Partition column name
    pub column: String,
    /// Partition value
    pub value: String,
    /// Number of rows in partition
    pub row_count: usize,
    /// Size in bytes
    pub size: usize,
}

/// Statistics for a column in a data file.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    /// Column name
    pub name: String,
    /// Number of nulls
    pub null_count: Option<i64>,
    /// Minimum value (as string for simplicity)
    pub min_value: Option<String>,
    /// Maximum value
    pub max_value: Option<String>,
}

/// Metadata for a data file.
#[derive(Debug, Clone)]
pub struct DataFileInfo {
    /// File path
    pub path: String,
    /// File format (parquet, orc, etc.)
    pub format: String,
    /// Number of rows
    pub row_count: usize,
    /// File size in bytes
    pub size: usize,
    /// Partition values
    pub partitions: Vec<PartitionInfo>,
    /// Column statistics
    pub column_stats: Vec<ColumnStats>,
}

impl DataFileInfo {
    /// Create new data file info.
    pub fn new(path: impl Into<String>, format: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            format: format.into(),
            row_count: 0,
            size: 0,
            partitions: vec![],
            column_stats: vec![],
        }
    }

    /// Set row count.
    pub fn with_row_count(mut self, count: usize) -> Self {
        self.row_count = count;
        self
    }

    /// Set file size.
    pub fn with_size(mut self, size: usize) -> Self {
        self.size = size;
        self
    }
}

/// Open a lakehouse table, auto-detecting the format.
pub fn open_table(path: impl AsRef<Path>) -> Result<Box<dyn LakehouseTable>> {
    let path = path.as_ref();

    // Check for Delta Lake (_delta_log directory)
    let delta_log = path.join("_delta_log");
    if delta_log.exists() && delta_log.is_dir() {
        let table = DeltaTable::open(path)?;
        return Ok(Box::new(table));
    }

    // Check for Iceberg (metadata directory)
    let iceberg_metadata = path.join("metadata");
    if iceberg_metadata.exists() && iceberg_metadata.is_dir() {
        let table = IcebergTable::open(path)?;
        return Ok(Box::new(table));
    }

    Err(BlazeError::invalid_argument(format!(
        "Path '{}' is not a recognized lakehouse table format",
        path.display()
    )))
}

/// Table format type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFormat {
    /// Delta Lake format
    Delta,
    /// Apache Iceberg format
    Iceberg,
}

impl std::fmt::Display for TableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableFormat::Delta => write!(f, "Delta Lake"),
            TableFormat::Iceberg => write!(f, "Apache Iceberg"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_properties() {
        let props = TableProperties::new("/path/to/table", "delta")
            .with_format_version("2")
            .with_num_files(10)
            .with_total_size(1024 * 1024)
            .with_property("custom.key", "value");

        assert_eq!(props.location, "/path/to/table");
        assert_eq!(props.format, "delta");
        assert_eq!(props.format_version, "2");
        assert_eq!(props.num_files, 10);
        assert_eq!(props.custom.get("custom.key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_data_file_info() {
        let info = DataFileInfo::new("/data/part-0000.parquet", "parquet")
            .with_row_count(10000)
            .with_size(1024 * 1024);

        assert_eq!(info.path, "/data/part-0000.parquet");
        assert_eq!(info.format, "parquet");
        assert_eq!(info.row_count, 10000);
    }

    #[test]
    fn test_table_format_display() {
        assert_eq!(format!("{}", TableFormat::Delta), "Delta Lake");
        assert_eq!(format!("{}", TableFormat::Iceberg), "Apache Iceberg");
    }
}
