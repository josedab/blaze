//! Snapshot and Version Management
//!
//! Common types for tracking table versions and snapshots across
//! different lakehouse formats.

use std::time::{SystemTime, UNIX_EPOCH};

/// A unique identifier for a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotId(pub i64);

impl SnapshotId {
    /// Create a new snapshot ID.
    pub fn new(id: i64) -> Self {
        Self(id)
    }

    /// Get the inner value.
    pub fn value(&self) -> i64 {
        self.0
    }
}

impl std::fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<i64> for SnapshotId {
    fn from(id: i64) -> Self {
        Self(id)
    }
}

/// Table version identifier (either version number or snapshot ID).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableVersion {
    /// Version number (for Delta Lake)
    Version(i64),
    /// Snapshot ID (for Iceberg)
    Snapshot(SnapshotId),
    /// Timestamp-based version
    Timestamp(i64),
}

impl TableVersion {
    /// Create a version number.
    pub fn version(v: i64) -> Self {
        TableVersion::Version(v)
    }

    /// Create a snapshot ID.
    pub fn snapshot(id: i64) -> Self {
        TableVersion::Snapshot(SnapshotId(id))
    }

    /// Create a timestamp version.
    pub fn timestamp(ts: i64) -> Self {
        TableVersion::Timestamp(ts)
    }

    /// Create a timestamp version from current time.
    pub fn now() -> Self {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        TableVersion::Timestamp(ts)
    }

    /// Get version number if this is a version.
    pub fn as_version(&self) -> Option<i64> {
        match self {
            TableVersion::Version(v) => Some(*v),
            _ => None,
        }
    }

    /// Get snapshot ID if this is a snapshot.
    pub fn as_snapshot(&self) -> Option<SnapshotId> {
        match self {
            TableVersion::Snapshot(id) => Some(*id),
            _ => None,
        }
    }

    /// Get timestamp if this is a timestamp version.
    pub fn as_timestamp(&self) -> Option<i64> {
        match self {
            TableVersion::Timestamp(ts) => Some(*ts),
            _ => None,
        }
    }
}

impl std::fmt::Display for TableVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TableVersion::Version(v) => write!(f, "v{}", v),
            TableVersion::Snapshot(id) => write!(f, "snapshot:{}", id),
            TableVersion::Timestamp(ts) => write!(f, "ts:{}", ts),
        }
    }
}

/// A snapshot of a table at a point in time.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Snapshot identifier
    pub id: SnapshotId,
    /// Parent snapshot ID (None for initial snapshot)
    pub parent_id: Option<SnapshotId>,
    /// Version number (for Delta Lake compatibility)
    pub version: Option<i64>,
    /// Timestamp when snapshot was created
    pub timestamp_ms: i64,
    /// Operation that created this snapshot
    pub operation: Operation,
    /// Summary of changes
    pub summary: SnapshotSummary,
    /// Manifest list location (for Iceberg)
    pub manifest_list: Option<String>,
}

impl Snapshot {
    /// Create a new snapshot.
    pub fn new(id: i64, timestamp_ms: i64, operation: Operation) -> Self {
        Self {
            id: SnapshotId(id),
            parent_id: None,
            version: None,
            timestamp_ms,
            operation,
            summary: SnapshotSummary::default(),
            manifest_list: None,
        }
    }

    /// Set parent snapshot.
    pub fn with_parent(mut self, parent_id: i64) -> Self {
        self.parent_id = Some(SnapshotId(parent_id));
        self
    }

    /// Set version number.
    pub fn with_version(mut self, version: i64) -> Self {
        self.version = Some(version);
        self
    }

    /// Set manifest list.
    pub fn with_manifest_list(mut self, path: impl Into<String>) -> Self {
        self.manifest_list = Some(path.into());
        self
    }

    /// Set summary.
    pub fn with_summary(mut self, summary: SnapshotSummary) -> Self {
        self.summary = summary;
        self
    }

    /// Get the table version for this snapshot.
    pub fn table_version(&self) -> TableVersion {
        if let Some(v) = self.version {
            TableVersion::Version(v)
        } else {
            TableVersion::Snapshot(self.id)
        }
    }
}

/// Operation that created a snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// Table creation
    Create,
    /// Append new data
    Append,
    /// Overwrite existing data
    Overwrite,
    /// Delete data
    Delete,
    /// Update data in place
    Update,
    /// Merge operation
    Merge,
    /// Compact files
    Compact,
    /// Optimize table
    Optimize,
    /// Schema change
    SchemaChange,
    /// Restore to previous version
    Restore,
    /// Unknown operation
    Unknown(String),
}

impl Operation {
    /// Parse operation from string.
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "create" => Operation::Create,
            "append" | "write" => Operation::Append,
            "overwrite" => Operation::Overwrite,
            "delete" => Operation::Delete,
            "update" => Operation::Update,
            "merge" => Operation::Merge,
            "compact" | "compaction" => Operation::Compact,
            "optimize" => Operation::Optimize,
            "schema" | "schemachange" | "schema_change" => Operation::SchemaChange,
            "restore" => Operation::Restore,
            _ => Operation::Unknown(s.to_string()),
        }
    }

    /// Convert to string.
    pub fn as_str(&self) -> &str {
        match self {
            Operation::Create => "CREATE",
            Operation::Append => "APPEND",
            Operation::Overwrite => "OVERWRITE",
            Operation::Delete => "DELETE",
            Operation::Update => "UPDATE",
            Operation::Merge => "MERGE",
            Operation::Compact => "COMPACT",
            Operation::Optimize => "OPTIMIZE",
            Operation::SchemaChange => "SCHEMA_CHANGE",
            Operation::Restore => "RESTORE",
            Operation::Unknown(s) => s,
        }
    }
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Summary statistics for a snapshot.
#[derive(Debug, Clone, Default)]
pub struct SnapshotSummary {
    /// Number of data files added
    pub added_files: i64,
    /// Number of data files removed
    pub removed_files: i64,
    /// Number of records added
    pub added_records: i64,
    /// Number of records removed
    pub removed_records: i64,
    /// Bytes added
    pub added_bytes: i64,
    /// Bytes removed
    pub removed_bytes: i64,
    /// Total data files after operation
    pub total_files: i64,
    /// Total records after operation
    pub total_records: i64,
    /// Additional metrics
    pub metrics: std::collections::HashMap<String, String>,
}

impl SnapshotSummary {
    /// Create a new snapshot summary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set added files count.
    pub fn with_added_files(mut self, count: i64) -> Self {
        self.added_files = count;
        self
    }

    /// Set removed files count.
    pub fn with_removed_files(mut self, count: i64) -> Self {
        self.removed_files = count;
        self
    }

    /// Set added records count.
    pub fn with_added_records(mut self, count: i64) -> Self {
        self.added_records = count;
        self
    }

    /// Set removed records count.
    pub fn with_removed_records(mut self, count: i64) -> Self {
        self.removed_records = count;
        self
    }

    /// Set total files.
    pub fn with_total_files(mut self, count: i64) -> Self {
        self.total_files = count;
        self
    }

    /// Set total records.
    pub fn with_total_records(mut self, count: i64) -> Self {
        self.total_records = count;
        self
    }

    /// Add a custom metric.
    pub fn with_metric(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metrics.insert(key.into(), value.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_id() {
        let id = SnapshotId::new(123);
        assert_eq!(id.value(), 123);
        assert_eq!(format!("{}", id), "123");
    }

    #[test]
    fn test_table_version() {
        let v = TableVersion::version(5);
        assert_eq!(v.as_version(), Some(5));
        assert_eq!(format!("{}", v), "v5");

        let s = TableVersion::snapshot(100);
        assert_eq!(s.as_snapshot(), Some(SnapshotId(100)));
        assert_eq!(format!("{}", s), "snapshot:100");
    }

    #[test]
    fn test_operation_parsing() {
        assert_eq!(Operation::from_str("append"), Operation::Append);
        assert_eq!(Operation::from_str("WRITE"), Operation::Append);
        assert_eq!(Operation::from_str("delete"), Operation::Delete);
        assert_eq!(
            Operation::from_str("custom"),
            Operation::Unknown("custom".to_string())
        );
    }

    #[test]
    fn test_snapshot_creation() {
        let snapshot = Snapshot::new(1, 1234567890000, Operation::Create)
            .with_version(0)
            .with_summary(
                SnapshotSummary::new()
                    .with_added_files(5)
                    .with_added_records(1000),
            );

        assert_eq!(snapshot.id.value(), 1);
        assert_eq!(snapshot.version, Some(0));
        assert_eq!(snapshot.operation, Operation::Create);
        assert_eq!(snapshot.summary.added_files, 5);
        assert_eq!(snapshot.summary.added_records, 1000);
    }

    #[test]
    fn test_snapshot_summary() {
        let summary = SnapshotSummary::new()
            .with_added_files(10)
            .with_removed_files(2)
            .with_total_records(50000)
            .with_metric("custom", "value");

        assert_eq!(summary.added_files, 10);
        assert_eq!(summary.removed_files, 2);
        assert_eq!(summary.total_records, 50000);
        assert_eq!(summary.metrics.get("custom"), Some(&"value".to_string()));
    }
}
