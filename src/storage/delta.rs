//! Delta Lake table implementation.
//!
//! This module provides support for reading Delta Lake tables with features like:
//! - Time travel via version numbers or timestamps
//! - Schema evolution tracking
//! - Parquet-based columnar storage

use std::any::Any;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use deltalake::datafusion::datasource::TableProvider as DeltaTableProvider;
use deltalake::DeltaTable as DeltaLakeTable;
use tokio::runtime::Runtime;
use url::Url;

use crate::catalog::{TableProvider, TableStatistics, TableType};
use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// A table backed by Delta Lake format.
///
/// Delta Lake provides ACID transactions, time travel, and schema evolution
/// on top of Parquet files.
#[derive(Debug)]
pub struct DeltaTable {
    /// Path to the Delta table directory
    path: PathBuf,
    /// The underlying Delta Lake table
    table: DeltaLakeTable,
    /// Schema of the table
    schema: Schema,
    /// Arrow schema
    arrow_schema: Arc<ArrowSchema>,
    /// Tokio runtime for async operations
    runtime: Arc<Runtime>,
    /// Time travel version (if specified)
    #[allow(dead_code)]
    version: Option<i64>,
    /// Time travel timestamp (if specified)
    #[allow(dead_code)]
    timestamp: Option<chrono::DateTime<chrono::Utc>>,
}

/// Options for opening a Delta Lake table.
#[derive(Debug, Clone, Default)]
pub struct DeltaTableOptions {
    /// Specific version to read (time travel)
    pub version: Option<i64>,
    /// Timestamp to read as of (time travel)
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Batch size for reading
    pub batch_size: usize,
}

fn path_to_url(path: &Path) -> Result<Url> {
    // Convert path to absolute
    let abs_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|e| BlazeError::internal(format!("Failed to get current dir: {}", e)))?
            .join(path)
    };

    Url::from_file_path(&abs_path)
        .map_err(|_| BlazeError::internal(format!("Failed to convert path to URL: {:?}", abs_path)))
}

impl DeltaTable {
    /// Open a Delta Lake table at the specified path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, DeltaTableOptions::default())
    }

    /// Open a Delta Lake table with specific options.
    pub fn open_with_options(path: impl AsRef<Path>, options: DeltaTableOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let runtime = Arc::new(
            Runtime::new()
                .map_err(|e| BlazeError::internal(format!("Failed to create runtime: {}", e)))?,
        );

        let table_url = path_to_url(&path)?;

        // Load the Delta table
        let table = runtime
            .block_on(async {
                // Handle time travel options
                if let Some(version) = options.version {
                    deltalake::open_table_with_version(table_url, version).await
                } else if let Some(ts) = options.timestamp {
                    // Load at specific timestamp using datetime
                    let mut table = deltalake::open_table(table_url).await?;
                    table.load_with_datetime(ts).await?;
                    Ok(table)
                } else {
                    deltalake::open_table(table_url).await
                }
            })
            .map_err(|e| BlazeError::internal(format!("Failed to open Delta table: {}", e)))?;

        // Get the schema from the Delta table using the TableProvider trait
        let arrow_schema = DeltaTableProvider::schema(&table);

        let schema = Schema::from_arrow(&arrow_schema)?;

        Ok(Self {
            path,
            table,
            schema,
            arrow_schema,
            runtime,
            version: options.version,
            timestamp: options.timestamp,
        })
    }

    /// Open a Delta table at a specific version (time travel).
    pub fn open_at_version(path: impl AsRef<Path>, version: i64) -> Result<Self> {
        Self::open_with_options(
            path,
            DeltaTableOptions {
                version: Some(version),
                ..Default::default()
            },
        )
    }

    /// Open a Delta table as of a specific timestamp (time travel).
    pub fn open_at_timestamp(
        path: impl AsRef<Path>,
        timestamp: chrono::DateTime<chrono::Utc>,
    ) -> Result<Self> {
        Self::open_with_options(
            path,
            DeltaTableOptions {
                timestamp: Some(timestamp),
                ..Default::default()
            },
        )
    }

    /// Get the current version of the table.
    pub fn version(&self) -> i64 {
        self.table.version().unwrap_or(0)
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// List all versions available in the Delta log.
    pub fn list_versions(&self) -> Result<Vec<DeltaVersionInfo>> {
        // Get version info from commit history
        let history = self
            .runtime
            .block_on(async { self.table.history(None).await })
            .map_err(|e| BlazeError::internal(format!("Failed to get table history: {}", e)))?;

        Ok(history
            .into_iter()
            .enumerate()
            .map(|(idx, commit)| DeltaVersionInfo {
                version: idx as i64,
                timestamp: commit.timestamp.map(|ts| {
                    chrono::DateTime::from_timestamp_millis(ts).unwrap_or_else(chrono::Utc::now)
                }),
                operation: commit.operation.unwrap_or_default(),
                num_files_added: commit
                    .operation_parameters
                    .as_ref()
                    .and_then(|p| p.get("numAddedFiles"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok()),
                num_files_removed: commit
                    .operation_parameters
                    .as_ref()
                    .and_then(|p| p.get("numRemovedFiles"))
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok()),
            })
            .collect())
    }

    /// Get the files that make up the current version of the table.
    pub fn files(&self) -> Vec<String> {
        match self.table.get_file_uris() {
            Ok(iter) => iter.collect(),
            Err(_) => vec![],
        }
    }

    /// Read all data from the Delta table.
    fn read_all(
        &self,
        projection: Option<&[usize]>,
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let files = self.files();
        if files.is_empty() {
            return Ok(vec![]);
        }

        let mut all_batches = Vec::new();
        let mut rows_collected = 0;

        for file_path in files {
            // Delta stores relative paths, convert to absolute
            let full_path = if file_path.starts_with("file://") {
                PathBuf::from(file_path.strip_prefix("file://").unwrap_or(&file_path))
            } else if Path::new(&file_path).is_absolute() {
                PathBuf::from(&file_path)
            } else {
                self.path.join(&file_path)
            };

            // Read the parquet file
            let file = match std::fs::File::open(&full_path) {
                Ok(f) => f,
                Err(e) => {
                    // Skip files that don't exist (may have been compacted)
                    tracing::warn!("Failed to open file {:?}: {}", full_path, e);
                    continue;
                }
            };

            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .map_err(|e| BlazeError::internal(format!("Failed to read parquet: {}", e)))?;

            let reader = builder
                .build()
                .map_err(|e| BlazeError::internal(format!("Failed to build reader: {}", e)))?;

            for batch_result in reader {
                let batch: RecordBatch = batch_result
                    .map_err(|e| BlazeError::internal(format!("Failed to read batch: {}", e)))?;

                // Apply projection if specified
                let projected = match projection {
                    Some(indices) => {
                        let columns: Vec<_> =
                            indices.iter().map(|&i| batch.column(i).clone()).collect();
                        let fields: Vec<_> = indices
                            .iter()
                            .map(|&i| batch.schema().field(i).clone())
                            .collect();
                        let schema = Arc::new(ArrowSchema::new(fields));
                        RecordBatch::try_new(schema, columns)?
                    }
                    None => batch,
                };

                // Apply limit if specified
                if let Some(limit) = limit {
                    let remaining = limit - rows_collected;
                    if remaining == 0 {
                        return Ok(all_batches);
                    }
                    if projected.num_rows() <= remaining {
                        rows_collected += projected.num_rows();
                        all_batches.push(projected);
                    } else {
                        all_batches.push(projected.slice(0, remaining));
                        return Ok(all_batches);
                    }
                } else {
                    all_batches.push(projected);
                }
            }
        }

        Ok(all_batches)
    }

    fn extract_statistics(&self) -> Option<TableStatistics> {
        // Get stats from Delta log using the files
        let files = self.files();

        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;

        // Read stats from the file metadata
        for file_path in files {
            let full_path = if file_path.starts_with("file://") {
                PathBuf::from(file_path.strip_prefix("file://").unwrap_or(&file_path))
            } else if Path::new(&file_path).is_absolute() {
                PathBuf::from(&file_path)
            } else {
                self.path.join(&file_path)
            };

            if let Ok(metadata) = std::fs::metadata(&full_path) {
                total_bytes += metadata.len() as usize;
            }

            // Try to read parquet metadata for row counts
            if let Ok(file) = std::fs::File::open(&full_path) {
                if let Ok(builder) =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                {
                    let metadata = builder.metadata();
                    for i in 0..metadata.num_row_groups() {
                        total_rows += metadata.row_group(i).num_rows() as usize;
                    }
                }
            }
        }

        Some(TableStatistics {
            num_rows: Some(total_rows),
            total_byte_size: Some(total_bytes),
            column_statistics: vec![], // Delta stats extraction would go here
        })
    }

    /// Write record batches to the Delta table.
    ///
    /// This creates a new version of the table with the written data.
    pub fn write(
        &mut self,
        batches: &[RecordBatch],
        mode: DeltaWriteMode,
    ) -> Result<DeltaWriteResult> {
        use deltalake::protocol::SaveMode;

        if batches.is_empty() {
            return Ok(DeltaWriteResult {
                version: self.version(),
                rows_written: 0,
                files_created: 0,
                bytes_written: 0,
            });
        }

        // Validate schema compatibility
        let batch_schema = batches[0].schema();
        if !self.schemas_compatible(&batch_schema) {
            return Err(BlazeError::internal(
                "Schema mismatch: batch schema is not compatible with table schema",
            ));
        }

        // Convert mode
        let save_mode = match mode {
            DeltaWriteMode::Append => SaveMode::Append,
            DeltaWriteMode::Overwrite => SaveMode::Overwrite,
            DeltaWriteMode::ErrorIfExists => SaveMode::ErrorIfExists,
        };

        // Count rows and estimate bytes
        let rows_written: usize = batches.iter().map(|b| b.num_rows()).sum();
        let batches_vec = batches.to_vec();

        // Estimate bytes written (rough estimate based on batch sizes)
        let bytes_written: usize = batches
            .iter()
            .map(|b| {
                b.columns()
                    .iter()
                    .map(|c| c.get_buffer_memory_size())
                    .sum::<usize>()
            })
            .sum();

        // Use the DeltaTable write method directly
        let result = self.runtime.block_on(async {
            // Use the write method on DeltaTable
            let new_table = self
                .table
                .clone()
                .write(batches_vec)
                .with_save_mode(save_mode)
                .await?;

            let new_version = new_table.version().unwrap_or(0);

            Ok::<_, deltalake::errors::DeltaTableError>((new_table, new_version))
        });

        match result {
            Ok((new_table, new_version)) => {
                // Update our table reference
                self.table = new_table;

                // Count files created (estimate based on batches)
                let files_created = 1; // Simplified: actual count depends on partition config

                Ok(DeltaWriteResult {
                    version: new_version,
                    rows_written,
                    files_created,
                    bytes_written,
                })
            }
            Err(e) => Err(BlazeError::internal(format!(
                "Failed to write to Delta table: {}",
                e
            ))),
        }
    }

    /// Check if two schemas are compatible for writing.
    fn schemas_compatible(&self, batch_schema: &ArrowSchema) -> bool {
        let table_schema = &self.arrow_schema;

        // Check that all batch columns exist in table with compatible types
        for batch_field in batch_schema.fields() {
            match table_schema.field_with_name(batch_field.name()) {
                Ok(table_field) => {
                    // Check type compatibility (simplified check)
                    if batch_field.data_type() != table_field.data_type() {
                        // Allow some implicit conversions
                        if !Self::types_compatible(batch_field.data_type(), table_field.data_type())
                        {
                            return false;
                        }
                    }
                }
                Err(_) => return false,
            }
        }
        true
    }

    /// Check if two Arrow types are compatible for writing.
    fn types_compatible(
        source: &arrow::datatypes::DataType,
        target: &arrow::datatypes::DataType,
    ) -> bool {
        use arrow::datatypes::DataType as ArrowDataType;

        match (source, target) {
            // Exact match
            (a, b) if a == b => true,
            // Integer promotions
            (ArrowDataType::Int8, ArrowDataType::Int16)
            | (ArrowDataType::Int8, ArrowDataType::Int32)
            | (ArrowDataType::Int8, ArrowDataType::Int64)
            | (ArrowDataType::Int16, ArrowDataType::Int32)
            | (ArrowDataType::Int16, ArrowDataType::Int64)
            | (ArrowDataType::Int32, ArrowDataType::Int64) => true,
            // Float promotions
            (ArrowDataType::Float32, ArrowDataType::Float64) => true,
            // String types
            (ArrowDataType::Utf8, ArrowDataType::LargeUtf8)
            | (ArrowDataType::LargeUtf8, ArrowDataType::Utf8) => true,
            _ => false,
        }
    }

    /// Insert new rows into the table.
    pub fn insert(&mut self, batches: &[RecordBatch]) -> Result<DeltaWriteResult> {
        self.write(batches, DeltaWriteMode::Append)
    }

    /// Compact small files in the Delta table.
    ///
    /// This operation rewrites small files into larger ones for better read performance.
    pub fn compact(&mut self, target_file_size_mb: usize) -> Result<DeltaWriteResult> {
        // Read all data
        let batches = self.read_all(None, None)?;

        if batches.is_empty() {
            return Ok(DeltaWriteResult {
                version: self.version(),
                rows_written: 0,
                files_created: 0,
                bytes_written: 0,
            });
        }

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let schema = batches[0].schema();

        // Estimate target row count per file based on target size
        // Rough estimate: 1MB ≈ 50,000 rows for typical data
        let rows_per_mb = 50_000;
        let target_rows_per_file = target_file_size_mb * rows_per_mb;

        // Combine batches into appropriately sized chunks
        let mut compacted_batches = Vec::new();
        let mut current_rows = 0;
        let mut current_batch_columns: Option<Vec<arrow::array::ArrayRef>> = None;

        for batch in &batches {
            if current_batch_columns.is_none() {
                current_batch_columns = Some(batch.columns().to_vec());
                current_rows = batch.num_rows();
            } else {
                // Concatenate with existing
                let existing = current_batch_columns.take().unwrap();
                let mut new_columns = Vec::with_capacity(schema.fields().len());

                for (i, col) in batch.columns().iter().enumerate() {
                    let concat = arrow::compute::concat(&[existing[i].as_ref(), col.as_ref()])?;
                    new_columns.push(concat);
                }

                current_rows += batch.num_rows();
                current_batch_columns = Some(new_columns);

                // Check if we've reached target size
                if current_rows >= target_rows_per_file {
                    let cols = current_batch_columns.take().unwrap();
                    let compacted = RecordBatch::try_new(schema.clone(), cols)?;
                    compacted_batches.push(compacted);
                    current_rows = 0;
                }
            }
        }

        // Don't forget the last batch
        if let Some(cols) = current_batch_columns {
            if !cols.is_empty() && current_rows > 0 {
                let compacted = RecordBatch::try_new(schema.clone(), cols)?;
                compacted_batches.push(compacted);
            }
        }

        // Write compacted data as overwrite
        self.write(&compacted_batches, DeltaWriteMode::Overwrite)
            .map(|mut result| {
                result.rows_written = total_rows;
                result
            })
    }
}

/// Information about a Delta table version.
#[derive(Debug, Clone)]
pub struct DeltaVersionInfo {
    /// Version number
    pub version: i64,
    /// Timestamp when version was created
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,
    /// Operation that created this version
    pub operation: String,
    /// Number of files added
    pub num_files_added: Option<i64>,
    /// Number of files removed
    pub num_files_removed: Option<i64>,
}

/// Result of a write operation to a Delta table.
#[derive(Debug, Clone)]
pub struct DeltaWriteResult {
    /// New version number after the write
    pub version: i64,
    /// Number of rows written
    pub rows_written: usize,
    /// Number of files created
    pub files_created: usize,
    /// Bytes written
    pub bytes_written: usize,
}

/// Write mode for Delta table operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeltaWriteMode {
    /// Append new data to existing data
    #[default]
    Append,
    /// Overwrite all existing data
    Overwrite,
    /// Error if table already has data
    ErrorIfExists,
}

impl TableProvider for DeltaTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> TableType {
        TableType::External
    }

    fn statistics(&self) -> Option<TableStatistics> {
        self.extract_statistics()
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        self.read_all(projection, limit)
    }

    fn supports_filter_pushdown(&self) -> bool {
        true
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn scan_with_filters(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn crate::planner::PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::BooleanArray;
        use arrow::compute::filter_record_batch;

        let batches = self.read_all(projection, None)?;

        if filters.is_empty() {
            // Apply limit directly if no filters
            if let Some(limit) = limit {
                let mut result = Vec::new();
                let mut rows = 0;
                for batch in batches {
                    if rows >= limit {
                        break;
                    }
                    let remaining = limit - rows;
                    if batch.num_rows() <= remaining {
                        rows += batch.num_rows();
                        result.push(batch);
                    } else {
                        result.push(batch.slice(0, remaining));
                        break;
                    }
                }
                return Ok(result);
            }
            return Ok(batches);
        }

        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch in batches {
            // Apply filters
            let mut filtered_batch = batch;
            for filter_expr in filters {
                let filter_result = filter_expr.evaluate(&filtered_batch)?;
                if let Some(bool_array) = filter_result.as_any().downcast_ref::<BooleanArray>() {
                    filtered_batch = filter_record_batch(&filtered_batch, bool_array)?;
                }
            }

            if filtered_batch.num_rows() == 0 {
                continue;
            }

            // Apply limit
            if let Some(limit) = limit {
                let remaining = limit - rows_collected;
                if remaining == 0 {
                    break;
                }
                if filtered_batch.num_rows() <= remaining {
                    rows_collected += filtered_batch.num_rows();
                    result.push(filtered_batch);
                } else {
                    result.push(filtered_batch.slice(0, remaining));
                    break;
                }
            } else {
                result.push(filtered_batch);
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Delta Lake tests require a Delta table to be present
    // These are integration tests that can be run with a test fixture

    #[test]
    fn test_delta_table_options_default() {
        let options = DeltaTableOptions::default();
        assert!(options.version.is_none());
        assert!(options.timestamp.is_none());
    }

    #[test]
    fn test_delta_write_mode_default() {
        let mode = DeltaWriteMode::default();
        assert_eq!(mode, DeltaWriteMode::Append);
    }

    #[test]
    fn test_delta_write_result() {
        let result = DeltaWriteResult {
            version: 1,
            rows_written: 100,
            files_created: 2,
            bytes_written: 1024,
        };

        assert_eq!(result.version, 1);
        assert_eq!(result.rows_written, 100);
        assert_eq!(result.files_created, 2);
        assert_eq!(result.bytes_written, 1024);
    }

    #[test]
    fn test_delta_version_info() {
        let info = DeltaVersionInfo {
            version: 5,
            timestamp: Some(chrono::Utc::now()),
            operation: "WRITE".to_string(),
            num_files_added: Some(3),
            num_files_removed: Some(1),
        };

        assert_eq!(info.version, 5);
        assert_eq!(info.operation, "WRITE");
        assert!(info.timestamp.is_some());
        assert_eq!(info.num_files_added, Some(3));
        assert_eq!(info.num_files_removed, Some(1));
    }

    #[test]
    fn test_types_compatible_exact_match() {
        use arrow::datatypes::DataType as ArrowDataType;

        assert!(DeltaTable::types_compatible(
            &ArrowDataType::Int32,
            &ArrowDataType::Int32
        ));
        assert!(DeltaTable::types_compatible(
            &ArrowDataType::Utf8,
            &ArrowDataType::Utf8
        ));
    }

    #[test]
    fn test_types_compatible_promotions() {
        use arrow::datatypes::DataType as ArrowDataType;

        // Integer promotions
        assert!(DeltaTable::types_compatible(
            &ArrowDataType::Int8,
            &ArrowDataType::Int64
        ));
        assert!(DeltaTable::types_compatible(
            &ArrowDataType::Int16,
            &ArrowDataType::Int32
        ));

        // Float promotions
        assert!(DeltaTable::types_compatible(
            &ArrowDataType::Float32,
            &ArrowDataType::Float64
        ));

        // String compatibility
        assert!(DeltaTable::types_compatible(
            &ArrowDataType::Utf8,
            &ArrowDataType::LargeUtf8
        ));
    }

    #[test]
    fn test_types_compatible_incompatible() {
        use arrow::datatypes::DataType as ArrowDataType;

        // Incompatible types
        assert!(!DeltaTable::types_compatible(
            &ArrowDataType::Int32,
            &ArrowDataType::Utf8
        ));
        assert!(!DeltaTable::types_compatible(
            &ArrowDataType::Boolean,
            &ArrowDataType::Int32
        ));
    }
}
