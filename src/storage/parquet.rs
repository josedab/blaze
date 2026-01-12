//! Parquet file table implementation.

use std::any::Any;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::catalog::{ColumnStatistics, TableProvider, TableStatistics, TableType};
use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// A table backed by a Parquet file.
#[derive(Debug)]
pub struct ParquetTable {
    /// Path to the Parquet file
    path: PathBuf,
    /// Schema of the table
    schema: Schema,
    /// Arrow schema
    arrow_schema: Arc<ArrowSchema>,
    /// Parquet options
    options: ParquetOptions,
    /// Cached statistics
    statistics: Option<TableStatistics>,
}

/// Options for reading Parquet files.
#[derive(Debug, Clone)]
pub struct ParquetOptions {
    /// Batch size for reading
    pub batch_size: usize,
    /// Whether to use statistics for pruning
    pub use_statistics: bool,
    /// Row groups to read (None means all)
    pub row_groups: Option<Vec<usize>>,
}

impl Default for ParquetOptions {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            use_statistics: true,
            row_groups: None,
        }
    }
}

impl ParquetTable {
    /// Open a Parquet file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, ParquetOptions::default())
    }

    /// Open a Parquet file with custom options.
    pub fn open_with_options(path: impl AsRef<Path>, options: ParquetOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Open file and read metadata
        let file = File::open(&path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        let arrow_schema = builder.schema().clone();
        let schema = Schema::from_arrow(&arrow_schema)?;

        // Extract statistics from parquet metadata
        let statistics = Self::extract_statistics(&path)?;

        Ok(Self {
            path,
            schema,
            arrow_schema,
            options,
            statistics,
        })
    }

    /// Create a Parquet table with a predefined schema.
    pub fn with_schema(path: impl AsRef<Path>, schema: Schema, options: ParquetOptions) -> Self {
        let arrow_schema = Arc::new(schema.to_arrow());
        Self {
            path: path.as_ref().to_path_buf(),
            schema,
            arrow_schema,
            options,
            statistics: None,
        }
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the options.
    pub fn options(&self) -> &ParquetOptions {
        &self.options
    }

    fn extract_statistics(path: &Path) -> Result<Option<TableStatistics>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata();

        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;

        for i in 0..metadata.num_row_groups() {
            let rg = metadata.row_group(i);
            total_rows += rg.num_rows() as usize;
            total_bytes += rg.total_byte_size() as usize;
        }

        // Extract column statistics from row group metadata
        let schema = builder.schema();
        let num_columns = schema.fields().len();
        let mut column_statistics = Vec::with_capacity(num_columns);

        for col_idx in 0..num_columns {
            let mut total_null_count: Option<usize> = Some(0);
            let mut min_value: Option<String> = None;
            let mut max_value: Option<String> = None;

            // Aggregate statistics across all row groups
            for rg_idx in 0..metadata.num_row_groups() {
                let rg = metadata.row_group(rg_idx);
                if let Some(col_chunk) = rg.column(col_idx).statistics() {
                    // Accumulate null counts
                    if let (Some(total), Some(chunk_nulls)) = (total_null_count, col_chunk.null_count_opt()) {
                        total_null_count = Some(total + chunk_nulls as usize);
                    } else {
                        total_null_count = None;
                    }

                    // Track min/max (using first row group's values as representative)
                    if rg_idx == 0 {
                        min_value = col_chunk.min_bytes_opt()
                            .map(|b| Self::bytes_to_string(b));
                        max_value = col_chunk.max_bytes_opt()
                            .map(|b| Self::bytes_to_string(b));
                    }
                } else {
                    total_null_count = None;
                }
            }

            column_statistics.push(ColumnStatistics {
                null_count: total_null_count,
                distinct_count: None, // Not available in parquet metadata
                min_value,
                max_value,
            });
        }

        Ok(Some(TableStatistics {
            num_rows: Some(total_rows),
            total_byte_size: Some(total_bytes),
            column_statistics,
        }))
    }

    /// Convert bytes to a displayable string for statistics
    fn bytes_to_string(bytes: &[u8]) -> String {
        // Try to interpret as UTF-8 string first
        if let Ok(s) = std::str::from_utf8(bytes) {
            return s.to_string();
        }
        // Otherwise format as numeric for common sizes
        match bytes.len() {
            4 => {
                let val = i32::from_le_bytes(bytes.try_into().unwrap_or([0; 4]));
                val.to_string()
            }
            8 => {
                let val = i64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                val.to_string()
            }
            _ => {
                // Fallback to byte representation
                format!("{:?}", bytes)
            }
        }
    }

    fn create_reader(&self) -> Result<impl Iterator<Item = std::result::Result<RecordBatch, arrow::error::ArrowError>>> {
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(self.options.batch_size);

        let reader = builder.build()?;
        Ok(reader)
    }
}

impl TableProvider for ParquetTable {
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
        self.statistics.clone()
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let reader = self.create_reader()?;
        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch_result in reader {
            let batch = batch_result?;

            let projected = match projection {
                Some(indices) => {
                    let columns: Vec<_> = indices.iter().map(|&i| batch.column(i).clone()).collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| batch.schema().field(i).clone())
                        .collect();
                    let schema = Arc::new(ArrowSchema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                }
                None => batch,
            };

            if let Some(limit) = limit {
                let remaining = limit - rows_collected;
                if remaining == 0 {
                    break;
                }
                if projected.num_rows() <= remaining {
                    rows_collected += projected.num_rows();
                    result.push(projected);
                } else {
                    result.push(projected.slice(0, remaining));
                    break;
                }
            } else {
                result.push(projected);
            }
        }

        Ok(result)
    }

    fn supports_filter_pushdown(&self) -> bool {
        // Parquet supports predicate pushdown via row group pruning
        true
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

/// Write record batches to a Parquet file.
pub fn write_parquet(path: impl AsRef<Path>, batches: &[RecordBatch]) -> Result<()> {
    write_parquet_with_options(path, batches, None)
}

/// Write record batches to a Parquet file with custom writer properties.
pub fn write_parquet_with_options(
    path: impl AsRef<Path>,
    batches: &[RecordBatch],
    props: Option<WriterProperties>,
) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let schema = batches[0].schema();
    let file = File::create(path)?;

    let props = props.unwrap_or_else(|| {
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build()
    });

    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    writer.close()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use tempfile::NamedTempFile;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
        ]));

        let id = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let value = Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500]));

        RecordBatch::try_new(schema, vec![id, value]).unwrap()
    }

    #[test]
    fn test_write_and_read_parquet() {
        let batch = create_test_batch();
        let temp_file = NamedTempFile::new().unwrap();

        // Write
        write_parquet(temp_file.path(), &[batch.clone()]).unwrap();

        // Read
        let table = ParquetTable::open(temp_file.path()).unwrap();
        assert_eq!(table.schema().len(), 2);

        let batches = table.scan(None, &[], None).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_parquet_with_projection() {
        let batch = create_test_batch();
        let temp_file = NamedTempFile::new().unwrap();

        write_parquet(temp_file.path(), &[batch]).unwrap();

        let table = ParquetTable::open(temp_file.path()).unwrap();
        let batches = table.scan(Some(&[0]), &[], None).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[test]
    fn test_parquet_with_limit() {
        let batch = create_test_batch();
        let temp_file = NamedTempFile::new().unwrap();

        write_parquet(temp_file.path(), &[batch]).unwrap();

        let table = ParquetTable::open(temp_file.path()).unwrap();
        let batches = table.scan(None, &[], Some(3)).unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_parquet_statistics() {
        let batch = create_test_batch();
        let temp_file = NamedTempFile::new().unwrap();

        write_parquet(temp_file.path(), &[batch]).unwrap();

        let table = ParquetTable::open(temp_file.path()).unwrap();
        let stats = table.statistics().unwrap();

        assert_eq!(stats.num_rows, Some(5));
        assert!(stats.total_byte_size.is_some());
    }
}
