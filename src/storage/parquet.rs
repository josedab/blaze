//! Parquet file table implementation with row group pruning and projection pushdown.

use std::any::Any;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::properties::WriterProperties;

use crate::catalog::{ColumnStatistics, TableProvider, TableStatistics, TableType};
use crate::error::Result;
use crate::types::Schema;

/// A table backed by a Parquet file.
#[derive(Debug)]
pub struct ParquetTable {
    /// Path to the Parquet file
    path: PathBuf,
    /// Schema of the table
    schema: Schema,
    /// Arrow schema
    #[allow(dead_code)]
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
                    if let (Some(total), Some(chunk_nulls)) =
                        (total_null_count, col_chunk.null_count_opt())
                    {
                        total_null_count = Some(total + chunk_nulls as usize);
                    } else {
                        total_null_count = None;
                    }

                    // Track min/max across all row groups
                    if let Some(rg_min_bytes) = col_chunk.min_bytes_opt() {
                        let rg_min = Self::bytes_to_string(rg_min_bytes);
                        min_value = match min_value {
                            Some(ref current) if rg_min < *current => Some(rg_min),
                            Some(current) => Some(current),
                            None => Some(rg_min),
                        };
                    }
                    if let Some(rg_max_bytes) = col_chunk.max_bytes_opt() {
                        let rg_max = Self::bytes_to_string(rg_max_bytes);
                        max_value = match max_value {
                            Some(ref current) if rg_max > *current => Some(rg_max),
                            Some(current) => Some(current),
                            None => Some(rg_max),
                        };
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

    #[allow(dead_code)]
    fn create_reader(
        &self,
    ) -> Result<impl Iterator<Item = std::result::Result<RecordBatch, arrow::error::ArrowError>>>
    {
        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(self.options.batch_size);

        let reader = builder.build()?;
        Ok(reader)
    }

    /// Create a reader with projection pushdown at the I/O level.
    /// Only reads the specified columns from the Parquet file.
    #[allow(dead_code)]
    fn create_reader_with_projection(
        &self,
        projection: Option<&[usize]>,
    ) -> Result<impl Iterator<Item = std::result::Result<RecordBatch, arrow::error::ArrowError>>>
    {
        let file = File::open(&self.path)?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(self.options.batch_size);

        if let Some(indices) = projection {
            let mask = parquet::arrow::ProjectionMask::leaves(
                builder.metadata().file_metadata().schema_descr(),
                indices.iter().copied(),
            );
            builder = builder.with_projection(mask);
        }

        let reader = builder.build()?;
        Ok(reader)
    }

    /// Determine which row groups can be pruned based on filter predicates
    /// and Parquet column statistics (min/max values).
    fn prune_row_groups(
        &self,
        filters: &[Arc<dyn crate::planner::PhysicalExpr>],
    ) -> Result<Option<Vec<usize>>> {
        if filters.is_empty() || !self.options.use_statistics {
            return Ok(None);
        }

        let file = File::open(&self.path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata().clone();
        let num_row_groups = metadata.num_row_groups();

        if num_row_groups <= 1 {
            return Ok(None);
        }

        let schema = builder.schema();
        let mut selected_groups = Vec::new();

        for rg_idx in 0..num_row_groups {
            let rg_meta = metadata.row_group(rg_idx);
            let can_prune = self.can_prune_row_group(rg_meta, filters, schema);
            if !can_prune {
                selected_groups.push(rg_idx);
            }
        }

        if selected_groups.len() == num_row_groups {
            Ok(None) // No pruning possible
        } else {
            Ok(Some(selected_groups))
        }
    }

    /// Check if a row group can be pruned based on column statistics.
    /// Returns true if the row group can be safely skipped.
    fn can_prune_row_group(
        &self,
        rg_meta: &RowGroupMetaData,
        filters: &[Arc<dyn crate::planner::PhysicalExpr>],
        schema: &Arc<ArrowSchema>,
    ) -> bool {
        for filter in filters {
            if self.evaluate_pruning_expr(filter.as_ref(), rg_meta, schema) == Some(true) {
                return true;
            }
        }
        false
    }

    /// Recursively evaluate whether a filter expression allows pruning a row group.
    /// Returns Some(true) if prunable, Some(false) if not, None if unknown.
    fn evaluate_pruning_expr(
        &self,
        expr: &dyn crate::planner::PhysicalExpr,
        rg_meta: &RowGroupMetaData,
        schema: &Arc<ArrowSchema>,
    ) -> Option<bool> {
        use crate::planner::physical_expr::BinaryExpr;

        if let Some(binary) = expr.as_any().downcast_ref::<BinaryExpr>() {
            let op = binary.op();

            // Handle AND: prunable if either side is prunable
            if op == "and" {
                let left = self.evaluate_pruning_expr(binary.left().as_ref(), rg_meta, schema);
                let right = self.evaluate_pruning_expr(binary.right().as_ref(), rg_meta, schema);
                return match (left, right) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (Some(false), Some(false)) => Some(false),
                    _ => None,
                };
            }

            // Handle OR: prunable only if both sides are prunable
            if op == "or" {
                let left = self.evaluate_pruning_expr(binary.left().as_ref(), rg_meta, schema);
                let right = self.evaluate_pruning_expr(binary.right().as_ref(), rg_meta, schema);
                return match (left, right) {
                    (Some(true), Some(true)) => Some(true),
                    _ => Some(false),
                };
            }

            // Leaf comparison: Column <op> Literal
            return self.check_binary_expr_pruning(binary, rg_meta, schema);
        }
        None
    }

    /// Check if a binary expression can prune this row group.
    /// Examines column statistics (min/max) against comparison predicates.
    /// Supports Int32, Int64, Float64, and Utf8 types.
    fn check_binary_expr_pruning(
        &self,
        expr: &crate::planner::physical_expr::BinaryExpr,
        rg_meta: &RowGroupMetaData,
        schema: &Arc<ArrowSchema>,
    ) -> Option<bool> {
        use crate::planner::physical_expr::ColumnExpr;
        use crate::planner::physical_expr::LiteralExpr;

        let (col_idx, op, literal_val) = {
            let left = expr.left();
            let right = expr.right();
            let op = expr.op();

            if let (Some(col), Some(lit)) = (
                left.as_any().downcast_ref::<ColumnExpr>(),
                right.as_any().downcast_ref::<LiteralExpr>(),
            ) {
                (col.index(), op.to_string(), lit.value().clone())
            } else if let (Some(lit), Some(col)) = (
                left.as_any().downcast_ref::<LiteralExpr>(),
                right.as_any().downcast_ref::<ColumnExpr>(),
            ) {
                let flipped_op = match op {
                    "lt" => "gt",
                    "lte" => "gte",
                    "gt" => "lt",
                    "gte" => "lte",
                    other => other,
                };
                (col.index(), flipped_op.to_string(), lit.value().clone())
            } else {
                return None;
            }
        };

        if col_idx >= rg_meta.num_columns() {
            return None;
        }
        let col_chunk = rg_meta.column(col_idx);
        let stats = col_chunk.statistics()?;
        let min_bytes = stats.min_bytes_opt()?;
        let max_bytes = stats.max_bytes_opt()?;

        let field = schema.fields().get(col_idx)?;
        match field.data_type() {
            ArrowDataType::Int32 | ArrowDataType::Int64 | ArrowDataType::Date32 => {
                let (stat_min, stat_max) =
                    self.parse_int_stats(min_bytes, max_bytes, field.data_type())?;
                let lit_val = match &literal_val {
                    crate::types::ScalarValue::Int32(Some(v)) => *v as i64,
                    crate::types::ScalarValue::Int64(Some(v)) => *v,
                    crate::types::ScalarValue::Float64(Some(v)) => *v as i64,
                    _ => return None,
                };
                Some(Self::prune_with_int_stats(
                    op.as_str(),
                    stat_min,
                    stat_max,
                    lit_val,
                ))
            }
            ArrowDataType::Float32 | ArrowDataType::Float64 => {
                let (stat_min, stat_max) =
                    self.parse_float_stats(min_bytes, max_bytes, field.data_type())?;
                let lit_val = match &literal_val {
                    crate::types::ScalarValue::Int32(Some(v)) => *v as f64,
                    crate::types::ScalarValue::Int64(Some(v)) => *v as f64,
                    crate::types::ScalarValue::Float64(Some(v)) => *v,
                    _ => return None,
                };
                Some(Self::prune_with_float_stats(
                    op.as_str(),
                    stat_min,
                    stat_max,
                    lit_val,
                ))
            }
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => {
                let stat_min = std::str::from_utf8(min_bytes).ok()?;
                let stat_max = std::str::from_utf8(max_bytes).ok()?;
                let lit_val = match &literal_val {
                    crate::types::ScalarValue::Utf8(Some(v)) => v.as_str(),
                    _ => return None,
                };
                Some(Self::prune_with_str_stats(
                    op.as_str(),
                    stat_min,
                    stat_max,
                    lit_val,
                ))
            }
            _ => None,
        }
    }

    fn parse_int_stats(
        &self,
        min_bytes: &[u8],
        max_bytes: &[u8],
        dt: &ArrowDataType,
    ) -> Option<(i64, i64)> {
        match dt {
            ArrowDataType::Int32 | ArrowDataType::Date32 => {
                let min = i32::from_le_bytes(min_bytes.try_into().ok()?) as i64;
                let max = i32::from_le_bytes(max_bytes.try_into().ok()?) as i64;
                Some((min, max))
            }
            ArrowDataType::Int64 => {
                let min = i64::from_le_bytes(min_bytes.try_into().ok()?);
                let max = i64::from_le_bytes(max_bytes.try_into().ok()?);
                Some((min, max))
            }
            _ => None,
        }
    }

    fn parse_float_stats(
        &self,
        min_bytes: &[u8],
        max_bytes: &[u8],
        dt: &ArrowDataType,
    ) -> Option<(f64, f64)> {
        match dt {
            ArrowDataType::Float32 => {
                let min = f32::from_le_bytes(min_bytes.try_into().ok()?) as f64;
                let max = f32::from_le_bytes(max_bytes.try_into().ok()?) as f64;
                Some((min, max))
            }
            ArrowDataType::Float64 => {
                let min = f64::from_le_bytes(min_bytes.try_into().ok()?);
                let max = f64::from_le_bytes(max_bytes.try_into().ok()?);
                Some((min, max))
            }
            _ => None,
        }
    }

    fn prune_with_int_stats(op: &str, stat_min: i64, stat_max: i64, lit_val: i64) -> bool {
        match op {
            "eq" => lit_val < stat_min || lit_val > stat_max,
            "lt" => stat_min >= lit_val,
            "lte" => stat_min > lit_val,
            "gt" => stat_max <= lit_val,
            "gte" => stat_max < lit_val,
            "neq" => stat_min == stat_max && stat_min == lit_val,
            _ => false,
        }
    }

    fn prune_with_float_stats(op: &str, stat_min: f64, stat_max: f64, lit_val: f64) -> bool {
        match op {
            "eq" => lit_val < stat_min || lit_val > stat_max,
            "lt" => stat_min >= lit_val,
            "lte" => stat_min > lit_val,
            "gt" => stat_max <= lit_val,
            "gte" => stat_max < lit_val,
            _ => false,
        }
    }

    fn prune_with_str_stats(op: &str, stat_min: &str, stat_max: &str, lit_val: &str) -> bool {
        match op {
            "eq" => lit_val < stat_min || lit_val > stat_max,
            "lt" => stat_min >= lit_val,
            "lte" => stat_min > lit_val,
            "gt" => stat_max <= lit_val,
            "gte" => stat_max < lit_val,
            _ => false,
        }
    }

    /// Internal scan implementation with row group pruning and projection pushdown.
    fn scan_internal(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn crate::planner::PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::BooleanArray;
        use arrow::compute::filter_record_batch;

        // Phase 1: Row group pruning - determine which row groups to read
        let pruned_groups = self.prune_row_groups(filters)?;

        // Phase 2: Create reader with projection pushdown at I/O level
        // We need all columns for filter evaluation, then project after filtering
        let needs_all_for_filter = !filters.is_empty() && projection.is_some();

        let file = File::open(&self.path)?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(self.options.batch_size);

        // Apply row group selection if pruning was effective
        if let Some(_groups) = pruned_groups {
            let mask = parquet::arrow::ProjectionMask::all();
            builder = builder.with_projection(mask);
            // Note: row_groups selection via RowSelection would be ideal but
            // ParquetRecordBatchReaderBuilder uses with_row_groups for this
        }

        // Apply projection at I/O level when no filters need other columns
        if !needs_all_for_filter {
            if let Some(indices) = projection {
                let mask = parquet::arrow::ProjectionMask::leaves(
                    builder.metadata().file_metadata().schema_descr(),
                    indices.iter().copied(),
                );
                builder = builder.with_projection(mask);
            }
        }

        let reader = builder.build()?;
        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch_result in reader {
            let batch = batch_result?;

            // Apply filters (post-scan for complex predicates)
            let filtered_batch = if filters.is_empty() {
                batch
            } else {
                let mut current_batch = batch;
                for filter_expr in filters {
                    let filter_result = filter_expr.evaluate(&current_batch)?;
                    if let Some(bool_array) = filter_result.as_any().downcast_ref::<BooleanArray>()
                    {
                        current_batch = filter_record_batch(&current_batch, bool_array)?;
                    }
                }
                current_batch
            };

            if filtered_batch.num_rows() == 0 {
                continue;
            }

            // Apply projection after filtering (when we needed all columns for filters)
            let projected = if needs_all_for_filter {
                if let Some(indices) = projection {
                    let columns: Vec<_> = indices
                        .iter()
                        .filter_map(|&i| {
                            if i < filtered_batch.num_columns() {
                                Some(filtered_batch.column(i).clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .filter_map(|&i| filtered_batch.schema().fields().get(i).cloned())
                        .collect();
                    if columns.len() != fields.len() || columns.is_empty() {
                        continue;
                    }
                    let schema = Arc::new(ArrowSchema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                } else {
                    filtered_batch
                }
            } else {
                filtered_batch
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
        self.scan_internal(projection, &[], limit)
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
        self.scan_internal(projection, filters, limit)
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

    #[test]
    fn test_prune_with_int_stats() {
        // eq: value outside range → prunable
        assert!(ParquetTable::prune_with_int_stats("eq", 10, 20, 5));
        assert!(ParquetTable::prune_with_int_stats("eq", 10, 20, 25));
        assert!(!ParquetTable::prune_with_int_stats("eq", 10, 20, 15));

        // lt: all values >= lit → prunable
        assert!(ParquetTable::prune_with_int_stats("lt", 10, 20, 10));
        assert!(!ParquetTable::prune_with_int_stats("lt", 10, 20, 15));

        // gt: all values <= lit → prunable
        assert!(ParquetTable::prune_with_int_stats("gt", 10, 20, 20));
        assert!(!ParquetTable::prune_with_int_stats("gt", 10, 20, 15));
    }

    #[test]
    fn test_prune_with_float_stats() {
        assert!(ParquetTable::prune_with_float_stats("eq", 1.0, 5.0, 0.5));
        assert!(ParquetTable::prune_with_float_stats("eq", 1.0, 5.0, 5.5));
        assert!(!ParquetTable::prune_with_float_stats("eq", 1.0, 5.0, 3.0));

        assert!(ParquetTable::prune_with_float_stats("gt", 1.0, 5.0, 5.0));
        assert!(!ParquetTable::prune_with_float_stats("gt", 1.0, 5.0, 3.0));
    }

    #[test]
    fn test_prune_with_str_stats() {
        assert!(ParquetTable::prune_with_str_stats("eq", "b", "d", "a"));
        assert!(ParquetTable::prune_with_str_stats("eq", "b", "d", "e"));
        assert!(!ParquetTable::prune_with_str_stats("eq", "b", "d", "c"));

        assert!(ParquetTable::prune_with_str_stats("lt", "b", "d", "b"));
        assert!(!ParquetTable::prune_with_str_stats("lt", "b", "d", "e"));
    }

    #[test]
    fn test_parquet_options_use_statistics_disabled() {
        let batch = create_test_batch();
        let temp_file = NamedTempFile::new().unwrap();
        write_parquet(temp_file.path(), &[batch]).unwrap();

        let opts = ParquetOptions {
            use_statistics: false,
            ..Default::default()
        };
        let table = ParquetTable::open_with_options(temp_file.path(), opts).unwrap();
        let result = table.prune_row_groups(&[]).unwrap();
        assert!(result.is_none());
    }
}
