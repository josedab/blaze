//! CSV file table implementation.

use std::any::Any;
use std::collections::HashSet;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, Int64Array, Float64Array, StringArray};
use arrow::csv::{Reader as CsvReader, ReaderBuilder};
use arrow::datatypes::{Schema as ArrowSchema, DataType};
use arrow::record_batch::RecordBatch;

use crate::catalog::{ColumnStatistics, TableProvider, TableStatistics, TableType};
use crate::error::Result;
use crate::types::Schema;

/// A table backed by a CSV file.
#[derive(Debug)]
pub struct CsvTable {
    /// Path to the CSV file
    path: PathBuf,
    /// Schema of the table
    schema: Schema,
    /// Arrow schema
    arrow_schema: Arc<ArrowSchema>,
    /// CSV options
    options: CsvOptions,
    /// Cached statistics (computed lazily on first scan)
    statistics: parking_lot::RwLock<Option<TableStatistics>>,
}

/// Options for reading CSV files.
#[derive(Debug, Clone)]
pub struct CsvOptions {
    /// Delimiter character
    pub delimiter: u8,
    /// Whether the file has a header row
    pub has_header: bool,
    /// Batch size for reading
    pub batch_size: usize,
    /// Maximum number of records to infer schema from
    pub schema_infer_max_records: usize,
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: b',',
            has_header: true,
            batch_size: 8192,
            schema_infer_max_records: 1000,
        }
    }
}

impl CsvTable {
    /// Open a CSV file.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_options(path, CsvOptions::default())
    }

    /// Open a CSV file with custom options.
    pub fn open_with_options(path: impl AsRef<Path>, options: CsvOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Infer schema from file
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        let (arrow_schema, _) = arrow::csv::reader::Format::default()
            .with_delimiter(options.delimiter)
            .with_header(options.has_header)
            .infer_schema(&mut reader, Some(options.schema_infer_max_records))?;

        let arrow_schema = Arc::new(arrow_schema);
        let schema = Schema::from_arrow(&arrow_schema)?;

        Ok(Self {
            path,
            schema,
            arrow_schema,
            options,
            statistics: parking_lot::RwLock::new(None),
        })
    }

    /// Create a CSV table with a predefined schema.
    pub fn with_schema(path: impl AsRef<Path>, schema: Schema, options: CsvOptions) -> Self {
        let arrow_schema = Arc::new(schema.to_arrow());
        Self {
            path: path.as_ref().to_path_buf(),
            schema,
            arrow_schema,
            options,
            statistics: parking_lot::RwLock::new(None),
        }
    }

    /// Get the file path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the options.
    pub fn options(&self) -> &CsvOptions {
        &self.options
    }

    fn create_reader(&self) -> Result<CsvReader<BufReader<File>>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);

        let csv_reader = ReaderBuilder::new(self.arrow_schema.clone())
            .with_delimiter(self.options.delimiter)
            .with_header(self.options.has_header)
            .with_batch_size(self.options.batch_size)
            .build(reader)?;

        Ok(csv_reader)
    }

    /// Internal scan implementation that handles both regular scan and filter pushdown
    fn scan_internal(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn crate::planner::PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::BooleanArray;
        use arrow::compute::filter_record_batch;

        let mut reader = self.create_reader()?;
        let mut result = Vec::new();
        let mut rows_collected = 0;

        while let Some(batch_result) = reader.next() {
            let batch = batch_result?;

            // Apply filters first (before projection) since filters may reference
            // columns that aren't in the projection
            let filtered_batch = if filters.is_empty() {
                batch
            } else {
                let mut current_batch = batch;
                for filter_expr in filters {
                    let filter_result = filter_expr.evaluate(&current_batch)?;
                    if let Some(bool_array) = filter_result.as_any().downcast_ref::<BooleanArray>() {
                        current_batch = filter_record_batch(&current_batch, bool_array)?;
                    }
                    // If filter result is not boolean, skip it (shouldn't happen with valid filters)
                }
                current_batch
            };

            // Skip empty batches after filtering
            if filtered_batch.num_rows() == 0 {
                continue;
            }

            // Apply projection
            let projected = match projection {
                Some(indices) => {
                    let columns: Vec<_> = indices.iter().map(|&i| filtered_batch.column(i).clone()).collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| filtered_batch.schema().field(i).clone())
                        .collect();
                    let schema = Arc::new(ArrowSchema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                }
                None => filtered_batch,
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

    /// Compute statistics by reading the file
    fn compute_statistics(&self) -> Result<TableStatistics> {
        let mut reader = self.create_reader()?;
        let mut total_rows = 0usize;
        let mut total_bytes = 0usize;
        let num_columns = self.schema.len();

        // Track statistics per column
        let mut null_counts: Vec<usize> = vec![0; num_columns];
        let mut min_values: Vec<Option<String>> = vec![None; num_columns];
        let mut max_values: Vec<Option<String>> = vec![None; num_columns];
        let mut distinct_sets: Vec<HashSet<String>> = vec![HashSet::new(); num_columns];

        while let Some(batch_result) = reader.next() {
            let batch = batch_result?;
            total_rows += batch.num_rows();
            total_bytes += batch.get_array_memory_size();

            // Compute statistics for each column
            for (col_idx, col) in batch.columns().iter().enumerate() {
                null_counts[col_idx] += col.null_count();

                let data_type = col.data_type();

                // Extract min/max/distinct based on data type
                match data_type {
                    DataType::Int64 => {
                        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    let val = arr.value(i);
                                    let val_str = val.to_string();
                                    distinct_sets[col_idx].insert(val_str.clone());

                                    let update_min = min_values[col_idx]
                                        .as_ref()
                                        .map(|m| val < m.parse::<i64>().unwrap_or(i64::MAX))
                                        .unwrap_or(true);
                                    if update_min {
                                        min_values[col_idx] = Some(val_str.clone());
                                    }

                                    let update_max = max_values[col_idx]
                                        .as_ref()
                                        .map(|m| val > m.parse::<i64>().unwrap_or(i64::MIN))
                                        .unwrap_or(true);
                                    if update_max {
                                        max_values[col_idx] = Some(val_str);
                                    }
                                }
                            }
                        }
                    }
                    DataType::Float64 => {
                        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    let val = arr.value(i);
                                    let val_str = val.to_string();
                                    distinct_sets[col_idx].insert(val_str.clone());

                                    let update_min = min_values[col_idx]
                                        .as_ref()
                                        .map(|m| val < m.parse::<f64>().unwrap_or(f64::MAX))
                                        .unwrap_or(true);
                                    if update_min {
                                        min_values[col_idx] = Some(val_str.clone());
                                    }

                                    let update_max = max_values[col_idx]
                                        .as_ref()
                                        .map(|m| val > m.parse::<f64>().unwrap_or(f64::MIN))
                                        .unwrap_or(true);
                                    if update_max {
                                        max_values[col_idx] = Some(val_str);
                                    }
                                }
                            }
                        }
                    }
                    DataType::Utf8 => {
                        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
                            for i in 0..arr.len() {
                                if !arr.is_null(i) {
                                    let val = arr.value(i).to_string();
                                    distinct_sets[col_idx].insert(val.clone());

                                    let update_min = min_values[col_idx]
                                        .as_ref()
                                        .map(|m| val < *m)
                                        .unwrap_or(true);
                                    if update_min {
                                        min_values[col_idx] = Some(val.clone());
                                    }

                                    let update_max = max_values[col_idx]
                                        .as_ref()
                                        .map(|m| val > *m)
                                        .unwrap_or(true);
                                    if update_max {
                                        max_values[col_idx] = Some(val);
                                    }
                                }
                            }
                        }
                    }
                    _ => {
                        // For other types, just track null counts
                    }
                }
            }
        }

        // Build column statistics
        let column_statistics: Vec<ColumnStatistics> = (0..num_columns)
            .map(|col_idx| ColumnStatistics {
                null_count: Some(null_counts[col_idx]),
                distinct_count: Some(distinct_sets[col_idx].len()),
                min_value: min_values[col_idx].clone(),
                max_value: max_values[col_idx].clone(),
            })
            .collect();

        Ok(TableStatistics {
            num_rows: Some(total_rows),
            total_byte_size: Some(total_bytes),
            column_statistics,
        })
    }
}

impl TableProvider for CsvTable {
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
        // Check if we have cached statistics
        if let Some(stats) = self.statistics.read().as_ref() {
            return Some(stats.clone());
        }

        // Compute statistics and cache them
        if let Ok(stats) = self.compute_statistics() {
            *self.statistics.write() = Some(stats.clone());
            return Some(stats);
        }

        None
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

/// Write record batches to a CSV file.
pub fn write_csv(path: impl AsRef<Path>, batches: &[RecordBatch]) -> Result<()> {
    use arrow::csv::Writer;

    if batches.is_empty() {
        return Ok(());
    }

    let file = File::create(path)?;
    let mut writer = Writer::new(file);

    for batch in batches {
        writer.write(batch)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn create_test_csv() -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        writeln!(file, "id,name,value").unwrap();
        writeln!(file, "1,Alice,100").unwrap();
        writeln!(file, "2,Bob,200").unwrap();
        writeln!(file, "3,Charlie,300").unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_csv_table_open() {
        let csv_file = create_test_csv();
        let table = CsvTable::open(csv_file.path()).unwrap();

        assert_eq!(table.schema().len(), 3);
    }

    #[test]
    fn test_csv_table_scan() {
        let csv_file = create_test_csv();
        let table = CsvTable::open(csv_file.path()).unwrap();

        let batches = table.scan(None, &[], None).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_csv_table_scan_with_limit() {
        let csv_file = create_test_csv();
        let table = CsvTable::open(csv_file.path()).unwrap();

        let batches = table.scan(None, &[], Some(2)).unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
