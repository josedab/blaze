//! In-memory table implementation.

use std::any::Any;
use std::sync::Arc;

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;

use crate::catalog::{ColumnStatistics, TableProvider, TableStatistics, TableType};
use crate::error::{BlazeError, Result};
use crate::planner::PhysicalExpr;
use crate::types::{Field, Schema};

/// An in-memory table backed by Arrow RecordBatches.
#[derive(Debug)]
pub struct MemoryTable {
    schema: Schema,
    batches: RwLock<Vec<RecordBatch>>,
}

impl MemoryTable {
    /// Create a new memory table with schema and data.
    pub fn new(schema: Schema, batches: Vec<RecordBatch>) -> Self {
        Self {
            schema,
            batches: RwLock::new(batches),
        }
    }

    /// Create an empty memory table.
    pub fn empty(schema: Schema) -> Self {
        Self {
            schema,
            batches: RwLock::new(Vec::new()),
        }
    }

    /// Get the number of rows in the table.
    pub fn num_rows(&self) -> usize {
        self.batches.read().iter().map(|b| b.num_rows()).sum()
    }

    /// Get the number of batches.
    pub fn num_batches(&self) -> usize {
        self.batches.read().len()
    }

    /// Append batches to the table.
    pub fn append(&self, mut new_batches: Vec<RecordBatch>) {
        self.batches.write().append(&mut new_batches);
    }

    /// Clear all data from the table.
    pub fn clear(&self) {
        self.batches.write().clear();
    }

    /// Replace all batches in the table.
    pub fn replace(&self, new_batches: Vec<RecordBatch>) {
        *self.batches.write() = new_batches;
    }

    /// Get a clone of all batches in the table.
    pub fn batches(&self) -> Vec<RecordBatch> {
        self.batches.read().clone()
    }

    /// Return a new MemoryTable with an additional column filled with nulls.
    pub fn with_added_column(
        &self,
        name: &str,
        data_type: arrow::datatypes::DataType,
        nullable: bool,
    ) -> Result<Self> {
        if !nullable {
            return Err(BlazeError::analysis(
                "ADD COLUMN with NOT NULL requires a default value, which is not yet supported",
            ));
        }
        let mut fields: Vec<Field> = self.schema.fields().to_vec();
        fields.push(Field::new(
            name,
            crate::types::DataType::from_arrow(&data_type)?,
            nullable,
        ));
        let new_schema = Schema::new(fields);

        let batches = self.batches.read();
        let new_batches: Vec<RecordBatch> = batches
            .iter()
            .map(|batch| {
                let mut columns: Vec<Arc<dyn arrow::array::Array>> =
                    batch.columns().to_vec();
                columns.push(arrow::array::new_null_array(&data_type, batch.num_rows()));
                let arrow_schema = Arc::new(new_schema.to_arrow());
                RecordBatch::try_new(arrow_schema, columns)
                    .map_err(|e| BlazeError::execution(format!("Failed to add column: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self::new(new_schema, new_batches))
    }

    /// Return a new MemoryTable with the specified column removed.
    pub fn with_dropped_column(&self, name: &str) -> Result<Self> {
        let idx = self.schema.index_of(name).ok_or_else(|| {
            BlazeError::analysis(format!("Column '{name}' not found in table"))
        })?;

        let fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != idx)
            .map(|(_, f)| f.clone())
            .collect();
        if fields.is_empty() {
            return Err(BlazeError::analysis(
                "Cannot drop the last column of a table",
            ));
        }
        let new_schema = Schema::new(fields);

        let batches = self.batches.read();
        let new_batches: Vec<RecordBatch> = batches
            .iter()
            .map(|batch| {
                let columns: Vec<Arc<dyn arrow::array::Array>> = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| *i != idx)
                    .map(|(_, c)| c.clone())
                    .collect();
                let arrow_schema = Arc::new(new_schema.to_arrow());
                RecordBatch::try_new(arrow_schema, columns)
                    .map_err(|e| BlazeError::execution(format!("Failed to drop column: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self::new(new_schema, new_batches))
    }

    /// Return a new MemoryTable with a column renamed.
    pub fn with_renamed_column(&self, old_name: &str, new_name: &str) -> Result<Self> {
        let idx = self.schema.index_of(old_name).ok_or_else(|| {
            BlazeError::analysis(format!("Column '{old_name}' not found in table"))
        })?;

        let fields: Vec<Field> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if i == idx {
                    Field::new(new_name, f.data_type().clone(), f.is_nullable())
                } else {
                    f.clone()
                }
            })
            .collect();
        let new_schema = Schema::new(fields);

        let batches = self.batches.read();
        let new_batches: Vec<RecordBatch> = batches
            .iter()
            .map(|batch| {
                let arrow_schema = Arc::new(new_schema.to_arrow());
                RecordBatch::try_new(arrow_schema, batch.columns().to_vec())
                    .map_err(|e| BlazeError::execution(format!("Failed to rename column: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self::new(new_schema, new_batches))
    }
}

impl TableProvider for MemoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<TableStatistics> {
        let batches = self.batches.read();
        let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let total_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        // Compute column statistics
        let num_columns = self.schema.len();
        let mut null_counts: Vec<usize> = vec![0; num_columns];

        for batch in batches.iter() {
            for (col_idx, col) in batch.columns().iter().enumerate() {
                null_counts[col_idx] += col.null_count();
            }
        }

        let column_statistics: Vec<ColumnStatistics> = null_counts
            .into_iter()
            .map(|null_count| ColumnStatistics {
                null_count: Some(null_count),
                distinct_count: None,
                min_value: None,
                max_value: None,
            })
            .collect();

        Some(TableStatistics {
            num_rows: Some(num_rows),
            total_byte_size: Some(total_bytes),
            column_statistics,
        })
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self.batches.read();
        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch in batches.iter() {
            let projected = match projection {
                Some(indices) => {
                    let columns: Vec<_> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| batch.schema().field(i).clone())
                        .collect();
                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                }
                None => batch.clone(),
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
        true
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn scan_with_filters(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self.batches.read();
        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch in batches.iter() {
            // Apply filters first (before projection for correct column indices)
            let filtered_batch = if filters.is_empty() {
                batch.clone()
            } else {
                let mut current_batch = batch.clone();
                for filter in filters {
                    if current_batch.num_rows() == 0 {
                        break;
                    }
                    let filter_array = filter.evaluate(&current_batch)?;
                    let filter_array = filter_array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            crate::error::BlazeError::type_error("Filter must return boolean")
                        })?;
                    current_batch = filter_record_batch(&current_batch, filter_array)?;
                }
                current_batch
            };

            if filtered_batch.num_rows() == 0 {
                continue;
            }

            // Apply projection
            let projected = match projection {
                Some(indices) => {
                    let columns: Vec<_> = indices
                        .iter()
                        .map(|&i| filtered_batch.column(i).clone())
                        .collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| filtered_batch.schema().field(i).clone())
                        .collect();
                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                }
                None => filtered_batch,
            };

            // Apply limit
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

    fn insert(&self, batches: Vec<RecordBatch>) -> Result<usize> {
        let count: usize = batches.iter().map(|b| b.num_rows()).sum();
        self.append(batches);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};
    use arrow::array::Int64Array;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int64, false),
        ]));

        let a = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let b = Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]));

        RecordBatch::try_new(schema, vec![a, b]).unwrap()
    }

    #[test]
    fn test_memory_table() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);

        let batch = create_test_batch();
        let table = MemoryTable::new(schema, vec![batch]);

        assert_eq!(table.num_rows(), 5);
        assert_eq!(table.num_batches(), 1);
    }

    #[test]
    fn test_scan_with_projection() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);

        let batch = create_test_batch();
        let table = MemoryTable::new(schema, vec![batch]);

        let result = table.scan(Some(&[0]), &[], None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_columns(), 1);
    }

    #[test]
    fn test_scan_with_limit() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);

        let batch = create_test_batch();
        let table = MemoryTable::new(schema, vec![batch]);

        let result = table.scan(None, &[], Some(3)).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }
}
