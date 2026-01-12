//! Data Partitioning Strategies
//!
//! This module provides different partitioning strategies for distributing data
//! across parallel workers.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use arrow::array::{Array, ArrayRef, Int32Array, Int64Array, StringArray, UInt64Array};
use arrow::compute;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};

/// Partitioning scheme for data distribution.
#[derive(Debug, Clone, PartialEq)]
pub enum Partitioning {
    /// Hash partitioning on specified columns
    Hash { columns: Vec<usize>, num_partitions: usize },
    /// Range partitioning on a column with boundaries
    Range { column: usize, boundaries: Vec<String>, num_partitions: usize },
    /// Round-robin partitioning
    RoundRobin { num_partitions: usize },
    /// Single partition (no partitioning)
    Single,
    /// Unknown partitioning (e.g., from external source)
    Unknown { num_partitions: usize },
}

impl Partitioning {
    /// Get the number of partitions.
    pub fn num_partitions(&self) -> usize {
        match self {
            Partitioning::Hash { num_partitions, .. } => *num_partitions,
            Partitioning::Range { num_partitions, .. } => *num_partitions,
            Partitioning::RoundRobin { num_partitions } => *num_partitions,
            Partitioning::Single => 1,
            Partitioning::Unknown { num_partitions } => *num_partitions,
        }
    }

    /// Check if this is a single partition.
    pub fn is_single(&self) -> bool {
        matches!(self, Partitioning::Single) || self.num_partitions() == 1
    }
}

/// Hash partitioner for distributing data based on hash of key columns.
#[derive(Debug, Clone)]
pub struct HashPartitioner {
    num_partitions: usize,
    key_columns: Vec<usize>,
}

impl HashPartitioner {
    /// Create a new hash partitioner.
    pub fn new(num_partitions: usize, key_columns: Vec<usize>) -> Self {
        Self {
            num_partitions: num_partitions.max(1),
            key_columns,
        }
    }

    /// Partition a record batch by hash.
    pub fn partition(&self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        if self.num_partitions == 1 {
            return Ok(vec![batch.clone()]);
        }

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(vec![RecordBatch::new_empty(batch.schema()); self.num_partitions]);
        }

        // Compute hash for each row
        let hashes = self.compute_hashes(batch)?;

        // Group row indices by partition
        let mut partition_indices: Vec<Vec<u64>> = vec![Vec::new(); self.num_partitions];
        for (row_idx, hash) in hashes.iter().enumerate() {
            let partition_id = (*hash as usize) % self.num_partitions;
            partition_indices[partition_id].push(row_idx as u64);
        }

        // Create batches for each partition
        let mut result = Vec::with_capacity(self.num_partitions);
        for indices in partition_indices {
            if indices.is_empty() {
                result.push(RecordBatch::new_empty(batch.schema()));
            } else {
                let indices_array = UInt64Array::from(indices);
                let arrays: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .map(|col| compute::take(col.as_ref(), &indices_array, None))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| BlazeError::execution(format!("Failed to partition: {}", e)))?;
                result.push(RecordBatch::try_new(batch.schema(), arrays)
                    .map_err(|e| BlazeError::execution(format!("Failed to create batch: {}", e)))?);
            }
        }

        Ok(result)
    }

    /// Compute hash values for key columns.
    fn compute_hashes(&self, batch: &RecordBatch) -> Result<Vec<u64>> {
        let num_rows = batch.num_rows();
        let mut hashes = vec![0u64; num_rows];

        for &col_idx in &self.key_columns {
            if col_idx >= batch.num_columns() {
                return Err(BlazeError::execution(format!(
                    "Key column index {} out of bounds",
                    col_idx
                )));
            }

            let column = batch.column(col_idx);
            self.hash_column(column, &mut hashes)?;
        }

        Ok(hashes)
    }

    /// Hash a column's values into the hash array.
    fn hash_column(&self, column: &ArrayRef, hashes: &mut [u64]) -> Result<()> {
        if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !int_array.is_null(i) {
                    let mut hasher = DefaultHasher::new();
                    int_array.value(i).hash(&mut hasher);
                    *hash = hash.wrapping_add(hasher.finish());
                }
            }
        } else if let Some(int_array) = column.as_any().downcast_ref::<Int32Array>() {
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !int_array.is_null(i) {
                    let mut hasher = DefaultHasher::new();
                    int_array.value(i).hash(&mut hasher);
                    *hash = hash.wrapping_add(hasher.finish());
                }
            }
        } else if let Some(str_array) = column.as_any().downcast_ref::<StringArray>() {
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !str_array.is_null(i) {
                    let mut hasher = DefaultHasher::new();
                    str_array.value(i).hash(&mut hasher);
                    *hash = hash.wrapping_add(hasher.finish());
                }
            }
        } else {
            // For other types, use string representation
            for (i, hash) in hashes.iter_mut().enumerate() {
                if !column.is_null(i) {
                    let mut hasher = DefaultHasher::new();
                    i.hash(&mut hasher); // Use row index as fallback
                    *hash = hash.wrapping_add(hasher.finish());
                }
            }
        }

        Ok(())
    }

    /// Get partition ID for a single value.
    pub fn partition_for_value<T: Hash>(&self, value: &T) -> usize {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_partitions
    }
}

/// Range partitioner for distributing data based on value ranges.
#[derive(Debug, Clone)]
pub struct RangePartitioner {
    num_partitions: usize,
    key_column: usize,
    boundaries: Vec<i64>, // Sorted boundary values
}

impl RangePartitioner {
    /// Create a new range partitioner with explicit boundaries.
    pub fn new(num_partitions: usize, key_column: usize, boundaries: Vec<i64>) -> Self {
        let mut sorted_boundaries = boundaries;
        sorted_boundaries.sort();
        Self {
            num_partitions: num_partitions.max(1),
            key_column,
            boundaries: sorted_boundaries,
        }
    }

    /// Create a range partitioner with evenly spaced boundaries.
    pub fn uniform(num_partitions: usize, key_column: usize, min: i64, max: i64) -> Self {
        let range = (max - min) / (num_partitions as i64);
        let boundaries: Vec<i64> = (1..num_partitions as i64)
            .map(|i| min + i * range)
            .collect();

        Self {
            num_partitions: num_partitions.max(1),
            key_column,
            boundaries,
        }
    }

    /// Partition a record batch by range.
    pub fn partition(&self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        if self.num_partitions == 1 {
            return Ok(vec![batch.clone()]);
        }

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(vec![RecordBatch::new_empty(batch.schema()); self.num_partitions]);
        }

        // Get the key column
        let column = batch.column(self.key_column);

        // Determine partition for each row
        let mut partition_indices: Vec<Vec<u64>> = vec![Vec::new(); self.num_partitions];

        if let Some(int_array) = column.as_any().downcast_ref::<Int64Array>() {
            for row_idx in 0..num_rows {
                let partition_id = if int_array.is_null(row_idx) {
                    0 // Nulls go to first partition
                } else {
                    self.partition_for_value(int_array.value(row_idx))
                };
                partition_indices[partition_id].push(row_idx as u64);
            }
        } else {
            // For non-integer types, use first partition
            for row_idx in 0..num_rows {
                partition_indices[0].push(row_idx as u64);
            }
        }

        // Create batches for each partition
        let mut result = Vec::with_capacity(self.num_partitions);
        for indices in partition_indices {
            if indices.is_empty() {
                result.push(RecordBatch::new_empty(batch.schema()));
            } else {
                let indices_array = UInt64Array::from(indices);
                let arrays: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .map(|col| compute::take(col.as_ref(), &indices_array, None))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| BlazeError::execution(format!("Failed to partition: {}", e)))?;
                result.push(RecordBatch::try_new(batch.schema(), arrays)
                    .map_err(|e| BlazeError::execution(format!("Failed to create batch: {}", e)))?);
            }
        }

        Ok(result)
    }

    /// Get partition ID for a value.
    pub fn partition_for_value(&self, value: i64) -> usize {
        // Binary search for the partition
        match self.boundaries.binary_search(&value) {
            Ok(idx) => idx + 1, // Value matches boundary, go to next partition
            Err(idx) => idx,    // Value falls in partition idx
        }
        .min(self.num_partitions - 1)
    }
}

/// Round-robin partitioner for even distribution.
#[derive(Debug, Clone)]
pub struct RoundRobinPartitioner {
    num_partitions: usize,
}

impl RoundRobinPartitioner {
    /// Create a new round-robin partitioner.
    pub fn new(num_partitions: usize) -> Self {
        Self {
            num_partitions: num_partitions.max(1),
        }
    }

    /// Partition a record batch round-robin.
    pub fn partition(&self, batch: &RecordBatch) -> Result<Vec<RecordBatch>> {
        if self.num_partitions == 1 {
            return Ok(vec![batch.clone()]);
        }

        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(vec![RecordBatch::new_empty(batch.schema()); self.num_partitions]);
        }

        // Group row indices by partition (round-robin)
        let mut partition_indices: Vec<Vec<u64>> = vec![Vec::new(); self.num_partitions];
        for row_idx in 0..num_rows {
            let partition_id = row_idx % self.num_partitions;
            partition_indices[partition_id].push(row_idx as u64);
        }

        // Create batches for each partition
        let mut result = Vec::with_capacity(self.num_partitions);
        for indices in partition_indices {
            if indices.is_empty() {
                result.push(RecordBatch::new_empty(batch.schema()));
            } else {
                let indices_array = UInt64Array::from(indices);
                let arrays: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .map(|col| compute::take(col.as_ref(), &indices_array, None))
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|e| BlazeError::execution(format!("Failed to partition: {}", e)))?;
                result.push(RecordBatch::try_new(batch.schema(), arrays)
                    .map_err(|e| BlazeError::execution(format!("Failed to create batch: {}", e)))?);
            }
        }

        Ok(result)
    }
}

/// Utility to coalesce multiple batches into one.
pub fn coalesce_batches(batches: Vec<RecordBatch>) -> Result<Option<RecordBatch>> {
    if batches.is_empty() {
        return Ok(None);
    }

    if batches.len() == 1 {
        return Ok(Some(batches.into_iter().next().unwrap()));
    }

    let schema = batches[0].schema();

    // Concatenate all arrays
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for col_idx in 0..schema.fields().len() {
        let arrays: Vec<&dyn Array> = batches
            .iter()
            .map(|b| b.column(col_idx).as_ref())
            .collect();

        let concatenated = compute::concat(&arrays)
            .map_err(|e| BlazeError::execution(format!("Failed to concatenate: {}", e)))?;

        columns.push(concatenated);
    }

    let result = RecordBatch::try_new(schema, columns)
        .map_err(|e| BlazeError::execution(format!("Failed to create batch: {}", e)))?;

    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_test_batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int64Array::from(ids.to_vec());
        let name_array = StringArray::from(names.to_vec());

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn test_hash_partitioner() {
        let batch = make_test_batch(&[1, 2, 3, 4, 5, 6, 7, 8], &["a", "b", "c", "d", "e", "f", "g", "h"]);

        let partitioner = HashPartitioner::new(4, vec![0]);
        let partitions = partitioner.partition(&batch).unwrap();

        assert_eq!(partitions.len(), 4);

        // All rows should be distributed
        let total_rows: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total_rows, 8);
    }

    #[test]
    fn test_range_partitioner() {
        let batch = make_test_batch(&[10, 20, 30, 40, 50, 60, 70, 80], &["a", "b", "c", "d", "e", "f", "g", "h"]);

        let partitioner = RangePartitioner::uniform(4, 0, 0, 100);
        let partitions = partitioner.partition(&batch).unwrap();

        assert_eq!(partitions.len(), 4);

        let total_rows: usize = partitions.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total_rows, 8);
    }

    #[test]
    fn test_round_robin_partitioner() {
        let batch = make_test_batch(&[1, 2, 3, 4, 5, 6], &["a", "b", "c", "d", "e", "f"]);

        let partitioner = RoundRobinPartitioner::new(3);
        let partitions = partitioner.partition(&batch).unwrap();

        assert_eq!(partitions.len(), 3);

        // Round-robin should distribute evenly
        for partition in &partitions {
            assert_eq!(partition.num_rows(), 2);
        }
    }

    #[test]
    fn test_coalesce_batches() {
        let batch1 = make_test_batch(&[1, 2], &["a", "b"]);
        let batch2 = make_test_batch(&[3, 4], &["c", "d"]);

        let coalesced = coalesce_batches(vec![batch1, batch2]).unwrap().unwrap();

        assert_eq!(coalesced.num_rows(), 4);
    }

    #[test]
    fn test_single_partition() {
        let batch = make_test_batch(&[1, 2, 3], &["a", "b", "c"]);

        let partitioner = HashPartitioner::new(1, vec![0]);
        let partitions = partitioner.partition(&batch).unwrap();

        assert_eq!(partitions.len(), 1);
        assert_eq!(partitions[0].num_rows(), 3);
    }
}
