//! Hash join and sort-merge join operators.
//!
//! Supports spill-to-disk when the build side exceeds a configurable memory
//! threshold. When spilling is enabled, both sides are hash-partitioned and
//! each partition is joined independently, keeping peak memory bounded.

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{
    new_null_array, Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array, UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Schema as ArrowSchema};

use crate::error::{BlazeError, Result};
use crate::planner::JoinType;

use super::try_downcast;

/// Configuration for hash join spill-to-disk behavior.
#[derive(Debug, Clone)]
pub struct HashJoinSpillConfig {
    /// Maximum bytes for the build side before spilling to disk
    pub memory_threshold: usize,
    /// Directory for spill files (defaults to system temp dir)
    pub spill_dir: Option<PathBuf>,
    /// Number of hash partitions when spilling
    pub num_partitions: usize,
    /// Whether spill-to-disk is enabled
    pub enabled: bool,
}

impl Default for HashJoinSpillConfig {
    fn default() -> Self {
        Self {
            memory_threshold: 256 * 1024 * 1024, // 256 MB
            spill_dir: None,
            num_partitions: 16,
            enabled: false,
        }
    }
}

/// Estimate the in-memory byte size of a `RecordBatch` by summing column buffer sizes.
fn estimate_batch_bytes(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum()
}

/// Hash table for hash join operations.
///
/// The hash table is built from the "build" side (typically the smaller table)
/// and then probed with the "probe" side to find matching rows.
#[derive(Debug)]
pub struct JoinHashTable {
    /// Map from hash key to list of (batch_idx, row_idx) pairs
    map: HashMap<u64, Vec<(usize, usize)>>,
    /// The batches stored in the hash table
    batches: Vec<RecordBatch>,
    /// Total number of rows
    num_rows: usize,
}

impl JoinHashTable {
    /// Build a hash table from record batches using the given key expressions.
    pub fn build(
        batches: Vec<RecordBatch>,
        key_exprs: &[Arc<dyn crate::planner::PhysicalExpr>],
    ) -> Result<Self> {
        let mut map: HashMap<u64, Vec<(usize, usize)>> = HashMap::new();
        let mut total_rows = 0;

        for (batch_idx, batch) in batches.iter().enumerate() {
            // Evaluate key expressions to get key columns
            let key_columns: Vec<ArrayRef> = key_exprs
                .iter()
                .map(|expr| expr.evaluate(batch))
                .collect::<Result<Vec<_>>>()?;

            let hashes = Self::compute_hashes_batch(&key_columns, batch.num_rows())?;

            for (row, hash) in hashes.into_iter().enumerate() {
                map.entry(hash).or_default().push((batch_idx, row));
            }
            total_rows += batch.num_rows();
        }

        Ok(Self {
            map,
            batches,
            num_rows: total_rows,
        })
    }

    /// Probe the hash table with a key hash.
    pub fn probe(&self, hash: u64) -> Option<&Vec<(usize, usize)>> {
        self.map.get(&hash)
    }

    /// Get a row's columns from the stored batches.
    pub fn get_row_arrays(&self, batch_idx: usize, row_idx: usize) -> Vec<ArrayRef> {
        let batch = &self.batches[batch_idx];
        batch
            .columns()
            .iter()
            .map(|col| col.slice(row_idx, 1))
            .collect()
    }

    /// Get the batch at the given index.
    pub fn batch(&self, batch_idx: usize) -> &RecordBatch {
        &self.batches[batch_idx]
    }

    /// Get all batches.
    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Get total number of rows.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Compute hash from arrays at a specific row.
    pub fn compute_hash_from_arrays(arrays: &[ArrayRef], row: usize) -> Result<u64> {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();

        for array in arrays {
            Self::hash_array_value(array, row, &mut hasher)?;
        }

        Ok(hasher.finish())
    }

    fn hash_array_value<H: Hasher>(array: &ArrayRef, row: usize, hasher: &mut H) -> Result<()> {
        if array.is_null(row) {
            // Use a special marker for NULL
            0xDEADBEEFu64.hash(hasher);
            return Ok(());
        }

        match array.data_type() {
            DataType::Boolean => {
                let arr = try_downcast::<BooleanArray>(array, "BooleanArray")?;
                arr.value(row).hash(hasher);
            }
            DataType::Int8 => {
                let arr = try_downcast::<Int8Array>(array, "Int8Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Int16 => {
                let arr = try_downcast::<Int16Array>(array, "Int16Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(array, "Int32Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(array, "Int64Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::UInt32 => {
                let arr = try_downcast::<UInt32Array>(array, "UInt32Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::UInt64 => {
                let arr = try_downcast::<UInt64Array>(array, "UInt64Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Float32 => {
                let arr = try_downcast::<Float32Array>(array, "Float32Array")?;
                arr.value(row).to_bits().hash(hasher);
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(array, "Float64Array")?;
                arr.value(row).to_bits().hash(hasher);
            }
            DataType::Utf8 => {
                let arr = try_downcast::<StringArray>(array, "StringArray")?;
                arr.value(row).hash(hasher);
            }
            DataType::Date32 => {
                let arr = try_downcast::<Date32Array>(array, "Date32Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Date64 => {
                let arr = try_downcast::<Date64Array>(array, "Date64Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Timestamp(unit, _) => {
                // All timestamp types store i64 internally, but need to downcast to correct type
                use arrow::datatypes::TimeUnit;
                let value: i64 = match unit {
                    TimeUnit::Second => {
                        let arr =
                            try_downcast::<TimestampSecondArray>(array, "TimestampSecondArray")?;
                        arr.value(row)
                    }
                    TimeUnit::Millisecond => {
                        let arr = try_downcast::<TimestampMillisecondArray>(
                            array,
                            "TimestampMillisecondArray",
                        )?;
                        arr.value(row)
                    }
                    TimeUnit::Microsecond => {
                        let arr = try_downcast::<TimestampMicrosecondArray>(
                            array,
                            "TimestampMicrosecondArray",
                        )?;
                        arr.value(row)
                    }
                    TimeUnit::Nanosecond => {
                        let arr = try_downcast::<TimestampNanosecondArray>(
                            array,
                            "TimestampNanosecondArray",
                        )?;
                        arr.value(row)
                    }
                };
                value.hash(hasher);
            }
            DataType::Decimal128(_, _) => {
                let arr = try_downcast::<Decimal128Array>(array, "Decimal128Array")?;
                arr.value(row).hash(hasher);
            }
            dt => {
                return Err(BlazeError::not_implemented(format!(
                    "JOIN hash for type {:?}. Supported types: Boolean, Int8-64, UInt32-64, Float32-64, Utf8, Date32, Date64, Timestamp, Decimal128.",
                    dt
                )));
            }
        }

        Ok(())
    }

    /// Compute hash values for all rows in the given key columns at once.
    ///
    /// This is more efficient than calling `compute_hash_from_arrays` per row
    /// because it processes each column across all rows before moving to the next,
    /// improving cache locality and enabling future SIMD optimizations.
    fn compute_hashes_batch(key_columns: &[ArrayRef], num_rows: usize) -> Result<Vec<u64>> {
        let mut hashes = vec![0u64; num_rows];

        for col in key_columns {
            Self::hash_column_batch(col, &mut hashes)?;
        }

        Ok(hashes)
    }

    fn hash_column_batch(array: &ArrayRef, hashes: &mut [u64]) -> Result<()> {
        use std::collections::hash_map::DefaultHasher;

        macro_rules! hash_primitive_batch {
            ($array_type:ty, $array:expr, $hashes:expr, $write_fn:ident) => {{
                let arr = try_downcast::<$array_type>($array, stringify!($array_type))?;
                for (i, hash) in $hashes.iter_mut().enumerate() {
                    let mut hasher = DefaultHasher::new();
                    hasher.write_u64(*hash);
                    if arr.is_null(i) {
                        0xDEADBEEFu64.hash(&mut hasher);
                    } else {
                        hasher.$write_fn(arr.value(i));
                    }
                    *hash = hasher.finish();
                }
            }};
        }

        match array.data_type() {
            DataType::Boolean => {
                let arr = try_downcast::<BooleanArray>(array, "BooleanArray")?;
                for (i, hash) in hashes.iter_mut().enumerate() {
                    let mut hasher = DefaultHasher::new();
                    hasher.write_u64(*hash);
                    if arr.is_null(i) {
                        0xDEADBEEFu64.hash(&mut hasher);
                    } else {
                        arr.value(i).hash(&mut hasher);
                    }
                    *hash = hasher.finish();
                }
            }
            DataType::Int8 => {
                hash_primitive_batch!(Int8Array, array, hashes, write_i8);
            }
            DataType::Int16 => {
                hash_primitive_batch!(Int16Array, array, hashes, write_i16);
            }
            DataType::Int32 => {
                hash_primitive_batch!(Int32Array, array, hashes, write_i32);
            }
            DataType::Int64 => {
                hash_primitive_batch!(Int64Array, array, hashes, write_i64);
            }
            DataType::UInt32 => {
                hash_primitive_batch!(UInt32Array, array, hashes, write_u32);
            }
            DataType::UInt64 => {
                hash_primitive_batch!(UInt64Array, array, hashes, write_u64);
            }
            DataType::Float32 => {
                let arr = try_downcast::<Float32Array>(array, "Float32Array")?;
                for (i, hash) in hashes.iter_mut().enumerate() {
                    let mut hasher = DefaultHasher::new();
                    hasher.write_u64(*hash);
                    if arr.is_null(i) {
                        0xDEADBEEFu64.hash(&mut hasher);
                    } else {
                        hasher.write_u32(arr.value(i).to_bits());
                    }
                    *hash = hasher.finish();
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(array, "Float64Array")?;
                for (i, hash) in hashes.iter_mut().enumerate() {
                    let mut hasher = DefaultHasher::new();
                    hasher.write_u64(*hash);
                    if arr.is_null(i) {
                        0xDEADBEEFu64.hash(&mut hasher);
                    } else {
                        hasher.write_u64(arr.value(i).to_bits());
                    }
                    *hash = hasher.finish();
                }
            }
            DataType::Utf8 => {
                let arr = try_downcast::<StringArray>(array, "StringArray")?;
                for (i, hash) in hashes.iter_mut().enumerate() {
                    let mut hasher = DefaultHasher::new();
                    hasher.write_u64(*hash);
                    if arr.is_null(i) {
                        0xDEADBEEFu64.hash(&mut hasher);
                    } else {
                        arr.value(i).hash(&mut hasher);
                    }
                    *hash = hasher.finish();
                }
            }
            DataType::Date32 => {
                hash_primitive_batch!(Date32Array, array, hashes, write_i32);
            }
            DataType::Date64 => {
                hash_primitive_batch!(Date64Array, array, hashes, write_i64);
            }
            DataType::Timestamp(unit, _) => {
                use arrow::datatypes::TimeUnit;
                match unit {
                    TimeUnit::Second => {
                        hash_primitive_batch!(TimestampSecondArray, array, hashes, write_i64);
                    }
                    TimeUnit::Millisecond => {
                        hash_primitive_batch!(TimestampMillisecondArray, array, hashes, write_i64);
                    }
                    TimeUnit::Microsecond => {
                        hash_primitive_batch!(TimestampMicrosecondArray, array, hashes, write_i64);
                    }
                    TimeUnit::Nanosecond => {
                        hash_primitive_batch!(TimestampNanosecondArray, array, hashes, write_i64);
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let arr = try_downcast::<Decimal128Array>(array, "Decimal128Array")?;
                for (i, hash) in hashes.iter_mut().enumerate() {
                    let mut hasher = DefaultHasher::new();
                    hasher.write_u64(*hash);
                    if arr.is_null(i) {
                        0xDEADBEEFu64.hash(&mut hasher);
                    } else {
                        arr.value(i).hash(&mut hasher);
                    }
                    *hash = hasher.finish();
                }
            }
            dt => {
                return Err(BlazeError::not_implemented(format!(
                    "JOIN batch hash for type {:?}. Supported types: Boolean, Int8-64, UInt32-64, Float32-64, Utf8, Date32, Date64, Timestamp, Decimal128.",
                    dt
                )));
            }
        }

        Ok(())
    }

    /// Compare two rows for equality across join keys.
    pub fn keys_equal(
        left_arrays: &[ArrayRef],
        left_row: usize,
        right_arrays: &[ArrayRef],
        right_row: usize,
    ) -> Result<bool> {
        if left_arrays.len() != right_arrays.len() {
            return Ok(false);
        }

        for (left_arr, right_arr) in left_arrays.iter().zip(right_arrays.iter()) {
            if !Self::values_equal(left_arr, left_row, right_arr, right_row)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn values_equal(
        left: &ArrayRef,
        left_row: usize,
        right: &ArrayRef,
        right_row: usize,
    ) -> Result<bool> {
        // Handle NULLs - NULL != NULL in join context
        if left.is_null(left_row) || right.is_null(right_row) {
            return Ok(false);
        }

        match (left.data_type(), right.data_type()) {
            (DataType::Boolean, DataType::Boolean) => {
                let l = try_downcast::<BooleanArray>(left, "BooleanArray")?;
                let r = try_downcast::<BooleanArray>(right, "BooleanArray")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::Int8, DataType::Int8) => {
                let l = try_downcast::<Int8Array>(left, "Int8Array")?;
                let r = try_downcast::<Int8Array>(right, "Int8Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::Int16, DataType::Int16) => {
                let l = try_downcast::<Int16Array>(left, "Int16Array")?;
                let r = try_downcast::<Int16Array>(right, "Int16Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::Int32, DataType::Int32) => {
                let l = try_downcast::<Int32Array>(left, "Int32Array")?;
                let r = try_downcast::<Int32Array>(right, "Int32Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::Int64, DataType::Int64) => {
                let l = try_downcast::<Int64Array>(left, "Int64Array")?;
                let r = try_downcast::<Int64Array>(right, "Int64Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::UInt32, DataType::UInt32) => {
                let l = try_downcast::<UInt32Array>(left, "UInt32Array")?;
                let r = try_downcast::<UInt32Array>(right, "UInt32Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::UInt64, DataType::UInt64) => {
                let l = try_downcast::<UInt64Array>(left, "UInt64Array")?;
                let r = try_downcast::<UInt64Array>(right, "UInt64Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::Float32, DataType::Float32) => {
                let l = try_downcast::<Float32Array>(left, "Float32Array")?;
                let r = try_downcast::<Float32Array>(right, "Float32Array")?;
                // Use to_bits for consistent float comparison
                Ok(l.value(left_row).to_bits() == r.value(right_row).to_bits())
            }
            (DataType::Float64, DataType::Float64) => {
                let l = try_downcast::<Float64Array>(left, "Float64Array")?;
                let r = try_downcast::<Float64Array>(right, "Float64Array")?;
                // Use to_bits for consistent float comparison
                Ok(l.value(left_row).to_bits() == r.value(right_row).to_bits())
            }
            (DataType::Utf8, DataType::Utf8) => {
                let l = try_downcast::<StringArray>(left, "StringArray")?;
                let r = try_downcast::<StringArray>(right, "StringArray")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::Date32, DataType::Date32) => {
                let l = try_downcast::<Date32Array>(left, "Date32Array")?;
                let r = try_downcast::<Date32Array>(right, "Date32Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::Date64, DataType::Date64) => {
                let l = try_downcast::<Date64Array>(left, "Date64Array")?;
                let r = try_downcast::<Date64Array>(right, "Date64Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (DataType::Timestamp(l_unit, _), DataType::Timestamp(r_unit, _)) => {
                // For timestamps, compare raw i64 values (must have same time unit)
                if l_unit != r_unit {
                    return Err(BlazeError::type_error(format!(
                        "Cannot compare timestamps with different units: {:?} vs {:?}",
                        l_unit, r_unit
                    )));
                }
                // All timestamp arrays store i64 internally
                use arrow::datatypes::TimeUnit;
                match l_unit {
                    TimeUnit::Second => {
                        let l = try_downcast::<TimestampSecondArray>(left, "TimestampSecondArray")?;
                        let r = try_downcast::<TimestampSecondArray>(right, "TimestampSecondArray")?;
                        Ok(l.value(left_row) == r.value(right_row))
                    }
                    TimeUnit::Millisecond => {
                        let l = try_downcast::<TimestampMillisecondArray>(left, "TimestampMillisecondArray")?;
                        let r = try_downcast::<TimestampMillisecondArray>(right, "TimestampMillisecondArray")?;
                        Ok(l.value(left_row) == r.value(right_row))
                    }
                    TimeUnit::Microsecond => {
                        let l = try_downcast::<TimestampMicrosecondArray>(left, "TimestampMicrosecondArray")?;
                        let r = try_downcast::<TimestampMicrosecondArray>(right, "TimestampMicrosecondArray")?;
                        Ok(l.value(left_row) == r.value(right_row))
                    }
                    TimeUnit::Nanosecond => {
                        let l = try_downcast::<TimestampNanosecondArray>(left, "TimestampNanosecondArray")?;
                        let r = try_downcast::<TimestampNanosecondArray>(right, "TimestampNanosecondArray")?;
                        Ok(l.value(left_row) == r.value(right_row))
                    }
                }
            }
            (DataType::Decimal128(_, _), DataType::Decimal128(_, _)) => {
                let l = try_downcast::<Decimal128Array>(left, "Decimal128Array")?;
                let r = try_downcast::<Decimal128Array>(right, "Decimal128Array")?;
                Ok(l.value(left_row) == r.value(right_row))
            }
            (l_dt, r_dt) => Err(BlazeError::not_implemented(format!(
                "JOIN comparison between {:?} and {:?}. Supported types: Boolean, Int8-64, UInt32-64, Float32-64, Utf8, Date32, Date64, Timestamp, Decimal128.",
                l_dt, r_dt
            ))),
        }
    }
}

/// Hash join operator that joins two tables using a hash table.
pub struct HashJoinOperator;

impl HashJoinOperator {
    /// Execute a hash join between left and right batches.
    pub fn execute(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        left_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        right_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        // Early return for empty inputs
        if left_batches.is_empty() || right_batches.is_empty() {
            return Self::handle_empty_input(left_batches, right_batches, join_type, output_schema);
        }

        // Build phase: create hash table from right (build) side
        let hash_table = JoinHashTable::build(right_batches.clone(), right_keys)?;

        // Probe phase: probe with left side
        let mut result_batches = Vec::new();

        // Track which right rows have been matched (for right/full outer joins)
        let mut right_matched: HashSet<(usize, usize)> = HashSet::new();

        for left_batch in &left_batches {
            // Evaluate left key expressions
            let left_key_arrays: Vec<ArrayRef> = left_keys
                .iter()
                .map(|expr| expr.evaluate(left_batch))
                .collect::<Result<Vec<_>>>()?;

            let batch_result = Self::probe_and_join(
                left_batch,
                &left_key_arrays,
                &hash_table,
                right_keys,
                join_type,
                output_schema,
                &mut right_matched,
            )?;

            if let Some(batch) = batch_result {
                if batch.num_rows() > 0 {
                    result_batches.push(batch);
                }
            }
        }

        // For right outer and full outer joins, add unmatched right rows
        if matches!(join_type, JoinType::Right | JoinType::Full) {
            let unmatched_right = Self::collect_unmatched_right(
                &hash_table,
                &right_matched,
                &left_batches,
                output_schema,
            )?;
            if let Some(batch) = unmatched_right {
                if batch.num_rows() > 0 {
                    result_batches.push(batch);
                }
            }
        }

        Ok(result_batches)
    }

    /// Execute a hash join with spill-to-disk support.
    ///
    /// When spill is enabled and the build side exceeds the memory threshold,
    /// both sides are hash-partitioned into `num_partitions` buckets and each
    /// partition is joined independently, keeping peak memory bounded.
    pub fn execute_with_spill(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        left_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        right_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        output_schema: &Arc<ArrowSchema>,
        config: &HashJoinSpillConfig,
    ) -> Result<Vec<RecordBatch>> {
        if !config.enabled {
            return Self::execute(
                left_batches,
                right_batches,
                join_type,
                left_keys,
                right_keys,
                output_schema,
            );
        }

        let build_bytes: usize = right_batches.iter().map(estimate_batch_bytes).sum();

        if build_bytes <= config.memory_threshold {
            return Self::execute(
                left_batches,
                right_batches,
                join_type,
                left_keys,
                right_keys,
                output_schema,
            );
        }

        tracing::warn!(
            build_bytes,
            threshold = config.memory_threshold,
            num_partitions = config.num_partitions,
            "Hash join build side ({} bytes) exceeds threshold ({} bytes), \
             spilling to disk with {} partitions",
            build_bytes,
            config.memory_threshold,
            config.num_partitions,
        );

        let spill_dir = config
            .spill_dir
            .clone()
            .unwrap_or_else(|| std::env::temp_dir().join("blaze_hashjoin_spill"));
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            BlazeError::execution(format!(
                "Failed to create spill directory {}: {e}",
                spill_dir.display()
            ))
        })?;

        let num_parts = config.num_partitions;

        // Partition the build (right) side by hash of join keys
        let mut build_partitions: Vec<Vec<RecordBatch>> =
            (0..num_parts).map(|_| Vec::new()).collect();
        for batch in &right_batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let key_arrays: Vec<ArrayRef> = right_keys
                .iter()
                .map(|k| k.evaluate(batch))
                .collect::<Result<Vec<_>>>()?;
            let hashes = JoinHashTable::compute_hashes_batch(&key_arrays, batch.num_rows())?;

            let mut partition_indices: Vec<Vec<u32>> = (0..num_parts).map(|_| Vec::new()).collect();
            for (row, hash) in hashes.iter().enumerate() {
                let part = (*hash as usize) % num_parts;
                partition_indices[part].push(row as u32);
            }

            for (part_idx, indices) in partition_indices.iter().enumerate() {
                if !indices.is_empty() {
                    let partitioned = Self::take_batch(batch, indices)?;
                    build_partitions[part_idx].push(partitioned);
                }
            }
        }

        // Partition the probe (left) side by hash of join keys
        let mut probe_partitions: Vec<Vec<RecordBatch>> =
            (0..num_parts).map(|_| Vec::new()).collect();
        for batch in &left_batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let key_arrays: Vec<ArrayRef> = left_keys
                .iter()
                .map(|k| k.evaluate(batch))
                .collect::<Result<Vec<_>>>()?;
            let hashes = JoinHashTable::compute_hashes_batch(&key_arrays, batch.num_rows())?;

            let mut partition_indices: Vec<Vec<u32>> = (0..num_parts).map(|_| Vec::new()).collect();
            for (row, hash) in hashes.iter().enumerate() {
                let part = (*hash as usize) % num_parts;
                partition_indices[part].push(row as u32);
            }

            for (part_idx, indices) in partition_indices.iter().enumerate() {
                if !indices.is_empty() {
                    let partitioned = Self::take_batch(batch, indices)?;
                    probe_partitions[part_idx].push(partitioned);
                }
            }
        }

        // Spill partitions to disk and process each independently
        let mut results = Vec::new();

        for part_idx in 0..num_parts {
            let build_part = std::mem::take(&mut build_partitions[part_idx]);
            let probe_part = std::mem::take(&mut probe_partitions[part_idx]);

            if build_part.is_empty() && probe_part.is_empty() {
                continue;
            }

            // Spill build partition to IPC file to free memory
            let build_part = if !build_part.is_empty() {
                let path = Self::write_spill_file(&spill_dir, part_idx, "build", &build_part)?;
                let reloaded = Self::read_spill_file(&path)?;
                let _ = std::fs::remove_file(&path);
                reloaded
            } else {
                build_part
            };

            let mut part_results = Self::execute(
                probe_part,
                build_part,
                join_type,
                left_keys,
                right_keys,
                output_schema,
            )?;
            results.append(&mut part_results);
        }

        // Clean up spill directory (best-effort)
        let _ = std::fs::remove_dir(&spill_dir);

        Ok(results)
    }

    /// Take rows from a `RecordBatch` by index.
    fn take_batch(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch> {
        let idx_array = UInt32Array::from(indices.to_vec());
        let columns: Result<Vec<ArrayRef>> = batch
            .columns()
            .iter()
            .map(|col| Ok(compute::take(col.as_ref(), &idx_array, None)?))
            .collect();
        Ok(RecordBatch::try_new(batch.schema(), columns?)?)
    }

    /// Write batches to an IPC spill file.
    fn write_spill_file(
        spill_dir: &std::path::Path,
        partition: usize,
        side: &str,
        batches: &[RecordBatch],
    ) -> Result<PathBuf> {
        use arrow::ipc::writer::FileWriter;

        let path = spill_dir.join(format!(
            "blaze_hashjoin_{side}_{partition}_{}.ipc",
            std::process::id()
        ));
        let file = std::fs::File::create(&path).map_err(|e| {
            BlazeError::execution(format!(
                "Failed to create spill file {}: {e}",
                path.display()
            ))
        })?;
        let mut writer = FileWriter::try_new(file, &batches[0].schema())?;
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        Ok(path)
    }

    /// Read batches back from an IPC spill file.
    fn read_spill_file(path: &std::path::Path) -> Result<Vec<RecordBatch>> {
        use arrow::ipc::reader::FileReader;
        use std::io::BufReader;

        let file = std::fs::File::open(path).map_err(|e| {
            BlazeError::execution(format!("Failed to open spill file {}: {e}", path.display()))
        })?;
        let reader = FileReader::try_new(BufReader::new(file), None)?;
        let mut batches = Vec::new();
        for batch_result in reader {
            batches.push(batch_result?);
        }
        Ok(batches)
    }

    fn handle_empty_input(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        match join_type {
            JoinType::Inner | JoinType::LeftSemi | JoinType::RightSemi => {
                // No matches possible
                Ok(vec![])
            }
            JoinType::Left | JoinType::LeftAnti => {
                if left_batches.is_empty() {
                    Ok(vec![])
                } else {
                    // Return left with NULLs for right columns
                    Self::pad_left_with_nulls(left_batches, output_schema)
                }
            }
            JoinType::Right | JoinType::RightAnti => {
                if right_batches.is_empty() {
                    Ok(vec![])
                } else {
                    // Return right with NULLs for left columns
                    Self::pad_right_with_nulls(right_batches, output_schema)
                }
            }
            JoinType::Full => {
                let mut result = Vec::new();
                if !left_batches.is_empty() {
                    result.extend(Self::pad_left_with_nulls(left_batches, output_schema)?);
                }
                if !right_batches.is_empty() {
                    result.extend(Self::pad_right_with_nulls(right_batches, output_schema)?);
                }
                Ok(result)
            }
            JoinType::Cross => {
                // Cross join with empty = empty
                Ok(vec![])
            }
        }
    }

    fn probe_and_join(
        left_batch: &RecordBatch,
        left_key_arrays: &[ArrayRef],
        hash_table: &JoinHashTable,
        right_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        join_type: JoinType,
        output_schema: &Arc<ArrowSchema>,
        right_matched: &mut HashSet<(usize, usize)>,
    ) -> Result<Option<RecordBatch>> {
        let left_num_cols = left_batch.num_columns();
        let right_num_cols = output_schema.fields().len() - left_num_cols;

        // Collect indices for building result
        let mut left_indices: Vec<usize> = Vec::new();
        let mut right_indices: Vec<Option<(usize, usize)>> = Vec::new();

        // Batch-hash all probe-side keys at once
        let probe_hashes =
            JoinHashTable::compute_hashes_batch(left_key_arrays, left_batch.num_rows())?;

        for (left_row, hash) in probe_hashes.into_iter().enumerate() {
            let matches = hash_table.probe(hash);
            let mut found_match = false;

            if let Some(candidates) = matches {
                for &(batch_idx, row_idx) in candidates {
                    // Get right key values and verify equality (handle hash collisions)
                    let right_batch = hash_table.batch(batch_idx);
                    let right_key_arrays: Vec<ArrayRef> = right_keys
                        .iter()
                        .map(|expr| expr.evaluate(right_batch))
                        .collect::<Result<Vec<_>>>()?;

                    if JoinHashTable::keys_equal(
                        left_key_arrays,
                        left_row,
                        &right_key_arrays,
                        row_idx,
                    )? {
                        found_match = true;
                        right_matched.insert((batch_idx, row_idx));

                        match join_type {
                            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                                left_indices.push(left_row);
                                right_indices.push(Some((batch_idx, row_idx)));
                            }
                            JoinType::LeftSemi => {
                                // Only include left row once
                                left_indices.push(left_row);
                                break;
                            }
                            JoinType::RightSemi => {
                                // Include right row (handled separately)
                            }
                            JoinType::LeftAnti => {
                                // Don't include if match found
                            }
                            JoinType::RightAnti => {
                                // Handled separately
                            }
                            JoinType::Cross => {
                                // Cross join doesn't use hash join
                            }
                        }
                    }
                }
            }

            // Handle unmatched left rows for left/full outer and anti joins
            if !found_match {
                match join_type {
                    JoinType::Left | JoinType::Full => {
                        left_indices.push(left_row);
                        right_indices.push(None);
                    }
                    JoinType::LeftAnti => {
                        left_indices.push(left_row);
                    }
                    _ => {}
                }
            }
        }

        // Build result batch
        Self::build_result_batch(
            left_batch,
            hash_table,
            &left_indices,
            &right_indices,
            join_type,
            output_schema,
            left_num_cols,
            right_num_cols,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build_result_batch(
        left_batch: &RecordBatch,
        hash_table: &JoinHashTable,
        left_indices: &[usize],
        right_indices: &[Option<(usize, usize)>],
        join_type: JoinType,
        output_schema: &Arc<ArrowSchema>,
        left_num_cols: usize,
        right_num_cols: usize,
    ) -> Result<Option<RecordBatch>> {
        if left_indices.is_empty() {
            return Ok(None);
        }

        let _num_rows = left_indices.len();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(output_schema.fields().len());

        // For semi/anti joins, only include left columns
        let include_right = !matches!(join_type, JoinType::LeftSemi | JoinType::LeftAnti);

        // Take left columns
        for col_idx in 0..left_num_cols {
            let col = left_batch.column(col_idx);
            let taken = Self::take_array(col, left_indices)?;
            columns.push(taken);
        }

        // Take right columns (if applicable)
        if include_right {
            for col_idx in 0..right_num_cols {
                let taken = Self::take_right_column(
                    hash_table,
                    col_idx,
                    right_indices,
                    output_schema.field(left_num_cols + col_idx).data_type(),
                )?;
                columns.push(taken);
            }
        }

        let batch = RecordBatch::try_new(output_schema.clone(), columns)?;
        Ok(Some(batch))
    }

    fn take_array(array: &ArrayRef, indices: &[usize]) -> Result<ArrayRef> {
        use arrow::array::UInt32Array;
        use arrow::compute::take;

        let indices_array =
            UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

        Ok(take(array.as_ref(), &indices_array, None)?)
    }

    fn take_right_column(
        hash_table: &JoinHashTable,
        col_idx: usize,
        indices: &[Option<(usize, usize)>],
        data_type: &DataType,
    ) -> Result<ArrayRef> {
        use arrow::array::*;

        // Build the column by taking values from hash table batches
        let num_rows = indices.len();

        match data_type {
            DataType::Int32 => {
                let mut builder = Int32Builder::with_capacity(num_rows);
                for idx in indices {
                    match idx {
                        Some((batch_idx, row_idx)) => {
                            let batch = hash_table.batch(*batch_idx);
                            let col = batch.column(col_idx);
                            let arr = try_downcast::<Int32Array>(col, "Int32Array")?;
                            if arr.is_null(*row_idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(arr.value(*row_idx));
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for idx in indices {
                    match idx {
                        Some((batch_idx, row_idx)) => {
                            let batch = hash_table.batch(*batch_idx);
                            let col = batch.column(col_idx);
                            let arr = try_downcast::<Int64Array>(col, "Int64Array")?;
                            if arr.is_null(*row_idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(arr.value(*row_idx));
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for idx in indices {
                    match idx {
                        Some((batch_idx, row_idx)) => {
                            let batch = hash_table.batch(*batch_idx);
                            let col = batch.column(col_idx);
                            let arr = try_downcast::<Float64Array>(col, "Float64Array")?;
                            if arr.is_null(*row_idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(arr.value(*row_idx));
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for idx in indices {
                    match idx {
                        Some((batch_idx, row_idx)) => {
                            let batch = hash_table.batch(*batch_idx);
                            let col = batch.column(col_idx);
                            let arr = try_downcast::<StringArray>(col, "StringArray")?;
                            if arr.is_null(*row_idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(arr.value(*row_idx));
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for idx in indices {
                    match idx {
                        Some((batch_idx, row_idx)) => {
                            let batch = hash_table.batch(*batch_idx);
                            let col = batch.column(col_idx);
                            let arr = try_downcast::<BooleanArray>(col, "BooleanArray")?;
                            if arr.is_null(*row_idx) {
                                builder.append_null();
                            } else {
                                builder.append_value(arr.value(*row_idx));
                            }
                        }
                        None => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => {
                // For unsupported types, create null array
                Ok(new_null_array(data_type, num_rows))
            }
        }
    }

    fn collect_unmatched_right(
        hash_table: &JoinHashTable,
        right_matched: &HashSet<(usize, usize)>,
        left_batches: &[RecordBatch],
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Option<RecordBatch>> {
        if hash_table.num_rows() == 0 {
            return Ok(None);
        }

        let left_num_cols = if left_batches.is_empty() {
            0
        } else {
            left_batches[0].num_columns()
        };

        let mut left_null_cols: Vec<ArrayRef> = Vec::new();
        let mut right_cols: Vec<Vec<ArrayRef>> = Vec::new();

        // Collect unmatched right rows
        for (batch_idx, batch) in hash_table.batches().iter().enumerate() {
            for row_idx in 0..batch.num_rows() {
                if !right_matched.contains(&(batch_idx, row_idx)) {
                    // This right row was not matched
                    if right_cols.is_empty() {
                        right_cols = vec![Vec::new(); batch.num_columns()];
                    }
                    for (col_idx, col) in batch.columns().iter().enumerate() {
                        right_cols[col_idx].push(col.slice(row_idx, 1));
                    }
                }
            }
        }

        if right_cols.is_empty() || right_cols[0].is_empty() {
            return Ok(None);
        }

        let num_unmatched = right_cols[0].len();

        // Build null arrays for left side
        for i in 0..left_num_cols {
            let dt = output_schema.field(i).data_type();
            left_null_cols.push(new_null_array(dt, num_unmatched));
        }

        // Concatenate right columns
        let mut columns: Vec<ArrayRef> = left_null_cols;
        for col_arrays in right_cols {
            let refs: Vec<&dyn Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            let concatenated = compute::concat(&refs)?;
            columns.push(concatenated);
        }

        let batch = RecordBatch::try_new(output_schema.clone(), columns)?;
        Ok(Some(batch))
    }

    fn pad_left_with_nulls(
        left_batches: Vec<RecordBatch>,
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let left_num_cols = if left_batches.is_empty() {
            0
        } else {
            left_batches[0].num_columns()
        };
        let right_num_cols = output_schema.fields().len() - left_num_cols;

        for batch in left_batches {
            let num_rows = batch.num_rows();
            let mut columns: Vec<ArrayRef> = batch.columns().to_vec();

            // Add null columns for right side
            for i in 0..right_num_cols {
                let dt = output_schema.field(left_num_cols + i).data_type();
                columns.push(new_null_array(dt, num_rows));
            }

            result.push(RecordBatch::try_new(output_schema.clone(), columns)?);
        }

        Ok(result)
    }

    fn pad_right_with_nulls(
        right_batches: Vec<RecordBatch>,
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        let mut result = Vec::new();
        let total_cols = output_schema.fields().len();
        let right_num_cols = if right_batches.is_empty() {
            0
        } else {
            right_batches[0].num_columns()
        };
        let left_num_cols = total_cols - right_num_cols;

        for batch in right_batches {
            let num_rows = batch.num_rows();
            let mut columns: Vec<ArrayRef> = Vec::new();

            // Add null columns for left side
            for i in 0..left_num_cols {
                let dt = output_schema.field(i).data_type();
                columns.push(new_null_array(dt, num_rows));
            }

            // Add right columns
            columns.extend(batch.columns().iter().cloned());

            result.push(RecordBatch::try_new(output_schema.clone(), columns)?);
        }

        Ok(result)
    }
}

/// Sort-merge join operator.
/// Assumes inputs are already sorted by join keys.
pub struct SortMergeJoinOperator;

impl SortMergeJoinOperator {
    /// Execute a sort-merge join between left and right batches.
    /// Both inputs should already be sorted by their respective join keys.
    pub fn execute(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        left_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        right_keys: &[Arc<dyn crate::planner::PhysicalExpr>],
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        // Handle empty inputs
        if left_batches.is_empty() || right_batches.is_empty() {
            return HashJoinOperator::execute(
                left_batches,
                right_batches,
                join_type,
                left_keys,
                right_keys,
                output_schema,
            );
        }

        // Concatenate all batches for simpler processing
        let left_combined = Self::concat_batches(&left_batches)?;
        let right_combined = Self::concat_batches(&right_batches)?;

        // Evaluate key expressions
        let left_key_arrays: Vec<ArrayRef> = left_keys
            .iter()
            .map(|expr| expr.evaluate(&left_combined))
            .collect::<Result<Vec<_>>>()?;

        let right_key_arrays: Vec<ArrayRef> = right_keys
            .iter()
            .map(|expr| expr.evaluate(&right_combined))
            .collect::<Result<Vec<_>>>()?;

        // Sort indices for both sides by their keys
        let left_indices = Self::sort_indices(&left_key_arrays)?;
        let right_indices = Self::sort_indices(&right_key_arrays)?;

        // Perform merge join
        let (output_left_indices, output_right_indices) = Self::merge_join(
            &left_key_arrays,
            &right_key_arrays,
            &left_indices,
            &right_indices,
            join_type,
        )?;

        // Build output batch
        Self::build_output(
            &left_combined,
            &right_combined,
            &output_left_indices,
            &output_right_indices,
            output_schema,
        )
    }

    fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Err(crate::error::BlazeError::execution("Empty batches"));
        }
        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }
        let schema = batches[0].schema();
        Ok(arrow::compute::concat_batches(&schema, batches)?)
    }

    fn sort_indices(key_arrays: &[ArrayRef]) -> Result<Vec<usize>> {
        use arrow::compute::SortColumn;

        let num_rows = key_arrays.first().map(|a| a.len()).unwrap_or(0);

        if key_arrays.is_empty() || num_rows == 0 {
            return Ok(Vec::new());
        }

        // Build sort columns
        let sort_columns: Vec<SortColumn> = key_arrays
            .iter()
            .map(|arr| SortColumn {
                values: arr.clone(),
                options: Some(arrow::compute::SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            })
            .collect();

        // Use lexicographic sort
        let indices = arrow::compute::lexsort_to_indices(&sort_columns, None)?;

        Ok(indices.values().iter().map(|&i| i as usize).collect())
    }

    fn compare_keys(
        left_keys: &[ArrayRef],
        right_keys: &[ArrayRef],
        left_row: usize,
        right_row: usize,
    ) -> Result<std::cmp::Ordering> {
        use std::cmp::Ordering;

        for (left_arr, right_arr) in left_keys.iter().zip(right_keys.iter()) {
            let cmp = Self::compare_values(left_arr, right_arr, left_row, right_row)?;
            if cmp != Ordering::Equal {
                return Ok(cmp);
            }
        }
        Ok(Ordering::Equal)
    }

    fn compare_values(
        left_arr: &ArrayRef,
        right_arr: &ArrayRef,
        left_row: usize,
        right_row: usize,
    ) -> Result<std::cmp::Ordering> {
        use arrow::array::*;
        use std::cmp::Ordering;

        // Handle nulls
        let left_null = left_arr.is_null(left_row);
        let right_null = right_arr.is_null(right_row);
        match (left_null, right_null) {
            (true, true) => return Ok(Ordering::Equal),
            (true, false) => return Ok(Ordering::Less),
            (false, true) => return Ok(Ordering::Greater),
            (false, false) => {}
        }

        match left_arr.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let left = try_downcast::<Int64Array>(left_arr, "Int64Array")?.value(left_row);
                let right = try_downcast::<Int64Array>(right_arr, "Int64Array")?.value(right_row);
                Ok(left.cmp(&right))
            }
            arrow::datatypes::DataType::Int32 => {
                let left = try_downcast::<Int32Array>(left_arr, "Int32Array")?.value(left_row);
                let right = try_downcast::<Int32Array>(right_arr, "Int32Array")?.value(right_row);
                Ok(left.cmp(&right))
            }
            arrow::datatypes::DataType::Float64 => {
                let left = try_downcast::<Float64Array>(left_arr, "Float64Array")?.value(left_row);
                let right = try_downcast::<Float64Array>(right_arr, "Float64Array")?.value(right_row);
                Ok(left.partial_cmp(&right).unwrap_or(Ordering::Equal))
            }
            arrow::datatypes::DataType::Utf8 => {
                let left = try_downcast::<StringArray>(left_arr, "StringArray")?.value(left_row);
                let right = try_downcast::<StringArray>(right_arr, "StringArray")?.value(right_row);
                Ok(left.cmp(right))
            }
            arrow::datatypes::DataType::Boolean => {
                let left = try_downcast::<BooleanArray>(left_arr, "BooleanArray")?.value(left_row);
                let right = try_downcast::<BooleanArray>(right_arr, "BooleanArray")?.value(right_row);
                Ok(left.cmp(&right))
            }
            arrow::datatypes::DataType::Date32 => {
                let left = try_downcast::<Date32Array>(left_arr, "Date32Array")?.value(left_row);
                let right = try_downcast::<Date32Array>(right_arr, "Date32Array")?.value(right_row);
                Ok(left.cmp(&right))
            }
            arrow::datatypes::DataType::Date64 => {
                let left = try_downcast::<Date64Array>(left_arr, "Date64Array")?.value(left_row);
                let right = try_downcast::<Date64Array>(right_arr, "Date64Array")?.value(right_row);
                Ok(left.cmp(&right))
            }
            arrow::datatypes::DataType::Timestamp(unit, _) => {
                use arrow::datatypes::TimeUnit;
                let (left_val, right_val): (i64, i64) = match unit {
                    TimeUnit::Second => {
                        let left = try_downcast::<TimestampSecondArray>(left_arr, "TimestampSecondArray")?.value(left_row);
                        let right = try_downcast::<TimestampSecondArray>(right_arr, "TimestampSecondArray")?.value(right_row);
                        (left, right)
                    }
                    TimeUnit::Millisecond => {
                        let left = try_downcast::<TimestampMillisecondArray>(left_arr, "TimestampMillisecondArray")?.value(left_row);
                        let right = try_downcast::<TimestampMillisecondArray>(right_arr, "TimestampMillisecondArray")?.value(right_row);
                        (left, right)
                    }
                    TimeUnit::Microsecond => {
                        let left = try_downcast::<TimestampMicrosecondArray>(left_arr, "TimestampMicrosecondArray")?.value(left_row);
                        let right = try_downcast::<TimestampMicrosecondArray>(right_arr, "TimestampMicrosecondArray")?.value(right_row);
                        (left, right)
                    }
                    TimeUnit::Nanosecond => {
                        let left = try_downcast::<TimestampNanosecondArray>(left_arr, "TimestampNanosecondArray")?.value(left_row);
                        let right = try_downcast::<TimestampNanosecondArray>(right_arr, "TimestampNanosecondArray")?.value(right_row);
                        (left, right)
                    }
                };
                Ok(left_val.cmp(&right_val))
            }
            _ => Err(crate::error::BlazeError::type_error(format!(
                "Sort-merge join does not support key type {:?}. Supported: Int32, Int64, Float64, Utf8, Boolean, Date32, Date64, Timestamp",
                left_arr.data_type()
            ))),
        }
    }

    fn merge_join(
        left_keys: &[ArrayRef],
        right_keys: &[ArrayRef],
        left_sorted: &[usize],
        right_sorted: &[usize],
        join_type: JoinType,
    ) -> Result<(Vec<Option<usize>>, Vec<Option<usize>>)> {
        use std::cmp::Ordering;
        use std::collections::HashSet;

        let mut output_left: Vec<Option<usize>> = Vec::new();
        let mut output_right: Vec<Option<usize>> = Vec::new();

        let mut left_matched: HashSet<usize> = HashSet::new();
        let mut right_matched: HashSet<usize> = HashSet::new();

        let mut i = 0;
        let mut j = 0;

        while i < left_sorted.len() && j < right_sorted.len() {
            let left_row = left_sorted[i];
            let right_row = right_sorted[j];

            let cmp = Self::compare_keys(left_keys, right_keys, left_row, right_row)?;

            match cmp {
                Ordering::Less => {
                    // Left is smaller, no match for this left row (yet)
                    i += 1;
                }
                Ordering::Greater => {
                    // Right is smaller, no match for this right row
                    j += 1;
                }
                Ordering::Equal => {
                    // Found matching keys - need to handle all matches
                    // Find the range of equal keys on the left
                    let mut left_end = i + 1;
                    while left_end < left_sorted.len() {
                        let cmp = Self::compare_keys(
                            left_keys,
                            left_keys,
                            left_sorted[left_end],
                            left_row,
                        )?;
                        if cmp != Ordering::Equal {
                            break;
                        }
                        left_end += 1;
                    }

                    // Find the range of equal keys on the right
                    let mut right_end = j + 1;
                    while right_end < right_sorted.len() {
                        let cmp = Self::compare_keys(
                            right_keys,
                            right_keys,
                            right_sorted[right_end],
                            right_row,
                        )?;
                        if cmp != Ordering::Equal {
                            break;
                        }
                        right_end += 1;
                    }

                    // Output cartesian product of matching ranges
                    for left_sorted_item in left_sorted.iter().take(left_end).skip(i) {
                        let l_row = *left_sorted_item;
                        for right_sorted_item in right_sorted.iter().take(right_end).skip(j) {
                            let r_row = *right_sorted_item;
                            match join_type {
                                JoinType::Inner
                                | JoinType::Left
                                | JoinType::Right
                                | JoinType::Full => {
                                    output_left.push(Some(l_row));
                                    output_right.push(Some(r_row));
                                }
                                JoinType::LeftSemi => {
                                    if !left_matched.contains(&l_row) {
                                        output_left.push(Some(l_row));
                                        output_right.push(None);
                                    }
                                }
                                JoinType::RightSemi => {
                                    if !right_matched.contains(&r_row) {
                                        output_left.push(None);
                                        output_right.push(Some(r_row));
                                    }
                                }
                                JoinType::LeftAnti | JoinType::RightAnti => {
                                    // Don't output matches for anti joins
                                }
                                JoinType::Cross => {}
                            }
                            left_matched.insert(l_row);
                            right_matched.insert(r_row);
                        }
                    }

                    i = left_end;
                    j = right_end;
                }
            }
        }

        // Handle unmatched rows for outer/anti joins
        match join_type {
            JoinType::Left | JoinType::Full => {
                for &l_row in left_sorted {
                    if !left_matched.contains(&l_row) {
                        output_left.push(Some(l_row));
                        output_right.push(None);
                    }
                }
            }
            JoinType::LeftAnti => {
                for &l_row in left_sorted {
                    if !left_matched.contains(&l_row) {
                        output_left.push(Some(l_row));
                        output_right.push(None);
                    }
                }
            }
            _ => {}
        }

        match join_type {
            JoinType::Right | JoinType::Full => {
                for &r_row in right_sorted {
                    if !right_matched.contains(&r_row) {
                        output_left.push(None);
                        output_right.push(Some(r_row));
                    }
                }
            }
            JoinType::RightAnti => {
                for &r_row in right_sorted {
                    if !right_matched.contains(&r_row) {
                        output_left.push(None);
                        output_right.push(Some(r_row));
                    }
                }
            }
            _ => {}
        }

        Ok((output_left, output_right))
    }

    fn build_output(
        left_batch: &RecordBatch,
        right_batch: &RecordBatch,
        left_indices: &[Option<usize>],
        right_indices: &[Option<usize>],
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::*;

        if left_indices.is_empty() {
            return Ok(vec![]);
        }

        let left_num_cols = left_batch.num_columns();
        let mut columns = Vec::with_capacity(output_schema.fields().len());

        // Build left columns
        for col_idx in 0..left_num_cols {
            let col = left_batch.column(col_idx);
            let output_col = Self::take_with_nulls(col, left_indices)?;
            columns.push(output_col);
        }

        // Build right columns
        for col_idx in 0..right_batch.num_columns() {
            let col = right_batch.column(col_idx);
            let output_col = Self::take_with_nulls(col, right_indices)?;
            columns.push(output_col);
        }

        let batch = RecordBatch::try_new(output_schema.clone(), columns)?;
        Ok(vec![batch])
    }

    fn take_with_nulls(array: &ArrayRef, indices: &[Option<usize>]) -> Result<ArrayRef> {
        use arrow::array::*;

        match array.data_type() {
            arrow::datatypes::DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(array, "Int64Array")?;
                let result: Int64Array = indices.iter()
                    .map(|opt| opt.map(|i| arr.value(i)))
                    .collect();
                Ok(Arc::new(result))
            }
            arrow::datatypes::DataType::Int32 => {
                let arr = try_downcast::<Int32Array>(array, "Int32Array")?;
                let result: Int32Array = indices.iter()
                    .map(|opt| opt.map(|i| arr.value(i)))
                    .collect();
                Ok(Arc::new(result))
            }
            arrow::datatypes::DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(array, "Float64Array")?;
                let result: Float64Array = indices.iter()
                    .map(|opt| opt.map(|i| arr.value(i)))
                    .collect();
                Ok(Arc::new(result))
            }
            arrow::datatypes::DataType::Utf8 => {
                let arr = try_downcast::<StringArray>(array, "StringArray")?;
                let mut builder = StringBuilder::new();
                for opt in indices {
                    match opt {
                        Some(i) if !arr.is_null(*i) => builder.append_value(arr.value(*i)),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            arrow::datatypes::DataType::Boolean => {
                let arr = try_downcast::<BooleanArray>(array, "BooleanArray")?;
                let result: BooleanArray = indices.iter()
                    .map(|opt| opt.and_then(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }))
                    .collect();
                Ok(Arc::new(result))
            }
            arrow::datatypes::DataType::Date32 => {
                let arr = try_downcast::<Date32Array>(array, "Date32Array")?;
                let result: Date32Array = indices.iter()
                    .map(|opt| opt.and_then(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }))
                    .collect();
                Ok(Arc::new(result))
            }
            arrow::datatypes::DataType::Date64 => {
                let arr = try_downcast::<Date64Array>(array, "Date64Array")?;
                let result: Date64Array = indices.iter()
                    .map(|opt| opt.and_then(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }))
                    .collect();
                Ok(Arc::new(result))
            }
            arrow::datatypes::DataType::Timestamp(unit, tz) => {
                use arrow::datatypes::TimeUnit;
                match unit {
                    TimeUnit::Second => {
                        let arr = try_downcast::<TimestampSecondArray>(array, "TimestampSecondArray")?;
                        let result: TimestampSecondArray = indices.iter()
                            .map(|opt| opt.and_then(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }))
                            .collect();
                        Ok(Arc::new(result.with_timezone_opt(tz.clone())))
                    }
                    TimeUnit::Millisecond => {
                        let arr = try_downcast::<TimestampMillisecondArray>(array, "TimestampMillisecondArray")?;
                        let result: TimestampMillisecondArray = indices.iter()
                            .map(|opt| opt.and_then(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }))
                            .collect();
                        Ok(Arc::new(result.with_timezone_opt(tz.clone())))
                    }
                    TimeUnit::Microsecond => {
                        let arr = try_downcast::<TimestampMicrosecondArray>(array, "TimestampMicrosecondArray")?;
                        let result: TimestampMicrosecondArray = indices.iter()
                            .map(|opt| opt.and_then(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }))
                            .collect();
                        Ok(Arc::new(result.with_timezone_opt(tz.clone())))
                    }
                    TimeUnit::Nanosecond => {
                        let arr = try_downcast::<TimestampNanosecondArray>(array, "TimestampNanosecondArray")?;
                        let result: TimestampNanosecondArray = indices.iter()
                            .map(|opt| opt.and_then(|i| if arr.is_null(i) { None } else { Some(arr.value(i)) }))
                            .collect();
                        Ok(Arc::new(result.with_timezone_opt(tz.clone())))
                    }
                }
            }
            dt => Err(crate::error::BlazeError::type_error(format!(
                "Sort-merge join does not support column type {:?}. Supported: Int32, Int64, Float64, Utf8, Boolean, Date32, Date64, Timestamp",
                dt
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{ColumnExpr, JoinType};
    use arrow::datatypes::{DataType, Field, Schema};

    /// Helper: creates simple left/right tables for join tests.
    ///
    /// Left: (id: [1, 2, 3], val: [10, 20, 30])
    /// Right: (id: [2, 1, 4], val: [200, 100, 400])
    /// Inner join on id should produce 2 rows.
    fn make_simple_join_inputs() -> (
        RecordBatch,
        RecordBatch,
        Arc<ArrowSchema>,
        Arc<dyn crate::planner::PhysicalExpr>,
        Arc<dyn crate::planner::PhysicalExpr>,
    ) {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));
        let right_batch = RecordBatch::try_new(
            right_schema,
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 4])),
                Arc::new(Int64Array::from(vec![200, 100, 400])),
            ],
        )
        .unwrap();

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));

        (left_batch, right_batch, output_schema, left_key, right_key)
    }

    #[test]
    fn test_hash_join_spill_config_default() {
        let config = HashJoinSpillConfig::default();
        assert_eq!(config.memory_threshold, 256 * 1024 * 1024);
        assert!(!config.enabled);
        assert!(config.spill_dir.is_none());
        assert_eq!(config.num_partitions, 16);
    }

    #[test]
    fn test_hash_join_with_spill_disabled() {
        let (left_batch, right_batch, output_schema, left_key, right_key) =
            make_simple_join_inputs();

        let config = HashJoinSpillConfig::default();
        let result = HashJoinOperator::execute_with_spill(
            vec![left_batch],
            vec![right_batch],
            JoinType::Inner,
            &[left_key],
            &[right_key],
            &output_schema,
            &config,
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_hash_join_with_spill() {
        // Use a very small threshold (1 byte) to force the spill path
        let spill_dir = tempfile::tempdir().unwrap();
        let config = HashJoinSpillConfig {
            memory_threshold: 1,
            spill_dir: Some(spill_dir.path().to_path_buf()),
            num_partitions: 4,
            enabled: true,
        };

        let (left_batch, right_batch, output_schema, left_key, right_key) =
            make_simple_join_inputs();

        let result = HashJoinOperator::execute_with_spill(
            vec![left_batch],
            vec![right_batch],
            JoinType::Inner,
            &[left_key],
            &[right_key],
            &output_schema,
            &config,
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Collect result values to verify correctness
        let mut result_pairs: Vec<(i64, i64)> = Vec::new();
        for batch in &result {
            let left_col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let right_col = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                result_pairs.push((left_col.value(i), right_col.value(i)));
            }
        }
        result_pairs.sort();
        assert_eq!(result_pairs, vec![(1, 1), (2, 2)]);

        // Verify spill files were cleaned up
        let remaining: Vec<_> = std::fs::read_dir(spill_dir.path())
            .map(|rd| rd.filter_map(|e| e.ok()).collect())
            .unwrap_or_default();
        assert!(remaining.is_empty(), "Spill files should be cleaned up");
    }

    #[test]
    fn test_hash_join_below_threshold_no_spill() {
        // Threshold larger than data — should use in-memory path
        let config = HashJoinSpillConfig {
            memory_threshold: 1024 * 1024,
            spill_dir: None,
            num_partitions: 4,
            enabled: true,
        };

        let (left_batch, right_batch, output_schema, left_key, right_key) =
            make_simple_join_inputs();

        let result = HashJoinOperator::execute_with_spill(
            vec![left_batch],
            vec![right_batch],
            JoinType::Inner,
            &[left_key],
            &[right_key],
            &output_schema,
            &config,
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }
}
