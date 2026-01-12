//! Physical operators for query execution.

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Float32Array, Int32Array, Int64Array,
    Int16Array, Int8Array, UInt32Array, UInt64Array, Date32Array, Date64Array,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, RecordBatch, StringArray, new_null_array,
};
use arrow::compute;
use arrow::datatypes::{DataType, Schema as ArrowSchema};

use crate::error::{BlazeError, Result};
use crate::planner::JoinType;

/// Helper to downcast an array with a proper error message.
/// Use this instead of `.unwrap()` on downcasts for better error reporting.
fn try_downcast<'a, T: 'static>(array: &'a ArrayRef, expected_type: &str) -> Result<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| BlazeError::type_error(format!(
            "Failed to downcast array to {}. Actual type: {:?}",
            expected_type,
            array.data_type()
        )))
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

            for row in 0..batch.num_rows() {
                let hash = Self::compute_hash_from_arrays(&key_columns, row)?;
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
                        let arr = try_downcast::<TimestampSecondArray>(array, "TimestampSecondArray")?;
                        arr.value(row)
                    }
                    TimeUnit::Millisecond => {
                        let arr = try_downcast::<TimestampMillisecondArray>(array, "TimestampMillisecondArray")?;
                        arr.value(row)
                    }
                    TimeUnit::Microsecond => {
                        let arr = try_downcast::<TimestampMicrosecondArray>(array, "TimestampMicrosecondArray")?;
                        arr.value(row)
                    }
                    TimeUnit::Nanosecond => {
                        let arr = try_downcast::<TimestampNanosecondArray>(array, "TimestampNanosecondArray")?;
                        arr.value(row)
                    }
                };
                value.hash(hasher);
            }
            dt => {
                return Err(BlazeError::not_implemented(format!(
                    "Hash for type {:?}. Supported types: Boolean, Int8-64, UInt32-64, Float32-64, Utf8, Date32, Date64, Timestamp",
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
            (l_dt, r_dt) => Err(BlazeError::not_implemented(format!(
                "Comparison between {:?} and {:?}. Supported: Boolean, Int8-64, UInt32-64, Float32-64, Utf8, Date32, Date64, Timestamp",
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
            return Self::handle_empty_input(
                left_batches,
                right_batches,
                join_type,
                output_schema,
            );
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

        for left_row in 0..left_batch.num_rows() {
            let hash = JoinHashTable::compute_hash_from_arrays(left_key_arrays, left_row)?;

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
                    &output_schema.field(left_num_cols + col_idx).data_type(),
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

        let indices_array = UInt32Array::from(
            indices.iter().map(|&i| i as u32).collect::<Vec<_>>()
        );

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

        let num_rows = key_arrays.first()
            .map(|a| a.len())
            .unwrap_or(0);

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
                            left_keys, left_keys,
                            left_sorted[left_end], left_row
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
                            right_keys, right_keys,
                            right_sorted[right_end], right_row
                        )?;
                        if cmp != Ordering::Equal {
                            break;
                        }
                        right_end += 1;
                    }

                    // Output cartesian product of matching ranges
                    for li in i..left_end {
                        let l_row = left_sorted[li];
                        for rj in j..right_end {
                            let r_row = right_sorted[rj];
                            match join_type {
                                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
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

/// Cross join operator - produces cartesian product.
pub struct CrossJoinOperator;

impl CrossJoinOperator {
    pub fn execute(
        left_batches: Vec<RecordBatch>,
        right_batches: Vec<RecordBatch>,
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        if left_batches.is_empty() || right_batches.is_empty() {
            return Ok(vec![]);
        }

        let mut result = Vec::new();

        for left_batch in &left_batches {
            for right_batch in &right_batches {
                let cross_batch = Self::cross_batch(left_batch, right_batch, output_schema)?;
                if cross_batch.num_rows() > 0 {
                    result.push(cross_batch);
                }
            }
        }

        Ok(result)
    }

    /// Maximum rows allowed in a cross join result to prevent memory exhaustion.
    /// 10 million rows is a reasonable limit for in-memory processing.
    const MAX_CROSS_JOIN_ROWS: usize = 10_000_000;

    fn cross_batch(
        left: &RecordBatch,
        right: &RecordBatch,
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<RecordBatch> {
        let left_rows = left.num_rows();
        let right_rows = right.num_rows();

        // Check for overflow and excessive result size
        let total_rows = left_rows.checked_mul(right_rows).ok_or_else(|| {
            BlazeError::resource_exhausted(format!(
                "Cross join would produce too many rows: {} x {} overflows",
                left_rows, right_rows
            ))
        })?;

        if total_rows > Self::MAX_CROSS_JOIN_ROWS {
            return Err(BlazeError::resource_exhausted(format!(
                "Cross join would produce {} rows, exceeding limit of {}. \
                Consider adding a WHERE clause or using a different join strategy.",
                total_rows, Self::MAX_CROSS_JOIN_ROWS
            )));
        }

        if total_rows == 0 {
            return Ok(RecordBatch::new_empty(output_schema.clone()));
        }

        let mut columns: Vec<ArrayRef> = Vec::new();

        // Repeat each left row `right_rows` times
        for col in left.columns() {
            let repeated = Self::repeat_each(col, right_rows)?;
            columns.push(repeated);
        }

        // Tile right columns `left_rows` times
        for col in right.columns() {
            let tiled = Self::tile(col, left_rows)?;
            columns.push(tiled);
        }

        Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
    }

    fn repeat_each(array: &ArrayRef, times: usize) -> Result<ArrayRef> {
        use arrow::array::*;

        let mut indices = Vec::with_capacity(array.len() * times);
        for i in 0..array.len() {
            for _ in 0..times {
                indices.push(i as u32);
            }
        }

        let indices_array = UInt32Array::from(indices);
        Ok(compute::take(array.as_ref(), &indices_array, None)?)
    }

    fn tile(array: &ArrayRef, times: usize) -> Result<ArrayRef> {
        let refs: Vec<&dyn Array> = (0..times).map(|_| array.as_ref()).collect();
        Ok(compute::concat(&refs)?)
    }
}

/// Legacy HashTable for backward compatibility.
pub struct HashTable {
    /// Map from hash key to row indices
    map: HashMap<u64, Vec<usize>>,
    /// The batches stored in the hash table
    batches: Vec<RecordBatch>,
}

impl HashTable {
    /// Create a new hash table from record batches.
    pub fn build(batches: Vec<RecordBatch>, key_indices: &[usize]) -> Result<Self> {
        let mut map: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut row_offset = 0;

        for batch in &batches {
            for row in 0..batch.num_rows() {
                let hash = Self::compute_hash(batch, row, key_indices)?;
                map.entry(hash).or_default().push(row_offset + row);
            }
            row_offset += batch.num_rows();
        }

        Ok(Self { map, batches })
    }

    /// Probe the hash table with a key.
    pub fn probe(&self, hash: u64) -> Option<&Vec<usize>> {
        self.map.get(&hash)
    }

    /// Get a row from the stored batches.
    pub fn get_row(&self, row_idx: usize) -> Option<Vec<ArrayRef>> {
        let mut offset = 0;
        for batch in &self.batches {
            if row_idx < offset + batch.num_rows() {
                let local_idx = row_idx - offset;
                return Some(
                    batch
                        .columns()
                        .iter()
                        .map(|col| col.slice(local_idx, 1))
                        .collect(),
                );
            }
            offset += batch.num_rows();
        }
        None
    }

    fn compute_hash(batch: &RecordBatch, row: usize, key_indices: &[usize]) -> Result<u64> {
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();

        for &idx in key_indices {
            let col = batch.column(idx);
            Self::hash_value(col, row, &mut hasher)?;
        }

        Ok(hasher.finish())
    }

    fn hash_value<H: Hasher>(array: &ArrayRef, row: usize, hasher: &mut H) -> Result<()> {
        if array.is_null(row) {
            0u8.hash(hasher);
            return Ok(());
        }

        match array.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(array, "Int64Array")?;
                arr.value(row).hash(hasher);
            }
            DataType::Utf8 => {
                let arr = try_downcast::<StringArray>(array, "StringArray")?;
                arr.value(row).hash(hasher);
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(array, "Float64Array")?;
                arr.value(row).to_bits().hash(hasher);
            }
            dt => {
                return Err(BlazeError::not_implemented(format!(
                    "Hash for type {:?}",
                    dt
                )));
            }
        }

        Ok(())
    }
}

/// Aggregate accumulator trait.
pub trait Accumulator: Send + Sync {
    /// Update the accumulator with new values.
    fn update(&mut self, values: &ArrayRef) -> Result<()>;

    /// Merge with another accumulator.
    fn merge(&mut self, other: &dyn Accumulator) -> Result<()>;

    /// Finalize and return the result.
    fn finalize(&self) -> Result<ArrayRef>;

    /// Reset the accumulator.
    fn reset(&mut self);

    /// Return self as Any for downcasting.
    fn as_any(&self) -> &dyn std::any::Any;
}

/// COUNT accumulator.
pub struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Default for CountAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for CountAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        // Count non-null values
        self.count += (values.len() - values.null_count()) as i64;
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<CountAccumulator>() {
            self.count += other.count;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from(vec![self.count])))
    }

    fn reset(&mut self) {
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// SUM accumulator for numeric types.
pub struct SumAccumulator {
    sum: Option<f64>,
}

impl SumAccumulator {
    pub fn new() -> Self {
        Self { sum: None }
    }
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for SumAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let sum = match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                compute::sum(arr).map(|v| v as f64)
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                compute::sum(arr)
            }
            _ => None,
        };

        if let Some(s) = sum {
            self.sum = Some(self.sum.unwrap_or(0.0) + s);
        }

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<SumAccumulator>() {
            if let Some(s) = other.sum {
                self.sum = Some(self.sum.unwrap_or(0.0) + s);
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Float64Array::from(vec![self.sum])))
    }

    fn reset(&mut self) {
        self.sum = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// AVG accumulator.
pub struct AvgAccumulator {
    sum: f64,
    count: u64,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Default for AvgAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for AvgAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let (sum, count) = match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                let sum = compute::sum(arr).unwrap_or(0) as f64;
                let count = (arr.len() - arr.null_count()) as u64;
                (sum, count)
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                let sum = compute::sum(arr).unwrap_or(0.0);
                let count = (arr.len() - arr.null_count()) as u64;
                (sum, count)
            }
            _ => (0.0, 0),
        };

        self.sum += sum;
        self.count += count;

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<AvgAccumulator>() {
            self.sum += other.sum;
            self.count += other.count;
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let avg = if self.count > 0 {
            Some(self.sum / self.count as f64)
        } else {
            None
        };
        Ok(Arc::new(Float64Array::from(vec![avg])))
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MIN accumulator.
pub struct MinAccumulator {
    min: Option<f64>,
}

impl MinAccumulator {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Default for MinAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for MinAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let min = match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                compute::min(arr).map(|v| v as f64)
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                compute::min(arr)
            }
            _ => None,
        };

        if let Some(m) = min {
            self.min = Some(self.min.map(|curr| curr.min(m)).unwrap_or(m));
        }

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<MinAccumulator>() {
            if let Some(m) = other.min {
                self.min = Some(self.min.map(|curr| curr.min(m)).unwrap_or(m));
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Float64Array::from(vec![self.min])))
    }

    fn reset(&mut self) {
        self.min = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// MAX accumulator.
pub struct MaxAccumulator {
    max: Option<f64>,
}

impl MaxAccumulator {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Default for MaxAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for MaxAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        let max = match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                compute::max(arr).map(|v| v as f64)
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                compute::max(arr)
            }
            _ => None,
        };

        if let Some(m) = max {
            self.max = Some(self.max.map(|curr| curr.max(m)).unwrap_or(m));
        }

        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<MaxAccumulator>() {
            if let Some(m) = other.max {
                self.max = Some(self.max.map(|curr| curr.max(m)).unwrap_or(m));
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        Ok(Arc::new(Float64Array::from(vec![self.max])))
    }

    fn reset(&mut self) {
        self.max = None;
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// APPROX_COUNT_DISTINCT accumulator using HyperLogLog.
pub struct ApproxCountDistinctAccumulator {
    hll: crate::approx::HyperLogLog,
}

impl ApproxCountDistinctAccumulator {
    pub fn new() -> Self {
        Self {
            hll: crate::approx::HyperLogLog::default_precision(),
        }
    }
}

impl Default for ApproxCountDistinctAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for ApproxCountDistinctAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_i64(arr.value(i));
                    }
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_f64(arr.value(i));
                    }
                }
            }
            DataType::Utf8 => {
                let arr = try_downcast::<StringArray>(values, "StringArray")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.hll.add_str(arr.value(i));
                    }
                }
            }
            _ => {
                // Hash the array as bytes for other types
                for i in 0..values.len() {
                    if !values.is_null(i) {
                        self.hll.add(&i);
                    }
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<ApproxCountDistinctAccumulator>() {
            self.hll.merge(&other.hll);
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let estimate = self.hll.estimate() as i64;
        Ok(Arc::new(Int64Array::from(vec![estimate])))
    }

    fn reset(&mut self) {
        self.hll = crate::approx::HyperLogLog::default_precision();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// APPROX_PERCENTILE accumulator using T-Digest.
pub struct ApproxPercentileAccumulator {
    tdigest: crate::approx::TDigest,
    percentile: f64,
}

impl ApproxPercentileAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self {
            tdigest: crate::approx::TDigest::default_compression(),
            percentile: percentile.clamp(0.0, 1.0),
        }
    }

    pub fn median() -> Self {
        Self::new(0.5)
    }
}

impl Default for ApproxPercentileAccumulator {
    fn default() -> Self {
        Self::median()
    }
}

impl Accumulator for ApproxPercentileAccumulator {
    fn update(&mut self, values: &ArrayRef) -> Result<()> {
        match values.data_type() {
            DataType::Int64 => {
                let arr = try_downcast::<Int64Array>(values, "Int64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i) as f64);
                    }
                }
            }
            DataType::Float64 => {
                let arr = try_downcast::<Float64Array>(values, "Float64Array")?;
                for i in 0..arr.len() {
                    if !arr.is_null(i) {
                        self.tdigest.add(arr.value(i));
                    }
                }
            }
            _ => {} // Ignore non-numeric types
        }
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(other) = other.as_any().downcast_ref::<ApproxPercentileAccumulator>() {
            self.tdigest.merge(&other.tdigest);
        }
        Ok(())
    }

    fn finalize(&self) -> Result<ArrayRef> {
        let mut td = self.tdigest.clone();
        let result = td.quantile(self.percentile);
        Ok(Arc::new(Float64Array::from(vec![result])))
    }

    fn reset(&mut self) {
        self.tdigest = crate::approx::TDigest::default_compression();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Hash aggregate operator for GROUP BY queries.
pub struct HashAggregateOperator;

impl HashAggregateOperator {
    /// Execute hash aggregation on input batches.
    ///
    /// Groups rows by the group_by expressions and computes aggregates for each group.
    pub fn execute(
        group_by: &[Arc<dyn crate::planner::PhysicalExpr>],
        aggr_exprs: &[crate::planner::AggregateExpr],
        output_schema: &Arc<ArrowSchema>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        

        // Handle case with no grouping (global aggregates)
        if group_by.is_empty() {
            return Self::execute_global_aggregate(aggr_exprs, output_schema, input_batches);
        }

        // Build hash map: group_key_hash -> (group_key_values, accumulators)
        let mut groups: HashMap<u64, (Vec<ArrayRef>, Vec<Box<dyn Accumulator>>)> = HashMap::new();

        for batch in &input_batches {
            if batch.num_rows() == 0 {
                continue;
            }

            // Evaluate group by expressions
            let group_arrays: Vec<ArrayRef> = group_by
                .iter()
                .map(|expr| expr.evaluate(batch))
                .collect::<Result<Vec<_>>>()?;

            // Evaluate aggregate input expressions
            let aggr_input_arrays: Vec<Vec<ArrayRef>> = aggr_exprs
                .iter()
                .map(|agg| {
                    agg.args
                        .iter()
                        .map(|arg| arg.evaluate(batch))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;

            // Process each row
            for row in 0..batch.num_rows() {
                let hash = JoinHashTable::compute_hash_from_arrays(&group_arrays, row)?;

                let entry = groups.entry(hash).or_insert_with(|| {
                    // Extract group key values for this row
                    let key_values: Vec<ArrayRef> = group_arrays
                        .iter()
                        .map(|arr| arr.slice(row, 1))
                        .collect();

                    // Create accumulators for this group
                    let accumulators: Vec<Box<dyn Accumulator>> = aggr_exprs
                        .iter()
                        .map(|agg| Self::create_accumulator(&agg.func))
                        .collect();

                    (key_values, accumulators)
                });

                // Update accumulators with values from this row
                for (i, agg_inputs) in aggr_input_arrays.iter().enumerate() {
                    // For simplicity, use the first argument (most aggregates have one arg)
                    if !agg_inputs.is_empty() {
                        let value_slice = agg_inputs[0].slice(row, 1);
                        entry.1[i].update(&value_slice)?;
                    } else {
                        // COUNT(*) case - count the row
                        let one = Arc::new(Int64Array::from(vec![1i64])) as ArrayRef;
                        entry.1[i].update(&one)?;
                    }
                }
            }
        }

        // Handle empty input with no groups
        if groups.is_empty() && !input_batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(output_schema.clone())]);
        }

        // Build output batch from groups
        Self::build_output(groups, group_by.len(), aggr_exprs.len(), output_schema)
    }

    fn execute_global_aggregate(
        aggr_exprs: &[crate::planner::AggregateExpr],
        output_schema: &Arc<ArrowSchema>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        

        // Create single set of accumulators
        let mut accumulators: Vec<Box<dyn Accumulator>> = aggr_exprs
            .iter()
            .map(|agg| Self::create_accumulator(&agg.func))
            .collect();

        // Process all input
        for batch in &input_batches {
            if batch.num_rows() == 0 {
                continue;
            }

            for (i, agg) in aggr_exprs.iter().enumerate() {
                if !agg.args.is_empty() {
                    let values = agg.args[0].evaluate(batch)?;
                    accumulators[i].update(&values)?;
                } else {
                    // COUNT(*) - count all rows
                    let ones = Arc::new(Int64Array::from(vec![1i64; batch.num_rows()])) as ArrayRef;
                    accumulators[i].update(&ones)?;
                }
            }
        }

        // Finalize and build output
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(aggr_exprs.len());
        for acc in &accumulators {
            columns.push(acc.finalize()?);
        }

        Ok(vec![RecordBatch::try_new(output_schema.clone(), columns)?])
    }

    fn create_accumulator(func: &crate::planner::AggregateFunc) -> Box<dyn Accumulator> {
        use crate::planner::AggregateFunc;

        match func {
            AggregateFunc::Count => Box::new(CountAccumulator::new()),
            AggregateFunc::Sum => Box::new(SumAccumulator::new()),
            AggregateFunc::Avg => Box::new(AvgAccumulator::new()),
            AggregateFunc::Min => Box::new(MinAccumulator::new()),
            AggregateFunc::Max => Box::new(MaxAccumulator::new()),
            AggregateFunc::ApproxCountDistinct => Box::new(ApproxCountDistinctAccumulator::new()),
            AggregateFunc::ApproxPercentile => Box::new(ApproxPercentileAccumulator::new(0.5)), // Default to median
            AggregateFunc::ApproxMedian => Box::new(ApproxPercentileAccumulator::median()),
            _ => Box::new(CountAccumulator::new()), // Fallback
        }
    }

    fn build_output(
        groups: HashMap<u64, (Vec<ArrayRef>, Vec<Box<dyn Accumulator>>)>,
        num_group_cols: usize,
        num_agg_cols: usize,
        output_schema: &Arc<ArrowSchema>,
    ) -> Result<Vec<RecordBatch>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }

        let _num_groups = groups.len();

        // Collect group keys and finalized aggregates
        let mut group_col_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_group_cols];
        let mut agg_col_arrays: Vec<Vec<ArrayRef>> = vec![Vec::new(); num_agg_cols];

        for (_hash, (key_values, accumulators)) in groups {
            for (i, key_val) in key_values.into_iter().enumerate() {
                group_col_arrays[i].push(key_val);
            }
            for (i, acc) in accumulators.iter().enumerate() {
                agg_col_arrays[i].push(acc.finalize()?);
            }
        }

        // Concatenate arrays for each column
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(num_group_cols + num_agg_cols);

        for col_arrays in group_col_arrays {
            let refs: Vec<&dyn Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            columns.push(compute::concat(&refs)?);
        }

        for col_arrays in agg_col_arrays {
            let refs: Vec<&dyn Array> = col_arrays.iter().map(|a| a.as_ref()).collect();
            columns.push(compute::concat(&refs)?);
        }

        Ok(vec![RecordBatch::try_new(output_schema.clone(), columns)?])
    }
}

/// Sort operator using Arrow's sort kernels.
pub struct SortOperator;

impl SortOperator {
    /// Execute sort on input batches.
    ///
    /// Combines all batches, sorts by the given expressions, and returns sorted output.
    pub fn execute(
        sort_exprs: &[crate::planner::SortExpr],
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::compute::{concat_batches, lexsort_to_indices, take};

        if input_batches.is_empty() {
            return Ok(vec![]);
        }

        // Get schema from first batch
        let schema = input_batches[0].schema();

        // Combine all batches into one
        let combined = concat_batches(&schema, &input_batches)?;

        if combined.num_rows() == 0 {
            return Ok(vec![combined]);
        }

        // Evaluate sort expressions and build sort columns
        let mut sort_columns: Vec<arrow::compute::SortColumn> = Vec::with_capacity(sort_exprs.len());

        for expr in sort_exprs {
            let values = expr.expr.evaluate(&combined)?;
            sort_columns.push(arrow::compute::SortColumn {
                values,
                options: Some(arrow::compute::SortOptions {
                    descending: !expr.ascending,
                    nulls_first: expr.nulls_first,
                }),
            });
        }

        // Get sorted indices
        let indices = lexsort_to_indices(&sort_columns, None)?;

        // Take columns in sorted order
        let sorted_columns: Result<Vec<ArrayRef>> = combined
            .columns()
            .iter()
            .map(|col| {
                Ok(take(col.as_ref(), &indices, None)?)
            })
            .collect();

        Ok(vec![RecordBatch::try_new(schema, sorted_columns?)?])
    }
}

/// Memory manager for tracking and limiting memory usage.
#[derive(Debug)]
pub struct MemoryManager {
    /// Maximum memory budget in bytes
    max_memory: usize,
    /// Current memory usage in bytes
    used_memory: std::sync::atomic::AtomicUsize,
}

impl MemoryManager {
    /// Create a new memory manager with the given budget.
    pub fn new(max_memory: usize) -> Self {
        Self {
            max_memory,
            used_memory: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Create a memory manager with default budget (1GB).
    pub fn default_budget() -> Self {
        Self::new(1024 * 1024 * 1024) // 1GB
    }

    /// Try to reserve memory. Returns true if successful.
    pub fn try_reserve(&self, bytes: usize) -> bool {
        use std::sync::atomic::Ordering;

        let current = self.used_memory.load(Ordering::Relaxed);
        if current + bytes <= self.max_memory {
            self.used_memory.fetch_add(bytes, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Reserve memory, returning error if over budget.
    pub fn reserve(&self, bytes: usize) -> Result<MemoryReservation<'_>> {
        if self.try_reserve(bytes) {
            Ok(MemoryReservation {
                manager: self,
                bytes,
            })
        } else {
            Err(BlazeError::resource_exhausted(format!(
                "Memory limit exceeded: requested {} bytes, {} of {} used",
                bytes,
                self.used_memory.load(std::sync::atomic::Ordering::Relaxed),
                self.max_memory
            )))
        }
    }

    /// Release memory.
    pub fn release(&self, bytes: usize) {
        use std::sync::atomic::Ordering;
        self.used_memory.fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Get current memory usage.
    pub fn used(&self) -> usize {
        self.used_memory.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Get maximum memory budget.
    pub fn max(&self) -> usize {
        self.max_memory
    }

    /// Get available memory.
    pub fn available(&self) -> usize {
        self.max_memory.saturating_sub(self.used())
    }
}

impl Default for MemoryManager {
    fn default() -> Self {
        Self::default_budget()
    }
}

/// A reservation of memory that is released when dropped.
pub struct MemoryReservation<'a> {
    manager: &'a MemoryManager,
    bytes: usize,
}

impl<'a> MemoryReservation<'a> {
    /// Get the number of reserved bytes.
    pub fn size(&self) -> usize {
        self.bytes
    }

    /// Grow the reservation by additional bytes.
    pub fn grow(&mut self, additional: usize) -> Result<()> {
        if self.manager.try_reserve(additional) {
            self.bytes += additional;
            Ok(())
        } else {
            Err(BlazeError::resource_exhausted("Memory limit exceeded"))
        }
    }

    /// Shrink the reservation by the given bytes.
    pub fn shrink(&mut self, bytes: usize) {
        let release = bytes.min(self.bytes);
        self.manager.release(release);
        self.bytes -= release;
    }
}

impl<'a> Drop for MemoryReservation<'a> {
    fn drop(&mut self) {
        self.manager.release(self.bytes);
    }
}

/// Window function operator.
///
/// Executes window functions over partitions of data with ordering.
pub struct WindowOperator;

impl WindowOperator {
    /// Execute window functions on input batches.
    pub fn execute(
        window_exprs: &[crate::planner::PhysicalWindowExpr],
        schema: &Arc<ArrowSchema>,
        input_batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>> {
        

        if input_batches.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema.clone())]);
        }

        // Combine all input batches into a single batch for window processing
        let combined = Self::combine_batches(&input_batches)?;
        if combined.num_rows() == 0 {
            return Ok(vec![RecordBatch::new_empty(schema.clone())]);
        }

        let num_rows = combined.num_rows();
        let mut result_columns: Vec<ArrayRef> = Vec::new();

        // First, include all columns from the input
        for i in 0..combined.num_columns() {
            result_columns.push(combined.column(i).clone());
        }

        // Then compute each window expression
        for window_expr in window_exprs {
            let window_result = Self::compute_window_function(
                &window_expr.func,
                &window_expr.args,
                &window_expr.partition_by,
                &window_expr.order_by,
                &combined,
                num_rows,
            )?;
            result_columns.push(window_result);
        }

        Ok(vec![RecordBatch::try_new(schema.clone(), result_columns)?])
    }

    /// Combine multiple batches into a single batch.
    fn combine_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
        if batches.is_empty() {
            return Err(BlazeError::internal("No batches to combine"));
        }

        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        let schema = batches[0].schema();
        Ok(arrow::compute::concat_batches(&schema, batches)?)
    }

    /// Compute a window function result.
    fn compute_window_function(
        func: &crate::planner::WindowFunction,
        args: &[Arc<dyn crate::planner::PhysicalExpr>],
        partition_by: &[Arc<dyn crate::planner::PhysicalExpr>],
        order_by: &[crate::planner::SortExpr],
        batch: &RecordBatch,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        use crate::planner::WindowFunction;

        // Get partition groups
        let partitions = Self::compute_partitions(partition_by, batch)?;

        // Get sort indices within each partition if ordering is specified
        let sorted_indices = if !order_by.is_empty() {
            Some(Self::compute_sort_indices(order_by, batch, &partitions)?)
        } else {
            None
        };

        match func {
            WindowFunction::RowNumber => {
                Self::compute_row_number(&partitions, sorted_indices.as_ref(), num_rows)
            }
            WindowFunction::Rank => {
                Self::compute_rank(&partitions, order_by, batch, sorted_indices.as_ref(), num_rows)
            }
            WindowFunction::DenseRank => {
                Self::compute_dense_rank(&partitions, order_by, batch, sorted_indices.as_ref(), num_rows)
            }
            WindowFunction::Lead => {
                let offset = Self::get_offset_arg(args, batch, 1)?;
                Self::compute_lead_lag(args, batch, &partitions, sorted_indices.as_ref(), offset, num_rows)
            }
            WindowFunction::Lag => {
                let offset = Self::get_offset_arg(args, batch, 1)?;
                Self::compute_lead_lag(args, batch, &partitions, sorted_indices.as_ref(), -(offset as i64), num_rows)
            }
            WindowFunction::FirstValue => {
                Self::compute_first_value(args, batch, &partitions, sorted_indices.as_ref(), num_rows)
            }
            WindowFunction::LastValue => {
                Self::compute_last_value(args, batch, &partitions, sorted_indices.as_ref(), num_rows)
            }
            WindowFunction::Aggregate(agg_func) => {
                Self::compute_window_aggregate(agg_func, args, batch, &partitions, num_rows)
            }
            _ => Err(BlazeError::not_implemented(format!(
                "Window function: {:?}",
                func
            ))),
        }
    }

    /// Compute partition groups - returns actual row indices for each partition.
    /// This correctly handles non-contiguous partition groups and hash collisions.
    fn compute_partitions(
        partition_by: &[Arc<dyn crate::planner::PhysicalExpr>],
        batch: &RecordBatch,
    ) -> Result<Vec<Vec<usize>>> {
        if partition_by.is_empty() {
            // Single partition containing all rows
            return Ok(vec![(0..batch.num_rows()).collect()]);
        }

        // Evaluate partition expressions
        let partition_keys: Vec<ArrayRef> = partition_by
            .iter()
            .map(|expr| expr.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        // Group rows by partition key - use hash as initial bucket, then verify equality
        // This handles hash collisions correctly
        let mut partition_map: HashMap<u64, Vec<(usize, Vec<usize>)>> = HashMap::new();

        for row in 0..batch.num_rows() {
            let hash = Self::hash_row(&partition_keys, row);
            let bucket = partition_map.entry(hash).or_default();

            // Find an existing partition with matching values (handling hash collisions)
            let mut found = false;
            for (representative_row, indices) in bucket.iter_mut() {
                if Self::rows_equal(&partition_keys, *representative_row, row) {
                    indices.push(row);
                    found = true;
                    break;
                }
            }

            if !found {
                // Start a new partition group
                bucket.push((row, vec![row]));
            }
        }

        // Flatten the buckets into partition groups
        let mut result = Vec::new();
        for (_hash, groups) in partition_map {
            for (_representative, indices) in groups {
                result.push(indices);
            }
        }

        Ok(result)
    }

    /// Check if two rows have equal values for all partition keys.
    fn rows_equal(arrays: &[ArrayRef], row_a: usize, row_b: usize) -> bool {
        for array in arrays {
            if !Self::values_equal(array, row_a, row_b) {
                return false;
            }
        }
        true
    }

    /// Check if two values in an array are equal.
    fn values_equal(array: &ArrayRef, a: usize, b: usize) -> bool {
        // Handle nulls
        let a_null = array.is_null(a);
        let b_null = array.is_null(b);
        if a_null && b_null {
            return true;
        }
        if a_null != b_null {
            return false;
        }

        match array.data_type() {
            DataType::Boolean => {
                if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::Int8 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int8Array>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::Int16 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int16Array>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::Int32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::Int64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::UInt32 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::UInt64 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::Float32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                    let va = arr.value(a);
                    let vb = arr.value(b);
                    va == vb || (va.is_nan() && vb.is_nan())
                } else {
                    false
                }
            }
            DataType::Float64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                    let va = arr.value(a);
                    let vb = arr.value(b);
                    va == vb || (va.is_nan() && vb.is_nan())
                } else {
                    false
                }
            }
            DataType::Utf8 => {
                if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::Date32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::Date64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Date64Array>() {
                    arr.value(a) == arr.value(b)
                } else {
                    false
                }
            }
            DataType::Timestamp(unit, _) => {
                use arrow::datatypes::TimeUnit;
                match unit {
                    TimeUnit::Second => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                            arr.value(a) == arr.value(b)
                        } else {
                            false
                        }
                    }
                    TimeUnit::Millisecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                            arr.value(a) == arr.value(b)
                        } else {
                            false
                        }
                    }
                    TimeUnit::Microsecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                            arr.value(a) == arr.value(b)
                        } else {
                            false
                        }
                    }
                    TimeUnit::Nanosecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                            arr.value(a) == arr.value(b)
                        } else {
                            false
                        }
                    }
                }
            }
            // For unsupported types, use string representation comparison as fallback
            _ => {
                let str_a = arrow::util::display::array_value_to_string(array, a);
                let str_b = arrow::util::display::array_value_to_string(array, b);
                match (str_a, str_b) {
                    (Ok(a), Ok(b)) => a == b,
                    _ => false,
                }
            }
        }
    }

    /// Hash a single row from multiple arrays.
    fn hash_row(arrays: &[ArrayRef], row: usize) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        for array in arrays {
            Self::hash_array_value(array, row, &mut hasher);
        }
        hasher.finish()
    }

    /// Hash a single value from an array.
    fn hash_array_value<H: Hasher>(array: &ArrayRef, row: usize, hasher: &mut H) {
        if array.is_null(row) {
            0xDEADBEEFu64.hash(hasher);
            return;
        }

        match array.data_type() {
            DataType::Boolean => {
                if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::Int8 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int8Array>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::Int16 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int16Array>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::Int32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::Int64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::UInt32 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::UInt64 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::Float32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                    arr.value(row).to_bits().hash(hasher);
                }
            }
            DataType::Float64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                    arr.value(row).to_bits().hash(hasher);
                }
            }
            DataType::Utf8 => {
                if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::Date32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::Date64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Date64Array>() {
                    arr.value(row).hash(hasher);
                }
            }
            DataType::Timestamp(unit, _) => {
                use arrow::datatypes::TimeUnit;
                match unit {
                    TimeUnit::Second => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                            arr.value(row).hash(hasher);
                        }
                    }
                    TimeUnit::Millisecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                            arr.value(row).hash(hasher);
                        }
                    }
                    TimeUnit::Microsecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                            arr.value(row).hash(hasher);
                        }
                    }
                    TimeUnit::Nanosecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                            arr.value(row).hash(hasher);
                        }
                    }
                }
            }
            // For unsupported types, use string representation
            _ => {
                let value_str = format!("{:?}", arrow::util::display::array_value_to_string(array, row));
                value_str.hash(hasher);
            }
        }
    }

    /// Compute sort indices within partitions.
    fn compute_sort_indices(
        order_by: &[crate::planner::SortExpr],
        batch: &RecordBatch,
        partitions: &[Vec<usize>],
    ) -> Result<Vec<Vec<usize>>> {
        let mut result = Vec::with_capacity(partitions.len());

        for partition_rows in partitions {
            let mut indices: Vec<usize> = partition_rows.clone();

            // Sort based on order by expressions
            if !order_by.is_empty() {
                let sort_keys: Vec<ArrayRef> = order_by
                    .iter()
                    .map(|sort_expr| sort_expr.expr.evaluate(batch))
                    .collect::<Result<Vec<_>>>()?;

                indices.sort_by(|&a, &b| {
                    for (i, sort_expr) in order_by.iter().enumerate() {
                        let cmp = Self::compare_values(&sort_keys[i], a, b);
                        let cmp = if sort_expr.ascending { cmp } else { cmp.reverse() };
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            result.push(indices);
        }

        Ok(result)
    }

    /// Compare two values from an array for sorting.
    fn compare_values(array: &ArrayRef, a: usize, b: usize) -> std::cmp::Ordering {
        use std::cmp::Ordering;

        if array.is_null(a) && array.is_null(b) {
            return Ordering::Equal;
        }
        if array.is_null(a) {
            return Ordering::Greater; // nulls last
        }
        if array.is_null(b) {
            return Ordering::Less;
        }

        match array.data_type() {
            DataType::Boolean => {
                if let Some(arr) = array.as_any().downcast_ref::<BooleanArray>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::Int8 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int8Array>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::Int16 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int16Array>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::Int32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::Int64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::UInt32 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt32Array>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::UInt64 => {
                if let Some(arr) = array.as_any().downcast_ref::<UInt64Array>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::Float32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float32Array>() {
                    arr.value(a).partial_cmp(&arr.value(b)).unwrap_or(Ordering::Equal)
                } else {
                    Ordering::Equal
                }
            }
            DataType::Float64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                    arr.value(a).partial_cmp(&arr.value(b)).unwrap_or(Ordering::Equal)
                } else {
                    Ordering::Equal
                }
            }
            DataType::Utf8 => {
                if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                    arr.value(a).cmp(arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::Date32 => {
                if let Some(arr) = array.as_any().downcast_ref::<Date32Array>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::Date64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Date64Array>() {
                    arr.value(a).cmp(&arr.value(b))
                } else {
                    Ordering::Equal
                }
            }
            DataType::Timestamp(unit, _) => {
                use arrow::datatypes::TimeUnit;
                match unit {
                    TimeUnit::Second => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampSecondArray>() {
                            arr.value(a).cmp(&arr.value(b))
                        } else {
                            Ordering::Equal
                        }
                    }
                    TimeUnit::Millisecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampMillisecondArray>() {
                            arr.value(a).cmp(&arr.value(b))
                        } else {
                            Ordering::Equal
                        }
                    }
                    TimeUnit::Microsecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                            arr.value(a).cmp(&arr.value(b))
                        } else {
                            Ordering::Equal
                        }
                    }
                    TimeUnit::Nanosecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>() {
                            arr.value(a).cmp(&arr.value(b))
                        } else {
                            Ordering::Equal
                        }
                    }
                }
            }
            // For unsupported types, use string comparison as fallback
            _ => {
                let str_a = arrow::util::display::array_value_to_string(array, a);
                let str_b = arrow::util::display::array_value_to_string(array, b);
                match (str_a, str_b) {
                    (Ok(a), Ok(b)) => a.cmp(&b),
                    _ => Ordering::Equal,
                }
            }
        }
    }

    /// Compute ROW_NUMBER().
    fn compute_row_number(
        partitions: &[Vec<usize>],
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        let mut result = vec![0i64; num_rows];

        for (part_idx, partition_rows) in partitions.iter().enumerate() {
            let indices = sorted_indices
                .map(|si| si[part_idx].clone())
                .unwrap_or_else(|| partition_rows.clone());

            for (row_num, &idx) in indices.iter().enumerate() {
                result[idx] = (row_num + 1) as i64;
            }
        }

        Ok(Arc::new(Int64Array::from(result)))
    }

    /// Compute RANK().
    fn compute_rank(
        partitions: &[Vec<usize>],
        order_by: &[crate::planner::SortExpr],
        batch: &RecordBatch,
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        let mut result = vec![0i64; num_rows];

        if order_by.is_empty() {
            // Without ORDER BY, all rows have rank 1
            for idx in 0..num_rows {
                result[idx] = 1;
            }
            return Ok(Arc::new(Int64Array::from(result)));
        }

        let sort_keys: Vec<ArrayRef> = order_by
            .iter()
            .map(|sort_expr| sort_expr.expr.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        for (part_idx, partition_rows) in partitions.iter().enumerate() {
            let indices = sorted_indices
                .as_ref()
                .map(|si| si[part_idx].clone())
                .unwrap_or_else(|| partition_rows.clone());

            if indices.is_empty() {
                continue;
            }

            let mut rank = 1i64;
            result[indices[0]] = rank;

            for i in 1..indices.len() {
                let prev_idx = indices[i - 1];
                let curr_idx = indices[i];

                // Check if values differ
                let same = sort_keys.iter().all(|key| {
                    Self::compare_values(key, prev_idx, curr_idx) == std::cmp::Ordering::Equal
                });

                if !same {
                    rank = (i + 1) as i64; // gap
                }
                result[curr_idx] = rank;
            }
        }

        Ok(Arc::new(Int64Array::from(result)))
    }

    /// Compute DENSE_RANK().
    fn compute_dense_rank(
        partitions: &[Vec<usize>],
        order_by: &[crate::planner::SortExpr],
        batch: &RecordBatch,
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        let mut result = vec![0i64; num_rows];

        if order_by.is_empty() {
            for idx in 0..num_rows {
                result[idx] = 1;
            }
            return Ok(Arc::new(Int64Array::from(result)));
        }

        let sort_keys: Vec<ArrayRef> = order_by
            .iter()
            .map(|sort_expr| sort_expr.expr.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        for (part_idx, partition_rows) in partitions.iter().enumerate() {
            let indices = sorted_indices
                .as_ref()
                .map(|si| si[part_idx].clone())
                .unwrap_or_else(|| partition_rows.clone());

            if indices.is_empty() {
                continue;
            }

            let mut rank = 1i64;
            result[indices[0]] = rank;

            for i in 1..indices.len() {
                let prev_idx = indices[i - 1];
                let curr_idx = indices[i];

                let same = sort_keys.iter().all(|key| {
                    Self::compare_values(key, prev_idx, curr_idx) == std::cmp::Ordering::Equal
                });

                if !same {
                    rank += 1; // no gap
                }
                result[curr_idx] = rank;
            }
        }

        Ok(Arc::new(Int64Array::from(result)))
    }

    /// Get offset argument for LAG/LEAD.
    fn get_offset_arg(
        args: &[Arc<dyn crate::planner::PhysicalExpr>],
        _batch: &RecordBatch,
        default: i64,
    ) -> Result<i64> {
        if args.len() >= 2 {
            // For now, assume literal offset
            Ok(default)
        } else {
            Ok(default)
        }
    }

    /// Compute LAG or LEAD.
    fn compute_lead_lag(
        args: &[Arc<dyn crate::planner::PhysicalExpr>],
        batch: &RecordBatch,
        partitions: &[Vec<usize>],
        sorted_indices: Option<&Vec<Vec<usize>>>,
        offset: i64,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if args.is_empty() {
            return Err(BlazeError::analysis("LAG/LEAD requires at least one argument"));
        }

        let value_array = args[0].evaluate(batch)?;

        // Create result array with nulls - initialize with nulls for all rows
        let mut result_values: Vec<Option<i64>> = vec![None; num_rows];

        for (part_idx, partition_rows) in partitions.iter().enumerate() {
            let indices = sorted_indices
                .as_ref()
                .map(|si| si[part_idx].clone())
                .unwrap_or_else(|| partition_rows.clone());

            for (pos, &curr_idx) in indices.iter().enumerate() {
                let target_pos = pos as i64 + offset;
                if target_pos >= 0 && (target_pos as usize) < indices.len() {
                    let target_idx = indices[target_pos as usize];
                    if !value_array.is_null(target_idx) {
                        if let Some(int_arr) = value_array.as_any().downcast_ref::<Int64Array>() {
                            result_values[curr_idx] = Some(int_arr.value(target_idx));
                        }
                    }
                }
            }
        }

        // Build result array from values
        let array: Int64Array = result_values.into_iter().collect();
        Ok(Arc::new(array))
    }

    /// Compute FIRST_VALUE.
    fn compute_first_value(
        args: &[Arc<dyn crate::planner::PhysicalExpr>],
        batch: &RecordBatch,
        partitions: &[Vec<usize>],
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if args.is_empty() {
            return Err(BlazeError::analysis("FIRST_VALUE requires one argument"));
        }

        let value_array = args[0].evaluate(batch)?;
        let mut result_values: Vec<Option<i64>> = vec![None; num_rows];

        for (part_idx, partition_rows) in partitions.iter().enumerate() {
            let indices = sorted_indices
                .as_ref()
                .map(|si| si[part_idx].clone())
                .unwrap_or_else(|| partition_rows.clone());

            if indices.is_empty() {
                continue;
            }

            let first_idx = indices[0];
            let first_value = if !value_array.is_null(first_idx) {
                value_array.as_any().downcast_ref::<Int64Array>()
                    .map(|arr| Some(arr.value(first_idx)))
                    .unwrap_or(None)
            } else {
                None
            };

            for &idx in &indices {
                result_values[idx] = first_value;
            }
        }

        Ok(Arc::new(Int64Array::from(result_values)))
    }

    /// Compute LAST_VALUE.
    fn compute_last_value(
        args: &[Arc<dyn crate::planner::PhysicalExpr>],
        batch: &RecordBatch,
        partitions: &[Vec<usize>],
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if args.is_empty() {
            return Err(BlazeError::analysis("LAST_VALUE requires one argument"));
        }

        let value_array = args[0].evaluate(batch)?;
        let mut result_values: Vec<Option<i64>> = vec![None; num_rows];

        for (part_idx, partition_rows) in partitions.iter().enumerate() {
            let indices = sorted_indices
                .as_ref()
                .map(|si| si[part_idx].clone())
                .unwrap_or_else(|| partition_rows.clone());

            if indices.is_empty() {
                continue;
            }

            let last_idx = indices[indices.len() - 1];
            let last_value = if !value_array.is_null(last_idx) {
                value_array.as_any().downcast_ref::<Int64Array>()
                    .map(|arr| Some(arr.value(last_idx)))
                    .unwrap_or(None)
            } else {
                None
            };

            for &idx in &indices {
                result_values[idx] = last_value;
            }
        }

        Ok(Arc::new(Int64Array::from(result_values)))
    }

    /// Compute aggregate as window function.
    fn compute_window_aggregate(
        agg_func: &crate::planner::AggregateFunc,
        args: &[Arc<dyn crate::planner::PhysicalExpr>],
        batch: &RecordBatch,
        partitions: &[Vec<usize>],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        use crate::planner::AggregateFunc;

        let value_array = if !args.is_empty() {
            Some(args[0].evaluate(batch)?)
        } else {
            None
        };

        let mut result = vec![0.0f64; num_rows];

        for partition_rows in partitions {

            let agg_value = match agg_func {
                AggregateFunc::Count => partition_rows.len() as f64,
                AggregateFunc::Sum => {
                    if let Some(arr) = &value_array {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            partition_rows.iter()
                                .filter(|&&i| !int_arr.is_null(i))
                                .map(|&i| int_arr.value(i) as f64)
                                .sum()
                        } else if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
                            partition_rows.iter()
                                .filter(|&&i| !float_arr.is_null(i))
                                .map(|&i| float_arr.value(i))
                                .sum()
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    }
                }
                AggregateFunc::Avg => {
                    if let Some(arr) = &value_array {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            let values: Vec<f64> = partition_rows.iter()
                                .filter(|&&i| !int_arr.is_null(i))
                                .map(|&i| int_arr.value(i) as f64)
                                .collect();
                            if values.is_empty() {
                                0.0
                            } else {
                                values.iter().sum::<f64>() / values.len() as f64
                            }
                        } else if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
                            let values: Vec<f64> = partition_rows.iter()
                                .filter(|&&i| !float_arr.is_null(i))
                                .map(|&i| float_arr.value(i))
                                .collect();
                            if values.is_empty() {
                                0.0
                            } else {
                                values.iter().sum::<f64>() / values.len() as f64
                            }
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    }
                }
                AggregateFunc::Min => {
                    if let Some(arr) = &value_array {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            partition_rows.iter()
                                .filter(|&&i| !int_arr.is_null(i))
                                .map(|&i| int_arr.value(i) as f64)
                                .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                                .unwrap_or(0.0)
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    }
                }
                AggregateFunc::Max => {
                    if let Some(arr) = &value_array {
                        if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                            partition_rows.iter()
                                .filter(|&&i| !int_arr.is_null(i))
                                .map(|&i| int_arr.value(i) as f64)
                                .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                                .unwrap_or(0.0)
                        } else {
                            0.0
                        }
                    } else {
                        0.0
                    }
                }
                _ => 0.0,
            };

            for &idx in partition_rows {
                result[idx] = agg_value;
            }
        }

        Ok(Arc::new(Float64Array::from(result)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use crate::planner::ColumnExpr;

    fn create_test_batch(
        schema: Arc<ArrowSchema>,
        id_values: Vec<i64>,
        name_values: Vec<&str>,
    ) -> RecordBatch {
        let id_array = Arc::new(Int64Array::from(id_values)) as ArrayRef;
        let name_array = Arc::new(StringArray::from(name_values)) as ArrayRef;
        RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
    }

    fn make_schema(fields: Vec<(&str, DataType)>) -> Arc<ArrowSchema> {
        let fields: Vec<Field> = fields
            .into_iter()
            .map(|(name, dt)| Field::new(name, dt, true))
            .collect();
        Arc::new(Schema::new(fields))
    }

    #[test]
    fn test_hash_join_inner() {
        // Left table: users (id, name)
        let left_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
        ]);
        let left_batch = create_test_batch(
            left_schema.clone(),
            vec![1, 2, 3],
            vec!["Alice", "Bob", "Charlie"],
        );

        // Right table: orders (user_id, product)
        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch = create_test_batch(
            right_schema.clone(),
            vec![1, 1, 2, 4],
            vec!["Widget", "Gadget", "Thing", "Stuff"],
        );

        // Output schema: (id, name, user_id, product)
        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        // Key expressions
        let left_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![left_batch],
            vec![right_batch],
            JoinType::Inner,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        // Should have 3 rows: Alice-Widget, Alice-Gadget, Bob-Thing
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_hash_join_left_outer() {
        let left_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
        ]);
        let left_batch = create_test_batch(
            left_schema.clone(),
            vec![1, 2, 3],
            vec!["Alice", "Bob", "Charlie"],
        );

        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch = create_test_batch(
            right_schema.clone(),
            vec![1, 2],
            vec!["Widget", "Gadget"],
        );

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![left_batch],
            vec![right_batch],
            JoinType::Left,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        // Should have 3 rows: Alice-Widget, Bob-Gadget, Charlie-NULL
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify Charlie has NULL for right side
        let batch = &result[0];
        let user_id_col = batch.column(2);
        // Charlie is at index 2, should be NULL
        assert!(user_id_col.is_null(2));
    }

    #[test]
    fn test_hash_join_right_outer() {
        let left_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
        ]);
        let left_batch = create_test_batch(
            left_schema.clone(),
            vec![1, 2],
            vec!["Alice", "Bob"],
        );

        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch = create_test_batch(
            right_schema.clone(),
            vec![1, 2, 3],
            vec!["Widget", "Gadget", "Thing"],
        );

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![left_batch],
            vec![right_batch],
            JoinType::Right,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        // Should have 3 rows: Alice-Widget, Bob-Gadget, NULL-Thing
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_hash_join_left_semi() {
        let left_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
        ]);
        let left_batch = create_test_batch(
            left_schema.clone(),
            vec![1, 2, 3],
            vec!["Alice", "Bob", "Charlie"],
        );

        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch = create_test_batch(
            right_schema.clone(),
            vec![1, 1, 2], // Alice and Bob exist, Charlie doesn't
            vec!["Widget", "Gadget", "Thing"],
        );

        // Semi join only returns left columns
        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![left_batch],
            vec![right_batch],
            JoinType::LeftSemi,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        // Should have 2 rows: Alice and Bob (each appears once even though Alice has 2 matches)
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Verify only 2 columns (left side only)
        assert_eq!(result[0].num_columns(), 2);
    }

    #[test]
    fn test_hash_join_left_anti() {
        let left_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
        ]);
        let left_batch = create_test_batch(
            left_schema.clone(),
            vec![1, 2, 3],
            vec!["Alice", "Bob", "Charlie"],
        );

        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch = create_test_batch(
            right_schema.clone(),
            vec![1, 2], // Alice and Bob exist, Charlie doesn't
            vec!["Widget", "Thing"],
        );

        // Anti join only returns left columns
        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![left_batch],
            vec![right_batch],
            JoinType::LeftAnti,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        // Should have 1 row: Charlie (the only one with no match)
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);

        // Verify it's Charlie
        let batch = &result[0];
        let name_col = batch.column(1);
        let name_arr = name_col.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(name_arr.value(0), "Charlie");
    }

    #[test]
    fn test_cross_join() {
        let left_schema = make_schema(vec![
            ("a", DataType::Int64),
        ]);
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();

        let right_schema = make_schema(vec![
            ("b", DataType::Int64),
        ]);
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef],
        )
        .unwrap();

        let output_schema = make_schema(vec![
            ("a", DataType::Int64),
            ("b", DataType::Int64),
        ]);

        let result = CrossJoinOperator::execute(
            vec![left_batch],
            vec![right_batch],
            &output_schema,
        )
        .unwrap();

        // 2 x 3 = 6 rows
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6);

        // Verify the pattern: (1,10), (1,20), (1,30), (2,10), (2,20), (2,30)
        let batch = &result[0];
        let a_col = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let b_col = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(a_col.value(0), 1);
        assert_eq!(b_col.value(0), 10);
        assert_eq!(a_col.value(1), 1);
        assert_eq!(b_col.value(1), 20);
        assert_eq!(a_col.value(2), 1);
        assert_eq!(b_col.value(2), 30);
        assert_eq!(a_col.value(3), 2);
        assert_eq!(b_col.value(3), 10);
    }

    #[test]
    fn test_hash_join_empty_inputs() {
        let left_schema = make_schema(vec![
            ("id", DataType::Int64),
        ]);
        let right_schema = make_schema(vec![
            ("id", DataType::Int64),
        ]);
        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("id", DataType::Int64),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("id", 0));

        // Inner join with empty right = empty
        let result = HashJoinOperator::execute(
            vec![RecordBatch::try_new(
                left_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
            ).unwrap()],
            vec![],
            JoinType::Inner,
            &[left_key.clone()],
            &[right_key.clone()],
            &output_schema,
        )
        .unwrap();
        assert!(result.is_empty());

        // Left join with empty right = all left rows with NULLs
        let result = HashJoinOperator::execute(
            vec![RecordBatch::try_new(
                left_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
            ).unwrap()],
            vec![],
            JoinType::Left,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_count_accumulator() {
        let mut acc = CountAccumulator::new();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        acc.update(&values).unwrap();

        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 5);
    }

    #[test]
    fn test_sum_accumulator() {
        let mut acc = SumAccumulator::new();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        acc.update(&values).unwrap();

        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 15.0);
    }

    #[test]
    fn test_avg_accumulator() {
        let mut acc = AvgAccumulator::new();
        let values: ArrayRef = Arc::new(Float64Array::from(vec![10.0, 20.0, 30.0]));
        acc.update(&values).unwrap();

        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 20.0);
    }

    #[test]
    fn test_min_max_accumulator() {
        let mut min_acc = MinAccumulator::new();
        let mut max_acc = MaxAccumulator::new();

        let values: ArrayRef = Arc::new(Int64Array::from(vec![5, 2, 8, 1, 9]));
        min_acc.update(&values).unwrap();
        max_acc.update(&values).unwrap();

        let min_result = min_acc.finalize().unwrap();
        let min_arr = min_result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(min_arr.value(0), 1.0);

        let max_result = max_acc.finalize().unwrap();
        let max_arr = max_result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(max_arr.value(0), 9.0);
    }
}
