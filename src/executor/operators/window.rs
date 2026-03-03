//! Window function operator.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, RecordBatch, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Schema as ArrowSchema};

use crate::error::{BlazeError, Result};

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
            WindowFunction::Rank => Self::compute_rank(
                &partitions,
                order_by,
                batch,
                sorted_indices.as_ref(),
                num_rows,
            ),
            WindowFunction::DenseRank => Self::compute_dense_rank(
                &partitions,
                order_by,
                batch,
                sorted_indices.as_ref(),
                num_rows,
            ),
            WindowFunction::Ntile => {
                Self::compute_ntile(args, batch, &partitions, sorted_indices.as_ref(), num_rows)
            }
            WindowFunction::PercentRank => Self::compute_percent_rank(
                &partitions,
                order_by,
                batch,
                sorted_indices.as_ref(),
                num_rows,
            ),
            WindowFunction::CumeDist => Self::compute_cume_dist(
                &partitions,
                order_by,
                batch,
                sorted_indices.as_ref(),
                num_rows,
            ),
            WindowFunction::Lead => {
                let offset = Self::get_offset_arg(args, batch, 1)?;
                Self::compute_lead_lag(
                    args,
                    batch,
                    &partitions,
                    sorted_indices.as_ref(),
                    offset,
                    num_rows,
                )
            }
            WindowFunction::Lag => {
                let offset = Self::get_offset_arg(args, batch, 1)?;
                Self::compute_lead_lag(
                    args,
                    batch,
                    &partitions,
                    sorted_indices.as_ref(),
                    -offset,
                    num_rows,
                )
            }
            WindowFunction::FirstValue => Self::compute_first_value(
                args,
                batch,
                &partitions,
                sorted_indices.as_ref(),
                num_rows,
            ),
            WindowFunction::LastValue => Self::compute_last_value(
                args,
                batch,
                &partitions,
                sorted_indices.as_ref(),
                num_rows,
            ),
            WindowFunction::NthValue => {
                Self::compute_nth_value(args, batch, &partitions, sorted_indices.as_ref(), num_rows)
            }
            WindowFunction::Aggregate(agg_func) => {
                Self::compute_window_aggregate(agg_func, args, batch, &partitions, num_rows)
            }
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
                        if let Some(arr) =
                            array.as_any().downcast_ref::<TimestampMillisecondArray>()
                        {
                            arr.value(a) == arr.value(b)
                        } else {
                            false
                        }
                    }
                    TimeUnit::Microsecond => {
                        if let Some(arr) =
                            array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        {
                            arr.value(a) == arr.value(b)
                        } else {
                            false
                        }
                    }
                    TimeUnit::Nanosecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>()
                        {
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
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

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
                        if let Some(arr) =
                            array.as_any().downcast_ref::<TimestampMillisecondArray>()
                        {
                            arr.value(row).hash(hasher);
                        }
                    }
                    TimeUnit::Microsecond => {
                        if let Some(arr) =
                            array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        {
                            arr.value(row).hash(hasher);
                        }
                    }
                    TimeUnit::Nanosecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>()
                        {
                            arr.value(row).hash(hasher);
                        }
                    }
                }
            }
            // For unsupported types, use string representation
            _ => {
                let value_str = format!(
                    "{:?}",
                    arrow::util::display::array_value_to_string(array, row)
                );
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
                        let cmp = if sort_expr.ascending {
                            cmp
                        } else {
                            cmp.reverse()
                        };
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
                    arr.value(a)
                        .partial_cmp(&arr.value(b))
                        .unwrap_or(Ordering::Equal)
                } else {
                    Ordering::Equal
                }
            }
            DataType::Float64 => {
                if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
                    arr.value(a)
                        .partial_cmp(&arr.value(b))
                        .unwrap_or(Ordering::Equal)
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
                        if let Some(arr) =
                            array.as_any().downcast_ref::<TimestampMillisecondArray>()
                        {
                            arr.value(a).cmp(&arr.value(b))
                        } else {
                            Ordering::Equal
                        }
                    }
                    TimeUnit::Microsecond => {
                        if let Some(arr) =
                            array.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        {
                            arr.value(a).cmp(&arr.value(b))
                        } else {
                            Ordering::Equal
                        }
                    }
                    TimeUnit::Nanosecond => {
                        if let Some(arr) = array.as_any().downcast_ref::<TimestampNanosecondArray>()
                        {
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
            for item in result.iter_mut().take(num_rows) {
                *item = 1;
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
            for item in result.iter_mut().take(num_rows) {
                *item = 1;
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
            return Err(BlazeError::analysis(
                "LAG/LEAD requires at least one argument",
            ));
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
                value_array
                    .as_any()
                    .downcast_ref::<Int64Array>()
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
                value_array
                    .as_any()
                    .downcast_ref::<Int64Array>()
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

    /// Compute NTILE(n) - divides rows into n buckets.
    fn compute_ntile(
        args: &[Arc<dyn crate::planner::PhysicalExpr>],
        batch: &RecordBatch,
        partitions: &[Vec<usize>],
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        // Get the number of buckets from the argument
        let n_buckets = if !args.is_empty() {
            let n_array = args[0].evaluate(batch)?;
            if let Some(int_arr) = n_array.as_any().downcast_ref::<Int64Array>() {
                if !int_arr.is_empty() && !int_arr.is_null(0) {
                    int_arr.value(0).max(1) as usize
                } else {
                    1
                }
            } else {
                1
            }
        } else {
            return Err(BlazeError::analysis("NTILE requires one argument"));
        };

        let mut result = vec![0i64; num_rows];

        for (part_idx, partition_rows) in partitions.iter().enumerate() {
            let indices = sorted_indices
                .as_ref()
                .map(|si| si[part_idx].clone())
                .unwrap_or_else(|| partition_rows.clone());

            let partition_size = indices.len();
            if partition_size == 0 {
                continue;
            }

            // Calculate bucket sizes
            // Each bucket gets at least partition_size / n_buckets rows
            // The first (partition_size % n_buckets) buckets get one extra row
            let base_size = partition_size / n_buckets;
            let extra = partition_size % n_buckets;

            let mut row_pos = 0;
            for bucket in 0..n_buckets {
                let bucket_size = base_size + if bucket < extra { 1 } else { 0 };
                for _ in 0..bucket_size {
                    if row_pos < indices.len() {
                        result[indices[row_pos]] = (bucket + 1) as i64;
                        row_pos += 1;
                    }
                }
            }
        }

        Ok(Arc::new(Int64Array::from(result)))
    }

    /// Compute PERCENT_RANK() - relative rank from 0 to 1.
    /// Formula: (rank - 1) / (partition_size - 1)
    fn compute_percent_rank(
        partitions: &[Vec<usize>],
        order_by: &[crate::planner::SortExpr],
        batch: &RecordBatch,
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        let mut result = vec![0.0f64; num_rows];

        if order_by.is_empty() {
            // Without ORDER BY, all rows have percent_rank 0
            return Ok(Arc::new(Float64Array::from(result)));
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

            let partition_size = indices.len();
            if partition_size <= 1 {
                // Single row or empty partition has percent_rank 0
                for &idx in &indices {
                    result[idx] = 0.0;
                }
                continue;
            }

            // Calculate rank (with gaps) first
            let mut ranks = vec![1i64; partition_size];
            for i in 1..indices.len() {
                let prev_idx = indices[i - 1];
                let curr_idx = indices[i];

                let same = sort_keys.iter().all(|key| {
                    Self::compare_values(key, prev_idx, curr_idx) == std::cmp::Ordering::Equal
                });

                if same {
                    ranks[i] = ranks[i - 1];
                } else {
                    ranks[i] = (i + 1) as i64;
                }
            }

            // Convert to percent_rank
            for (i, &idx) in indices.iter().enumerate() {
                result[idx] = (ranks[i] - 1) as f64 / (partition_size - 1) as f64;
            }
        }

        Ok(Arc::new(Float64Array::from(result)))
    }

    /// Compute CUME_DIST() - cumulative distribution.
    /// Formula: count of rows <= current row / partition_size
    fn compute_cume_dist(
        partitions: &[Vec<usize>],
        order_by: &[crate::planner::SortExpr],
        batch: &RecordBatch,
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        let mut result = vec![1.0f64; num_rows];

        if order_by.is_empty() {
            // Without ORDER BY, all rows have cume_dist 1
            return Ok(Arc::new(Float64Array::from(result)));
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

            let partition_size = indices.len();
            if partition_size == 0 {
                continue;
            }

            // Group rows by their sort key values (count rows <= current)
            let mut i = 0;
            while i < indices.len() {
                // Find all rows with the same sort key value
                let mut j = i + 1;
                while j < indices.len() {
                    let same = sort_keys.iter().all(|key| {
                        Self::compare_values(key, indices[i], indices[j])
                            == std::cmp::Ordering::Equal
                    });
                    if !same {
                        break;
                    }
                    j += 1;
                }

                // All rows from i to j-1 have the same cume_dist = j / partition_size
                let cume_dist = j as f64 / partition_size as f64;
                for k in i..j {
                    result[indices[k]] = cume_dist;
                }

                i = j;
            }
        }

        Ok(Arc::new(Float64Array::from(result)))
    }

    /// Compute NTH_VALUE(expr, n) - returns the nth value in the window.
    fn compute_nth_value(
        args: &[Arc<dyn crate::planner::PhysicalExpr>],
        batch: &RecordBatch,
        partitions: &[Vec<usize>],
        sorted_indices: Option<&Vec<Vec<usize>>>,
        num_rows: usize,
    ) -> Result<ArrayRef> {
        if args.len() < 2 {
            return Err(BlazeError::analysis("NTH_VALUE requires two arguments"));
        }

        let value_array = args[0].evaluate(batch)?;

        // Get the n value (1-indexed position)
        let n_array = args[1].evaluate(batch)?;
        let n = if let Some(int_arr) = n_array.as_any().downcast_ref::<Int64Array>() {
            if !int_arr.is_empty() && !int_arr.is_null(0) {
                int_arr.value(0) as usize
            } else {
                1
            }
        } else {
            1
        };

        if n == 0 {
            return Err(BlazeError::analysis("NTH_VALUE position must be >= 1"));
        }

        let mut result_values: Vec<Option<i64>> = vec![None; num_rows];

        for (part_idx, partition_rows) in partitions.iter().enumerate() {
            let indices = sorted_indices
                .as_ref()
                .map(|si| si[part_idx].clone())
                .unwrap_or_else(|| partition_rows.clone());

            if indices.len() < n {
                // Not enough rows in partition
                continue;
            }

            // Get the nth value (1-indexed, so n-1 for 0-indexed)
            let nth_idx = indices[n - 1];
            let nth_value = if !value_array.is_null(nth_idx) {
                value_array
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .map(|arr| Some(arr.value(nth_idx)))
                    .unwrap_or(None)
            } else {
                None
            };

            // Apply to all rows in partition
            for &idx in &indices {
                result_values[idx] = nth_value;
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
                            partition_rows
                                .iter()
                                .filter(|&&i| !int_arr.is_null(i))
                                .map(|&i| int_arr.value(i) as f64)
                                .sum()
                        } else if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>()
                        {
                            partition_rows
                                .iter()
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
                            let values: Vec<f64> = partition_rows
                                .iter()
                                .filter(|&&i| !int_arr.is_null(i))
                                .map(|&i| int_arr.value(i) as f64)
                                .collect();
                            if values.is_empty() {
                                0.0
                            } else {
                                values.iter().sum::<f64>() / values.len() as f64
                            }
                        } else if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>()
                        {
                            let values: Vec<f64> = partition_rows
                                .iter()
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
                            partition_rows
                                .iter()
                                .filter(|&&i| !int_arr.is_null(i))
                                .map(|&i| int_arr.value(i) as f64)
                                .min_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                })
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
                            partition_rows
                                .iter()
                                .filter(|&&i| !int_arr.is_null(i))
                                .map(|&i| int_arr.value(i) as f64)
                                .max_by(|a, b| {
                                    a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                                })
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
