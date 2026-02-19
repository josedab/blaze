//! Cross join operator - produces cartesian product.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute;
use arrow::datatypes::Schema as ArrowSchema;

use crate::error::{BlazeError, Result};

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
                total_rows,
                Self::MAX_CROSS_JOIN_ROWS
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

