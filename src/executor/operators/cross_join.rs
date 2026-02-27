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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_batch(name: &str, values: Vec<i64>) -> (Arc<ArrowSchema>, RecordBatch) {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(values))],
        )
        .unwrap();
        (schema, batch)
    }

    #[test]
    fn test_cross_join_basic() {
        let (_, left) = make_batch("a", vec![1, 2]);
        let (_, right) = make_batch("b", vec![10, 20]);
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let result = CrossJoinOperator::execute(vec![left], vec![right], &output_schema).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4); // 2 × 2
    }

    #[test]
    fn test_cross_join_empty_left() {
        let (schema_a, _) = make_batch("a", vec![1]);
        let left_empty = RecordBatch::new_empty(schema_a);
        let (_, right) = make_batch("b", vec![10, 20]);
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let result =
            CrossJoinOperator::execute(vec![left_empty], vec![right], &output_schema).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_cross_join_exceeds_limit() {
        // Create batches that would produce > MAX_CROSS_JOIN_ROWS
        let large_values: Vec<i64> = (0..3163).collect();
        let (_, left) = make_batch("a", large_values.clone());
        let (_, right) = make_batch("b", large_values);
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));

        // 3163 × 3163 = ~10M, which exceeds MAX_CROSS_JOIN_ROWS
        let result = CrossJoinOperator::execute(vec![left], vec![right], &output_schema);
        assert!(result.is_err());
    }
}

