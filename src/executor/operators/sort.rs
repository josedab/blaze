//! Sort operator using Arrow's sort kernels.

use arrow::array::{ArrayRef, RecordBatch};

use crate::error::Result;

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
        let mut sort_columns: Vec<arrow::compute::SortColumn> =
            Vec::with_capacity(sort_exprs.len());

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
            .map(|col| Ok(take(col.as_ref(), &indices, None)?))
            .collect();

        Ok(vec![RecordBatch::try_new(schema, sorted_columns?)?])
    }
}

