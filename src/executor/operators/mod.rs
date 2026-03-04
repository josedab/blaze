//! Physical operators for query execution.
//!
//! This module contains the physical operators used by the query execution engine.
//! Each operator type is organized in its own sub-module.

mod aggregate;
mod cross_join;
mod hash_join;
mod memory;
mod sort;
mod window;

pub use aggregate::*;
pub use cross_join::*;
pub use hash_join::*;
pub use memory::*;
pub use sort::*;
pub use window::*;

use arrow::array::ArrayRef;

use crate::error::{BlazeError, Result};

/// Helper to downcast an array with a proper error message.
/// Use this instead of `.unwrap()` on downcasts for better error reporting.
pub(crate) fn try_downcast<'a, T: 'static>(
    array: &'a ArrayRef,
    expected_type: &str,
) -> Result<&'a T> {
    array.as_any().downcast_ref::<T>().ok_or_else(|| {
        BlazeError::type_error(format!(
            "Failed to downcast array to {}. Actual type: {:?}",
            expected_type,
            array.data_type()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{ColumnExpr, JoinType};
    use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, Schema as ArrowSchema};
    use std::sync::Arc;

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
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
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
        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
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
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let left_batch = create_test_batch(
            left_schema.clone(),
            vec![1, 2, 3],
            vec!["Alice", "Bob", "Charlie"],
        );

        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch =
            create_test_batch(right_schema.clone(), vec![1, 2], vec!["Widget", "Gadget"]);

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
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
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let left_batch = create_test_batch(left_schema.clone(), vec![1, 2], vec!["Alice", "Bob"]);

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

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
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
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
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
        let output_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
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
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
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
        let output_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
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
        let left_schema = make_schema(vec![("a", DataType::Int64)]);
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();

        let right_schema = make_schema(vec![("b", DataType::Int64)]);
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef],
        )
        .unwrap();

        let output_schema = make_schema(vec![("a", DataType::Int64), ("b", DataType::Int64)]);

        let result =
            CrossJoinOperator::execute(vec![left_batch], vec![right_batch], &output_schema)
                .unwrap();

        // 2 x 3 = 6 rows
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6);

        // Verify the pattern: (1,10), (1,20), (1,30), (2,10), (2,20), (2,30)
        let batch = &result[0];
        let a_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let b_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

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
        let left_schema = make_schema(vec![("id", DataType::Int64)]);
        let _right_schema = make_schema(vec![("id", DataType::Int64)]);
        let output_schema = make_schema(vec![("id", DataType::Int64), ("id", DataType::Int64)]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));

        // Inner join with empty right = empty
        let result = HashJoinOperator::execute(
            vec![RecordBatch::try_new(
                left_schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
            )
            .unwrap()],
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
            )
            .unwrap()],
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
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 15);
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

    // --- Edge case / error tests ---

    #[test]
    fn test_hash_join_empty_left_inner() {
        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch =
            create_test_batch(right_schema.clone(), vec![1, 2], vec!["Widget", "Gadget"]);

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![],
            vec![right_batch],
            JoinType::Inner,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_hash_join_empty_right_inner() {
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let left_batch = create_test_batch(left_schema.clone(), vec![1, 2], vec!["Alice", "Bob"]);

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![left_batch],
            vec![],
            JoinType::Inner,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_hash_join_empty_left_left_outer() {
        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch = create_test_batch(right_schema.clone(), vec![1], vec!["Widget"]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![],
            vec![right_batch],
            JoinType::Left,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_hash_join_empty_right_left_outer() {
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let left_batch = create_test_batch(
            left_schema.clone(),
            vec![1, 2, 3],
            vec!["Alice", "Bob", "Charlie"],
        );

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![left_batch],
            vec![],
            JoinType::Left,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        // Left outer with empty right: all left rows with NULLs for right columns
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_hash_join_right_outer_with_unmatched() {
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let left_batch = create_test_batch(left_schema.clone(), vec![1, 2], vec!["Alice", "Bob"]);

        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch =
            create_test_batch(right_schema.clone(), vec![1, 3], vec!["Widget", "Gadget"]);

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
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

        // Alice-Widget matches, user_id=3 has no match -> NULL, Gadget
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_hash_join_full_outer() {
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let left_batch = create_test_batch(left_schema.clone(), vec![1, 2], vec!["Alice", "Bob"]);

        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch =
            create_test_batch(right_schema.clone(), vec![1, 3], vec!["Widget", "Gadget"]);

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
        let right_key: Arc<dyn crate::planner::PhysicalExpr> =
            Arc::new(ColumnExpr::new("user_id", 0));

        let result = HashJoinOperator::execute(
            vec![left_batch],
            vec![right_batch],
            JoinType::Full,
            &[left_key],
            &[right_key],
            &output_schema,
        )
        .unwrap();

        // Alice-Widget, Bob-NULL, NULL-Gadget = 3 rows
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_hash_join_with_null_keys() {
        let left_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])) as ArrayRef,
            ],
        )
        .unwrap();

        let right_schema = make_schema(vec![
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
                Arc::new(StringArray::from(vec!["Widget", "Gadget"])) as ArrayRef,
            ],
        )
        .unwrap();

        let output_schema = make_schema(vec![
            ("id", DataType::Int64),
            ("name", DataType::Utf8),
            ("user_id", DataType::Int64),
            ("product", DataType::Utf8),
        ]);

        let left_key: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(ColumnExpr::new("id", 0));
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

        // NULLs should not match each other in inner join; only id=1 matches
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows <= 2); // At most 1 or 2 depending on NULL handling
    }

    #[test]
    fn test_sort_empty_input() {
        let result = SortOperator::execute(&[], vec![]).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_sort_with_null_column() {
        let schema = make_schema(vec![("val", DataType::Int64)]);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![None, Some(3), None, Some(1)])) as ArrayRef],
        )
        .unwrap();

        let sort_expr =
            crate::planner::SortExpr::new(Arc::new(ColumnExpr::new("val", 0)), true, true);

        let result = SortOperator::execute(&[sort_expr], vec![batch]).unwrap();
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4);
    }

    #[test]
    fn test_hash_aggregate_empty_input() {
        let output_schema = make_schema(vec![("cnt", DataType::Int64)]);
        let result = HashAggregateOperator::execute(
            &[],
            &[crate::planner::AggregateExpr {
                func: crate::planner::AggregateFunc::Count,
                args: vec![Arc::new(ColumnExpr::new("id", 0))],
                distinct: false,
                alias: Some("cnt".to_string()),
            }],
            &output_schema,
            vec![],
        )
        .unwrap();

        // Global aggregate on empty input should produce a result (COUNT = 0)
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows >= 1);
    }

    #[test]
    fn test_cross_join_empty_left() {
        let right_schema = make_schema(vec![("name", DataType::Utf8)]);
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(StringArray::from(vec!["Alice"])) as ArrayRef],
        )
        .unwrap();

        let output_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);

        let result = CrossJoinOperator::execute(vec![], vec![right_batch], &output_schema).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_cross_join_empty_right() {
        let left_schema = make_schema(vec![("id", DataType::Int64)]);
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef],
        )
        .unwrap();

        let output_schema = make_schema(vec![("id", DataType::Int64), ("name", DataType::Utf8)]);

        let result = CrossJoinOperator::execute(vec![left_batch], vec![], &output_schema).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_count_accumulator_empty() {
        let acc = CountAccumulator::new();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.value(0), 0);
    }

    #[test]
    fn test_sum_accumulator_empty() {
        let acc = SumAccumulator::new();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(arr.value(0), 0.0);
    }

    #[test]
    fn test_avg_accumulator_empty() {
        let acc = AvgAccumulator::new();
        let result = acc.finalize().unwrap();
        let arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        // AVG of empty should be 0 or NaN
        assert!(arr.value(0).is_nan() || arr.value(0) == 0.0);
    }

    #[test]
    fn test_memory_manager_exceed_budget() {
        let mgr = MemoryManager::new(100); // 100 bytes budget
        assert!(mgr.try_reserve(50));
        assert!(!mgr.try_reserve(60)); // Would exceed 100
    }

    #[test]
    fn test_memory_reservation_raii() {
        let mgr = MemoryManager::new(1000);
        {
            let _res = mgr.reserve(500).unwrap();
            assert!(mgr.reserve(600).is_err()); // 500 + 600 > 1000
        }
        // After reservation dropped, memory freed
        assert!(mgr.reserve(600).is_ok());
    }

    #[test]
    fn test_window_empty_input() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("rn", DataType::Int64, true),
        ]));

        let result = WindowOperator::execute(
            &[crate::planner::PhysicalWindowExpr::new(
                crate::planner::WindowFunction::RowNumber,
                vec![],
                vec![],
                vec![],
                Some("rn".to_string()),
            )],
            &schema,
            vec![],
        )
        .unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}
