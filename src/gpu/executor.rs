//! GPU Query Executor
//!
//! This module provides GPU-accelerated query execution, including
//! execution planning and operator implementations.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};
use super::{
    GpuConfig,
    memory::{GpuMemoryPool, MemoryTransfer},
    kernels::{KernelRegistry, AggregationType},
};

/// GPU query executor.
pub struct GpuExecutor {
    /// Configuration
    config: GpuConfig,
    /// Kernel registry
    kernel_registry: KernelRegistry,
    /// Memory transfer handler
    transfer: MemoryTransfer,
}

impl GpuExecutor {
    /// Create a new GPU executor.
    pub fn new(config: GpuConfig) -> Self {
        Self {
            transfer: MemoryTransfer::new(config.async_transfers),
            config,
            kernel_registry: KernelRegistry::new(),
        }
    }

    /// Execute a GPU execution plan.
    pub fn execute(
        &self,
        plan: &GpuExecutionPlan,
        input: &[RecordBatch],
        pool: &GpuMemoryPool,
    ) -> Result<Vec<RecordBatch>> {
        if !self.config.enabled {
            return Err(BlazeError::execution("GPU execution is disabled"));
        }

        if input.is_empty() {
            return Ok(vec![]);
        }

        // Check if input is large enough for GPU execution
        let total_size: usize = input.iter().map(|b| b.get_array_memory_size()).sum();
        if total_size < self.config.min_gpu_batch_size {
            return Err(BlazeError::execution(
                "Input too small for GPU execution",
            ));
        }

        // Execute operators in the plan
        let mut current_batches = input.to_vec();

        for operator in &plan.operators {
            current_batches = self.execute_operator(operator, &current_batches, pool)?;
        }

        Ok(current_batches)
    }

    /// Execute a single GPU operator.
    fn execute_operator(
        &self,
        operator: &GpuOperator,
        input: &[RecordBatch],
        _pool: &GpuMemoryPool,
    ) -> Result<Vec<RecordBatch>> {
        // Get the kernel for this operator
        let kernel_name = operator.kernel_name();
        let _kernel = self.kernel_registry.get(&kernel_name).ok_or_else(|| {
            BlazeError::execution(format!("Kernel not found: {}", kernel_name))
        })?;

        // For now, simulate GPU execution by passing through
        // In a real implementation, this would:
        // 1. Transfer data to GPU
        // 2. Execute kernel
        // 3. Transfer results back

        // Simulate operation based on type
        match operator {
            GpuOperator::Filter { predicate } => {
                // Simulate filter - in real impl would use GPU kernel
                self.simulate_filter(input, predicate)
            }
            GpuOperator::Project { columns } => {
                self.simulate_project(input, columns)
            }
            GpuOperator::Aggregate { group_by, aggregates } => {
                self.simulate_aggregate(input, group_by, aggregates)
            }
            GpuOperator::Sort { sort_keys } => {
                self.simulate_sort(input, sort_keys)
            }
            GpuOperator::Join { join_type: _, left_keys: _, right_keys: _ } => {
                // Join requires two inputs - for now just pass through
                Ok(input.to_vec())
            }
            GpuOperator::Limit { limit } => {
                self.simulate_limit(input, *limit)
            }
        }
    }

    /// Simulate filter operation.
    fn simulate_filter(
        &self,
        input: &[RecordBatch],
        _predicate: &GpuPredicate,
    ) -> Result<Vec<RecordBatch>> {
        // In real implementation, would execute filter on GPU
        // For now, pass through all data
        Ok(input.to_vec())
    }

    /// Simulate project operation.
    fn simulate_project(
        &self,
        input: &[RecordBatch],
        columns: &[usize],
    ) -> Result<Vec<RecordBatch>> {
        if columns.is_empty() {
            return Ok(input.to_vec());
        }

        let mut result = Vec::with_capacity(input.len());
        for batch in input {
            let projected_columns: Vec<Arc<dyn arrow::array::Array>> = columns
                .iter()
                .filter_map(|&idx| {
                    if idx < batch.num_columns() {
                        Some(batch.column(idx).clone())
                    } else {
                        None
                    }
                })
                .collect();

            if projected_columns.is_empty() {
                continue;
            }

            // Build projected schema
            let projected_fields: Vec<arrow::datatypes::FieldRef> = columns
                .iter()
                .filter_map(|&idx| {
                    if idx < batch.schema().fields().len() {
                        Some(batch.schema().field(idx).clone().into())
                    } else {
                        None
                    }
                })
                .collect();

            let schema = Arc::new(arrow::datatypes::Schema::new(projected_fields));
            let projected_batch = RecordBatch::try_new(schema, projected_columns)
                .map_err(|e| BlazeError::execution(format!("Projection failed: {}", e)))?;

            result.push(projected_batch);
        }

        Ok(result)
    }

    /// Simulate aggregate operation.
    fn simulate_aggregate(
        &self,
        input: &[RecordBatch],
        _group_by: &[usize],
        _aggregates: &[(usize, AggregationType)],
    ) -> Result<Vec<RecordBatch>> {
        // In real implementation, would execute aggregation on GPU
        // For now, pass through
        Ok(input.to_vec())
    }

    /// Simulate sort operation.
    fn simulate_sort(
        &self,
        input: &[RecordBatch],
        _sort_keys: &[(usize, SortDirection)],
    ) -> Result<Vec<RecordBatch>> {
        // In real implementation, would execute sort on GPU
        // For now, pass through
        Ok(input.to_vec())
    }

    /// Simulate limit operation.
    fn simulate_limit(
        &self,
        input: &[RecordBatch],
        limit: usize,
    ) -> Result<Vec<RecordBatch>> {
        let mut remaining = limit;
        let mut result = Vec::new();

        for batch in input {
            if remaining == 0 {
                break;
            }

            if batch.num_rows() <= remaining {
                result.push(batch.clone());
                remaining -= batch.num_rows();
            } else {
                // Slice the batch
                let sliced = batch.slice(0, remaining);
                result.push(sliced);
                remaining = 0;
            }
        }

        Ok(result)
    }

    /// Check if an operator can be executed on GPU.
    pub fn can_execute(&self, operator: &GpuOperator) -> bool {
        let kernel_name = operator.kernel_name();
        self.kernel_registry.contains(&kernel_name)
    }

    /// Get execution statistics.
    pub fn stats(&self) -> ExecutorStats {
        ExecutorStats {
            transfers: self.transfer.stats().clone(),
            kernels_executed: 0,
            total_gpu_time_us: 0,
        }
    }
}

/// GPU execution plan.
#[derive(Debug, Clone)]
pub struct GpuExecutionPlan {
    /// Operators to execute
    pub operators: Vec<GpuOperator>,
    /// Estimated output rows
    pub estimated_rows: usize,
    /// Estimated output bytes
    pub estimated_bytes: usize,
}

impl GpuExecutionPlan {
    /// Create a new execution plan.
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
            estimated_rows: 0,
            estimated_bytes: 0,
        }
    }

    /// Add an operator to the plan.
    pub fn add_operator(&mut self, operator: GpuOperator) {
        self.operators.push(operator);
    }

    /// Set estimated output size.
    pub fn with_estimates(mut self, rows: usize, bytes: usize) -> Self {
        self.estimated_rows = rows;
        self.estimated_bytes = bytes;
        self
    }

    /// Check if the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }

    /// Get the number of operators.
    pub fn len(&self) -> usize {
        self.operators.len()
    }
}

impl Default for GpuExecutionPlan {
    fn default() -> Self {
        Self::new()
    }
}

/// GPU operator in an execution plan.
#[derive(Debug, Clone)]
pub enum GpuOperator {
    /// Filter rows based on predicate
    Filter {
        /// Filter predicate
        predicate: GpuPredicate,
    },
    /// Project columns
    Project {
        /// Column indices to project
        columns: Vec<usize>,
    },
    /// Aggregate with optional grouping
    Aggregate {
        /// Group by column indices
        group_by: Vec<usize>,
        /// Aggregate expressions (column index, aggregation type)
        aggregates: Vec<(usize, AggregationType)>,
    },
    /// Sort by columns
    Sort {
        /// Sort keys (column index, direction)
        sort_keys: Vec<(usize, SortDirection)>,
    },
    /// Join two inputs
    Join {
        /// Join type
        join_type: GpuJoinType,
        /// Left key column indices
        left_keys: Vec<usize>,
        /// Right key column indices
        right_keys: Vec<usize>,
    },
    /// Limit rows
    Limit {
        /// Maximum rows to return
        limit: usize,
    },
}

impl GpuOperator {
    /// Get the kernel name for this operator.
    pub fn kernel_name(&self) -> String {
        match self {
            GpuOperator::Filter { .. } => "filter".to_string(),
            GpuOperator::Project { .. } => "project".to_string(),
            GpuOperator::Aggregate { .. } => "aggregate".to_string(),
            GpuOperator::Sort { .. } => "sort".to_string(),
            GpuOperator::Join { .. } => "join".to_string(),
            GpuOperator::Limit { .. } => "project".to_string(), // Reuse project kernel
        }
    }

    /// Create a filter operator.
    pub fn filter(predicate: GpuPredicate) -> Self {
        GpuOperator::Filter { predicate }
    }

    /// Create a project operator.
    pub fn project(columns: Vec<usize>) -> Self {
        GpuOperator::Project { columns }
    }

    /// Create an aggregate operator.
    pub fn aggregate(
        group_by: Vec<usize>,
        aggregates: Vec<(usize, AggregationType)>,
    ) -> Self {
        GpuOperator::Aggregate { group_by, aggregates }
    }

    /// Create a sort operator.
    pub fn sort(sort_keys: Vec<(usize, SortDirection)>) -> Self {
        GpuOperator::Sort { sort_keys }
    }

    /// Create a join operator.
    pub fn join(
        join_type: GpuJoinType,
        left_keys: Vec<usize>,
        right_keys: Vec<usize>,
    ) -> Self {
        GpuOperator::Join { join_type, left_keys, right_keys }
    }

    /// Create a limit operator.
    pub fn limit(limit: usize) -> Self {
        GpuOperator::Limit { limit }
    }
}

/// GPU predicate for filtering.
#[derive(Debug, Clone)]
pub enum GpuPredicate {
    /// Column equals value
    Eq { column: usize, value: ScalarValue },
    /// Column not equals value
    Ne { column: usize, value: ScalarValue },
    /// Column less than value
    Lt { column: usize, value: ScalarValue },
    /// Column less than or equal value
    Le { column: usize, value: ScalarValue },
    /// Column greater than value
    Gt { column: usize, value: ScalarValue },
    /// Column greater than or equal value
    Ge { column: usize, value: ScalarValue },
    /// Column is null
    IsNull { column: usize },
    /// Column is not null
    IsNotNull { column: usize },
    /// AND of predicates
    And { left: Box<GpuPredicate>, right: Box<GpuPredicate> },
    /// OR of predicates
    Or { left: Box<GpuPredicate>, right: Box<GpuPredicate> },
    /// NOT of predicate
    Not { predicate: Box<GpuPredicate> },
}

impl GpuPredicate {
    /// Create an equality predicate.
    pub fn eq(column: usize, value: ScalarValue) -> Self {
        GpuPredicate::Eq { column, value }
    }

    /// Create a greater than predicate.
    pub fn gt(column: usize, value: ScalarValue) -> Self {
        GpuPredicate::Gt { column, value }
    }

    /// Create a less than predicate.
    pub fn lt(column: usize, value: ScalarValue) -> Self {
        GpuPredicate::Lt { column, value }
    }

    /// Create an AND predicate.
    pub fn and(left: GpuPredicate, right: GpuPredicate) -> Self {
        GpuPredicate::And {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create an OR predicate.
    pub fn or(left: GpuPredicate, right: GpuPredicate) -> Self {
        GpuPredicate::Or {
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    /// Create a NOT predicate.
    pub fn not(predicate: GpuPredicate) -> Self {
        GpuPredicate::Not {
            predicate: Box::new(predicate),
        }
    }
}

/// Scalar value for predicates.
#[derive(Debug, Clone)]
pub enum ScalarValue {
    /// Null value
    Null,
    /// Boolean
    Boolean(bool),
    /// 64-bit integer
    Int64(i64),
    /// 64-bit float
    Float64(f64),
    /// String
    Utf8(String),
}

impl ScalarValue {
    /// Create an integer value.
    pub fn int64(v: i64) -> Self {
        ScalarValue::Int64(v)
    }

    /// Create a float value.
    pub fn float64(v: f64) -> Self {
        ScalarValue::Float64(v)
    }

    /// Create a string value.
    pub fn utf8(s: impl Into<String>) -> Self {
        ScalarValue::Utf8(s.into())
    }
}

/// Sort direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortDirection {
    /// Ascending order
    Ascending,
    /// Descending order
    Descending,
}

/// GPU join type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GpuJoinType {
    /// Inner join
    Inner,
    /// Left outer join
    Left,
    /// Right outer join
    Right,
    /// Full outer join
    Full,
    /// Semi join
    Semi,
    /// Anti join
    Anti,
}

/// Executor statistics.
#[derive(Debug, Clone, Default)]
pub struct ExecutorStats {
    /// Transfer statistics
    pub transfers: super::memory::TransferStats,
    /// Number of kernels executed
    pub kernels_executed: u64,
    /// Total GPU time in microseconds
    pub total_gpu_time_us: u64,
}

/// Builder for GPU execution plans.
pub struct GpuPlanBuilder {
    /// Plan being built
    plan: GpuExecutionPlan,
}

impl GpuPlanBuilder {
    /// Create a new plan builder.
    pub fn new() -> Self {
        Self {
            plan: GpuExecutionPlan::new(),
        }
    }

    /// Add a filter operator.
    pub fn filter(mut self, predicate: GpuPredicate) -> Self {
        self.plan.add_operator(GpuOperator::filter(predicate));
        self
    }

    /// Add a project operator.
    pub fn project(mut self, columns: Vec<usize>) -> Self {
        self.plan.add_operator(GpuOperator::project(columns));
        self
    }

    /// Add an aggregate operator.
    pub fn aggregate(
        mut self,
        group_by: Vec<usize>,
        aggregates: Vec<(usize, AggregationType)>,
    ) -> Self {
        self.plan.add_operator(GpuOperator::aggregate(group_by, aggregates));
        self
    }

    /// Add a sort operator.
    pub fn sort(mut self, sort_keys: Vec<(usize, SortDirection)>) -> Self {
        self.plan.add_operator(GpuOperator::sort(sort_keys));
        self
    }

    /// Add a limit operator.
    pub fn limit(mut self, limit: usize) -> Self {
        self.plan.add_operator(GpuOperator::limit(limit));
        self
    }

    /// Build the execution plan.
    pub fn build(self) -> GpuExecutionPlan {
        self.plan
    }
}

impl Default for GpuPlanBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(rows: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Int64, false),
        ]);

        let a: Vec<i64> = (0..rows as i64).collect();
        let b: Vec<i64> = (0..rows as i64).map(|x| x * 2).collect();
        let c: Vec<i64> = (0..rows as i64).map(|x| x * 3).collect();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(a)),
                Arc::new(Int64Array::from(b)),
                Arc::new(Int64Array::from(c)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_gpu_executor_creation() {
        let config = GpuConfig::default();
        let executor = GpuExecutor::new(config);

        assert!(executor.can_execute(&GpuOperator::filter(
            GpuPredicate::eq(0, ScalarValue::int64(42))
        )));
    }

    #[test]
    fn test_gpu_execution_plan() {
        let plan = GpuPlanBuilder::new()
            .filter(GpuPredicate::gt(0, ScalarValue::int64(10)))
            .project(vec![0, 1])
            .limit(100)
            .build();

        assert_eq!(plan.len(), 3);
        assert!(!plan.is_empty());
    }

    #[test]
    fn test_gpu_predicate() {
        let pred1 = GpuPredicate::eq(0, ScalarValue::int64(42));
        let pred2 = GpuPredicate::gt(1, ScalarValue::float64(3.14));
        let combined = GpuPredicate::and(pred1, pred2);

        match combined {
            GpuPredicate::And { .. } => {}
            _ => panic!("Expected AND predicate"),
        }
    }

    #[test]
    fn test_gpu_operator_kernel_name() {
        assert_eq!(
            GpuOperator::filter(GpuPredicate::eq(0, ScalarValue::int64(1)))
                .kernel_name(),
            "filter"
        );
        assert_eq!(GpuOperator::project(vec![0]).kernel_name(), "project");
        assert_eq!(
            GpuOperator::aggregate(vec![0], vec![(1, AggregationType::Sum)])
                .kernel_name(),
            "aggregate"
        );
    }

    #[test]
    fn test_simulate_project() {
        let config = GpuConfig::default();
        let executor = GpuExecutor::new(config);

        let batch = create_test_batch(100);
        let result = executor.simulate_project(&[batch], &[0, 2]).unwrap();

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].num_columns(), 2);
        assert_eq!(result[0].num_rows(), 100);
    }

    #[test]
    fn test_simulate_limit() {
        let config = GpuConfig::default();
        let executor = GpuExecutor::new(config);

        let batch = create_test_batch(100);
        let result = executor.simulate_limit(&[batch], 50).unwrap();

        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 50);
    }

    #[test]
    fn test_scalar_value() {
        let int_val = ScalarValue::int64(42);
        let float_val = ScalarValue::float64(3.14);
        let str_val = ScalarValue::utf8("hello");

        match int_val {
            ScalarValue::Int64(v) => assert_eq!(v, 42),
            _ => panic!("Expected Int64"),
        }

        match float_val {
            ScalarValue::Float64(v) => assert!((v - 3.14).abs() < 0.001),
            _ => panic!("Expected Float64"),
        }

        match str_val {
            ScalarValue::Utf8(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected Utf8"),
        }
    }

    #[test]
    fn test_sort_direction() {
        let sort_keys = vec![
            (0, SortDirection::Ascending),
            (1, SortDirection::Descending),
        ];

        assert_eq!(sort_keys[0].1, SortDirection::Ascending);
        assert_eq!(sort_keys[1].1, SortDirection::Descending);
    }

    #[test]
    fn test_gpu_join_type() {
        let join = GpuOperator::join(
            GpuJoinType::Inner,
            vec![0],
            vec![0],
        );

        match join {
            GpuOperator::Join { join_type, .. } => {
                assert_eq!(join_type, GpuJoinType::Inner);
            }
            _ => panic!("Expected Join operator"),
        }
    }

    #[test]
    fn test_plan_builder() {
        let plan = GpuPlanBuilder::new()
            .filter(GpuPredicate::gt(0, ScalarValue::int64(0)))
            .project(vec![0, 1])
            .aggregate(vec![0], vec![(1, AggregationType::Sum)])
            .sort(vec![(0, SortDirection::Ascending)])
            .limit(10)
            .build();

        assert_eq!(plan.operators.len(), 5);
    }
}
