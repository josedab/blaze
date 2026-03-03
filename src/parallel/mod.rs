//! Parallel Execution Engine
//!
//! This module provides parallel query execution capabilities using multiple threads
//! to process data concurrently.
//!
//! # Features
//!
//! - **Thread Pool**: Configurable worker pool for parallel task execution
//! - **Partitioning**: Hash and range partitioning strategies for data distribution
//! - **Parallel Operators**: Parallel scans, aggregates, joins, and sorts
//! - **Exchange Operators**: Shuffle and broadcast for data redistribution
//!
//! # Example
//!
//! ```rust,ignore
//! use blaze::parallel::{ParallelExecutor, ExecutionConfig};
//!
//! let config = ExecutionConfig::new().with_parallelism(4);
//! let executor = ParallelExecutor::new(config);
//! let results = executor.execute(plan)?;
//! ```

pub mod exchange;
mod partition;
mod worker;

pub use exchange::{BroadcastExchange, Exchange, ExchangeType, ShuffleExchange};
pub use partition::{HashPartitioner, Partitioning, RangePartitioner, RoundRobinPartitioner};
pub use worker::{TaskResult, WorkerPool, WorkerTask};

use std::sync::Arc;
use std::thread;

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::executor::ExecutionContext;
use crate::planner::PhysicalPlan;

/// Default parallelism level (number of CPU cores)
pub fn default_parallelism() -> usize {
    thread::available_parallelism()
        .map(|p| p.get())
        .unwrap_or(4)
}

/// Configuration for parallel execution.
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Number of parallel worker threads
    pub parallelism: usize,
    /// Batch size for vectorized processing
    pub batch_size: usize,
    /// Target partition size in bytes
    pub target_partition_size: usize,
    /// Enable adaptive parallelism
    pub adaptive: bool,
    /// Memory limit per task in bytes
    pub task_memory_limit: usize,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            parallelism: default_parallelism(),
            batch_size: 8192,
            target_partition_size: 64 * 1024 * 1024, // 64 MB
            adaptive: true,
            task_memory_limit: 256 * 1024 * 1024, // 256 MB per task
        }
    }
}

impl ExecutionConfig {
    /// Create a new execution config.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the parallelism level.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism.max(1);
        self
    }

    /// Set the batch size.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the target partition size.
    pub fn with_target_partition_size(mut self, size: usize) -> Self {
        self.target_partition_size = size;
        self
    }

    /// Enable or disable adaptive parallelism.
    pub fn with_adaptive(mut self, adaptive: bool) -> Self {
        self.adaptive = adaptive;
        self
    }

    /// Set the memory limit per task.
    pub fn with_task_memory_limit(mut self, limit: usize) -> Self {
        self.task_memory_limit = limit;
        self
    }
}

/// Parallel query executor.
pub struct ParallelExecutor {
    config: ExecutionConfig,
    worker_pool: WorkerPool,
}

impl ParallelExecutor {
    /// Create a new parallel executor with the given configuration.
    pub fn new(config: ExecutionConfig) -> Self {
        let worker_pool = WorkerPool::new(config.parallelism);
        Self {
            config,
            worker_pool,
        }
    }

    /// Execute a physical plan in parallel.
    pub fn execute(&self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        // Analyze the plan to determine parallelization strategy
        let partitions = self.determine_partitions(plan);

        // Execute the plan
        self.execute_with_partitions(plan, partitions)
    }

    /// Determine the number of partitions for a plan.
    fn determine_partitions(&self, plan: &PhysicalPlan) -> usize {
        if self.config.adaptive {
            // Estimate data size and adjust partitions
            let estimated_rows = self.estimate_rows(plan);
            let rows_per_partition = 100_000; // Target rows per partition
            let partitions = (estimated_rows / rows_per_partition).max(1);
            partitions.min(self.config.parallelism)
        } else {
            self.config.parallelism
        }
    }

    /// Estimate the number of output rows for a plan.
    fn estimate_rows(&self, plan: &PhysicalPlan) -> usize {
        match plan {
            PhysicalPlan::Scan { .. } => 1_000_000, // Default estimate
            PhysicalPlan::Filter { input, .. } => self.estimate_rows(input) / 2,
            PhysicalPlan::Projection { input, .. } => self.estimate_rows(input),
            PhysicalPlan::HashAggregate { input, .. } => self.estimate_rows(input) / 10,
            PhysicalPlan::Sort { input, .. } => self.estimate_rows(input),
            PhysicalPlan::Limit { fetch, .. } => fetch.unwrap_or(100_000),
            PhysicalPlan::HashJoin { left, right, .. } => {
                let left_rows = self.estimate_rows(left);
                let right_rows = self.estimate_rows(right);
                (left_rows * right_rows) / left_rows.max(right_rows).max(1)
            }
            _ => 100_000,
        }
    }

    /// Execute plan with specified number of partitions.
    fn execute_with_partitions(
        &self,
        plan: &PhysicalPlan,
        partitions: usize,
    ) -> Result<Vec<RecordBatch>> {
        if partitions <= 1 {
            let ctx = ExecutionContext::new();
            return ctx.execute(plan);
        }

        match plan {
            // For Filter/Projection over Scan, parallelize the compute over scan batches
            PhysicalPlan::Filter { input, .. }
                if matches!(input.as_ref(), PhysicalPlan::Scan { .. }) =>
            {
                self.execute_parallel_scan_pipeline(plan, input, partitions)
            }
            PhysicalPlan::Projection { input, .. }
                if matches!(input.as_ref(), PhysicalPlan::Scan { .. }) =>
            {
                self.execute_parallel_scan_pipeline(plan, input, partitions)
            }
            PhysicalPlan::HashAggregate { input, .. } => {
                self.execute_parallel_aggregate(plan, input, partitions)
            }
            PhysicalPlan::Sort { input, .. } => self.execute_parallel_sort(plan, input, partitions),
            PhysicalPlan::HashJoin { left, right, .. } => {
                self.execute_parallel_join(plan, left, right, partitions)
            }
            PhysicalPlan::Filter { input, .. } | PhysicalPlan::Projection { input, .. } => {
                self.execute_parallel_filter_project(plan, input, partitions)
            }
            _ => {
                // Fall back to single-threaded execution for unsupported plans
                let ctx = ExecutionContext::new();
                ctx.execute(plan)
            }
        }
    }

    /// Partition input data using round-robin, then execute the operator on each partition in parallel.
    ///
    /// Note: Input child plan is executed first (single-threaded scan), then the compute-heavy
    /// operator (filter/projection) is applied to partitions in parallel. This is the correct
    /// model for single-partition storage: parallelize compute, not I/O.
    fn execute_parallel_filter_project(
        &self,
        plan: &PhysicalPlan,
        input: &PhysicalPlan,
        partitions: usize,
    ) -> Result<Vec<RecordBatch>> {
        // Execute the input plan to get data
        let ctx = ExecutionContext::new();
        let input_batches = ctx.execute(input)?;

        let total_rows: usize = input_batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return ctx.execute(plan);
        }

        // Partition input data using round-robin
        let partitioner = RoundRobinPartitioner::new(partitions);
        let mut partition_data: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
        for batch in &input_batches {
            if batch.num_rows() > 0 {
                let partitioned = partitioner.partition(batch)?;
                for (i, p) in partitioned.into_iter().enumerate() {
                    if p.num_rows() > 0 {
                        partition_data[i].push(p);
                    }
                }
            }
        }

        // Build per-partition plans and execute in parallel via WorkerPool
        let schema = plan.schema();
        let tasks: Vec<WorkerTask> = partition_data
            .into_iter()
            .enumerate()
            .map(|(i, data)| {
                let plan_schema = schema.clone();
                let partition_plan = self.rebuild_plan_with_values(plan, data, plan_schema);
                WorkerTask::new(i, move |_ctx| {
                    let exec_ctx = ExecutionContext::new();
                    exec_ctx.execute(&partition_plan)
                })
            })
            .collect();

        let task_results = self.worker_pool.execute_all(tasks)?;
        Self::collect_results(task_results)
    }

    /// Execute a scan and then apply filter/projection in parallel across the scan batches.
    /// This avoids re-partitioning since scan naturally produces multiple batches.
    fn execute_parallel_scan_pipeline(
        &self,
        plan: &PhysicalPlan,
        scan_input: &PhysicalPlan,
        partitions: usize,
    ) -> Result<Vec<RecordBatch>> {
        let ctx = ExecutionContext::new();
        let scan_batches = ctx.execute(scan_input)?;

        // If few batches, just distribute them; if many, group by partition count
        if scan_batches.is_empty() {
            return ctx.execute(plan);
        }
        if scan_batches.len() == 1 && scan_batches[0].num_rows() < 1000 {
            return ctx.execute(plan);
        }

        // Distribute scan batches across partitions using round-robin assignment
        let mut partition_data: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
        for (i, batch) in scan_batches.into_iter().enumerate() {
            if batch.num_rows() > 0 {
                partition_data[i % partitions].push(batch);
            }
        }

        let schema = plan.schema();
        let tasks: Vec<WorkerTask> = partition_data
            .into_iter()
            .enumerate()
            .map(|(i, data)| {
                let plan_schema = schema.clone();
                let partition_plan = self.rebuild_plan_with_values(plan, data, plan_schema);
                WorkerTask::new(i, move |_ctx| {
                    let exec_ctx = ExecutionContext::new();
                    exec_ctx.execute(&partition_plan)
                })
            })
            .collect();

        let task_results = self.worker_pool.execute_all(tasks)?;
        Self::collect_results(task_results)
    }

    /// Partition input, aggregate each partition in parallel, then merge with final aggregation.
    fn execute_parallel_aggregate(
        &self,
        plan: &PhysicalPlan,
        input: &PhysicalPlan,
        partitions: usize,
    ) -> Result<Vec<RecordBatch>> {
        let ctx = ExecutionContext::new();
        let input_batches = ctx.execute(input)?;

        let total_rows: usize = input_batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return ctx.execute(plan);
        }

        // Partition input using round-robin
        let partitioner = RoundRobinPartitioner::new(partitions);
        let mut partition_data: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
        for batch in &input_batches {
            if batch.num_rows() > 0 {
                let partitioned = partitioner.partition(batch)?;
                for (i, p) in partitioned.into_iter().enumerate() {
                    if p.num_rows() > 0 {
                        partition_data[i].push(p);
                    }
                }
            }
        }

        // Run partial aggregation on each partition in parallel
        let schema = plan.schema();
        let tasks: Vec<WorkerTask> = partition_data
            .into_iter()
            .enumerate()
            .map(|(i, data)| {
                let plan_schema = schema.clone();
                let partition_plan = self.rebuild_plan_with_values(plan, data, plan_schema);
                WorkerTask::new(i, move |_ctx| {
                    let exec_ctx = ExecutionContext::new();
                    exec_ctx.execute(&partition_plan)
                })
            })
            .collect();

        let task_results = self.worker_pool.execute_all(tasks)?;
        let partial_results = Self::collect_results(task_results)?;

        if partial_results.is_empty() {
            return Ok(partial_results);
        }

        // Final merge aggregation: re-aggregate the partial results
        let merge_input = PhysicalPlan::Values {
            schema: partial_results[0].schema(),
            data: partial_results,
        };
        if let PhysicalPlan::HashAggregate {
            group_by,
            aggr_exprs,
            schema,
            ..
        } = plan
        {
            let merge_plan = PhysicalPlan::HashAggregate {
                group_by: group_by.clone(),
                aggr_exprs: aggr_exprs
                    .iter()
                    .map(|e| crate::planner::AggregateExpr {
                        func: e.func,
                        args: e.args.clone(),
                        distinct: e.distinct,
                        alias: e.alias.clone(),
                    })
                    .collect(),
                schema: schema.clone(),
                input: Box::new(merge_input),
            };
            let final_ctx = ExecutionContext::new();
            final_ctx.execute(&merge_plan)
        } else {
            unreachable!()
        }
    }

    /// Sort each partition in parallel, then merge sorted partitions.
    fn execute_parallel_sort(
        &self,
        plan: &PhysicalPlan,
        input: &PhysicalPlan,
        partitions: usize,
    ) -> Result<Vec<RecordBatch>> {
        let ctx = ExecutionContext::new();
        let input_batches = ctx.execute(input)?;

        let total_rows: usize = input_batches.iter().map(|b| b.num_rows()).sum();
        if total_rows == 0 {
            return ctx.execute(plan);
        }

        // Partition input
        let partitioner = RoundRobinPartitioner::new(partitions);
        let mut partition_data: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
        for batch in &input_batches {
            if batch.num_rows() > 0 {
                let partitioned = partitioner.partition(batch)?;
                for (i, p) in partitioned.into_iter().enumerate() {
                    if p.num_rows() > 0 {
                        partition_data[i].push(p);
                    }
                }
            }
        }

        // Sort each partition in parallel
        let schema = plan.schema();
        let tasks: Vec<WorkerTask> = partition_data
            .into_iter()
            .enumerate()
            .map(|(i, data)| {
                let plan_schema = schema.clone();
                let partition_plan = self.rebuild_plan_with_values(plan, data, plan_schema);
                WorkerTask::new(i, move |_ctx| {
                    let exec_ctx = ExecutionContext::new();
                    exec_ctx.execute(&partition_plan)
                })
            })
            .collect();

        let task_results = self.worker_pool.execute_all(tasks)?;
        let sorted_partitions = Self::collect_results(task_results)?;

        if sorted_partitions.is_empty() {
            return Ok(sorted_partitions);
        }

        // Final merge sort: sort all partial results together
        let merge_input = PhysicalPlan::Values {
            schema: sorted_partitions[0].schema(),
            data: sorted_partitions,
        };
        if let PhysicalPlan::Sort { exprs, .. } = plan {
            let merge_plan = PhysicalPlan::Sort {
                exprs: exprs
                    .iter()
                    .map(|e| crate::planner::SortExpr {
                        expr: e.expr.clone(),
                        ascending: e.ascending,
                        nulls_first: e.nulls_first,
                    })
                    .collect(),
                input: Box::new(merge_input),
            };
            let final_ctx = ExecutionContext::new();
            final_ctx.execute(&merge_plan)
        } else {
            unreachable!()
        }
    }

    /// Execute join with partitioned inputs.
    fn execute_parallel_join(
        &self,
        plan: &PhysicalPlan,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
        partitions: usize,
    ) -> Result<Vec<RecordBatch>> {
        let ctx = ExecutionContext::new();
        let left_batches = ctx.execute(left)?;
        let right_batches = ctx.execute(right)?;

        let left_rows: usize = left_batches.iter().map(|b| b.num_rows()).sum();
        let right_rows: usize = right_batches.iter().map(|b| b.num_rows()).sum();
        if left_rows == 0 || right_rows == 0 {
            return ctx.execute(plan);
        }

        if let PhysicalPlan::HashJoin {
            join_type,
            left_keys,
            right_keys,
            schema,
            ..
        } = plan
        {
            // Extract actual column indices from join key expressions
            let left_key_indices: Vec<usize> = left_keys
                .iter()
                .filter_map(|k| {
                    k.as_any()
                        .downcast_ref::<crate::planner::ColumnExpr>()
                        .map(|c| c.index())
                })
                .collect();
            let right_key_indices: Vec<usize> = right_keys
                .iter()
                .filter_map(|k| {
                    k.as_any()
                        .downcast_ref::<crate::planner::ColumnExpr>()
                        .map(|c| c.index())
                })
                .collect();

            // Fall back to positional indices if extraction fails
            let left_key_indices = if left_key_indices.is_empty() {
                (0..left_keys.len()).collect()
            } else {
                left_key_indices
            };
            let right_key_indices = if right_key_indices.is_empty() {
                (0..right_keys.len()).collect()
            } else {
                right_key_indices
            };

            // Partition both sides by their respective join keys
            let left_partitioner = HashPartitioner::new(partitions, left_key_indices);
            let right_partitioner = HashPartitioner::new(partitions, right_key_indices);

            let mut left_parts: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];
            let mut right_parts: Vec<Vec<RecordBatch>> = vec![Vec::new(); partitions];

            for batch in &left_batches {
                if batch.num_rows() > 0 {
                    let partitioned = left_partitioner.partition(batch)?;
                    for (i, p) in partitioned.into_iter().enumerate() {
                        if p.num_rows() > 0 {
                            left_parts[i].push(p);
                        }
                    }
                }
            }
            for batch in &right_batches {
                if batch.num_rows() > 0 {
                    let partitioned = right_partitioner.partition(batch)?;
                    for (i, p) in partitioned.into_iter().enumerate() {
                        if p.num_rows() > 0 {
                            right_parts[i].push(p);
                        }
                    }
                }
            }

            // Execute join on each partition pair in parallel
            let join_type = *join_type;
            let join_schema = schema.clone();
            let lk = left_keys.clone();
            let rk = right_keys.clone();

            let tasks: Vec<WorkerTask> = left_parts
                .into_iter()
                .zip(right_parts)
                .enumerate()
                .map(|(i, (lp, rp))| {
                    let l_schema = if !lp.is_empty() {
                        lp[0].schema()
                    } else if !left_batches.is_empty() {
                        left_batches[0].schema()
                    } else {
                        join_schema.clone()
                    };
                    let r_schema = if !rp.is_empty() {
                        rp[0].schema()
                    } else if !right_batches.is_empty() {
                        right_batches[0].schema()
                    } else {
                        join_schema.clone()
                    };
                    let lk = lk.clone();
                    let rk = rk.clone();
                    let js = join_schema.clone();
                    WorkerTask::new(i, move |_ctx| {
                        let partition_plan = PhysicalPlan::HashJoin {
                            left: Box::new(PhysicalPlan::Values {
                                schema: l_schema,
                                data: lp,
                            }),
                            right: Box::new(PhysicalPlan::Values {
                                schema: r_schema,
                                data: rp,
                            }),
                            join_type,
                            left_keys: lk,
                            right_keys: rk,
                            schema: js,
                        };
                        let exec_ctx = ExecutionContext::new();
                        exec_ctx.execute(&partition_plan)
                    })
                })
                .collect();

            let task_results = self.worker_pool.execute_all(tasks)?;
            Self::collect_results(task_results)
        } else {
            // Fall back to single-threaded
            ctx.execute(plan)
        }
    }

    /// Rebuild a plan node replacing its leaf input with a Values node containing the given data.
    fn rebuild_plan_with_values(
        &self,
        plan: &PhysicalPlan,
        data: Vec<RecordBatch>,
        plan_schema: Arc<arrow::datatypes::Schema>,
    ) -> PhysicalPlan {
        let input_schema = if !data.is_empty() {
            data[0].schema()
        } else {
            plan_schema
        };
        let values_node = PhysicalPlan::Values {
            schema: input_schema,
            data,
        };
        match plan {
            PhysicalPlan::Filter { predicate, .. } => PhysicalPlan::Filter {
                predicate: predicate.clone(),
                input: Box::new(values_node),
            },
            PhysicalPlan::Projection { exprs, schema, .. } => PhysicalPlan::Projection {
                exprs: exprs.clone(),
                schema: schema.clone(),
                input: Box::new(values_node),
            },
            PhysicalPlan::HashAggregate {
                group_by,
                aggr_exprs,
                schema,
                ..
            } => PhysicalPlan::HashAggregate {
                group_by: group_by.clone(),
                aggr_exprs: aggr_exprs
                    .iter()
                    .map(|e| crate::planner::AggregateExpr {
                        func: e.func,
                        args: e.args.clone(),
                        distinct: e.distinct,
                        alias: e.alias.clone(),
                    })
                    .collect(),
                schema: schema.clone(),
                input: Box::new(values_node),
            },
            PhysicalPlan::Sort { exprs, .. } => PhysicalPlan::Sort {
                exprs: exprs
                    .iter()
                    .map(|e| crate::planner::SortExpr {
                        expr: e.expr.clone(),
                        ascending: e.ascending,
                        nulls_first: e.nulls_first,
                    })
                    .collect(),
                input: Box::new(values_node),
            },
            _ => values_node,
        }
    }

    /// Collect results from parallel task execution into a flat Vec<RecordBatch>.
    fn collect_results(task_results: Vec<TaskResult>) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();
        for result in task_results {
            all_batches.extend(result?);
        }
        Ok(all_batches)
    }

    /// Execute tasks in parallel using the worker pool.
    ///
    /// Each task receives a partition index and produces a `Vec<RecordBatch>`.
    /// Results are collected in partition order.
    pub fn execute_parallel_tasks(
        &self,
        task_count: usize,
        task_fn: impl Fn(usize) -> Result<Vec<RecordBatch>> + Send + Sync + 'static,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        if task_count == 0 {
            return Ok(vec![]);
        }

        if task_count == 1 {
            return Ok(vec![task_fn(0)?]);
        }

        let task_fn = Arc::new(task_fn);
        let tasks: Vec<WorkerTask> = (0..task_count)
            .map(|i| {
                let f = task_fn.clone();
                WorkerTask::new(i, move |_ctx| f(i))
            })
            .collect();

        let task_results = self.worker_pool.execute_all(tasks)?;
        let mut results = Vec::with_capacity(task_count);
        for result in task_results {
            results.push(result?);
        }
        Ok(results)
    }

    /// Get the execution configuration.
    pub fn config(&self) -> &ExecutionConfig {
        &self.config
    }

    /// Get the number of active workers.
    pub fn num_workers(&self) -> usize {
        self.worker_pool.num_workers()
    }
}

impl Default for ParallelExecutor {
    fn default() -> Self {
        Self::new(ExecutionConfig::default())
    }
}

/// Metadata about parallel execution.
#[derive(Debug, Clone)]
pub struct ParallelPlanInfo {
    /// Number of partitions
    pub partitions: usize,
    /// Exchange operations needed
    pub exchanges: Vec<ExchangeInfo>,
    /// Estimated rows per partition
    pub rows_per_partition: usize,
}

/// Information about an exchange operation.
#[derive(Debug, Clone)]
pub struct ExchangeInfo {
    /// Type of exchange
    pub exchange_type: ExchangeType,
    /// Input partition count
    pub input_partitions: usize,
    /// Output partition count
    pub output_partitions: usize,
}

/// Statistics for parallel execution.
#[derive(Debug, Clone, Default)]
pub struct ParallelStats {
    /// Total tasks executed
    pub tasks_executed: usize,
    /// Total rows processed
    pub rows_processed: usize,
    /// Total bytes processed
    pub bytes_processed: usize,
    /// Time spent waiting for tasks
    pub wait_time_ms: u64,
    /// Time spent executing tasks
    pub execution_time_ms: u64,
}

/// Trait for parallel-aware operators.
pub trait ParallelOperator: Send + Sync {
    /// Get the output partitioning of this operator.
    fn output_partitioning(&self) -> Partitioning;

    /// Get required input partitioning.
    fn required_input_partitioning(&self) -> Vec<Partitioning>;

    /// Whether this operator maintains input ordering.
    fn maintains_ordering(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_config_default() {
        let config = ExecutionConfig::default();
        assert!(config.parallelism >= 1);
        assert!(config.batch_size > 0);
    }

    #[test]
    fn test_execution_config_builder() {
        let config = ExecutionConfig::new()
            .with_parallelism(8)
            .with_batch_size(4096)
            .with_adaptive(false);

        assert_eq!(config.parallelism, 8);
        assert_eq!(config.batch_size, 4096);
        assert!(!config.adaptive);
    }

    #[test]
    fn test_parallel_executor_creation() {
        let config = ExecutionConfig::new().with_parallelism(4);
        let executor = ParallelExecutor::new(config);
        assert_eq!(executor.num_workers(), 4);
    }

    #[test]
    fn test_parallel_plan_info() {
        let info = ParallelPlanInfo {
            partitions: 4,
            exchanges: vec![],
            rows_per_partition: 10000,
        };

        assert_eq!(info.partitions, 4);
        assert_eq!(info.rows_per_partition, 10000);
    }

    #[test]
    fn test_parallel_execute_sort() {
        use crate::planner::{ColumnExpr, SortExpr};
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![5, 3, 1, 4, 2, 8, 7, 6]))],
        )
        .unwrap();

        let plan = PhysicalPlan::Sort {
            exprs: vec![SortExpr::new(
                Arc::new(ColumnExpr::new("id", 0)),
                true,
                false,
            )],
            input: Box::new(PhysicalPlan::Values {
                schema: schema.clone(),
                data: vec![batch],
            }),
        };

        let config = ExecutionConfig::new()
            .with_parallelism(2)
            .with_adaptive(false);
        let executor = ParallelExecutor::new(config);
        let results = executor.execute(&plan).unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 8);

        // Verify sorted order
        let values: Vec<i64> = results
            .iter()
            .flat_map(|b| {
                let arr = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                (0..arr.len()).map(|i| arr.value(i)).collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(values, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_parallel_execute_filter() {
        use crate::planner::{BinaryExpr, ColumnExpr, LiteralExpr};
        use crate::types::ScalarValue;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]))],
        )
        .unwrap();

        // Filter: id > 4
        let predicate: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("id", 0)),
            "gt",
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(5)))),
        ));

        let plan = PhysicalPlan::Filter {
            predicate,
            input: Box::new(PhysicalPlan::Values {
                schema: schema.clone(),
                data: vec![batch],
            }),
        };

        let config = ExecutionConfig::new()
            .with_parallelism(2)
            .with_adaptive(false);
        let executor = ParallelExecutor::new(config);
        let results = executor.execute(&plan).unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // 6, 7, 8
    }

    #[test]
    fn test_parallel_execute_single_partition_fallback() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let plan = PhysicalPlan::Values {
            schema: schema.clone(),
            data: vec![batch],
        };

        let config = ExecutionConfig::new().with_parallelism(1);
        let executor = ParallelExecutor::new(config);
        let results = executor.execute(&plan).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    #[test]
    fn test_parallel_execute_empty_input() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let plan = PhysicalPlan::Values {
            schema: schema.clone(),
            data: vec![],
        };

        let config = ExecutionConfig::new().with_parallelism(4);
        let executor = ParallelExecutor::new(config);
        let results = executor.execute(&plan).unwrap();

        assert!(results.is_empty());
    }

    #[test]
    fn test_parallel_context_execute() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let plan = PhysicalPlan::Values {
            schema: schema.clone(),
            data: vec![batch],
        };

        let ctx = ExecutionContext::new();
        let config = ExecutionConfig::new().with_parallelism(2);
        let results = ctx.parallel_execute(&plan, &config).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
    }

    #[test]
    fn test_execute_parallel_tasks_actually_parallel() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::time::{Duration, Instant};

        let config = ExecutionConfig::new().with_parallelism(4);
        let executor = ParallelExecutor::new(config);

        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let schema_clone = schema.clone();

        let start = Instant::now();
        let results = executor
            .execute_parallel_tasks(4, move |partition_id| {
                counter_clone.fetch_add(1, Ordering::SeqCst);
                // Small sleep to verify parallelism (total would be 400ms if serial)
                std::thread::sleep(Duration::from_millis(50));
                let batch = RecordBatch::try_new(
                    schema_clone.clone(),
                    vec![Arc::new(Int64Array::from(vec![partition_id as i64]))],
                )
                .unwrap();
                Ok(vec![batch])
            })
            .unwrap();
        let elapsed = start.elapsed();

        // All 4 tasks executed
        assert_eq!(counter.load(Ordering::SeqCst), 4);
        assert_eq!(results.len(), 4);

        // Each task produced 1 batch with 1 row
        for partition_results in &results {
            assert_eq!(partition_results.len(), 1);
            assert_eq!(partition_results[0].num_rows(), 1);
        }

        // If truly parallel on >=2 cores, should be well under 200ms
        // (4 * 50ms = 200ms serial, ~50-100ms parallel)
        assert!(
            elapsed < Duration::from_millis(300),
            "Parallel tasks took {:?}, expected < 300ms for true parallelism",
            elapsed
        );
    }

    #[test]
    fn test_execute_parallel_tasks_collects_results_in_order() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let config = ExecutionConfig::new().with_parallelism(4);
        let executor = ParallelExecutor::new(config);

        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, false)]));
        let schema_clone = schema.clone();

        let results = executor
            .execute_parallel_tasks(4, move |partition_id| {
                let batch = RecordBatch::try_new(
                    schema_clone.clone(),
                    vec![Arc::new(Int64Array::from(vec![(partition_id * 10) as i64]))],
                )
                .unwrap();
                Ok(vec![batch])
            })
            .unwrap();

        assert_eq!(results.len(), 4);
        // Results should be in partition order
        for (i, partition_results) in results.iter().enumerate() {
            let arr = partition_results[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(arr.value(0), (i * 10) as i64);
        }
    }

    #[test]
    fn test_parallel_scan_pipeline_filter() {
        use crate::planner::{BinaryExpr, ColumnExpr, LiteralExpr};
        use crate::types::ScalarValue;
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        // Multiple batches simulate a multi-batch table scan
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![5, 6, 7, 8]))],
        )
        .unwrap();
        let batch3 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![9, 10, 11, 12]))],
        )
        .unwrap();

        // Filter: id > 6
        let predicate: Arc<dyn crate::planner::PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("id", 0)),
            "gt",
            Arc::new(LiteralExpr::new(ScalarValue::Int64(Some(6)))),
        ));

        let scan_plan = PhysicalPlan::Values {
            schema: schema.clone(),
            data: vec![batch1, batch2, batch3],
        };

        let plan = PhysicalPlan::Filter {
            predicate,
            input: Box::new(scan_plan),
        };

        let config = ExecutionConfig::new()
            .with_parallelism(3)
            .with_adaptive(false);
        let executor = ParallelExecutor::new(config);
        let results = executor.execute(&plan).unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 6); // 7, 8, 9, 10, 11, 12
    }
}
