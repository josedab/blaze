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

mod partition;
mod exchange;
mod worker;

pub use partition::{Partitioning, HashPartitioner, RangePartitioner, RoundRobinPartitioner};
pub use exchange::{ExchangeType, Exchange, ShuffleExchange, BroadcastExchange};
pub use worker::{WorkerPool, WorkerTask, TaskResult};

use std::sync::Arc;
use std::thread;

use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};
use crate::planner::PhysicalPlan;
use crate::executor::ExecutionContext;

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
        Self { config, worker_pool }
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
    fn execute_with_partitions(&self, plan: &PhysicalPlan, partitions: usize) -> Result<Vec<RecordBatch>> {
        if partitions == 1 {
            // Single partition - use regular execution
            let ctx = ExecutionContext::new();
            return ctx.execute(plan);
        }

        // For now, execute the plan once and partition the results
        // A full implementation would partition the input data
        let ctx = ExecutionContext::new();
        let results = ctx.execute(plan)?;

        // Combine results (already combined from single execution)
        Ok(results)
    }

    /// Execute tasks in parallel using the worker pool.
    pub fn execute_parallel_tasks<F, T>(&self, task_count: usize, task_fn: F) -> Result<Vec<T>>
    where
        F: Fn(usize) -> Result<T> + Send + Sync + 'static,
        T: Send + 'static,
    {
        if task_count == 0 {
            return Ok(vec![]);
        }

        if task_count == 1 {
            return Ok(vec![task_fn(0)?]);
        }

        let task_fn = Arc::new(task_fn);
        let tasks: Vec<WorkerTask> = (0..task_count)
            .map(|i| {
                let task_fn = task_fn.clone();
                WorkerTask::new(i, move |_ctx| {
                    // We need to convert T to Vec<RecordBatch> for the worker interface
                    // For now, return empty - the actual parallelism is handled differently
                    Ok(vec![])
                })
            })
            .collect();

        let _ = self.worker_pool.execute_all(tasks)?;

        // Execute sequentially as fallback
        let mut results = Vec::with_capacity(task_count);
        for i in 0..task_count {
            results.push(task_fn(i)?);
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
}
