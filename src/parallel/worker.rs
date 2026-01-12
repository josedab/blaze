//! Worker Pool and Task Management
//!
//! This module provides a thread pool for parallel query execution,
//! managing worker threads and task distribution.

use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};

/// Task context passed to worker tasks.
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// Partition ID for this task
    pub partition_id: usize,
    /// Total number of partitions
    pub num_partitions: usize,
    /// Task start time
    pub start_time: Instant,
}

impl TaskContext {
    /// Create a new task context.
    pub fn new(partition_id: usize, num_partitions: usize) -> Self {
        Self {
            partition_id,
            num_partitions,
            start_time: Instant::now(),
        }
    }

    /// Get elapsed time since task start.
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }
}

/// Result of a worker task.
pub type TaskResult = Result<Vec<RecordBatch>>;

/// A task to be executed by a worker.
pub struct WorkerTask {
    /// Partition ID for this task
    pub partition_id: usize,
    /// The task function to execute
    task_fn: Box<dyn FnOnce(TaskContext) -> TaskResult + Send>,
}

impl WorkerTask {
    /// Create a new worker task.
    pub fn new<F>(partition_id: usize, task_fn: F) -> Self
    where
        F: FnOnce(TaskContext) -> TaskResult + Send + 'static,
    {
        Self {
            partition_id,
            task_fn: Box::new(task_fn),
        }
    }

    /// Execute the task with the given context.
    pub fn execute(self, num_partitions: usize) -> TaskResult {
        let context = TaskContext::new(self.partition_id, num_partitions);
        (self.task_fn)(context)
    }
}

/// Thread pool for parallel execution.
///
/// Uses scoped threads to avoid lifetime issues and ensure proper cleanup.
pub struct WorkerPool {
    num_workers: usize,
}

impl WorkerPool {
    /// Create a new worker pool with the specified number of workers.
    pub fn new(num_workers: usize) -> Self {
        Self {
            num_workers: num_workers.max(1),
        }
    }

    /// Get the number of workers.
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Execute all tasks and return results.
    pub fn execute_all(&self, tasks: Vec<WorkerTask>) -> Result<Vec<TaskResult>> {
        if tasks.is_empty() {
            return Ok(vec![]);
        }

        let num_tasks = tasks.len();

        // For single task, execute directly
        if num_tasks == 1 {
            let task = tasks.into_iter().next().unwrap();
            let result = task.execute(1);
            return Ok(vec![result]);
        }

        // Use scoped threads for parallel execution
        let results: Arc<Mutex<Vec<Option<TaskResult>>>> = Arc::new(Mutex::new(
            (0..num_tasks).map(|_| None).collect()
        ));

        // Collect tasks into Arc for sharing
        let tasks: Vec<_> = tasks.into_iter().map(|t| Mutex::new(Some(t))).collect();
        let tasks = Arc::new(tasks);
        let task_idx = Arc::new(AtomicUsize::new(0));

        // Use scoped threads
        thread::scope(|s| {
            let num_threads = self.num_workers.min(num_tasks);

            for _ in 0..num_threads {
                let tasks = tasks.clone();
                let results = results.clone();
                let task_idx = task_idx.clone();

                s.spawn(move || {
                    loop {
                        let idx = task_idx.fetch_add(1, Ordering::SeqCst);
                        if idx >= num_tasks {
                            break;
                        }

                        // Take the task
                        let task = {
                            let mut guard = tasks[idx].lock().unwrap();
                            guard.take()
                        };

                        if let Some(task) = task {
                            let partition_id = task.partition_id;
                            let result = task.execute(num_tasks);

                            // Store result
                            let mut results_guard = results.lock().unwrap();
                            results_guard[partition_id] = Some(result);
                        }
                    }
                });
            }
        });

        // Collect results
        let mut results_guard = results.lock().unwrap();
        let final_results: Vec<TaskResult> = results_guard
            .iter_mut()
            .map(|opt| opt.take().unwrap_or_else(|| {
                Err(BlazeError::execution("Task result missing"))
            }))
            .collect();

        Ok(final_results)
    }

    /// Execute a single task.
    pub fn execute_one<F>(&self, task_fn: F) -> TaskResult
    where
        F: FnOnce(TaskContext) -> TaskResult + Send + 'static,
    {
        let task = WorkerTask::new(0, task_fn);
        let results = self.execute_all(vec![task])?;
        results.into_iter().next().unwrap_or_else(|| {
            Err(BlazeError::execution("No result returned"))
        })
    }
}

impl Default for WorkerPool {
    fn default() -> Self {
        Self::new(thread::available_parallelism().map(|p| p.get()).unwrap_or(4))
    }
}

/// Statistics for worker pool.
#[derive(Debug, Clone, Default)]
pub struct WorkerPoolStats {
    /// Total tasks executed
    pub tasks_executed: usize,
    /// Total time spent executing tasks
    pub execution_time_ms: u64,
    /// Average task duration in milliseconds
    pub avg_task_duration_ms: f64,
    /// Tasks currently in queue
    pub queue_depth: usize,
}

/// Simple future-like wrapper for async task results.
pub struct TaskFuture<T> {
    result: Arc<Mutex<Option<T>>>,
    completed: Arc<std::sync::atomic::AtomicBool>,
}

impl<T> TaskFuture<T> {
    /// Create a new task future.
    pub fn new() -> (Self, TaskFutureSetter<T>) {
        let result = Arc::new(Mutex::new(None));
        let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let future = Self {
            result: result.clone(),
            completed: completed.clone(),
        };

        let setter = TaskFutureSetter {
            result,
            completed,
        };

        (future, setter)
    }

    /// Check if the result is ready.
    pub fn is_ready(&self) -> bool {
        self.completed.load(Ordering::SeqCst)
    }

    /// Try to get the result without blocking.
    pub fn try_get(&self) -> Option<T> {
        if self.completed.load(Ordering::SeqCst) {
            self.result.lock().unwrap().take()
        } else {
            None
        }
    }
}

impl<T> Default for TaskFuture<T> {
    fn default() -> Self {
        Self::new().0
    }
}

/// Setter for task future results.
pub struct TaskFutureSetter<T> {
    result: Arc<Mutex<Option<T>>>,
    completed: Arc<std::sync::atomic::AtomicBool>,
}

impl<T> TaskFutureSetter<T> {
    /// Set the result.
    pub fn set(self, value: T) {
        let mut guard = self.result.lock().unwrap();
        *guard = Some(value);
        self.completed.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicI32;

    #[test]
    fn test_worker_pool_creation() {
        let pool = WorkerPool::new(4);
        assert_eq!(pool.num_workers(), 4);
    }

    #[test]
    fn test_worker_pool_execute_tasks() {
        let pool = WorkerPool::new(4);

        let counter = Arc::new(AtomicI32::new(0));

        let tasks: Vec<WorkerTask> = (0..10)
            .map(|i| {
                let counter = counter.clone();
                WorkerTask::new(i, move |_ctx| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![])
                })
            })
            .collect();

        let results = pool.execute_all(tasks).unwrap();

        assert_eq!(results.len(), 10);
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[test]
    fn test_worker_pool_with_results() {
        let pool = WorkerPool::new(2);

        let tasks: Vec<WorkerTask> = (0..4)
            .map(|i| {
                WorkerTask::new(i, move |_ctx| {
                    // Simulate some work
                    thread::sleep(Duration::from_millis(10));
                    Ok(vec![]) // Return empty batch for test
                })
            })
            .collect();

        let results = pool.execute_all(tasks).unwrap();
        assert_eq!(results.len(), 4);

        for result in results {
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_task_context() {
        let ctx = TaskContext::new(2, 4);
        assert_eq!(ctx.partition_id, 2);
        assert_eq!(ctx.num_partitions, 4);

        thread::sleep(Duration::from_millis(10));
        assert!(ctx.elapsed() >= Duration::from_millis(10));
    }

    #[test]
    fn test_task_future() {
        let (future, setter) = TaskFuture::<i32>::new();

        assert!(!future.is_ready());

        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(10));
            setter.set(42);
        });

        handle.join().unwrap();
        assert!(future.is_ready());
    }

    #[test]
    fn test_single_task_execution() {
        let pool = WorkerPool::new(2);

        let result = pool.execute_one(|_ctx| {
            Ok(vec![])
        });

        assert!(result.is_ok());
    }

    #[test]
    fn test_empty_tasks() {
        let pool = WorkerPool::new(2);
        let results = pool.execute_all(vec![]).unwrap();
        assert!(results.is_empty());
    }
}
