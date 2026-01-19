//! Web Worker Support for Non-Blocking Queries
//!
//! Provides abstractions for executing queries in Web Workers
//! to prevent blocking the main browser thread.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

/// Unique ID for a query task
pub type QueryTaskId = u64;

/// Status of a query task
#[derive(Debug, Clone, PartialEq)]
pub enum QueryTaskStatus {
    /// Task is queued but not yet started
    Pending,
    /// Task is currently executing
    Running,
    /// Task completed successfully
    Completed { rows: usize, elapsed_ms: f64 },
    /// Task failed with an error
    Failed { error: String },
    /// Task was cancelled
    Cancelled,
}

/// A query task that can be executed in a Web Worker
#[derive(Debug, Clone)]
pub struct QueryTask {
    /// Unique task ID
    pub id: QueryTaskId,
    /// SQL query to execute
    pub sql: String,
    /// Task status
    pub status: QueryTaskStatus,
    /// Result as JSON (populated on completion)
    pub result_json: Option<String>,
    /// Maximum rows to return
    pub max_rows: Option<usize>,
    /// Output format
    pub format: OutputFormat,
}

/// Output format for worker results
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub enum OutputFormat {
    #[default]
    Json,
    Csv,
    ColumnarJson,
}

/// Manager for async query tasks (simulates Web Worker behavior)
pub struct WorkerQueryManager {
    next_id: AtomicU64,
    tasks: Mutex<HashMap<QueryTaskId, QueryTask>>,
    cancelled: Mutex<HashMap<QueryTaskId, AtomicBool>>,
}

impl WorkerQueryManager {
    /// Create a new worker query manager.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            tasks: Mutex::new(HashMap::new()),
            cancelled: Mutex::new(HashMap::new()),
        }
    }

    /// Submit a query for execution.
    pub fn submit_query(
        &self,
        sql: String,
        format: OutputFormat,
        max_rows: Option<usize>,
    ) -> QueryTaskId {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let task = QueryTask {
            id,
            sql,
            status: QueryTaskStatus::Pending,
            result_json: None,
            max_rows,
            format,
        };

        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.insert(id, task);
        }
        if let Ok(mut cancelled) = self.cancelled.lock() {
            cancelled.insert(id, AtomicBool::new(false));
        }

        id
    }

    /// Get the status of a query task.
    pub fn get_status(&self, task_id: QueryTaskId) -> Option<QueryTaskStatus> {
        self.tasks
            .lock()
            .ok()
            .and_then(|tasks| tasks.get(&task_id).map(|t| t.status.clone()))
    }

    /// Get the result of a completed query task.
    pub fn get_result(&self, task_id: QueryTaskId) -> Option<String> {
        self.tasks
            .lock()
            .ok()
            .and_then(|tasks| tasks.get(&task_id).and_then(|t| t.result_json.clone()))
    }

    /// Cancel a running query.
    pub fn cancel(&self, task_id: QueryTaskId) -> bool {
        if let Ok(cancelled) = self.cancelled.lock() {
            if let Some(flag) = cancelled.get(&task_id) {
                flag.store(true, Ordering::SeqCst);
                return true;
            }
        }
        false
    }

    /// Check if a task has been cancelled.
    pub fn is_cancelled(&self, task_id: QueryTaskId) -> bool {
        self.cancelled
            .lock()
            .ok()
            .and_then(|cancelled| cancelled.get(&task_id).map(|f| f.load(Ordering::SeqCst)))
            .unwrap_or(false)
    }

    /// Mark a task as running.
    pub fn mark_running(&self, task_id: QueryTaskId) {
        if let Ok(mut tasks) = self.tasks.lock() {
            if let Some(task) = tasks.get_mut(&task_id) {
                task.status = QueryTaskStatus::Running;
            }
        }
    }

    /// Mark a task as completed.
    pub fn mark_completed(
        &self,
        task_id: QueryTaskId,
        result_json: String,
        rows: usize,
        elapsed_ms: f64,
    ) {
        if let Ok(mut tasks) = self.tasks.lock() {
            if let Some(task) = tasks.get_mut(&task_id) {
                task.status = QueryTaskStatus::Completed { rows, elapsed_ms };
                task.result_json = Some(result_json);
            }
        }
    }

    /// Mark a task as failed.
    pub fn mark_failed(&self, task_id: QueryTaskId, error: String) {
        if let Ok(mut tasks) = self.tasks.lock() {
            if let Some(task) = tasks.get_mut(&task_id) {
                task.status = QueryTaskStatus::Failed { error };
            }
        }
    }

    /// Remove a completed/failed/cancelled task.
    pub fn remove_task(&self, task_id: QueryTaskId) -> Option<QueryTask> {
        self.tasks
            .lock()
            .ok()
            .and_then(|mut tasks| tasks.remove(&task_id))
    }

    /// List all task IDs and their statuses.
    pub fn list_tasks(&self) -> Vec<(QueryTaskId, QueryTaskStatus)> {
        self.tasks
            .lock()
            .ok()
            .map(|tasks| {
                tasks
                    .iter()
                    .map(|(id, t)| (*id, t.status.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Get the number of pending/running tasks.
    pub fn active_count(&self) -> usize {
        self.tasks
            .lock()
            .ok()
            .map(|tasks| {
                tasks
                    .values()
                    .filter(|t| {
                        matches!(
                            t.status,
                            QueryTaskStatus::Pending | QueryTaskStatus::Running
                        )
                    })
                    .count()
            })
            .unwrap_or(0)
    }
}

impl Default for WorkerQueryManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Configuration for WASM optimization
#[derive(Debug, Clone)]
pub struct WasmOptConfig {
    /// Maximum memory in bytes (default 256MB)
    pub max_memory: usize,
    /// Whether to use streaming for large results
    pub streaming_threshold: usize,
    /// Maximum result size in bytes
    pub max_result_size: usize,
    /// Enable memory pooling
    pub memory_pooling: bool,
    /// Batch size for WASM execution
    pub batch_size: usize,
}

impl Default for WasmOptConfig {
    fn default() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024,         // 256MB
            streaming_threshold: 10 * 1024 * 1024, // 10MB
            max_result_size: 50 * 1024 * 1024,     // 50MB
            memory_pooling: true,
            batch_size: 4096,
        }
    }
}

impl WasmOptConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_memory = bytes;
        self
    }

    pub fn with_streaming_threshold(mut self, bytes: usize) -> Self {
        self.streaming_threshold = bytes;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Estimate if a result set needs streaming based on row count
    pub fn needs_streaming(&self, estimated_rows: usize, avg_row_bytes: usize) -> bool {
        estimated_rows * avg_row_bytes > self.streaming_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_query_manager() {
        let mgr = WorkerQueryManager::new();

        let id = mgr.submit_query("SELECT 1".to_string(), OutputFormat::Json, None);
        assert_eq!(mgr.get_status(id), Some(QueryTaskStatus::Pending));
        assert_eq!(mgr.active_count(), 1);

        mgr.mark_running(id);
        assert_eq!(mgr.get_status(id), Some(QueryTaskStatus::Running));

        mgr.mark_completed(id, r#"[{"1":1}]"#.to_string(), 1, 0.5);
        assert!(matches!(
            mgr.get_status(id),
            Some(QueryTaskStatus::Completed { .. })
        ));
        assert_eq!(mgr.active_count(), 0);

        let result = mgr.get_result(id);
        assert!(result.is_some());
    }

    #[test]
    fn test_cancel_query() {
        let mgr = WorkerQueryManager::new();
        let id = mgr.submit_query("SELECT 1".to_string(), OutputFormat::Json, None);

        assert!(!mgr.is_cancelled(id));
        mgr.cancel(id);
        assert!(mgr.is_cancelled(id));
    }

    #[test]
    fn test_wasm_opt_config() {
        let config = WasmOptConfig::new()
            .with_max_memory(128 * 1024 * 1024)
            .with_batch_size(2048);

        assert_eq!(config.max_memory, 128 * 1024 * 1024);
        assert_eq!(config.batch_size, 2048);
        assert!(config.needs_streaming(100_000, 200));
        assert!(!config.needs_streaming(100, 100));
    }

    #[test]
    fn test_list_tasks() {
        let mgr = WorkerQueryManager::new();

        let id1 = mgr.submit_query("Q1".to_string(), OutputFormat::Json, None);
        let _id2 = mgr.submit_query("Q2".to_string(), OutputFormat::Csv, None);

        let tasks = mgr.list_tasks();
        assert_eq!(tasks.len(), 2);

        mgr.mark_failed(id1, "error".to_string());
        mgr.remove_task(id1);

        let tasks = mgr.list_tasks();
        assert_eq!(tasks.len(), 1);
    }
}
