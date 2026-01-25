//! Query progress tracking and cooperative cancellation.
//!
//! Provides real-time progress reporting and cancellation for long-running queries.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// A cancellation token that can be shared across threads.
/// When cancelled, operators should check `is_cancelled()` periodically and abort.
#[derive(Debug, Clone)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Signal cancellation.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    /// Check if cancellation has been requested.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Reset the token for reuse.
    pub fn reset(&self) {
        self.cancelled.store(false, Ordering::Release);
    }
}

impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

/// Real-time progress information for a running query.
#[derive(Debug, Clone)]
pub struct QueryProgress {
    inner: Arc<ProgressInner>,
}

#[derive(Debug)]
struct ProgressInner {
    rows_processed: AtomicU64,
    bytes_processed: AtomicU64,
    total_rows_estimate: AtomicU64,
    started_at: Instant,
    stage: parking_lot::RwLock<String>,
}

impl QueryProgress {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ProgressInner {
                rows_processed: AtomicU64::new(0),
                bytes_processed: AtomicU64::new(0),
                total_rows_estimate: AtomicU64::new(0),
                started_at: Instant::now(),
                stage: parking_lot::RwLock::new("initializing".into()),
            }),
        }
    }

    /// Record rows processed by an operator.
    pub fn add_rows(&self, count: u64) {
        self.inner
            .rows_processed
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Record bytes processed.
    pub fn add_bytes(&self, count: u64) {
        self.inner
            .bytes_processed
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Set the estimated total rows (for progress percentage).
    pub fn set_total_estimate(&self, total: u64) {
        self.inner
            .total_rows_estimate
            .store(total, Ordering::Relaxed);
    }

    /// Set the current execution stage name.
    pub fn set_stage(&self, stage: impl Into<String>) {
        *self.inner.stage.write() = stage.into();
    }

    /// Get current rows processed.
    pub fn rows_processed(&self) -> u64 {
        self.inner.rows_processed.load(Ordering::Relaxed)
    }

    /// Get current bytes processed.
    pub fn bytes_processed(&self) -> u64 {
        self.inner.bytes_processed.load(Ordering::Relaxed)
    }

    /// Get the estimated total rows.
    pub fn total_rows_estimate(&self) -> u64 {
        self.inner.total_rows_estimate.load(Ordering::Relaxed)
    }

    /// Get elapsed time since query start.
    pub fn elapsed(&self) -> std::time::Duration {
        self.inner.started_at.elapsed()
    }

    /// Get the current execution stage.
    pub fn stage(&self) -> String {
        self.inner.stage.read().clone()
    }

    /// Get progress percentage (0.0 to 1.0), or None if total is unknown.
    pub fn progress_fraction(&self) -> Option<f64> {
        let total = self.total_rows_estimate();
        if total == 0 {
            return None;
        }
        let processed = self.rows_processed();
        Some((processed as f64 / total as f64).min(1.0))
    }

    /// Estimate remaining time based on current throughput.
    pub fn estimated_remaining(&self) -> Option<std::time::Duration> {
        let fraction = self.progress_fraction()?;
        if fraction <= 0.0 {
            return None;
        }
        let elapsed = self.elapsed();
        let total_estimated = elapsed.as_secs_f64() / fraction;
        let remaining = total_estimated - elapsed.as_secs_f64();
        if remaining > 0.0 {
            Some(std::time::Duration::from_secs_f64(remaining))
        } else {
            Some(std::time::Duration::ZERO)
        }
    }

    /// Get current throughput in rows per second.
    pub fn rows_per_second(&self) -> f64 {
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            return 0.0;
        }
        self.rows_processed() as f64 / elapsed
    }

    /// Get a snapshot of the current progress as a displayable struct.
    pub fn snapshot(&self) -> ProgressSnapshot {
        ProgressSnapshot {
            rows_processed: self.rows_processed(),
            bytes_processed: self.bytes_processed(),
            total_rows_estimate: self.total_rows_estimate(),
            elapsed: self.elapsed(),
            stage: self.stage(),
            progress_pct: self.progress_fraction().map(|f| f * 100.0),
            rows_per_sec: self.rows_per_second(),
        }
    }
}

impl Default for QueryProgress {
    fn default() -> Self {
        Self::new()
    }
}

/// An immutable snapshot of query progress.
#[derive(Debug, Clone)]
pub struct ProgressSnapshot {
    pub rows_processed: u64,
    pub bytes_processed: u64,
    pub total_rows_estimate: u64,
    pub elapsed: std::time::Duration,
    pub stage: String,
    pub progress_pct: Option<f64>,
    pub rows_per_sec: f64,
}

impl std::fmt::Display for ProgressSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let elapsed_s = self.elapsed.as_secs_f64();
        write!(
            f,
            "[{:.1}s] {} | {} rows ({:.0} rows/s)",
            elapsed_s, self.stage, self.rows_processed, self.rows_per_sec,
        )?;
        if let Some(pct) = self.progress_pct {
            write!(f, " | {:.1}%", pct)?;
        }
        Ok(())
    }
}

/// A handle to a running query that provides progress and cancellation.
#[derive(Debug, Clone)]
pub struct QueryHandle {
    token: CancellationToken,
    progress: QueryProgress,
}

impl QueryHandle {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
            progress: QueryProgress::new(),
        }
    }

    /// Get the cancellation token.
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// Cancel the running query.
    pub fn cancel(&self) {
        self.token.cancel();
    }

    /// Check if the query has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Get the progress tracker.
    pub fn progress(&self) -> &QueryProgress {
        &self.progress
    }

    /// Get a progress snapshot.
    pub fn snapshot(&self) -> ProgressSnapshot {
        self.progress.snapshot()
    }
}

impl Default for QueryHandle {
    fn default() -> Self {
        Self::new()
    }
}

/// Type alias for a progress callback function.
pub type ProgressCallback = Box<dyn Fn(&ProgressSnapshot) + Send + Sync>;

/// A builder for configuring query execution with progress and cancellation.
pub struct QueryExecution {
    handle: QueryHandle,
    callbacks: Vec<ProgressCallback>,
}

impl QueryExecution {
    pub fn new() -> Self {
        Self {
            handle: QueryHandle::new(),
            callbacks: Vec::new(),
        }
    }

    /// Get a clone of the query handle for external use.
    pub fn handle(&self) -> QueryHandle {
        self.handle.clone()
    }

    /// Register a progress callback.
    pub fn on_progress(mut self, callback: ProgressCallback) -> Self {
        self.callbacks.push(callback);
        self
    }

    /// Notify all registered callbacks with current progress.
    pub fn notify_progress(&self) {
        let snapshot = self.handle.snapshot();
        for cb in &self.callbacks {
            cb(&snapshot);
        }
    }

    /// Get the cancellation token.
    pub fn token(&self) -> &CancellationToken {
        self.handle.token()
    }

    /// Get the progress tracker.
    pub fn progress(&self) -> &QueryProgress {
        self.handle.progress()
    }
}

impl Default for QueryExecution {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_cancellation_token() {
        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        token.cancel();
        assert!(token.is_cancelled());
        token.reset();
        assert!(!token.is_cancelled());
    }

    #[test]
    fn test_cancellation_token_shared() {
        let token = CancellationToken::new();
        let token2 = token.clone();

        assert!(!token2.is_cancelled());
        token.cancel();
        assert!(token2.is_cancelled());
    }

    #[test]
    fn test_query_progress_basic() {
        let progress = QueryProgress::new();
        progress.add_rows(100);
        progress.add_rows(200);
        assert_eq!(progress.rows_processed(), 300);

        progress.add_bytes(4096);
        assert_eq!(progress.bytes_processed(), 4096);
    }

    #[test]
    fn test_query_progress_percentage() {
        let progress = QueryProgress::new();
        assert!(progress.progress_fraction().is_none());

        progress.set_total_estimate(1000);
        progress.add_rows(250);
        let frac = progress.progress_fraction().unwrap();
        assert!((frac - 0.25).abs() < 0.01);
    }

    #[test]
    fn test_query_progress_stage() {
        let progress = QueryProgress::new();
        assert_eq!(progress.stage(), "initializing");
        progress.set_stage("scanning");
        assert_eq!(progress.stage(), "scanning");
    }

    #[test]
    fn test_query_handle_cancel() {
        let handle = QueryHandle::new();
        assert!(!handle.is_cancelled());
        handle.cancel();
        assert!(handle.is_cancelled());
    }

    #[test]
    fn test_progress_snapshot_display() {
        let progress = QueryProgress::new();
        progress.set_total_estimate(1000);
        progress.add_rows(500);
        progress.set_stage("filtering");
        let snap = progress.snapshot();
        let display = format!("{}", snap);
        assert!(display.contains("filtering"));
        assert!(display.contains("500 rows"));
    }

    #[test]
    fn test_query_execution_callbacks() {
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        let exec = QueryExecution::new().on_progress(Box::new(move |_snap| {
            counter_clone.fetch_add(1, Ordering::Relaxed);
        }));

        exec.progress().add_rows(100);
        exec.notify_progress();
        exec.notify_progress();

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_rows_per_second() {
        let progress = QueryProgress::new();
        progress.add_rows(1000);
        // Can't precisely test timing, but ensure it returns a non-negative value
        assert!(progress.rows_per_second() >= 0.0);
    }
}
