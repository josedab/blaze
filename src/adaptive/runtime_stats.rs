//! Runtime Statistics Collection
//!
//! This module provides types and utilities for collecting runtime statistics
//! during query execution to enable adaptive optimizations.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Statistics for a single partition.
#[derive(Debug, Clone)]
pub struct PartitionStats {
    /// Partition identifier
    pub partition_id: usize,
    /// Number of rows in this partition
    pub row_count: usize,
    /// Size in bytes
    pub byte_size: usize,
    /// Number of null values
    pub null_count: usize,
}

impl PartitionStats {
    /// Create new partition stats.
    pub fn new(partition_id: usize) -> Self {
        Self {
            partition_id,
            row_count: 0,
            byte_size: 0,
            null_count: 0,
        }
    }

    /// Set row count.
    pub fn with_row_count(mut self, count: usize) -> Self {
        self.row_count = count;
        self
    }

    /// Set byte size.
    pub fn with_byte_size(mut self, size: usize) -> Self {
        self.byte_size = size;
        self
    }
}

/// Statistics for an execution stage.
#[derive(Debug, Clone)]
pub struct StageStats {
    /// Stage identifier
    pub stage_id: usize,
    /// Total rows across all partitions
    pub total_rows: usize,
    /// Total bytes across all partitions
    pub total_bytes: usize,
    /// Per-partition statistics
    pub partitions: Vec<PartitionStats>,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

impl StageStats {
    /// Create new stage stats.
    pub fn new(stage_id: usize) -> Self {
        Self {
            stage_id,
            total_rows: 0,
            total_bytes: 0,
            partitions: vec![],
            execution_time_ms: 0,
        }
    }

    /// Get the average partition size in bytes.
    pub fn avg_partition_size(&self) -> usize {
        if self.partitions.is_empty() {
            return 0;
        }
        self.total_bytes / self.partitions.len()
    }

    /// Get the maximum partition size in bytes.
    pub fn max_partition_size(&self) -> usize {
        self.partitions.iter().map(|p| p.byte_size).max().unwrap_or(0)
    }

    /// Get the minimum partition size in bytes.
    pub fn min_partition_size(&self) -> usize {
        self.partitions.iter().map(|p| p.byte_size).min().unwrap_or(0)
    }

    /// Get the median partition size in bytes.
    pub fn median_partition_size(&self) -> usize {
        if self.partitions.is_empty() {
            return 0;
        }
        let mut sizes: Vec<usize> = self.partitions.iter().map(|p| p.byte_size).collect();
        sizes.sort_unstable();
        sizes[sizes.len() / 2]
    }

    /// Check if there is significant skew.
    pub fn has_skew(&self, threshold: f64) -> bool {
        let median = self.median_partition_size();
        if median == 0 {
            return false;
        }
        let max = self.max_partition_size();
        (max as f64 / median as f64) > threshold
    }

    /// Get skewed partition IDs (partitions larger than threshold * median).
    pub fn skewed_partitions(&self, threshold: f64) -> Vec<usize> {
        let median = self.median_partition_size();
        if median == 0 {
            return vec![];
        }
        let skew_threshold = (median as f64 * threshold) as usize;
        self.partitions
            .iter()
            .filter(|p| p.byte_size > skew_threshold)
            .map(|p| p.partition_id)
            .collect()
    }
}

/// Runtime statistics for the entire query.
#[derive(Debug, Clone)]
pub struct RuntimeStats {
    /// Per-stage statistics
    pub stages: HashMap<usize, StageStats>,
    /// Query start time
    pub start_time: Option<Instant>,
    /// Total execution time
    pub total_time: Option<Duration>,
    /// Peak memory usage in bytes
    pub peak_memory: usize,
    /// Total rows processed
    pub total_rows_processed: usize,
}

impl RuntimeStats {
    /// Create new runtime stats.
    pub fn new() -> Self {
        Self {
            stages: HashMap::new(),
            start_time: None,
            total_time: None,
            peak_memory: 0,
            total_rows_processed: 0,
        }
    }

    /// Start timing.
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Stop timing.
    pub fn stop(&mut self) {
        if let Some(start) = self.start_time {
            self.total_time = Some(start.elapsed());
        }
    }

    /// Record stage statistics.
    pub fn record_stage(&mut self, stage_id: usize, stats: StageStats) {
        self.total_rows_processed += stats.total_rows;
        self.stages.insert(stage_id, stats);
    }

    /// Get stage statistics.
    pub fn get_stage(&self, stage_id: usize) -> Option<&StageStats> {
        self.stages.get(&stage_id)
    }

    /// Get total bytes processed.
    pub fn total_bytes(&self) -> usize {
        self.stages.values().map(|s| s.total_bytes).sum()
    }

    /// Get execution time in milliseconds.
    pub fn execution_time_ms(&self) -> u64 {
        self.total_time.map(|d| d.as_millis() as u64).unwrap_or(0)
    }
}

impl Default for RuntimeStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Thread-safe statistics collector.
pub struct StatsCollector {
    /// Inner runtime stats
    stats: RwLock<RuntimeStats>,
}

impl StatsCollector {
    /// Create a new stats collector.
    pub fn new() -> Self {
        Self {
            stats: RwLock::new(RuntimeStats::new()),
        }
    }

    /// Start timing.
    pub fn start(&self) {
        if let Ok(mut stats) = self.stats.write() {
            stats.start();
        }
    }

    /// Stop timing.
    pub fn stop(&self) {
        if let Ok(mut stats) = self.stats.write() {
            stats.stop();
        }
    }

    /// Record stage statistics.
    pub fn record_stage(&self, stage_id: usize, stage_stats: StageStats) {
        if let Ok(mut stats) = self.stats.write() {
            stats.record_stage(stage_id, stage_stats);
        }
    }

    /// Get a snapshot of current statistics.
    pub fn snapshot(&self) -> RuntimeStats {
        self.stats.read().map(|s| s.clone()).unwrap_or_default()
    }

    /// Get stage statistics.
    pub fn get_stage(&self, stage_id: usize) -> Option<StageStats> {
        self.stats.read().ok().and_then(|s| s.get_stage(stage_id).cloned())
    }

    /// Update peak memory.
    pub fn update_memory(&self, bytes: usize) {
        if let Ok(mut stats) = self.stats.write() {
            if bytes > stats.peak_memory {
                stats.peak_memory = bytes;
            }
        }
    }
}

impl Default for StatsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Operator-level statistics.
#[derive(Debug, Clone)]
pub struct OperatorStats {
    /// Operator name
    pub name: String,
    /// Rows produced
    pub rows_produced: usize,
    /// Time spent in this operator
    pub time_ms: u64,
    /// Memory used
    pub memory_bytes: usize,
    /// Number of batches produced
    pub batches_produced: usize,
}

impl OperatorStats {
    /// Create new operator stats.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            rows_produced: 0,
            time_ms: 0,
            memory_bytes: 0,
            batches_produced: 0,
        }
    }

    /// Set rows produced.
    pub fn with_rows(mut self, rows: usize) -> Self {
        self.rows_produced = rows;
        self
    }

    /// Set time spent.
    pub fn with_time_ms(mut self, ms: u64) -> Self {
        self.time_ms = ms;
        self
    }

    /// Set memory used.
    pub fn with_memory(mut self, bytes: usize) -> Self {
        self.memory_bytes = bytes;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_stats() {
        let stats = PartitionStats::new(0)
            .with_row_count(1000)
            .with_byte_size(8192);

        assert_eq!(stats.partition_id, 0);
        assert_eq!(stats.row_count, 1000);
        assert_eq!(stats.byte_size, 8192);
    }

    #[test]
    fn test_stage_stats_aggregations() {
        let mut stage = StageStats::new(0);
        stage.total_bytes = 3000;
        stage.partitions = vec![
            PartitionStats { partition_id: 0, row_count: 100, byte_size: 1000, null_count: 0 },
            PartitionStats { partition_id: 1, row_count: 100, byte_size: 500, null_count: 0 },
            PartitionStats { partition_id: 2, row_count: 100, byte_size: 1500, null_count: 0 },
        ];

        assert_eq!(stage.avg_partition_size(), 1000);
        assert_eq!(stage.max_partition_size(), 1500);
        assert_eq!(stage.min_partition_size(), 500);
        assert_eq!(stage.median_partition_size(), 1000);
    }

    #[test]
    fn test_stage_stats_skew_detection() {
        let mut stage = StageStats::new(0);
        stage.partitions = vec![
            PartitionStats { partition_id: 0, row_count: 100, byte_size: 100, null_count: 0 },
            PartitionStats { partition_id: 1, row_count: 100, byte_size: 100, null_count: 0 },
            PartitionStats { partition_id: 2, row_count: 100, byte_size: 1000, null_count: 0 }, // Skewed
        ];

        assert!(stage.has_skew(5.0)); // 1000/100 = 10x > 5x threshold
        assert!(!stage.has_skew(15.0)); // 10x < 15x threshold

        let skewed = stage.skewed_partitions(5.0);
        assert_eq!(skewed, vec![2]);
    }

    #[test]
    fn test_runtime_stats() {
        let mut stats = RuntimeStats::new();

        let stage_stats = StageStats {
            stage_id: 0,
            total_rows: 1000,
            total_bytes: 8192,
            partitions: vec![],
            execution_time_ms: 100,
        };

        stats.record_stage(0, stage_stats);

        assert_eq!(stats.total_rows_processed, 1000);
        assert_eq!(stats.total_bytes(), 8192);
    }

    #[test]
    fn test_stats_collector() {
        let collector = StatsCollector::new();
        collector.start();

        let stage_stats = StageStats::new(0);
        collector.record_stage(0, stage_stats);

        let snapshot = collector.snapshot();
        assert!(snapshot.stages.contains_key(&0));
    }

    #[test]
    fn test_operator_stats() {
        let stats = OperatorStats::new("HashJoin")
            .with_rows(1000)
            .with_time_ms(50)
            .with_memory(65536);

        assert_eq!(stats.name, "HashJoin");
        assert_eq!(stats.rows_produced, 1000);
        assert_eq!(stats.time_ms, 50);
        assert_eq!(stats.memory_bytes, 65536);
    }
}
