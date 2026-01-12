//! Adaptive Query Execution (AQE)
//!
//! This module provides adaptive query execution capabilities that allow
//! the query engine to adapt the physical plan during execution based on
//! runtime statistics.
//!
//! # Features
//!
//! - **Runtime Statistics**: Collect cardinality and data distribution at runtime
//! - **Dynamic Join Strategy**: Switch join strategies based on actual data sizes
//! - **Partition Coalescing**: Merge small partitions to reduce overhead
//! - **Skew Handling**: Detect and handle data skew at runtime
//! - **Plan Re-optimization**: Re-optimize subplans based on collected statistics

mod runtime_stats;
mod adaptive_planner;
mod partition_coalesce;
mod skew_handler;

pub use runtime_stats::{RuntimeStats, PartitionStats, StageStats, StatsCollector};
pub use adaptive_planner::{AdaptivePlanner, AdaptiveContext, AdaptiveRule};
pub use partition_coalesce::{PartitionCoalescer, CoalesceStrategy};
pub use skew_handler::{SkewHandler, SkewDetector, SkewMitigation};

use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use crate::error::Result;
use crate::planner::PhysicalPlan;

/// Configuration for adaptive query execution.
#[derive(Debug, Clone)]
pub struct AdaptiveConfig {
    /// Whether AQE is enabled
    pub enabled: bool,
    /// Threshold for switching to broadcast join (in bytes)
    pub broadcast_join_threshold: usize,
    /// Minimum number of partitions after coalescing
    pub min_partitions: usize,
    /// Maximum size per partition after coalescing (in bytes)
    pub target_partition_size: usize,
    /// Threshold for detecting skew (ratio of largest to median)
    pub skew_threshold: f64,
    /// Whether to enable skew handling
    pub handle_skew: bool,
    /// Whether to enable dynamic partition coalescing
    pub coalesce_partitions: bool,
    /// Whether to enable dynamic join strategy selection
    pub dynamic_join_selection: bool,
}

impl Default for AdaptiveConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            broadcast_join_threshold: 10 * 1024 * 1024, // 10 MB
            min_partitions: 1,
            target_partition_size: 64 * 1024 * 1024, // 64 MB
            skew_threshold: 5.0, // 5x median is considered skewed
            handle_skew: true,
            coalesce_partitions: true,
            dynamic_join_selection: true,
        }
    }
}

impl AdaptiveConfig {
    /// Create a new adaptive configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable AQE.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Default::default()
        }
    }

    /// Set broadcast join threshold.
    pub fn with_broadcast_threshold(mut self, bytes: usize) -> Self {
        self.broadcast_join_threshold = bytes;
        self
    }

    /// Set target partition size.
    pub fn with_target_partition_size(mut self, bytes: usize) -> Self {
        self.target_partition_size = bytes;
        self
    }

    /// Set skew threshold.
    pub fn with_skew_threshold(mut self, threshold: f64) -> Self {
        self.skew_threshold = threshold;
        self
    }

    /// Enable or disable skew handling.
    pub fn with_skew_handling(mut self, enabled: bool) -> Self {
        self.handle_skew = enabled;
        self
    }
}

/// Adaptive execution engine.
pub struct AdaptiveExecutor {
    /// Configuration
    config: AdaptiveConfig,
    /// Statistics collector
    stats_collector: Arc<StatsCollector>,
    /// Adaptive planner
    planner: AdaptivePlanner,
    /// Partition coalescer
    coalescer: PartitionCoalescer,
    /// Skew handler
    skew_handler: SkewHandler,
}

impl AdaptiveExecutor {
    /// Create a new adaptive executor.
    pub fn new(config: AdaptiveConfig) -> Self {
        let stats_collector = Arc::new(StatsCollector::new());
        Self {
            planner: AdaptivePlanner::new(config.clone()),
            coalescer: PartitionCoalescer::new(config.clone()),
            skew_handler: SkewHandler::new(config.clone()),
            config,
            stats_collector,
        }
    }

    /// Execute a physical plan with adaptive optimization.
    pub fn execute(&self, plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        if !self.config.enabled {
            // Fallback to non-adaptive execution
            return self.execute_stage(plan);
        }

        // Get stage count
        let num_stages = self.count_stages(plan);

        // Execute the plan and collect statistics
        let results = self.execute_stage(plan)?;

        // Collect runtime statistics for analysis
        let stats = self.collect_stage_stats(0, &results);
        self.stats_collector.record_stage(0, stats.clone());

        // Check if we should adapt for future queries
        if num_stages > 1 {
            self.maybe_adapt_plan(0, &results)?;
        }

        Ok(results)
    }

    /// Get stage count for the plan.
    fn count_stages(&self, _plan: &PhysicalPlan) -> usize {
        // For now, treat the entire plan as a single stage
        // In a more complete implementation, this would count shuffle boundaries
        1
    }

    /// Execute a single stage.
    fn execute_stage(&self, _plan: &PhysicalPlan) -> Result<Vec<RecordBatch>> {
        // This would integrate with the main executor
        // For now, return empty results as placeholder
        Ok(vec![])
    }

    /// Collect statistics from stage execution results.
    fn collect_stage_stats(&self, stage_idx: usize, results: &[RecordBatch]) -> StageStats {
        let mut total_rows = 0;
        let mut total_bytes = 0;
        let mut partition_stats = Vec::new();

        for (idx, batch) in results.iter().enumerate() {
            let row_count = batch.num_rows();
            let byte_size = batch.get_array_memory_size();

            total_rows += row_count;
            total_bytes += byte_size;

            partition_stats.push(PartitionStats {
                partition_id: idx,
                row_count,
                byte_size,
                null_count: 0, // Would need deeper analysis
            });
        }

        StageStats {
            stage_id: stage_idx,
            total_rows,
            total_bytes,
            partitions: partition_stats,
            execution_time_ms: 0, // Would need timing
        }
    }

    /// Apply adaptive optimizations if needed.
    fn maybe_adapt_plan(&self, stage_idx: usize, results: &[RecordBatch]) -> Result<()> {
        let stats = self.collect_stage_stats(stage_idx, results);

        // Check for partition coalescing opportunity
        if self.config.coalesce_partitions {
            self.coalescer.maybe_coalesce(&stats);
        }

        // Check for skew
        if self.config.handle_skew {
            if let Some(_skew_info) = self.skew_handler.detect_skew(&stats) {
                // Handle skew by splitting large partitions
                self.skew_handler.apply_mitigation(&stats)?;
            }
        }

        // Check for dynamic join selection opportunity
        if self.config.dynamic_join_selection {
            if stats.total_bytes < self.config.broadcast_join_threshold {
                // Could switch to broadcast join for small datasets
                self.planner.mark_for_broadcast(stage_idx);
            }
        }

        Ok(())
    }

    /// Get collected statistics.
    pub fn get_stats(&self) -> &StatsCollector {
        &self.stats_collector
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_config_default() {
        let config = AdaptiveConfig::default();
        assert!(config.enabled);
        assert_eq!(config.broadcast_join_threshold, 10 * 1024 * 1024);
    }

    #[test]
    fn test_adaptive_config_builder() {
        let config = AdaptiveConfig::new()
            .with_broadcast_threshold(5 * 1024 * 1024)
            .with_target_partition_size(32 * 1024 * 1024)
            .with_skew_threshold(10.0);

        assert_eq!(config.broadcast_join_threshold, 5 * 1024 * 1024);
        assert_eq!(config.target_partition_size, 32 * 1024 * 1024);
        assert_eq!(config.skew_threshold, 10.0);
    }

    #[test]
    fn test_adaptive_config_disabled() {
        let config = AdaptiveConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_adaptive_executor_creation() {
        let config = AdaptiveConfig::default();
        let _executor = AdaptiveExecutor::new(config);
    }
}
