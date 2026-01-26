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

mod adaptive_planner;
mod partition_coalesce;
mod runtime_stats;
mod skew_handler;

pub use adaptive_planner::{AdaptiveContext, AdaptivePlanner, AdaptiveRule};
pub use partition_coalesce::{CoalesceStrategy, PartitionCoalescer};
pub use runtime_stats::{PartitionStats, RuntimeStats, StageStats, StatsCollector};
pub use skew_handler::{SkewDetector, SkewHandler, SkewMitigation};

use std::collections::HashMap;

use crate::error::Result;
use crate::planner::PhysicalPlan;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

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
            skew_threshold: 5.0,                     // 5x median is considered skewed
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

// ---------------------------------------------------------------------------
// Runtime Adaptor — mid-execution strategy switching
// ---------------------------------------------------------------------------

/// Join strategy that can be selected at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinStrategy {
    /// Hash-based join (default for large datasets)
    HashJoin,
    /// Sort-merge join (better when inputs are pre-sorted)
    SortMergeJoin,
    /// Broadcast join (one side is small enough to broadcast)
    BroadcastJoin,
    /// Nested loop join (fallback for non-equi joins)
    NestedLoopJoin,
}

/// A strategy switch decision with justification.
#[derive(Debug, Clone)]
pub struct StrategySwitch {
    /// Stage that was affected
    pub stage_id: usize,
    /// What strategy was being used
    pub from: JoinStrategy,
    /// What strategy we switched to
    pub to: JoinStrategy,
    /// Why the switch was made
    pub reason: String,
    /// Estimated cost ratio (new_cost / old_cost, < 1.0 means improvement)
    pub estimated_improvement: f64,
}

/// Tracks runtime execution metrics and recommends strategy switches.
pub struct RuntimeAdaptor {
    config: AdaptiveConfig,
    /// Actual vs estimated row counts per stage
    estimation_errors: parking_lot::RwLock<HashMap<usize, EstimationError>>,
    /// Strategy switches that have been applied
    switches: parking_lot::RwLock<Vec<StrategySwitch>>,
    /// Current strategy per stage
    current_strategies: parking_lot::RwLock<HashMap<usize, JoinStrategy>>,
}

/// Tracks estimation accuracy for a stage.
#[derive(Debug, Clone)]
pub struct EstimationError {
    pub stage_id: usize,
    pub estimated_rows: usize,
    pub actual_rows: usize,
    pub error_ratio: f64,
}

impl EstimationError {
    pub fn new(stage_id: usize, estimated_rows: usize, actual_rows: usize) -> Self {
        let error_ratio = if estimated_rows > 0 {
            actual_rows as f64 / estimated_rows as f64
        } else if actual_rows > 0 {
            f64::INFINITY
        } else {
            1.0
        };
        Self {
            stage_id,
            estimated_rows,
            actual_rows,
            error_ratio,
        }
    }

    /// Returns true if the estimation error is large enough to warrant re-optimization.
    pub fn is_significant(&self) -> bool {
        self.error_ratio > 10.0 || self.error_ratio < 0.1
    }
}

impl RuntimeAdaptor {
    /// Create a new runtime adaptor.
    pub fn new(config: AdaptiveConfig) -> Self {
        Self {
            config,
            estimation_errors: parking_lot::RwLock::new(HashMap::new()),
            switches: parking_lot::RwLock::new(Vec::new()),
            current_strategies: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Record the actual output of a stage and check if strategy should change.
    pub fn observe_stage(
        &self,
        stage_id: usize,
        estimated_rows: usize,
        actual_rows: usize,
        actual_bytes: usize,
    ) -> Option<StrategySwitch> {
        let error = EstimationError::new(stage_id, estimated_rows, actual_rows);
        self.estimation_errors
            .write()
            .insert(stage_id, error.clone());

        if !error.is_significant() || !self.config.dynamic_join_selection {
            return None;
        }

        let current = self
            .current_strategies
            .read()
            .get(&stage_id)
            .copied()
            .unwrap_or(JoinStrategy::HashJoin);

        let recommended = self.recommend_strategy(actual_rows, actual_bytes, current);

        if recommended != current {
            let switch = StrategySwitch {
                stage_id,
                from: current,
                to: recommended,
                reason: format!(
                    "Estimation error {:.1}x (est={}, actual={})",
                    error.error_ratio, estimated_rows, actual_rows
                ),
                estimated_improvement: self.estimate_improvement(current, recommended, actual_rows),
            };
            self.current_strategies
                .write()
                .insert(stage_id, recommended);
            self.switches.write().push(switch.clone());
            Some(switch)
        } else {
            None
        }
    }

    /// Recommend the best join strategy given actual metrics.
    fn recommend_strategy(
        &self,
        actual_rows: usize,
        actual_bytes: usize,
        _current: JoinStrategy,
    ) -> JoinStrategy {
        if actual_bytes < self.config.broadcast_join_threshold {
            JoinStrategy::BroadcastJoin
        } else if actual_rows < 1_000 {
            JoinStrategy::NestedLoopJoin
        } else {
            JoinStrategy::HashJoin
        }
    }

    /// Estimate the performance improvement of switching strategies.
    fn estimate_improvement(&self, from: JoinStrategy, to: JoinStrategy, rows: usize) -> f64 {
        let from_cost = Self::strategy_cost(from, rows);
        let to_cost = Self::strategy_cost(to, rows);
        if from_cost > 0.0 {
            to_cost / from_cost
        } else {
            1.0
        }
    }

    /// Simple cost model for join strategies.
    fn strategy_cost(strategy: JoinStrategy, rows: usize) -> f64 {
        let n = rows as f64;
        match strategy {
            JoinStrategy::HashJoin => n * 2.0, // O(n) build + O(n) probe
            JoinStrategy::SortMergeJoin => n * n.log2(), // O(n log n) sort + O(n) merge
            JoinStrategy::BroadcastJoin => n,  // O(n) broadcast + O(n) probe
            JoinStrategy::NestedLoopJoin => n * n, // O(n²)
        }
    }

    /// Get all strategy switches that have been applied.
    pub fn switches(&self) -> Vec<StrategySwitch> {
        self.switches.read().clone()
    }

    /// Get all estimation errors observed.
    pub fn estimation_errors(&self) -> Vec<EstimationError> {
        self.estimation_errors.read().values().cloned().collect()
    }

    /// Check if the adaptor has made any switches.
    pub fn has_adapted(&self) -> bool {
        !self.switches.read().is_empty()
    }

    /// Get the current strategy for a stage.
    pub fn current_strategy(&self, stage_id: usize) -> JoinStrategy {
        self.current_strategies
            .read()
            .get(&stage_id)
            .copied()
            .unwrap_or(JoinStrategy::HashJoin)
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

    #[test]
    fn test_estimation_error_significant() {
        // 20x overestimate → significant
        let err = EstimationError::new(0, 100, 2000);
        assert!(err.is_significant());

        // Close estimate → not significant
        let err = EstimationError::new(0, 100, 120);
        assert!(!err.is_significant());

        // Zero estimate with nonzero actual → significant
        let err = EstimationError::new(0, 0, 100);
        assert!(err.is_significant());
    }

    #[test]
    fn test_runtime_adaptor_no_switch_for_accurate_estimates() {
        let config = AdaptiveConfig::default();
        let adaptor = RuntimeAdaptor::new(config);

        // Accurate estimate → no switch
        let switch = adaptor.observe_stage(0, 1000, 1200, 50 * 1024 * 1024);
        assert!(switch.is_none());
        assert!(!adaptor.has_adapted());
    }

    #[test]
    fn test_runtime_adaptor_switch_to_broadcast() {
        let config = AdaptiveConfig::default();
        let adaptor = RuntimeAdaptor::new(config);

        // Estimated 1M rows but only got 100 rows → should switch to broadcast
        // (assuming bytes are below threshold)
        let switch = adaptor.observe_stage(0, 1_000_000, 100, 5_000);
        assert!(switch.is_some());
        let switch = switch.unwrap();
        assert_eq!(switch.to, JoinStrategy::BroadcastJoin);
        assert!(switch.estimated_improvement < 1.0); // improvement means ratio < 1
        assert!(adaptor.has_adapted());
        assert_eq!(adaptor.switches().len(), 1);
    }

    #[test]
    fn test_runtime_adaptor_current_strategy() {
        let config = AdaptiveConfig::default();
        let adaptor = RuntimeAdaptor::new(config);

        // Default strategy is HashJoin
        assert_eq!(adaptor.current_strategy(0), JoinStrategy::HashJoin);

        // After a switch, strategy should update
        adaptor.observe_stage(0, 1_000_000, 50, 1_000);
        assert_ne!(adaptor.current_strategy(0), JoinStrategy::HashJoin);
    }

    #[test]
    fn test_runtime_adaptor_estimation_errors() {
        let config = AdaptiveConfig::default();
        let adaptor = RuntimeAdaptor::new(config);

        adaptor.observe_stage(0, 100, 500, 50 * 1024 * 1024);
        adaptor.observe_stage(1, 200, 300, 50 * 1024 * 1024);

        let errors = adaptor.estimation_errors();
        assert_eq!(errors.len(), 2);
    }
}
