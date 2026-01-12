//! Adaptive Query Planner
//!
//! This module provides adaptive plan optimization based on runtime statistics.

use std::collections::HashSet;
use std::sync::RwLock;

use super::runtime_stats::StageStats;
use super::AdaptiveConfig;

/// Adaptive query planner that can modify execution plans at runtime.
pub struct AdaptivePlanner {
    /// Configuration
    config: AdaptiveConfig,
    /// Context for tracking adaptations
    context: RwLock<AdaptiveContext>,
}

impl AdaptivePlanner {
    /// Create a new adaptive planner.
    pub fn new(config: AdaptiveConfig) -> Self {
        Self {
            config,
            context: RwLock::new(AdaptiveContext::new()),
        }
    }

    /// Mark a stage for broadcast join.
    pub fn mark_for_broadcast(&self, stage_id: usize) {
        if let Ok(mut ctx) = self.context.write() {
            ctx.broadcast_stages.insert(stage_id);
        }
    }

    /// Check if a stage should use broadcast join.
    pub fn should_broadcast(&self, stage_id: usize) -> bool {
        self.context
            .read()
            .map(|ctx| ctx.broadcast_stages.contains(&stage_id))
            .unwrap_or(false)
    }

    /// Evaluate adaptive rules and return decisions.
    pub fn evaluate(&self, stats: &StageStats) -> Vec<AdaptiveDecision> {
        let mut decisions = Vec::new();

        // Evaluate each rule
        for rule in &self.rules() {
            if let Some(decision) = rule.evaluate(stats, &self.config) {
                decisions.push(decision);
            }
        }

        decisions
    }

    /// Get the adaptive rules.
    fn rules(&self) -> Vec<Box<dyn AdaptiveRule>> {
        vec![
            Box::new(BroadcastJoinRule),
            Box::new(PartitionPruningRule),
            Box::new(JoinReorderRule),
        ]
    }

    /// Get the current context.
    pub fn context(&self) -> AdaptiveContext {
        self.context.read().map(|c| c.clone()).unwrap_or_default()
    }
}

/// Context for adaptive execution decisions.
#[derive(Debug, Clone, Default)]
pub struct AdaptiveContext {
    /// Stages that should use broadcast join
    pub broadcast_stages: HashSet<usize>,
    /// Stages that have been re-optimized
    pub reoptimized_stages: HashSet<usize>,
    /// Number of partitions to coalesce to
    pub coalesce_targets: std::collections::HashMap<usize, usize>,
    /// Skewed partitions per stage
    pub skewed_partitions: std::collections::HashMap<usize, Vec<usize>>,
}

impl AdaptiveContext {
    /// Create a new adaptive context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a coalesce target for a stage.
    pub fn set_coalesce_target(&mut self, stage_id: usize, partitions: usize) {
        self.coalesce_targets.insert(stage_id, partitions);
    }

    /// Record skewed partitions for a stage.
    pub fn set_skewed_partitions(&mut self, stage_id: usize, partitions: Vec<usize>) {
        self.skewed_partitions.insert(stage_id, partitions);
    }

    /// Check if a stage needs re-optimization.
    pub fn needs_reoptimization(&self, stage_id: usize) -> bool {
        !self.reoptimized_stages.contains(&stage_id)
            && (self.broadcast_stages.contains(&stage_id)
                || self.coalesce_targets.contains_key(&stage_id)
                || self.skewed_partitions.contains_key(&stage_id))
    }
}

/// Trait for adaptive optimization rules.
pub trait AdaptiveRule: Send + Sync {
    /// Rule name.
    fn name(&self) -> &str;

    /// Check if this rule is applicable and return a decision.
    fn evaluate(
        &self,
        stats: &StageStats,
        config: &AdaptiveConfig,
    ) -> Option<AdaptiveDecision>;
}

/// Rule to convert sort-merge join to broadcast join for small datasets.
struct BroadcastJoinRule;

impl AdaptiveRule for BroadcastJoinRule {
    fn name(&self) -> &str {
        "BroadcastJoinRule"
    }

    fn evaluate(
        &self,
        stats: &StageStats,
        config: &AdaptiveConfig,
    ) -> Option<AdaptiveDecision> {
        if config.dynamic_join_selection && stats.total_bytes < config.broadcast_join_threshold {
            Some(AdaptiveDecision::UseBroadcastJoin { stage_id: stats.stage_id })
        } else {
            None
        }
    }
}

/// Rule to prune partitions based on runtime filter results.
struct PartitionPruningRule;

impl AdaptiveRule for PartitionPruningRule {
    fn name(&self) -> &str {
        "PartitionPruningRule"
    }

    fn evaluate(
        &self,
        stats: &StageStats,
        _config: &AdaptiveConfig,
    ) -> Option<AdaptiveDecision> {
        // Find empty partitions
        let empty_partitions: Vec<usize> = stats.partitions
            .iter()
            .filter(|p| p.row_count == 0)
            .map(|p| p.partition_id)
            .collect();

        if !empty_partitions.is_empty() {
            Some(AdaptiveDecision::PrunePartitions {
                stage_id: stats.stage_id,
                empty_partitions,
            })
        } else {
            None
        }
    }
}

/// Rule to reorder joins based on actual cardinalities.
struct JoinReorderRule;

impl AdaptiveRule for JoinReorderRule {
    fn name(&self) -> &str {
        "JoinReorderRule"
    }

    fn evaluate(
        &self,
        _stats: &StageStats,
        _config: &AdaptiveConfig,
    ) -> Option<AdaptiveDecision> {
        // Would check if this is a multi-join query
        // For now, return None as this is a placeholder
        None
    }
}

/// Decision made by the adaptive planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdaptiveDecision {
    /// No changes needed
    NoChange,
    /// Switch to broadcast join
    UseBroadcastJoin { stage_id: usize },
    /// Coalesce partitions
    CoalescePartitions { stage_id: usize, target: usize },
    /// Split skewed partitions
    SplitSkewedPartitions { stage_id: usize, partitions: Vec<usize> },
    /// Prune empty partitions
    PrunePartitions { stage_id: usize, empty_partitions: Vec<usize> },
}

impl AdaptiveDecision {
    /// Create a broadcast join decision.
    pub fn broadcast(stage_id: usize) -> Self {
        AdaptiveDecision::UseBroadcastJoin { stage_id }
    }

    /// Create a coalesce decision.
    pub fn coalesce(stage_id: usize, target: usize) -> Self {
        AdaptiveDecision::CoalescePartitions { stage_id, target }
    }

    /// Create a split skew decision.
    pub fn split_skew(stage_id: usize, partitions: Vec<usize>) -> Self {
        AdaptiveDecision::SplitSkewedPartitions { stage_id, partitions }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_planner_creation() {
        let config = AdaptiveConfig::default();
        let planner = AdaptivePlanner::new(config);

        assert!(!planner.should_broadcast(0));
    }

    #[test]
    fn test_mark_for_broadcast() {
        let config = AdaptiveConfig::default();
        let planner = AdaptivePlanner::new(config);

        planner.mark_for_broadcast(0);
        assert!(planner.should_broadcast(0));
        assert!(!planner.should_broadcast(1));
    }

    #[test]
    fn test_adaptive_context() {
        let mut ctx = AdaptiveContext::new();

        ctx.broadcast_stages.insert(0);
        ctx.set_coalesce_target(1, 4);
        ctx.set_skewed_partitions(2, vec![3, 5]);

        assert!(ctx.needs_reoptimization(0));
        assert!(ctx.needs_reoptimization(1));
        assert!(ctx.needs_reoptimization(2));
        assert!(!ctx.needs_reoptimization(3));
    }

    #[test]
    fn test_adaptive_decision() {
        let decision = AdaptiveDecision::broadcast(0);
        assert_eq!(decision, AdaptiveDecision::UseBroadcastJoin { stage_id: 0 });

        let decision = AdaptiveDecision::coalesce(1, 4);
        assert_eq!(decision, AdaptiveDecision::CoalescePartitions { stage_id: 1, target: 4 });
    }

    #[test]
    fn test_broadcast_join_rule_applicability() {
        let config = AdaptiveConfig::default()
            .with_broadcast_threshold(1024);

        let rule = BroadcastJoinRule;

        // Small dataset - should trigger broadcast decision
        let stats_small = StageStats {
            stage_id: 0,
            total_rows: 100,
            total_bytes: 512, // Under threshold
            partitions: vec![],
            execution_time_ms: 0,
        };

        // Large dataset - should not trigger
        let stats_large = StageStats {
            stage_id: 0,
            total_rows: 10000,
            total_bytes: 2048, // Over threshold
            partitions: vec![],
            execution_time_ms: 0,
        };

        assert!(rule.evaluate(&stats_small, &config).is_some());
        assert!(rule.evaluate(&stats_large, &config).is_none());
    }
}
