//! Partition Coalescing
//!
//! This module provides functionality to coalesce small partitions into
//! larger ones to reduce overhead and improve performance.

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};

use super::runtime_stats::{PartitionStats, StageStats};
use super::AdaptiveConfig;

/// Partition coalescer that merges small partitions.
pub struct PartitionCoalescer {
    /// Configuration
    config: AdaptiveConfig,
}

impl PartitionCoalescer {
    /// Create a new partition coalescer.
    pub fn new(config: AdaptiveConfig) -> Self {
        Self { config }
    }

    /// Check if coalescing should be applied and calculate target partition count.
    pub fn maybe_coalesce(&self, stats: &StageStats) -> Option<CoalesceDecision> {
        if !self.config.coalesce_partitions {
            return None;
        }

        if stats.partitions.is_empty() {
            return None;
        }

        // Calculate ideal number of partitions based on target size
        let total_bytes = stats.total_bytes;
        let target_partitions = (total_bytes / self.config.target_partition_size).max(1);
        let current_partitions = stats.partitions.len();

        // Only coalesce if we have significantly more partitions than needed
        if current_partitions > target_partitions * 2 {
            let target = target_partitions.max(self.config.min_partitions);
            Some(CoalesceDecision::Coalesce {
                from: current_partitions,
                to: target,
            })
        } else {
            None
        }
    }

    /// Create a coalesce plan that groups partitions.
    pub fn plan_coalesce(&self, stats: &StageStats, target_partitions: usize) -> CoalescePlan {
        let partitions: Vec<PartitionStats> = stats.partitions.clone();
        let target = target_partitions.max(1);

        // Sort partitions by size (descending) for better bin packing
        let mut sorted_partitions = partitions;
        sorted_partitions.sort_by(|a, b| b.byte_size.cmp(&a.byte_size));

        // Use greedy bin packing
        let mut groups: Vec<PartitionGroup> =
            (0..target).map(|id| PartitionGroup::new(id)).collect();

        for partition in sorted_partitions {
            // Find the group with the smallest total size
            let min_group = groups.iter_mut().min_by_key(|g| g.total_bytes).unwrap();

            min_group.add_partition(partition);
        }

        CoalescePlan { groups }
    }

    /// Apply coalescing strategy to determine partition groupings.
    pub fn apply_strategy(
        &self,
        partitions: &[PartitionStats],
        strategy: CoalesceStrategy,
    ) -> Vec<Vec<usize>> {
        match strategy {
            CoalesceStrategy::BySize { target_size } => {
                self.coalesce_by_size(partitions, target_size)
            }
            CoalesceStrategy::ByCount { target_count } => {
                self.coalesce_by_count(partitions, target_count)
            }
            CoalesceStrategy::Adjacent => self.coalesce_adjacent(partitions),
        }
    }

    /// Coalesce partitions to achieve target partition size.
    fn coalesce_by_size(
        &self,
        partitions: &[PartitionStats],
        target_size: usize,
    ) -> Vec<Vec<usize>> {
        let mut groups: Vec<Vec<usize>> = vec![];
        let mut current_group: Vec<usize> = vec![];
        let mut current_size = 0;

        for partition in partitions {
            if current_size + partition.byte_size > target_size && !current_group.is_empty() {
                groups.push(current_group);
                current_group = vec![];
                current_size = 0;
            }

            current_group.push(partition.partition_id);
            current_size += partition.byte_size;
        }

        if !current_group.is_empty() {
            groups.push(current_group);
        }

        groups
    }

    /// Coalesce partitions to achieve target partition count.
    fn coalesce_by_count(
        &self,
        partitions: &[PartitionStats],
        target_count: usize,
    ) -> Vec<Vec<usize>> {
        let target = target_count.max(1);
        let partitions_per_group = (partitions.len() + target - 1) / target;

        partitions
            .chunks(partitions_per_group)
            .map(|chunk| chunk.iter().map(|p| p.partition_id).collect())
            .collect()
    }

    /// Coalesce adjacent partitions (pairs).
    fn coalesce_adjacent(&self, partitions: &[PartitionStats]) -> Vec<Vec<usize>> {
        partitions
            .chunks(2)
            .map(|chunk| chunk.iter().map(|p| p.partition_id).collect())
            .collect()
    }
}

/// Coalescing strategy.
#[derive(Debug, Clone)]
pub enum CoalesceStrategy {
    /// Coalesce to achieve target partition size.
    BySize { target_size: usize },
    /// Coalesce to achieve target partition count.
    ByCount { target_count: usize },
    /// Coalesce adjacent partitions.
    Adjacent,
}

impl CoalesceStrategy {
    /// Create a by-size strategy.
    pub fn by_size(target_size: usize) -> Self {
        CoalesceStrategy::BySize { target_size }
    }

    /// Create a by-count strategy.
    pub fn by_count(target_count: usize) -> Self {
        CoalesceStrategy::ByCount { target_count }
    }

    /// Create an adjacent strategy.
    pub fn adjacent() -> Self {
        CoalesceStrategy::Adjacent
    }
}

/// Decision about whether to coalesce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoalesceDecision {
    /// No coalescing needed.
    None,
    /// Coalesce from `from` partitions to `to` partitions.
    Coalesce { from: usize, to: usize },
}

/// A group of partitions to be coalesced.
#[derive(Debug, Clone)]
pub struct PartitionGroup {
    /// Group identifier
    pub id: usize,
    /// Partition IDs in this group
    pub partition_ids: Vec<usize>,
    /// Total bytes in this group
    pub total_bytes: usize,
    /// Total rows in this group
    pub total_rows: usize,
}

impl PartitionGroup {
    /// Create a new partition group.
    pub fn new(id: usize) -> Self {
        Self {
            id,
            partition_ids: vec![],
            total_bytes: 0,
            total_rows: 0,
        }
    }

    /// Add a partition to this group.
    pub fn add_partition(&mut self, partition: PartitionStats) {
        self.partition_ids.push(partition.partition_id);
        self.total_bytes += partition.byte_size;
        self.total_rows += partition.row_count;
    }

    /// Check if this group is empty.
    pub fn is_empty(&self) -> bool {
        self.partition_ids.is_empty()
    }
}

/// Plan for coalescing partitions.
#[derive(Debug, Clone)]
pub struct CoalescePlan {
    /// Groups of partitions
    pub groups: Vec<PartitionGroup>,
}

impl CoalescePlan {
    /// Get the number of output partitions.
    pub fn num_output_partitions(&self) -> usize {
        self.groups.iter().filter(|g| !g.is_empty()).count()
    }

    /// Get partition IDs for a specific output partition.
    pub fn input_partitions(&self, output_partition: usize) -> Option<&[usize]> {
        self.groups
            .get(output_partition)
            .map(|g| g.partition_ids.as_slice())
    }
}

/// Coalesce multiple record batches into a single batch.
pub fn coalesce_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(BlazeError::invalid_argument(
            "Cannot coalesce empty batches",
        ));
    }

    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    let schema = batches[0].schema();
    concat_batches(&schema, batches)
        .map_err(|e| BlazeError::execution(format!("Failed to coalesce batches: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_partition_stats(sizes: &[(usize, usize, usize)]) -> Vec<PartitionStats> {
        sizes
            .iter()
            .map(|(id, rows, bytes)| PartitionStats {
                partition_id: *id,
                row_count: *rows,
                byte_size: *bytes,
                null_count: 0,
            })
            .collect()
    }

    #[test]
    fn test_coalesce_decision() {
        let config = AdaptiveConfig::default().with_target_partition_size(1024);

        let coalescer = PartitionCoalescer::new(config);

        // Many small partitions should trigger coalescing
        let stats = StageStats {
            stage_id: 0,
            total_rows: 100,
            total_bytes: 1024,
            partitions: make_partition_stats(&[
                (0, 10, 100),
                (1, 10, 100),
                (2, 10, 100),
                (3, 10, 100),
                (4, 10, 100),
                (5, 10, 100),
                (6, 10, 100),
                (7, 10, 100),
            ]),
            execution_time_ms: 0,
        };

        let decision = coalescer.maybe_coalesce(&stats);
        assert!(decision.is_some());
    }

    #[test]
    fn test_coalesce_by_count() {
        let config = AdaptiveConfig::default();
        let coalescer = PartitionCoalescer::new(config);

        let partitions = make_partition_stats(&[
            (0, 100, 1000),
            (1, 100, 1000),
            (2, 100, 1000),
            (3, 100, 1000),
        ]);

        let groups = coalescer.apply_strategy(&partitions, CoalesceStrategy::by_count(2));

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], vec![0, 1]);
        assert_eq!(groups[1], vec![2, 3]);
    }

    #[test]
    fn test_coalesce_by_size() {
        let config = AdaptiveConfig::default();
        let coalescer = PartitionCoalescer::new(config);

        let partitions =
            make_partition_stats(&[(0, 100, 100), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);

        let groups = coalescer.apply_strategy(&partitions, CoalesceStrategy::by_size(250));

        // Should group ~2-3 partitions together to reach 250 bytes
        assert!(groups.len() <= 3);
    }

    #[test]
    fn test_coalesce_adjacent() {
        let config = AdaptiveConfig::default();
        let coalescer = PartitionCoalescer::new(config);

        let partitions =
            make_partition_stats(&[(0, 100, 100), (1, 100, 100), (2, 100, 100), (3, 100, 100)]);

        let groups = coalescer.apply_strategy(&partitions, CoalesceStrategy::adjacent());

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], vec![0, 1]);
        assert_eq!(groups[1], vec![2, 3]);
    }

    #[test]
    fn test_partition_group() {
        let mut group = PartitionGroup::new(0);
        assert!(group.is_empty());

        group.add_partition(PartitionStats {
            partition_id: 1,
            row_count: 100,
            byte_size: 1000,
            null_count: 0,
        });

        assert!(!group.is_empty());
        assert_eq!(group.partition_ids, vec![1]);
        assert_eq!(group.total_bytes, 1000);
        assert_eq!(group.total_rows, 100);
    }

    #[test]
    fn test_coalesce_plan() {
        let config = AdaptiveConfig::default();
        let coalescer = PartitionCoalescer::new(config);

        let stats = StageStats {
            stage_id: 0,
            total_rows: 400,
            total_bytes: 4000,
            partitions: make_partition_stats(&[
                (0, 100, 1000),
                (1, 100, 1000),
                (2, 100, 1000),
                (3, 100, 1000),
            ]),
            execution_time_ms: 0,
        };

        let plan = coalescer.plan_coalesce(&stats, 2);

        assert_eq!(plan.num_output_partitions(), 2);
    }
}
