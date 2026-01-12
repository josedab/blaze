//! Skew Handling
//!
//! This module provides functionality to detect and handle data skew
//! at runtime to improve query performance.

use crate::error::Result;

use super::runtime_stats::{PartitionStats, StageStats};
use super::AdaptiveConfig;

/// Handler for data skew in partitions.
pub struct SkewHandler {
    /// Configuration
    config: AdaptiveConfig,
    /// Skew detector
    detector: SkewDetector,
}

impl SkewHandler {
    /// Create a new skew handler.
    pub fn new(config: AdaptiveConfig) -> Self {
        Self {
            detector: SkewDetector::new(config.skew_threshold),
            config,
        }
    }

    /// Detect skew in the given stats.
    pub fn detect_skew(&self, stats: &StageStats) -> Option<SkewInfo> {
        if !self.config.handle_skew {
            return None;
        }

        self.detector.detect(stats)
    }

    /// Apply mitigation strategy for detected skew.
    pub fn apply_mitigation(&self, stats: &StageStats) -> Result<SkewMitigation> {
        let skew_info = match self.detect_skew(stats) {
            Some(info) => info,
            None => return Ok(SkewMitigation::None),
        };

        // Determine mitigation strategy based on skew severity
        let mitigation = if skew_info.skew_ratio > 10.0 {
            // Severe skew - split into multiple sub-partitions
            let split_factor = (skew_info.skew_ratio / 2.0).ceil() as usize;
            SkewMitigation::Split {
                partition_ids: skew_info.skewed_partitions.clone(),
                split_factor,
            }
        } else {
            // Moderate skew - use salting
            SkewMitigation::Salt {
                partition_ids: skew_info.skewed_partitions.clone(),
                salt_factor: 4,
            }
        };

        Ok(mitigation)
    }

    /// Calculate optimal split factor for a skewed partition.
    pub fn calculate_split_factor(&self, partition_size: usize, target_size: usize) -> usize {
        if target_size == 0 {
            return 1;
        }
        ((partition_size as f64 / target_size as f64).ceil() as usize).max(1)
    }
}

/// Detector for data skew.
#[derive(Debug, Clone)]
pub struct SkewDetector {
    /// Threshold for detecting skew (ratio of max to median)
    threshold: f64,
}

impl SkewDetector {
    /// Create a new skew detector.
    pub fn new(threshold: f64) -> Self {
        Self { threshold }
    }

    /// Detect skew in the given stats.
    pub fn detect(&self, stats: &StageStats) -> Option<SkewInfo> {
        if stats.partitions.is_empty() {
            return None;
        }

        let median = stats.median_partition_size();
        if median == 0 {
            return None;
        }

        let max = stats.max_partition_size();
        let skew_ratio = max as f64 / median as f64;

        if skew_ratio <= self.threshold {
            return None;
        }

        // Find skewed partitions
        let skew_threshold_bytes = (median as f64 * self.threshold) as usize;
        let skewed_partitions: Vec<usize> = stats
            .partitions
            .iter()
            .filter(|p| p.byte_size > skew_threshold_bytes)
            .map(|p| p.partition_id)
            .collect();

        Some(SkewInfo {
            skew_ratio,
            skewed_partitions,
            median_size: median,
            max_size: max,
        })
    }

    /// Check if a specific partition is skewed.
    pub fn is_partition_skewed(&self, partition: &PartitionStats, median_size: usize) -> bool {
        if median_size == 0 {
            return false;
        }
        let ratio = partition.byte_size as f64 / median_size as f64;
        ratio > self.threshold
    }
}

impl Default for SkewDetector {
    fn default() -> Self {
        Self::new(5.0)
    }
}

/// Information about detected skew.
#[derive(Debug, Clone)]
pub struct SkewInfo {
    /// Ratio of max partition size to median
    pub skew_ratio: f64,
    /// IDs of skewed partitions
    pub skewed_partitions: Vec<usize>,
    /// Median partition size
    pub median_size: usize,
    /// Maximum partition size
    pub max_size: usize,
}

impl SkewInfo {
    /// Get the severity of the skew.
    pub fn severity(&self) -> SkewSeverity {
        if self.skew_ratio > 10.0 {
            SkewSeverity::Severe
        } else if self.skew_ratio > 5.0 {
            SkewSeverity::Moderate
        } else {
            SkewSeverity::Mild
        }
    }

    /// Get the number of skewed partitions.
    pub fn num_skewed_partitions(&self) -> usize {
        self.skewed_partitions.len()
    }
}

/// Severity level of skew.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkewSeverity {
    /// Mild skew (ratio 1-5x)
    Mild,
    /// Moderate skew (ratio 5-10x)
    Moderate,
    /// Severe skew (ratio > 10x)
    Severe,
}

/// Mitigation strategy for handling skew.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkewMitigation {
    /// No mitigation needed
    None,
    /// Split skewed partitions into sub-partitions
    Split {
        /// Partition IDs to split
        partition_ids: Vec<usize>,
        /// Number of sub-partitions per skewed partition
        split_factor: usize,
    },
    /// Apply salting to skewed partitions
    Salt {
        /// Partition IDs to salt
        partition_ids: Vec<usize>,
        /// Salt factor (number of salts)
        salt_factor: usize,
    },
    /// Replicate small side of join for skewed keys
    Replicate {
        /// Skewed key values
        skewed_keys: Vec<String>,
    },
}

impl SkewMitigation {
    /// Check if any mitigation is needed.
    pub fn is_needed(&self) -> bool {
        !matches!(self, SkewMitigation::None)
    }

    /// Create a split mitigation.
    pub fn split(partition_ids: Vec<usize>, split_factor: usize) -> Self {
        SkewMitigation::Split { partition_ids, split_factor }
    }

    /// Create a salt mitigation.
    pub fn salt(partition_ids: Vec<usize>, salt_factor: usize) -> Self {
        SkewMitigation::Salt { partition_ids, salt_factor }
    }
}

/// Statistics about skew handling operations.
#[derive(Debug, Clone, Default)]
pub struct SkewStats {
    /// Number of skewed partitions detected
    pub skewed_partitions_detected: usize,
    /// Number of partitions split
    pub partitions_split: usize,
    /// Number of partitions salted
    pub partitions_salted: usize,
    /// Total sub-partitions created from splitting
    pub sub_partitions_created: usize,
}

impl SkewStats {
    /// Create new skew stats.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a split operation.
    pub fn record_split(&mut self, partitions: usize, sub_partitions: usize) {
        self.partitions_split += partitions;
        self.sub_partitions_created += sub_partitions;
    }

    /// Record a salt operation.
    pub fn record_salt(&mut self, partitions: usize) {
        self.partitions_salted += partitions;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_stats_with_skew() -> StageStats {
        StageStats {
            stage_id: 0,
            total_rows: 1200,
            total_bytes: 12000,
            partitions: vec![
                PartitionStats { partition_id: 0, row_count: 100, byte_size: 1000, null_count: 0 },
                PartitionStats { partition_id: 1, row_count: 100, byte_size: 1000, null_count: 0 },
                PartitionStats { partition_id: 2, row_count: 1000, byte_size: 10000, null_count: 0 }, // Skewed
            ],
            execution_time_ms: 0,
        }
    }

    fn make_stats_no_skew() -> StageStats {
        StageStats {
            stage_id: 0,
            total_rows: 300,
            total_bytes: 3000,
            partitions: vec![
                PartitionStats { partition_id: 0, row_count: 100, byte_size: 1000, null_count: 0 },
                PartitionStats { partition_id: 1, row_count: 100, byte_size: 1000, null_count: 0 },
                PartitionStats { partition_id: 2, row_count: 100, byte_size: 1000, null_count: 0 },
            ],
            execution_time_ms: 0,
        }
    }

    #[test]
    fn test_skew_detector_detects_skew() {
        let detector = SkewDetector::new(5.0);
        let stats = make_stats_with_skew();

        let result = detector.detect(&stats);
        assert!(result.is_some());

        let info = result.unwrap();
        assert!(info.skew_ratio > 5.0);
        assert_eq!(info.skewed_partitions, vec![2]);
    }

    #[test]
    fn test_skew_detector_no_skew() {
        let detector = SkewDetector::new(5.0);
        let stats = make_stats_no_skew();

        let result = detector.detect(&stats);
        assert!(result.is_none());
    }

    #[test]
    fn test_skew_handler() {
        let config = AdaptiveConfig::default()
            .with_skew_threshold(5.0);
        let handler = SkewHandler::new(config);

        let stats = make_stats_with_skew();
        let skew_info = handler.detect_skew(&stats);
        assert!(skew_info.is_some());

        let mitigation = handler.apply_mitigation(&stats).unwrap();
        assert!(mitigation.is_needed());
    }

    #[test]
    fn test_skew_severity() {
        let mild = SkewInfo {
            skew_ratio: 3.0,
            skewed_partitions: vec![0],
            median_size: 1000,
            max_size: 3000,
        };
        assert_eq!(mild.severity(), SkewSeverity::Mild);

        let moderate = SkewInfo {
            skew_ratio: 7.0,
            skewed_partitions: vec![0],
            median_size: 1000,
            max_size: 7000,
        };
        assert_eq!(moderate.severity(), SkewSeverity::Moderate);

        let severe = SkewInfo {
            skew_ratio: 15.0,
            skewed_partitions: vec![0],
            median_size: 1000,
            max_size: 15000,
        };
        assert_eq!(severe.severity(), SkewSeverity::Severe);
    }

    #[test]
    fn test_skew_mitigation() {
        let split = SkewMitigation::split(vec![1, 2], 4);
        assert!(split.is_needed());

        let salt = SkewMitigation::salt(vec![1], 8);
        assert!(salt.is_needed());

        let none = SkewMitigation::None;
        assert!(!none.is_needed());
    }

    #[test]
    fn test_calculate_split_factor() {
        let config = AdaptiveConfig::default();
        let handler = SkewHandler::new(config);

        // 10000 bytes partition, 2000 target size -> 5 splits
        assert_eq!(handler.calculate_split_factor(10000, 2000), 5);

        // 5000 bytes partition, 2000 target size -> 3 splits
        assert_eq!(handler.calculate_split_factor(5000, 2000), 3);

        // Edge case: target size 0
        assert_eq!(handler.calculate_split_factor(5000, 0), 1);
    }

    #[test]
    fn test_skew_stats() {
        let mut stats = SkewStats::new();

        stats.skewed_partitions_detected = 2;
        stats.record_split(2, 8);
        stats.record_salt(1);

        assert_eq!(stats.partitions_split, 2);
        assert_eq!(stats.sub_partitions_created, 8);
        assert_eq!(stats.partitions_salted, 1);
    }
}
