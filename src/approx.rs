//! Approximate query processing algorithms.
//!
//! This module provides probabilistic data structures for approximate query processing:
//! - HyperLogLog for cardinality estimation (APPROX_COUNT_DISTINCT)
//! - T-Digest for percentile/quantile estimation (APPROX_PERCENTILE)

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// HyperLogLog for cardinality estimation.
///
/// HyperLogLog uses a small fixed amount of memory to estimate the number of
/// distinct elements in a dataset with high accuracy.
///
/// Typical error rate: ~1.04 / sqrt(m) where m is the number of registers.
/// With 2^14 registers (16KB), error is approximately 0.8%.
#[derive(Debug, Clone)]
pub struct HyperLogLog {
    /// The registers (M[0..m-1])
    registers: Vec<u8>,
    /// Number of registers (m = 2^p)
    num_registers: usize,
    /// Precision parameter (p bits for bucket addressing)
    precision: u8,
}

impl HyperLogLog {
    /// Create a new HyperLogLog with the given precision.
    ///
    /// Precision determines the number of registers: m = 2^precision.
    /// Higher precision = more accuracy but more memory.
    ///
    /// Typical values:
    /// - 10: 1KB, ~3.2% error
    /// - 12: 4KB, ~1.6% error
    /// - 14: 16KB, ~0.8% error (default)
    /// - 16: 64KB, ~0.4% error
    pub fn new(precision: u8) -> Self {
        let precision = precision.clamp(4, 18);
        let num_registers = 1 << precision;
        Self {
            registers: vec![0; num_registers],
            num_registers,
            precision,
        }
    }

    /// Create with default precision (14 bits, ~0.8% error).
    pub fn default_precision() -> Self {
        Self::new(14)
    }

    /// Add a value to the HyperLogLog.
    pub fn add<T: Hash>(&mut self, value: &T) {
        let hash = self.hash_value(value);
        // Use top p bits for bucket index
        let bucket = (hash >> (64 - self.precision)) as usize;

        // Shift left by p bits to get the remaining bits at the top of the word
        // Then count leading zeros to find the position of the first 1-bit
        let shifted = hash << self.precision;

        // leading_zeros() on u64 can return up to 64 if shifted is 0
        // We need to cap it at (64 - precision) since that's our effective bit width
        let max_leading_zeros = 64 - self.precision as u32;
        let leading_zeros = shifted.leading_zeros().min(max_leading_zeros);

        // rho is 1 + the number of leading zeros (position of first 1-bit, 1-indexed)
        let rho = leading_zeros as u8 + 1;

        if rho > self.registers[bucket] {
            self.registers[bucket] = rho;
        }
    }

    /// Add a raw i64 value.
    pub fn add_i64(&mut self, value: i64) {
        self.add(&value);
    }

    /// Add a raw f64 value.
    pub fn add_f64(&mut self, value: f64) {
        self.add(&value.to_bits());
    }

    /// Add a string value.
    pub fn add_str(&mut self, value: &str) {
        self.add(&value);
    }

    /// Estimate the cardinality.
    pub fn estimate(&self) -> u64 {
        let m = self.num_registers as f64;

        // Calculate the indicator function sum: Z = sum(2^(-M[j]))
        let indicator_sum: f64 = self.registers.iter().map(|&r| 2.0_f64.powi(-(r as i32))).sum();

        // Alpha constant for bias correction
        let alpha = match self.precision {
            4 => 0.673,
            5 => 0.697,
            6 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / m),
        };

        // HyperLogLog estimate: E = alpha * m^2 / Z
        let raw_estimate = alpha * m * m / indicator_sum;

        // Apply corrections for small/large cardinalities
        let estimate = if raw_estimate <= 2.5 * m {
            // Small range correction
            let zeros = self.registers.iter().filter(|&&r| r == 0).count() as f64;
            if zeros > 0.0 {
                m * (m / zeros).ln()
            } else {
                raw_estimate
            }
        } else if raw_estimate <= (1u64 << 32) as f64 / 30.0 {
            // Intermediate range - no correction needed
            raw_estimate
        } else {
            // Large range correction
            let two_32 = (1u64 << 32) as f64;
            -two_32 * (1.0 - raw_estimate / two_32).ln()
        };

        estimate.round() as u64
    }

    /// Merge another HyperLogLog into this one.
    pub fn merge(&mut self, other: &HyperLogLog) {
        if self.precision != other.precision {
            return;
        }

        for i in 0..self.num_registers {
            if other.registers[i] > self.registers[i] {
                self.registers[i] = other.registers[i];
            }
        }
    }

    fn hash_value<T: Hash>(&self, value: &T) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::default_precision()
    }
}

/// A centroid in the T-Digest.
#[derive(Debug, Clone, Copy)]
struct Centroid {
    mean: f64,
    weight: f64,
}

impl Centroid {
    fn new(mean: f64, weight: f64) -> Self {
        Self { mean, weight }
    }
}

/// T-Digest for percentile/quantile estimation.
///
/// T-Digest provides accurate percentile estimates for streaming data
/// with sub-linear memory usage. It's particularly accurate at the tails
/// (very low or very high percentiles).
#[derive(Debug, Clone)]
pub struct TDigest {
    /// Centroids sorted by mean
    centroids: Vec<Centroid>,
    /// Compression factor (higher = more accuracy, more memory)
    compression: f64,
    /// Total weight of all centroids
    total_weight: f64,
    /// Maximum number of centroids before compression
    max_centroids: usize,
    /// Unmerged values buffer
    buffer: Vec<f64>,
    /// Buffer capacity before triggering merge
    buffer_capacity: usize,
}

impl TDigest {
    /// Create a new T-Digest with the given compression factor.
    ///
    /// Higher compression = more centroids = more accuracy but more memory.
    /// Typical values: 100-500.
    pub fn new(compression: f64) -> Self {
        let compression = compression.max(10.0);
        let max_centroids = (compression * 2.0) as usize;
        Self {
            centroids: Vec::with_capacity(max_centroids),
            compression,
            total_weight: 0.0,
            max_centroids,
            buffer: Vec::with_capacity(1000),
            buffer_capacity: 1000,
        }
    }

    /// Create with default compression (200).
    pub fn default_compression() -> Self {
        Self::new(200.0)
    }

    /// Add a value to the digest.
    pub fn add(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }

        self.buffer.push(value);
        if self.buffer.len() >= self.buffer_capacity {
            self.flush_buffer();
        }
    }

    /// Add a value with a weight.
    pub fn add_weighted(&mut self, value: f64, weight: f64) {
        if value.is_nan() || weight <= 0.0 {
            return;
        }

        // For weighted values, add directly to centroids
        self.add_centroid(Centroid::new(value, weight));
        if self.centroids.len() > self.max_centroids {
            self.compress();
        }
    }

    /// Estimate the value at the given quantile (0.0 to 1.0).
    pub fn quantile(&mut self, q: f64) -> Option<f64> {
        self.flush_buffer();

        if self.centroids.is_empty() {
            return None;
        }

        if self.centroids.len() == 1 {
            return Some(self.centroids[0].mean);
        }

        let q = q.clamp(0.0, 1.0);
        let target_weight = q * self.total_weight;

        // Find the centroid containing our target weight
        let mut cumulative = 0.0;
        for i in 0..self.centroids.len() {
            let c = &self.centroids[i];
            let lower = cumulative;
            let upper = cumulative + c.weight;

            if target_weight <= upper {
                if i == 0 {
                    // First centroid
                    if self.centroids.len() == 1 {
                        return Some(c.mean);
                    }
                    let next = &self.centroids[1];
                    let delta = (target_weight - lower) / c.weight;
                    return Some(c.mean + delta * (next.mean - c.mean) / 2.0);
                }

                if i == self.centroids.len() - 1 {
                    // Last centroid
                    let prev = &self.centroids[i - 1];
                    let delta = (target_weight - lower) / c.weight;
                    return Some(c.mean - (1.0 - delta) * (c.mean - prev.mean) / 2.0);
                }

                // Interpolate between neighboring centroids
                let prev = &self.centroids[i - 1];
                let next = &self.centroids[i + 1];

                let left = (prev.mean + c.mean) / 2.0;
                let right = (c.mean + next.mean) / 2.0;

                let ratio = (target_weight - lower) / c.weight;
                return Some(left + ratio * (right - left));
            }

            cumulative = upper;
        }

        // Target weight is at or beyond the last centroid
        Some(self.centroids.last().unwrap().mean)
    }

    /// Estimate the median (50th percentile).
    pub fn median(&mut self) -> Option<f64> {
        self.quantile(0.5)
    }

    /// Merge another T-Digest into this one.
    pub fn merge(&mut self, other: &TDigest) {
        // First, add any unflushed values from the other's buffer
        for &value in &other.buffer {
            self.buffer.push(value);
        }

        // Flush our buffer to handle the new values
        self.flush_buffer();

        // Now merge the centroids
        for c in &other.centroids {
            self.add_centroid(*c);
        }
        self.compress();
    }

    fn flush_buffer(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        self.buffer.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        for value in std::mem::take(&mut self.buffer) {
            self.add_centroid(Centroid::new(value, 1.0));
        }

        self.compress();
    }

    fn add_centroid(&mut self, c: Centroid) {
        // Binary search for insertion point
        let pos = self
            .centroids
            .binary_search_by(|probe| probe.mean.partial_cmp(&c.mean).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or_else(|e| e);

        self.centroids.insert(pos, c);
        self.total_weight += c.weight;
    }

    fn compress(&mut self) {
        if self.centroids.len() <= 1 {
            return;
        }

        // Sort by mean
        self.centroids
            .sort_by(|a, b| a.mean.partial_cmp(&b.mean).unwrap_or(std::cmp::Ordering::Equal));

        let mut new_centroids = Vec::with_capacity(self.max_centroids);
        let mut cumulative = 0.0;

        let mut i = 0;
        while i < self.centroids.len() {
            let mut merged = self.centroids[i];
            let q_left = cumulative / self.total_weight;

            // Calculate the maximum weight for this quantile
            let k_limit = self.compression * self.k_scale(q_left);

            // Try to merge neighboring centroids
            while i + 1 < self.centroids.len() {
                let next = &self.centroids[i + 1];
                let new_weight = merged.weight + next.weight;

                // Check if we can merge
                if new_weight <= k_limit {
                    // Merge centroids
                    merged.mean =
                        (merged.mean * merged.weight + next.mean * next.weight) / new_weight;
                    merged.weight = new_weight;
                    i += 1;
                } else {
                    break;
                }
            }

            new_centroids.push(merged);
            cumulative += merged.weight;
            i += 1;
        }

        self.centroids = new_centroids;
    }

    fn k_scale(&self, q: f64) -> f64 {
        // Scale function that allows more centroids near the tails
        let q = q.clamp(0.0, 1.0);
        2.0 * q.min(1.0 - q)
    }
}

impl Default for TDigest {
    fn default() -> Self {
        Self::default_compression()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hyperloglog_basic() {
        let mut hll = HyperLogLog::new(12);

        // Add some distinct values
        for i in 0..10000 {
            hll.add(&i);
        }

        let estimate = hll.estimate();
        // Should be within ~3% of actual count
        assert!(estimate >= 9500 && estimate <= 10500, "Estimate was {}", estimate);
    }

    #[test]
    fn test_hyperloglog_duplicates() {
        let mut hll = HyperLogLog::new(14);

        // Add lots of duplicates
        for _ in 0..10000 {
            for i in 0..100 {
                hll.add(&i);
            }
        }

        let estimate = hll.estimate();
        // Should estimate ~100 distinct values
        assert!(estimate >= 90 && estimate <= 110, "Estimate was {}", estimate);
    }

    #[test]
    fn test_hyperloglog_merge() {
        let mut hll1 = HyperLogLog::new(12);
        let mut hll2 = HyperLogLog::new(12);

        for i in 0..5000 {
            hll1.add(&i);
        }
        for i in 5000..10000 {
            hll2.add(&i);
        }

        hll1.merge(&hll2);
        let estimate = hll1.estimate();
        assert!(estimate >= 9500 && estimate <= 10500, "Estimate was {}", estimate);
    }

    #[test]
    fn test_tdigest_basic() {
        let mut td = TDigest::new(100.0);

        // Add values 0..1000
        for i in 0..1000 {
            td.add(i as f64);
        }

        // Test median (should be ~500)
        let median = td.median().unwrap();
        assert!((median - 500.0).abs() < 50.0, "Median was {}", median);

        // Test 90th percentile (should be ~900)
        let p90 = td.quantile(0.9).unwrap();
        assert!((p90 - 900.0).abs() < 50.0, "P90 was {}", p90);

        // Test 10th percentile (should be ~100)
        let p10 = td.quantile(0.1).unwrap();
        assert!((p10 - 100.0).abs() < 50.0, "P10 was {}", p10);
    }

    #[test]
    fn test_tdigest_empty() {
        let mut td = TDigest::new(100.0);
        assert!(td.median().is_none());
        assert!(td.quantile(0.5).is_none());
    }

    #[test]
    fn test_tdigest_single_value() {
        let mut td = TDigest::new(100.0);
        td.add(42.0);

        assert_eq!(td.median().unwrap(), 42.0);
        assert_eq!(td.quantile(0.0).unwrap(), 42.0);
        assert_eq!(td.quantile(1.0).unwrap(), 42.0);
    }

    #[test]
    fn test_tdigest_merge() {
        let mut td1 = TDigest::new(100.0);
        let mut td2 = TDigest::new(100.0);

        for i in 0..500 {
            td1.add(i as f64);
        }
        for i in 500..1000 {
            td2.add(i as f64);
        }

        td1.merge(&td2);
        let median = td1.median().unwrap();
        assert!((median - 500.0).abs() < 50.0, "Median was {}", median);
    }
}
