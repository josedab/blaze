//! Approximate query processing algorithms.
//!
//! This module provides probabilistic data structures for approximate query processing:
//! - HyperLogLog for cardinality estimation (APPROX_COUNT_DISTINCT)
//! - T-Digest for percentile/quantile estimation (APPROX_PERCENTILE)
//! - Auto-sampling for large datasets
//! - Error bound estimation with confidence intervals

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::error::{BlazeError, Result};

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

    /// Return the standard error rate for this precision.
    pub fn standard_error(&self) -> f64 {
        let m = self.num_registers as f64;
        1.04 / m.sqrt()
    }

    /// Estimate cardinality with confidence interval.
    /// Returns (estimate, lower_bound, upper_bound) at ~95% confidence (2 standard errors).
    pub fn estimate_with_bounds(&self) -> (u64, u64, u64) {
        let estimate = self.estimate();
        let se = self.standard_error();
        let margin = (estimate as f64 * se * 2.0).ceil() as u64;
        let lower = estimate.saturating_sub(margin);
        let upper = estimate.saturating_add(margin);
        (estimate, lower, upper)
    }

    /// Add a raw i32 value.
    pub fn add_i32(&mut self, value: i32) {
        self.add(&(value as i64));
    }

    /// Add a raw u64 value.
    pub fn add_u64(&mut self, value: u64) {
        self.add(&value);
    }

    /// Estimate the cardinality.
    pub fn estimate(&self) -> u64 {
        let m = self.num_registers as f64;

        // Calculate the indicator function sum: Z = sum(2^(-M[j]))
        let indicator_sum: f64 = self
            .registers
            .iter()
            .map(|&r| 2.0_f64.powi(-(r as i32)))
            .sum();

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

        self.buffer
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        for value in std::mem::take(&mut self.buffer) {
            self.add_centroid(Centroid::new(value, 1.0));
        }

        self.compress();
    }

    fn add_centroid(&mut self, c: Centroid) {
        // Binary search for insertion point
        let pos = self
            .centroids
            .binary_search_by(|probe| {
                probe
                    .mean
                    .partial_cmp(&c.mean)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap_or_else(|e| e);

        self.centroids.insert(pos, c);
        self.total_weight += c.weight;
    }

    fn compress(&mut self) {
        if self.centroids.len() <= 1 {
            return;
        }

        // Sort by mean
        self.centroids.sort_by(|a, b| {
            a.mean
                .partial_cmp(&b.mean)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

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

/// Result of an approximate query with error bounds.
#[derive(Debug, Clone)]
pub struct ApproxResult {
    /// The estimated value
    pub estimate: f64,
    /// Lower bound at 95% confidence
    pub lower_bound: f64,
    /// Upper bound at 95% confidence
    pub upper_bound: f64,
    /// Relative error estimate (0.0 to 1.0)
    pub relative_error: f64,
}

impl ApproxResult {
    pub fn new(estimate: f64, relative_error: f64) -> Self {
        let margin = estimate.abs() * relative_error;
        Self {
            estimate,
            lower_bound: estimate - margin,
            upper_bound: estimate + margin,
            relative_error,
        }
    }

    pub fn exact(value: f64) -> Self {
        Self {
            estimate: value,
            lower_bound: value,
            upper_bound: value,
            relative_error: 0.0,
        }
    }
}

/// Auto-sampling configuration for approximate queries on large datasets.
#[derive(Debug, Clone)]
pub struct SamplingConfig {
    /// Target sample size (number of rows)
    pub sample_size: usize,
    /// Seed for reproducible sampling
    pub seed: Option<u64>,
    /// Maximum acceptable relative error
    pub max_error: f64,
}

impl Default for SamplingConfig {
    fn default() -> Self {
        Self {
            sample_size: 100_000,
            seed: None,
            max_error: 0.05,
        }
    }
}

impl SamplingConfig {
    pub fn new(sample_size: usize) -> Self {
        Self {
            sample_size,
            ..Default::default()
        }
    }

    /// Compute the sampling rate for a given total row count.
    /// Returns 1.0 if the dataset is smaller than the sample size (no sampling needed).
    pub fn sampling_rate(&self, total_rows: usize) -> f64 {
        if total_rows <= self.sample_size {
            1.0
        } else {
            self.sample_size as f64 / total_rows as f64
        }
    }

    /// Determine if sampling is needed for a given row count.
    pub fn should_sample(&self, total_rows: usize) -> bool {
        total_rows > self.sample_size
    }
}

/// Reservoir sampling for uniform random sampling from a stream.
#[derive(Debug, Clone)]
pub struct ReservoirSampler {
    reservoir: Vec<f64>,
    capacity: usize,
    count: usize,
    rng_state: u64,
}

impl ReservoirSampler {
    pub fn new(capacity: usize) -> Self {
        Self {
            reservoir: Vec::with_capacity(capacity),
            capacity,
            count: 0,
            rng_state: 12345,
        }
    }

    pub fn with_seed(capacity: usize, seed: u64) -> Self {
        Self {
            reservoir: Vec::with_capacity(capacity),
            capacity,
            count: 0,
            rng_state: seed,
        }
    }

    pub fn add(&mut self, value: f64) {
        self.count += 1;
        if self.reservoir.len() < self.capacity {
            self.reservoir.push(value);
        } else {
            let j = self.next_random() % self.count;
            if j < self.capacity {
                self.reservoir[j] = value;
            }
        }
    }

    pub fn samples(&self) -> &[f64] {
        &self.reservoir
    }

    pub fn total_count(&self) -> usize {
        self.count
    }

    // Simple xorshift64 PRNG for deterministic sampling
    fn next_random(&mut self) -> usize {
        self.rng_state ^= self.rng_state << 13;
        self.rng_state ^= self.rng_state >> 7;
        self.rng_state ^= self.rng_state << 17;
        self.rng_state as usize
    }
}

/// Count-Min Sketch for frequency estimation.
///
/// Estimates the frequency of elements in a data stream using sub-linear space.
/// Uses multiple hash functions to map elements to a table of counters,
/// returning the minimum count across all hash functions as the estimate.
#[derive(Debug, Clone)]
pub struct CountMinSketch {
    /// 2D table of counters (depth x width)
    table: Vec<Vec<u32>>,
    /// Number of columns
    width: usize,
    /// Number of rows (hash functions)
    depth: usize,
    /// Seeds for each hash function
    seeds: Vec<u64>,
    /// Total number of elements added
    total_count: u64,
}

impl CountMinSketch {
    /// Create a new Count-Min Sketch with the given width and depth.
    ///
    /// - `width`: Number of columns (higher = less error)
    /// - `depth`: Number of rows/hash functions (higher = lower failure probability)
    pub fn new(width: usize, depth: usize) -> Self {
        let width = width.max(1);
        let depth = depth.max(1);
        let seeds: Vec<u64> = (0..depth)
            .map(|i| 0x517cc1b727220a95u64.wrapping_add(i as u64))
            .collect();
        Self {
            table: vec![vec![0u32; width]; depth],
            width,
            depth,
            seeds,
            total_count: 0,
        }
    }

    /// Create a Count-Min Sketch sized for a target error rate.
    ///
    /// - `epsilon`: Maximum overcount as a fraction of total count (e.g., 0.01 for 1%)
    /// - `delta`: Probability that the estimate exceeds epsilon error (e.g., 0.01 for 99% confidence)
    pub fn with_error_rate(epsilon: f64, delta: f64) -> Self {
        let width = (std::f64::consts::E / epsilon).ceil() as usize;
        let depth = (1.0 / delta).ln().ceil() as usize;
        Self::new(width, depth)
    }

    /// Add a hashable value to the sketch.
    pub fn add<T: Hash>(&mut self, value: &T) {
        for i in 0..self.depth {
            let hash = Self::hash_with_seed(value, self.seeds[i]);
            let col = (hash as usize) % self.width;
            self.table[i][col] = self.table[i][col].saturating_add(1);
        }
        self.total_count += 1;
    }

    /// Add a string value to the sketch.
    pub fn add_str(&mut self, value: &str) {
        self.add(&value);
    }

    /// Estimate the frequency of a hashable value.
    ///
    /// Returns the minimum count across all hash functions, which is an
    /// upper bound on the true frequency.
    pub fn estimate<T: Hash>(&self, value: &T) -> u32 {
        let mut min_count = u32::MAX;
        for i in 0..self.depth {
            let hash = Self::hash_with_seed(value, self.seeds[i]);
            let col = (hash as usize) % self.width;
            min_count = min_count.min(self.table[i][col]);
        }
        min_count
    }

    /// Estimate the frequency of a string value.
    pub fn estimate_str(&self, value: &str) -> u32 {
        self.estimate(&value)
    }

    /// Merge another Count-Min Sketch into this one.
    ///
    /// Both sketches must have the same width and depth.
    pub fn merge(&mut self, other: &CountMinSketch) -> Result<()> {
        if self.width != other.width || self.depth != other.depth {
            return Err(BlazeError::invalid_argument(
                "Cannot merge CountMinSketch with different dimensions",
            ));
        }
        for i in 0..self.depth {
            for j in 0..self.width {
                self.table[i][j] = self.table[i][j].saturating_add(other.table[i][j]);
            }
        }
        self.total_count += other.total_count;
        Ok(())
    }

    /// Return the total number of elements added.
    pub fn total_count(&self) -> u64 {
        self.total_count
    }

    /// Approximate memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.width * self.depth * std::mem::size_of::<u32>()
            + self.depth * std::mem::size_of::<u64>()
    }

    fn hash_with_seed<T: Hash>(value: &T, seed: u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        value.hash(&mut hasher);
        hasher.finish()
    }
}

/// Bloom filter for probabilistic set membership testing.
///
/// A Bloom filter can definitively say an element is NOT in the set,
/// but may produce false positives (saying an element is present when it isn't).
/// It never produces false negatives.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    /// Bit array
    bits: Vec<bool>,
    /// Number of bits in the filter
    num_bits: usize,
    /// Number of hash functions
    num_hashes: usize,
    /// Number of elements inserted
    count: usize,
    /// Seeds for each hash function
    seeds: Vec<u64>,
}

impl BloomFilter {
    /// Create a new Bloom filter with the given number of bits and hash functions.
    pub fn new(num_bits: usize, num_hashes: usize) -> Self {
        let num_bits = num_bits.max(1);
        let num_hashes = num_hashes.max(1);
        let seeds: Vec<u64> = (0..num_hashes)
            .map(|i| 0x9e3779b97f4a7c15u64.wrapping_add(i as u64))
            .collect();
        Self {
            bits: vec![false; num_bits],
            num_bits,
            num_hashes,
            count: 0,
            seeds,
        }
    }

    /// Create a Bloom filter sized for an expected number of items and false positive probability.
    ///
    /// - `expected_items`: Expected number of distinct elements
    /// - `fpp`: Desired false positive probability (e.g., 0.01 for 1%)
    pub fn with_capacity(expected_items: usize, fpp: f64) -> Self {
        let fpp = fpp.clamp(1e-10, 1.0);
        let num_bits =
            (-(expected_items as f64) * fpp.ln() / (2.0_f64.ln().powi(2))).ceil() as usize;
        let num_hashes = ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln()).ceil() as usize;
        Self::new(num_bits.max(1), num_hashes.max(1))
    }

    /// Insert a hashable value into the filter.
    pub fn insert<T: Hash>(&mut self, value: &T) {
        for i in 0..self.num_hashes {
            let idx = self.hash_index(value, i);
            self.bits[idx] = true;
        }
        self.count += 1;
    }

    /// Insert a string value into the filter.
    pub fn insert_str(&mut self, value: &str) {
        self.insert(&value);
    }

    /// Check if a hashable value might be in the filter.
    ///
    /// Returns `false` if the value is definitely not in the set.
    /// Returns `true` if the value is probably in the set (may be a false positive).
    pub fn contains<T: Hash>(&self, value: &T) -> bool {
        for i in 0..self.num_hashes {
            let idx = self.hash_index(value, i);
            if !self.bits[idx] {
                return false;
            }
        }
        true
    }

    /// Check if a string value might be in the filter.
    pub fn contains_str(&self, value: &str) -> bool {
        self.contains(&value)
    }

    /// Estimate the current false positive rate based on the number of bits set.
    pub fn false_positive_rate(&self) -> f64 {
        let bits_set = self.bits.iter().filter(|&&b| b).count() as f64;
        let fill_ratio = bits_set / self.num_bits as f64;
        fill_ratio.powi(self.num_hashes as i32)
    }

    /// Return the number of elements inserted.
    pub fn count(&self) -> usize {
        self.count
    }

    /// Merge another Bloom filter into this one (set union).
    ///
    /// Both filters must have the same number of bits and hash functions.
    pub fn merge(&mut self, other: &BloomFilter) -> Result<()> {
        if self.num_bits != other.num_bits || self.num_hashes != other.num_hashes {
            return Err(BlazeError::invalid_argument(
                "Cannot merge BloomFilters with different parameters",
            ));
        }
        for i in 0..self.num_bits {
            self.bits[i] = self.bits[i] || other.bits[i];
        }
        self.count += other.count;
        Ok(())
    }

    /// Approximate memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.num_bits * std::mem::size_of::<bool>() + self.num_hashes * std::mem::size_of::<u64>()
    }

    fn hash_index<T: Hash>(&self, value: &T, hash_fn: usize) -> usize {
        let mut hasher = DefaultHasher::new();
        self.seeds[hash_fn].hash(&mut hasher);
        value.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_bits
    }
}

/// Approximate aggregate function types available via SQL.
///
/// Each variant maps to a SQL function name and carries the parameters
/// needed to construct the underlying probabilistic data structure.
#[derive(Debug, Clone)]
pub enum ApproxAggregateFunction {
    /// APPROX_COUNT_DISTINCT using HyperLogLog.
    ApproxCountDistinct {
        /// HyperLogLog precision (4-18)
        precision: u8,
    },
    /// APPROX_PERCENTILE using T-Digest.
    ApproxPercentile {
        /// Target percentile (0.0-1.0)
        percentile: f64,
        /// T-Digest compression factor
        compression: usize,
    },
    /// APPROX_TOP_K using Count-Min Sketch.
    ApproxTopK {
        /// Number of top elements to track
        k: usize,
        /// Count-Min Sketch width
        width: usize,
        /// Count-Min Sketch depth
        depth: usize,
    },
    /// APPROX_MEDIAN using T-Digest.
    ApproxMedian {
        /// T-Digest compression factor
        compression: usize,
    },
}

impl ApproxAggregateFunction {
    /// Return the SQL function name for this aggregate.
    pub fn sql_name(&self) -> &'static str {
        match self {
            Self::ApproxCountDistinct { .. } => "APPROX_COUNT_DISTINCT",
            Self::ApproxPercentile { .. } => "APPROX_PERCENTILE",
            Self::ApproxTopK { .. } => "APPROX_TOP_K",
            Self::ApproxMedian { .. } => "APPROX_MEDIAN",
        }
    }
}

/// Sampling method for TABLESAMPLE.
#[derive(Debug, Clone, PartialEq)]
pub enum SampleMethod {
    /// Bernoulli sampling: each row is independently included with a given probability.
    Bernoulli,
    /// System sampling: blocks of rows are included with a given probability.
    System,
    /// Reservoir sampling: exactly `size` rows are selected uniformly at random.
    Reservoir {
        /// Fixed sample size
        size: usize,
    },
}

/// Configuration for SQL TABLESAMPLE clause.
///
/// Supports BERNOULLI (row-level), SYSTEM (block-level), and RESERVOIR sampling.
#[derive(Debug, Clone)]
pub struct TablesampleConfig {
    /// The sampling method
    pub method: SampleMethod,
    /// Sampling percentage (0.0-100.0) for Bernoulli/System methods
    pub percentage: f64,
    /// Optional seed for reproducible results
    pub seed: Option<u64>,
}

impl TablesampleConfig {
    /// Create a Bernoulli (row-level) TABLESAMPLE configuration.
    pub fn bernoulli(percentage: f64) -> Self {
        Self {
            method: SampleMethod::Bernoulli,
            percentage: percentage.clamp(0.0, 100.0),
            seed: None,
        }
    }

    /// Create a System (block-level) TABLESAMPLE configuration.
    pub fn system(percentage: f64) -> Self {
        Self {
            method: SampleMethod::System,
            percentage: percentage.clamp(0.0, 100.0),
            seed: None,
        }
    }

    /// Set a seed for reproducible sampling.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = Some(seed);
        self
    }

    /// Determine whether a given row should be included in the sample.
    ///
    /// For Bernoulli: uses a hash of (row_index, seed) to decide independently per row.
    /// For System: uses a hash of (block_index, seed) where block size is derived from total_rows.
    /// For Reservoir: includes the row if `row_index < size`.
    pub fn should_include_row(&self, row_index: usize, total_rows: usize) -> bool {
        match &self.method {
            SampleMethod::Bernoulli => {
                let seed = self.seed.unwrap_or(0);
                let mut hasher = DefaultHasher::new();
                seed.hash(&mut hasher);
                row_index.hash(&mut hasher);
                let hash = hasher.finish();
                let threshold = (self.percentage / 100.0 * u64::MAX as f64) as u64;
                hash <= threshold
            }
            SampleMethod::System => {
                let block_size = (total_rows as f64 * self.percentage / 100.0).max(1.0) as usize;
                let block_index = row_index / block_size.max(1);
                let seed = self.seed.unwrap_or(0);
                let mut hasher = DefaultHasher::new();
                seed.hash(&mut hasher);
                block_index.hash(&mut hasher);
                let hash = hasher.finish();
                let threshold = (self.percentage / 100.0 * u64::MAX as f64) as u64;
                hash <= threshold
            }
            SampleMethod::Reservoir { size } => row_index < *size,
        }
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
        assert!(
            estimate >= 9500 && estimate <= 10500,
            "Estimate was {}",
            estimate
        );
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
        assert!(
            estimate >= 90 && estimate <= 110,
            "Estimate was {}",
            estimate
        );
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
        assert!(
            estimate >= 9500 && estimate <= 10500,
            "Estimate was {}",
            estimate
        );
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

    #[test]
    fn test_hyperloglog_error_bounds() {
        let mut hll = HyperLogLog::new(14);
        for i in 0..10000 {
            hll.add(&i);
        }
        let (estimate, lower, upper) = hll.estimate_with_bounds();
        assert!(lower <= estimate);
        assert!(upper >= estimate);
        assert!(lower <= 10000);
        assert!(upper >= 10000);
    }

    #[test]
    fn test_hyperloglog_standard_error() {
        let hll = HyperLogLog::new(14);
        let se = hll.standard_error();
        // For precision 14: 1.04 / sqrt(16384) ≈ 0.0081
        assert!((se - 0.0081).abs() < 0.001);
    }

    #[test]
    fn test_approx_result() {
        let result = ApproxResult::new(1000.0, 0.05);
        assert_eq!(result.estimate, 1000.0);
        assert_eq!(result.lower_bound, 950.0);
        assert_eq!(result.upper_bound, 1050.0);
    }

    #[test]
    fn test_sampling_config() {
        let config = SamplingConfig::new(1000);
        assert_eq!(config.sampling_rate(500), 1.0);
        assert!((config.sampling_rate(10000) - 0.1).abs() < 0.001);
        assert!(!config.should_sample(500));
        assert!(config.should_sample(10000));
    }

    #[test]
    fn test_reservoir_sampler() {
        let mut sampler = ReservoirSampler::new(100);
        for i in 0..10000 {
            sampler.add(i as f64);
        }
        assert_eq!(sampler.samples().len(), 100);
        assert_eq!(sampler.total_count(), 10000);
    }

    #[test]
    fn test_count_min_sketch_basic() {
        let mut cms = CountMinSketch::new(1000, 5);
        cms.add(&"hello");
        cms.add(&"hello");
        cms.add(&"world");

        assert_eq!(cms.estimate(&"hello"), 2);
        assert_eq!(cms.estimate(&"world"), 1);
        assert_eq!(cms.estimate(&"missing"), 0);
        assert_eq!(cms.total_count(), 3);
        assert!(cms.memory_bytes() > 0);
    }

    #[test]
    fn test_count_min_sketch_frequency() {
        let mut cms = CountMinSketch::with_error_rate(0.01, 0.01);
        // Add items with known frequencies
        for _ in 0..1000 {
            cms.add_str("frequent");
        }
        for _ in 0..100 {
            cms.add_str("moderate");
        }
        for _ in 0..10 {
            cms.add_str("rare");
        }

        let freq_est = cms.estimate_str("frequent");
        let mod_est = cms.estimate_str("moderate");
        let rare_est = cms.estimate_str("rare");

        // Count-Min Sketch overestimates, so estimates >= true count
        assert!(freq_est >= 1000, "frequent estimate was {}", freq_est);
        assert!(mod_est >= 100, "moderate estimate was {}", mod_est);
        assert!(rare_est >= 10, "rare estimate was {}", rare_est);
        // But shouldn't overestimate by too much with good sizing
        assert!(freq_est <= 1100, "frequent estimate was {}", freq_est);
        assert!(mod_est <= 200, "moderate estimate was {}", mod_est);
        assert_eq!(cms.total_count(), 1110);
    }

    #[test]
    fn test_bloom_filter_membership() {
        let mut bf = BloomFilter::new(10000, 7);
        bf.insert_str("apple");
        bf.insert_str("banana");
        bf.insert_str("cherry");

        assert!(bf.contains_str("apple"));
        assert!(bf.contains_str("banana"));
        assert!(bf.contains_str("cherry"));
        // Items not inserted should (almost certainly) not be found with a large filter
        assert!(!bf.contains_str("dragonfruit"));
        assert!(!bf.contains_str("elderberry"));
        assert_eq!(bf.count(), 3);
        assert!(bf.memory_bytes() > 0);
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut bf = BloomFilter::with_capacity(1000, 0.01);
        for i in 0..1000 {
            bf.insert(&i);
        }

        // Check false positive rate with items NOT in the set
        let mut false_positives = 0;
        let test_range = 10000;
        for i in 1000..(1000 + test_range) {
            if bf.contains(&i) {
                false_positives += 1;
            }
        }

        let fpr = false_positives as f64 / test_range as f64;
        // Should be roughly within the target FPP (allow some slack)
        assert!(fpr < 0.05, "False positive rate was {}", fpr);
        // The estimated FPR should be reasonable
        let estimated_fpr = bf.false_positive_rate();
        assert!(estimated_fpr < 0.1, "Estimated FPR was {}", estimated_fpr);
    }

    #[test]
    fn test_tablesample_config() {
        // Bernoulli sampling
        let config = TablesampleConfig::bernoulli(50.0).with_seed(42);
        assert_eq!(config.method, SampleMethod::Bernoulli);
        assert_eq!(config.percentage, 50.0);
        assert_eq!(config.seed, Some(42));

        // Should include roughly 50% of rows (with seed, results are deterministic)
        let mut included = 0;
        let total = 10000;
        for i in 0..total {
            if config.should_include_row(i, total) {
                included += 1;
            }
        }
        // Roughly 50% ± 5%
        assert!(
            included > 4000 && included < 6000,
            "Included {} out of {} rows",
            included,
            total
        );

        // System sampling
        let sys_config = TablesampleConfig::system(30.0);
        assert_eq!(sys_config.method, SampleMethod::System);
        assert_eq!(sys_config.percentage, 30.0);

        // Reservoir
        let res_config = TablesampleConfig {
            method: SampleMethod::Reservoir { size: 100 },
            percentage: 0.0,
            seed: None,
        };
        assert!(res_config.should_include_row(50, 1000));
        assert!(!res_config.should_include_row(150, 1000));
    }

    #[test]
    fn test_approx_aggregate_function_creation() {
        let acd = ApproxAggregateFunction::ApproxCountDistinct { precision: 14 };
        assert_eq!(acd.sql_name(), "APPROX_COUNT_DISTINCT");

        let ap = ApproxAggregateFunction::ApproxPercentile {
            percentile: 0.95,
            compression: 200,
        };
        assert_eq!(ap.sql_name(), "APPROX_PERCENTILE");

        let atk = ApproxAggregateFunction::ApproxTopK {
            k: 10,
            width: 1000,
            depth: 5,
        };
        assert_eq!(atk.sql_name(), "APPROX_TOP_K");

        let am = ApproxAggregateFunction::ApproxMedian { compression: 100 };
        assert_eq!(am.sql_name(), "APPROX_MEDIAN");
    }
}
