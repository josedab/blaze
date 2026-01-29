//! AI-Powered Query Optimizer
//!
//! Uses machine learning techniques to improve query optimization:
//! - **Workload Collector**: Records query plans, execution times, and cardinality errors
//! - **Learned Cardinality Model**: Predicts row counts based on historical data
//! - **Adaptive Strategy Selection**: Chooses join algorithms and parallelism based on learned patterns
//!
//! The model uses a lightweight gradient-based approach that doesn't require
//! external ML libraries—it learns from observed vs. estimated cardinalities.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::error::{BlazeError, Result};

/// A recorded query execution for training.
#[derive(Debug, Clone)]
pub struct WorkloadEntry {
    /// SQL query text
    pub sql: String,
    /// Execution duration
    pub duration: Duration,
    /// Number of rows returned
    pub rows_returned: usize,
    /// Estimated cardinality (from optimizer)
    pub estimated_cardinality: usize,
    /// Actual cardinality (from execution)
    pub actual_cardinality: usize,
    /// Join strategy used
    pub join_strategy: Option<String>,
    /// Timestamp of execution
    pub timestamp: Instant,
    /// Plan features (column names, table names, predicates)
    pub features: QueryFeatures,
}

/// Features extracted from a query plan for the learned model.
#[derive(Debug, Clone, Default)]
pub struct QueryFeatures {
    /// Table names involved
    pub tables: Vec<String>,
    /// Number of joins
    pub num_joins: usize,
    /// Number of predicates (WHERE conditions)
    pub num_predicates: usize,
    /// Number of aggregations
    pub num_aggregations: usize,
    /// Has ORDER BY
    pub has_order_by: bool,
    /// Has GROUP BY
    pub has_group_by: bool,
    /// Has LIMIT
    pub limit: Option<usize>,
    /// Column names in predicates
    pub predicate_columns: Vec<String>,
}

impl QueryFeatures {
    /// Convert to a feature vector for the model.
    pub fn to_feature_vector(&self) -> Vec<f64> {
        vec![
            self.tables.len() as f64,
            self.num_joins as f64,
            self.num_predicates as f64,
            self.num_aggregations as f64,
            if self.has_order_by { 1.0 } else { 0.0 },
            if self.has_group_by { 1.0 } else { 0.0 },
            self.limit.map(|l| l as f64).unwrap_or(0.0),
        ]
    }

    /// Number of features in the vector.
    pub const NUM_FEATURES: usize = 7;
}

/// Collects and stores query execution history.
pub struct WorkloadCollector {
    /// Recorded entries
    entries: RwLock<Vec<WorkloadEntry>>,
    /// Maximum entries to retain
    max_entries: usize,
    /// Aggregate statistics per table
    table_stats: RwLock<HashMap<String, TableWorkloadStats>>,
}

/// Aggregate statistics for a table from workload history.
#[derive(Debug, Clone, Default)]
pub struct TableWorkloadStats {
    /// Total queries involving this table
    pub query_count: u64,
    /// Average rows returned
    pub avg_rows_returned: f64,
    /// Average selectivity (rows_returned / estimated_total)
    pub avg_selectivity: f64,
    /// Common predicate columns
    pub common_predicate_columns: HashMap<String, u64>,
}

impl WorkloadCollector {
    pub fn new(max_entries: usize) -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            max_entries,
            table_stats: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_default_capacity() -> Self {
        Self::new(10_000)
    }

    /// Record a query execution.
    pub fn record(&self, entry: WorkloadEntry) {
        // Update table stats
        {
            let mut stats = self.table_stats.write();
            for table in &entry.features.tables {
                let ts = stats.entry(table.clone()).or_default();
                ts.query_count += 1;
                let n = ts.query_count as f64;
                ts.avg_rows_returned =
                    ts.avg_rows_returned * (n - 1.0) / n + entry.rows_returned as f64 / n;

                if entry.estimated_cardinality > 0 {
                    let selectivity =
                        entry.actual_cardinality as f64 / entry.estimated_cardinality as f64;
                    ts.avg_selectivity = ts.avg_selectivity * (n - 1.0) / n + selectivity / n;
                }

                for col in &entry.features.predicate_columns {
                    *ts.common_predicate_columns.entry(col.clone()).or_insert(0) += 1;
                }
            }
        }

        // Store entry
        let mut entries = self.entries.write();
        entries.push(entry);
        if entries.len() > self.max_entries {
            let drain_count = entries.len() - self.max_entries;
            entries.drain(0..drain_count);
        }
    }

    /// Get all recorded entries.
    pub fn entries(&self) -> Vec<WorkloadEntry> {
        self.entries.read().clone()
    }

    /// Get the number of recorded entries.
    pub fn num_entries(&self) -> usize {
        self.entries.read().len()
    }

    /// Get table-level statistics.
    pub fn table_stats(&self, table: &str) -> Option<TableWorkloadStats> {
        self.table_stats.read().get(table).cloned()
    }

    /// Get the most frequent queries.
    pub fn frequent_patterns(&self, top_n: usize) -> Vec<(QueryFeatures, usize)> {
        let entries = self.entries.read();
        let mut pattern_counts: HashMap<String, (QueryFeatures, usize)> = HashMap::new();

        for entry in entries.iter() {
            let key = format!("{:?}", entry.features.tables);
            let count = pattern_counts
                .entry(key)
                .or_insert((entry.features.clone(), 0));
            count.1 += 1;
        }

        let mut patterns: Vec<_> = pattern_counts.into_values().collect();
        patterns.sort_by(|a, b| b.1.cmp(&a.1));
        patterns.truncate(top_n);
        patterns
    }

    /// Clear all recorded entries.
    pub fn clear(&self) {
        self.entries.write().clear();
        self.table_stats.write().clear();
    }
}

/// A simple learned cardinality model using linear regression.
///
/// Learns weights for feature vector → estimated cardinality mapping.
/// Uses online gradient descent for incremental learning.
pub struct LearnedCardinalityModel {
    /// Model weights (one per feature + bias)
    weights: RwLock<Vec<f64>>,
    /// Learning rate
    learning_rate: f64,
    /// Number of training samples seen
    samples_seen: RwLock<u64>,
    /// Running mean absolute error
    mae: RwLock<f64>,
}

impl LearnedCardinalityModel {
    pub fn new(learning_rate: f64) -> Self {
        Self {
            weights: RwLock::new(vec![0.0; QueryFeatures::NUM_FEATURES + 1]),
            learning_rate,
            samples_seen: RwLock::new(0),
            mae: RwLock::new(0.0),
        }
    }

    pub fn default_model() -> Self {
        Self::new(0.01)
    }

    /// Train the model on a single observation.
    pub fn train(&self, features: &QueryFeatures, actual_cardinality: f64) {
        let x = features.to_feature_vector();
        let predicted = self.predict_raw(&x);
        let error = actual_cardinality.ln().max(0.0) - predicted;

        // Update weights via gradient descent (in log space)
        let mut weights = self.weights.write();
        for (i, &xi) in x.iter().enumerate() {
            weights[i] += self.learning_rate * error * xi;
        }
        // Bias term
        weights[QueryFeatures::NUM_FEATURES] += self.learning_rate * error;

        // Update running MAE
        let mut samples = self.samples_seen.write();
        *samples += 1;
        let n = *samples as f64;
        let mut mae = self.mae.write();
        *mae = *mae * (n - 1.0) / n + error.abs() / n;
    }

    /// Train on a batch of observations.
    pub fn train_batch(&self, data: &[(QueryFeatures, f64)]) {
        for (features, actual) in data {
            self.train(features, *actual);
        }
    }

    /// Predict cardinality for given features.
    pub fn predict(&self, features: &QueryFeatures) -> f64 {
        let x = features.to_feature_vector();
        let log_pred = self.predict_raw(&x);
        log_pred.exp().max(1.0)
    }

    fn predict_raw(&self, x: &[f64]) -> f64 {
        let weights = self.weights.read();
        let mut result = weights[QueryFeatures::NUM_FEATURES]; // bias
        for (i, &xi) in x.iter().enumerate() {
            if i < weights.len() - 1 {
                result += weights[i] * xi;
            }
        }
        result
    }

    /// Get the current mean absolute error.
    pub fn mae(&self) -> f64 {
        *self.mae.read()
    }

    /// Get the number of training samples seen.
    pub fn samples_seen(&self) -> u64 {
        *self.samples_seen.read()
    }

    /// Check if the model has enough training data to be useful.
    pub fn is_trained(&self) -> bool {
        *self.samples_seen.read() >= 10
    }
}

/// Join strategy recommendation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinStrategy {
    HashJoin,
    SortMergeJoin,
    NestedLoopJoin,
    BroadcastHashJoin,
}

impl std::fmt::Display for JoinStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::HashJoin => write!(f, "HashJoin"),
            Self::SortMergeJoin => write!(f, "SortMergeJoin"),
            Self::NestedLoopJoin => write!(f, "NestedLoopJoin"),
            Self::BroadcastHashJoin => write!(f, "BroadcastHashJoin"),
        }
    }
}

/// Adaptive strategy selector that uses workload history to make decisions.
pub struct AdaptiveStrategySelector {
    collector: Arc<WorkloadCollector>,
    cardinality_model: Arc<LearnedCardinalityModel>,
    /// Thresholds for strategy selection
    broadcast_threshold: usize,
    sort_merge_threshold: usize,
}

impl AdaptiveStrategySelector {
    pub fn new(
        collector: Arc<WorkloadCollector>,
        cardinality_model: Arc<LearnedCardinalityModel>,
    ) -> Self {
        Self {
            collector,
            cardinality_model,
            broadcast_threshold: 10_000,
            sort_merge_threshold: 1_000_000,
        }
    }

    /// Recommend a join strategy based on estimated cardinalities.
    pub fn recommend_join_strategy(
        &self,
        left_features: &QueryFeatures,
        right_features: &QueryFeatures,
    ) -> JoinStrategy {
        let left_card = if self.cardinality_model.is_trained() {
            self.cardinality_model.predict(left_features) as usize
        } else {
            1000 // default fallback
        };

        let right_card = if self.cardinality_model.is_trained() {
            self.cardinality_model.predict(right_features) as usize
        } else {
            1000
        };

        let smaller = left_card.min(right_card);
        let larger = left_card.max(right_card);

        if smaller <= self.broadcast_threshold {
            JoinStrategy::BroadcastHashJoin
        } else if larger > self.sort_merge_threshold {
            JoinStrategy::SortMergeJoin
        } else {
            JoinStrategy::HashJoin
        }
    }

    /// Recommend parallelism level based on workload history.
    pub fn recommend_parallelism(&self, features: &QueryFeatures) -> usize {
        let estimated_rows = if self.cardinality_model.is_trained() {
            self.cardinality_model.predict(features) as usize
        } else {
            10_000
        };

        if estimated_rows < 1_000 {
            1
        } else if estimated_rows < 100_000 {
            2
        } else if estimated_rows < 1_000_000 {
            4
        } else {
            8
        }
    }

    /// Recommend memory allocation based on workload history.
    pub fn recommend_memory_budget(&self, features: &QueryFeatures) -> usize {
        let estimated_rows = if self.cardinality_model.is_trained() {
            self.cardinality_model.predict(features) as usize
        } else {
            10_000
        };

        // Estimate ~100 bytes per row for memory budget
        let estimated_bytes = estimated_rows * 100;
        estimated_bytes.max(1024 * 1024) // Minimum 1MB
    }
}

/// The main AI optimizer that combines all components.
pub struct AiOptimizer {
    pub collector: Arc<WorkloadCollector>,
    pub cardinality_model: Arc<LearnedCardinalityModel>,
    pub strategy_selector: AdaptiveStrategySelector,
}

impl AiOptimizer {
    pub fn new() -> Self {
        let collector = Arc::new(WorkloadCollector::with_default_capacity());
        let model = Arc::new(LearnedCardinalityModel::default_model());
        let selector = AdaptiveStrategySelector::new(collector.clone(), model.clone());

        Self {
            collector,
            cardinality_model: model,
            strategy_selector: selector,
        }
    }

    /// Record a completed query and update the model.
    pub fn record_query(
        &self,
        sql: &str,
        features: QueryFeatures,
        estimated_cardinality: usize,
        actual_cardinality: usize,
        duration: Duration,
        rows_returned: usize,
    ) {
        // Train the cardinality model
        self.cardinality_model
            .train(&features, actual_cardinality as f64);

        // Record in workload collector
        self.collector.record(WorkloadEntry {
            sql: sql.to_string(),
            duration,
            rows_returned,
            estimated_cardinality,
            actual_cardinality,
            join_strategy: None,
            timestamp: Instant::now(),
            features,
        });
    }

    /// Get optimization recommendations for a query.
    pub fn get_recommendations(&self, features: &QueryFeatures) -> OptimizationRecommendation {
        OptimizationRecommendation {
            estimated_cardinality: self.cardinality_model.predict(features) as usize,
            parallelism: self.strategy_selector.recommend_parallelism(features),
            memory_budget: self.strategy_selector.recommend_memory_budget(features),
            model_confidence: if self.cardinality_model.is_trained() {
                // Confidence based on inverse of log-space MAE, clamped to [0, 1]
                let mae = self.cardinality_model.mae();
                (1.0 / (1.0 + mae)).max(0.01)
            } else {
                0.0
            },
        }
    }
}

impl Default for AiOptimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Recommendations from the AI optimizer.
#[derive(Debug, Clone)]
pub struct OptimizationRecommendation {
    /// Predicted cardinality
    pub estimated_cardinality: usize,
    /// Recommended parallelism level
    pub parallelism: usize,
    /// Recommended memory budget in bytes
    pub memory_budget: usize,
    /// Model confidence (0.0 to 1.0)
    pub model_confidence: f64,
}

// ---------------------------------------------------------------------------
// Bayesian Auto-Tuner
// ---------------------------------------------------------------------------

/// Bayesian-inspired auto-tuner for query engine parameters.
/// Uses observed performance to adjust batch_size, parallelism, and memory allocation.
pub struct BayesianAutoTuner {
    observations: RwLock<Vec<TuningObservation>>,
    current_config: RwLock<TuningConfig>,
    exploration_rate: f64,
}

/// A single tuning observation that records how a configuration performed.
#[derive(Debug, Clone)]
pub struct TuningObservation {
    pub config: TuningConfig,
    pub query_duration: Duration,
    pub rows_processed: usize,
    pub memory_used_bytes: usize,
    pub timestamp: Instant,
}

/// Configuration knobs that the auto-tuner adjusts.
#[derive(Debug, Clone, PartialEq)]
pub struct TuningConfig {
    pub batch_size: usize,
    pub parallelism: usize,
    pub hash_table_load_factor: f64,
    pub sort_memory_fraction: f64,
}

impl Default for TuningConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            parallelism: 4,
            hash_table_load_factor: 0.7,
            sort_memory_fraction: 0.25,
        }
    }
}

impl BayesianAutoTuner {
    pub fn new() -> Self {
        Self {
            observations: RwLock::new(Vec::new()),
            current_config: RwLock::new(TuningConfig::default()),
            exploration_rate: 0.1,
        }
    }

    /// Set the exploration rate (probability of random perturbation).
    pub fn with_exploration_rate(mut self, rate: f64) -> Self {
        self.exploration_rate = rate.clamp(0.0, 1.0);
        self
    }

    /// Record a tuning observation.
    pub fn record_observation(&self, obs: TuningObservation) {
        self.observations.write().push(obs);
    }

    /// Suggest a configuration based on observed performance.
    /// With probability `exploration_rate`, a random perturbation is applied
    /// to encourage exploration of the parameter space.
    pub fn suggest_config(&self) -> TuningConfig {
        let base = self.find_best_config().unwrap_or_default();

        // Deterministic perturbation derived from observation count to keep tests stable.
        let obs_len = self.observations.read().len();
        let should_explore = (obs_len % 10) < ((self.exploration_rate * 10.0) as usize);

        if should_explore && obs_len > 0 {
            let factor = if obs_len % 2 == 0 { 1.25 } else { 0.8 };
            TuningConfig {
                batch_size: ((base.batch_size as f64 * factor) as usize).max(1024),
                parallelism: (base.parallelism).max(1),
                hash_table_load_factor: (base.hash_table_load_factor * factor).clamp(0.3, 0.95),
                sort_memory_fraction: (base.sort_memory_fraction * factor).clamp(0.05, 0.75),
            }
        } else {
            base
        }
    }

    /// Return the current config snapshot.
    pub fn current_config(&self) -> TuningConfig {
        self.current_config.read().clone()
    }

    /// Compute throughput score (rows/sec) normalised by memory efficiency.
    pub fn throughput_score(obs: &TuningObservation) -> f64 {
        let secs = obs.query_duration.as_secs_f64().max(1e-9);
        let rows_per_sec = obs.rows_processed as f64 / secs;
        let mem_mb = (obs.memory_used_bytes as f64) / (1024.0 * 1024.0);
        // Favour high throughput, penalise excessive memory
        rows_per_sec / (1.0 + mem_mb.ln().max(0.0))
    }

    /// Find the config with the best throughput score.
    fn find_best_config(&self) -> Option<TuningConfig> {
        let obs = self.observations.read();
        obs.iter()
            .max_by(|a, b| {
                Self::throughput_score(a)
                    .partial_cmp(&Self::throughput_score(b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|o| o.config.clone())
    }
}

// ---------------------------------------------------------------------------
// Index & Materialization Recommendations
// ---------------------------------------------------------------------------

/// Index recommendation based on workload analysis.
#[derive(Debug, Clone)]
pub struct IndexRecommendation {
    pub table_name: String,
    pub columns: Vec<String>,
    pub index_type: IndexType,
    pub estimated_benefit: f64, // expected speedup factor
    pub reason: String,
}

/// Supported index types for recommendations.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    BTree,
    Hash,
    BloomFilter,
}

/// Materialized view recommendation.
#[derive(Debug, Clone)]
pub struct MaterializationRecommendation {
    pub suggested_query: String,
    pub source_tables: Vec<String>,
    pub estimated_speedup: f64,
    pub query_frequency: u64,
    pub reason: String,
}

/// Workload analyzer that produces index and materialization recommendations.
pub struct WorkloadAnalyzer {
    collector: Arc<WorkloadCollector>,
}

impl WorkloadAnalyzer {
    pub fn new(collector: Arc<WorkloadCollector>) -> Self {
        Self { collector }
    }

    /// Recommend indexes based on the most frequently filtered columns.
    pub fn recommend_indexes(&self, top_k: usize) -> Vec<IndexRecommendation> {
        let entries = self.collector.entries();
        // Accumulate (table, column) → frequency
        let mut col_freq: HashMap<(String, String), u64> = HashMap::new();
        for entry in &entries {
            for table in &entry.features.tables {
                for col in &entry.features.predicate_columns {
                    *col_freq.entry((table.clone(), col.clone())).or_insert(0) += 1;
                }
            }
        }

        let mut ranked: Vec<_> = col_freq.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1));
        ranked.truncate(top_k);

        ranked
            .into_iter()
            .map(|((table, col), freq)| {
                let index_type = if freq > 100 {
                    IndexType::Hash
                } else {
                    IndexType::BTree
                };
                let benefit = 1.0 + (freq as f64).ln();
                IndexRecommendation {
                    table_name: table.clone(),
                    columns: vec![col.clone()],
                    index_type,
                    estimated_benefit: benefit,
                    reason: format!(
                        "Column '{}' on table '{}' filtered {} times",
                        col, table, freq
                    ),
                }
            })
            .collect()
    }

    /// Recommend materializations for frequently repeated query patterns.
    pub fn recommend_materializations(&self, top_k: usize) -> Vec<MaterializationRecommendation> {
        let patterns = self.collector.frequent_patterns(top_k);

        patterns
            .into_iter()
            .filter(|(_, count)| *count >= 2)
            .map(|(features, count)| {
                let tables = features.tables.clone();
                let query = format!("SELECT * FROM {}", tables.join(", "));
                MaterializationRecommendation {
                    suggested_query: query,
                    source_tables: tables,
                    estimated_speedup: 1.0 + (count as f64).ln(),
                    query_frequency: count as u64,
                    reason: format!("Pattern repeated {} times", count),
                }
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Cost Model Validator
// ---------------------------------------------------------------------------

/// Validates cost model estimates against actual execution results.
pub struct CostModelValidator {
    validations: RwLock<Vec<CostValidation>>,
}

/// A single cost-model validation record.
#[derive(Debug, Clone)]
pub struct CostValidation {
    pub query_hash: String,
    pub estimated_cost: f64,
    pub actual_cost: f64,
    pub error_ratio: f64,
    pub timestamp: Instant,
}

impl CostModelValidator {
    pub fn new() -> Self {
        Self {
            validations: RwLock::new(Vec::new()),
        }
    }

    /// Record a validation entry (estimated vs actual cost).
    pub fn record_validation(&self, query_hash: impl Into<String>, estimated: f64, actual: f64) {
        let error_ratio = if actual > 0.0 {
            (estimated - actual).abs() / actual
        } else {
            0.0
        };
        self.validations.write().push(CostValidation {
            query_hash: query_hash.into(),
            estimated_cost: estimated,
            actual_cost: actual,
            error_ratio,
            timestamp: Instant::now(),
        });
    }

    /// Mean error ratio across all validations.
    pub fn mean_error_ratio(&self) -> f64 {
        let vals = self.validations.read();
        if vals.is_empty() {
            return 0.0;
        }
        vals.iter().map(|v| v.error_ratio).sum::<f64>() / vals.len() as f64
    }

    /// 90th-percentile error ratio.
    pub fn p90_error_ratio(&self) -> f64 {
        let vals = self.validations.read();
        if vals.is_empty() {
            return 0.0;
        }
        let mut ratios: Vec<f64> = vals.iter().map(|v| v.error_ratio).collect();
        ratios.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((ratios.len() as f64) * 0.9).ceil() as usize;
        ratios[idx.min(ratios.len() - 1)]
    }

    /// Return the top-k queries with the worst (highest) error ratio.
    pub fn worst_queries(&self, top_k: usize) -> Vec<CostValidation> {
        let vals = self.validations.read();
        let mut sorted: Vec<_> = vals.clone();
        sorted.sort_by(|a, b| {
            b.error_ratio
                .partial_cmp(&a.error_ratio)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        sorted.truncate(top_k);
        sorted
    }

    /// Returns `true` when the mean error ratio is below `threshold`.
    pub fn is_calibrated(&self, threshold: f64) -> bool {
        self.mean_error_ratio() < threshold
    }
}

// ---------------------------------------------------------------------------
// Workload Summarizer
// ---------------------------------------------------------------------------

/// Summarizes workload patterns into a human-readable report.
pub struct WorkloadSummarizer;

/// Summary statistics for a workload.
#[derive(Debug, Clone)]
pub struct WorkloadSummary {
    pub total_queries: usize,
    pub total_duration: Duration,
    pub avg_duration: Duration,
    pub p50_duration: Duration,
    pub p95_duration: Duration,
    pub top_tables: Vec<(String, u64)>,
    pub top_query_patterns: Vec<(String, u64)>,
    pub cardinality_accuracy: f64,
}

impl WorkloadSummarizer {
    /// Build a summary from the workload collector.
    pub fn summarize(collector: &WorkloadCollector) -> WorkloadSummary {
        let entries = collector.entries();
        let total_queries = entries.len();

        if total_queries == 0 {
            return WorkloadSummary {
                total_queries: 0,
                total_duration: Duration::ZERO,
                avg_duration: Duration::ZERO,
                p50_duration: Duration::ZERO,
                p95_duration: Duration::ZERO,
                top_tables: Vec::new(),
                top_query_patterns: Vec::new(),
                cardinality_accuracy: 0.0,
            };
        }

        let total_duration: Duration = entries.iter().map(|e| e.duration).sum();
        let avg_duration = total_duration / total_queries as u32;

        // Percentiles
        let mut durations: Vec<Duration> = entries.iter().map(|e| e.duration).collect();
        durations.sort();
        let p50_idx = (total_queries as f64 * 0.5).ceil() as usize;
        let p95_idx = (total_queries as f64 * 0.95).ceil() as usize;
        let p50_duration = durations[p50_idx.min(total_queries - 1)];
        let p95_duration = durations[p95_idx.min(total_queries - 1)];

        // Top tables
        let mut table_counts: HashMap<String, u64> = HashMap::new();
        for entry in &entries {
            for t in &entry.features.tables {
                *table_counts.entry(t.clone()).or_insert(0) += 1;
            }
        }
        let mut top_tables: Vec<_> = table_counts.into_iter().collect();
        top_tables.sort_by(|a, b| b.1.cmp(&a.1));
        top_tables.truncate(10);

        // Top query patterns (by table set)
        let patterns = collector.frequent_patterns(10);
        let top_query_patterns: Vec<(String, u64)> = patterns
            .into_iter()
            .map(|(f, c)| (format!("{:?}", f.tables), c as u64))
            .collect();

        // Cardinality accuracy: mean ratio of min(est,act)/max(est,act)
        let mut accuracy_sum = 0.0_f64;
        let mut accuracy_count = 0usize;
        for entry in &entries {
            if entry.estimated_cardinality > 0 && entry.actual_cardinality > 0 {
                let est = entry.estimated_cardinality as f64;
                let act = entry.actual_cardinality as f64;
                accuracy_sum += est.min(act) / est.max(act);
                accuracy_count += 1;
            }
        }
        let cardinality_accuracy = if accuracy_count > 0 {
            accuracy_sum / accuracy_count as f64
        } else {
            0.0
        };

        WorkloadSummary {
            total_queries,
            total_duration,
            avg_duration,
            p50_duration,
            p95_duration,
            top_tables,
            top_query_patterns,
            cardinality_accuracy,
        }
    }
}

impl WorkloadSummary {
    /// Format the summary as a human-readable report string.
    pub fn format_report(&self) -> String {
        let mut report = String::new();
        report.push_str("=== Workload Summary ===\n");
        report.push_str(&format!("Total queries: {}\n", self.total_queries));
        report.push_str(&format!("Total duration: {:?}\n", self.total_duration));
        report.push_str(&format!("Avg duration:   {:?}\n", self.avg_duration));
        report.push_str(&format!("P50 duration:   {:?}\n", self.p50_duration));
        report.push_str(&format!("P95 duration:   {:?}\n", self.p95_duration));
        report.push_str(&format!(
            "Cardinality accuracy: {:.2}%\n",
            self.cardinality_accuracy * 100.0
        ));
        if !self.top_tables.is_empty() {
            report.push_str("Top tables:\n");
            for (t, c) in &self.top_tables {
                report.push_str(&format!("  {} — {} queries\n", t, c));
            }
        }
        if !self.top_query_patterns.is_empty() {
            report.push_str("Top query patterns:\n");
            for (p, c) in &self.top_query_patterns {
                report.push_str(&format!("  {} — {} occurrences\n", p, c));
            }
        }
        report
    }
}

/// Result of an A/B test between two query plans.
#[derive(Debug, Clone)]
pub struct ABTestResult {
    pub plan_a_name: String,
    pub plan_b_name: String,
    pub plan_a_latency_ms: f64,
    pub plan_b_latency_ms: f64,
    pub winner: String,
    pub speedup_pct: f64,
    pub sample_size: usize,
}

/// A/B tester for comparing query plan strategies.
pub struct PlanABTester {
    results: RwLock<Vec<ABTestResult>>,
    exploration_rate: f64,
    counter: RwLock<u64>,
}

impl PlanABTester {
    pub fn new(exploration_rate: f64) -> Self {
        Self {
            results: RwLock::new(Vec::new()),
            exploration_rate,
            counter: RwLock::new(0),
        }
    }

    /// Returns true `exploration_rate` fraction of the time using a counter-based approach.
    pub fn should_explore(&self) -> bool {
        let mut counter = self.counter.write();
        *counter += 1;
        let threshold = (1.0 / self.exploration_rate).max(1.0) as u64;
        *counter % threshold == 0
    }

    pub fn record_result(&self, result: ABTestResult) {
        self.results.write().push(result);
    }

    /// Returns the plan with lower average latency across all results for the given pair.
    pub fn best_strategy(&self, plan_a: &str, plan_b: &str) -> Option<String> {
        let results = self.results.read();
        let mut a_total = 0.0;
        let mut b_total = 0.0;
        let mut count = 0usize;
        for r in results.iter() {
            if (r.plan_a_name == plan_a && r.plan_b_name == plan_b)
                || (r.plan_a_name == plan_b && r.plan_b_name == plan_a)
            {
                if r.plan_a_name == plan_a {
                    a_total += r.plan_a_latency_ms;
                    b_total += r.plan_b_latency_ms;
                } else {
                    a_total += r.plan_b_latency_ms;
                    b_total += r.plan_a_latency_ms;
                }
                count += 1;
            }
        }
        if count == 0 {
            return None;
        }
        if a_total / count as f64 <= b_total / count as f64 {
            Some(plan_a.to_string())
        } else {
            Some(plan_b.to_string())
        }
    }

    pub fn total_tests(&self) -> usize {
        self.results.read().len()
    }
}

/// Detects when a learned model's predictions drift from observed reality.
pub struct ModelDriftDetector {
    recent_errors: RwLock<Vec<f64>>,
    window_size: usize,
    drift_threshold: f64,
}

impl ModelDriftDetector {
    pub fn new(window_size: usize, drift_threshold: f64) -> Self {
        Self {
            recent_errors: RwLock::new(Vec::new()),
            window_size,
            drift_threshold,
        }
    }

    /// Records the q-error: max(estimated/actual, actual/estimated).
    pub fn record_prediction(&self, estimated: f64, actual: f64) {
        let q_error = if estimated <= 0.0 || actual <= 0.0 {
            1.0
        } else {
            (estimated / actual).max(actual / estimated)
        };
        let mut errors = self.recent_errors.write();
        errors.push(q_error);
        if errors.len() > self.window_size {
            errors.remove(0);
        }
    }

    /// Returns true if mean q-error in the window exceeds drift_threshold.
    pub fn is_drifting(&self) -> bool {
        self.mean_q_error() > self.drift_threshold
    }

    pub fn mean_q_error(&self) -> f64 {
        let errors = self.recent_errors.read();
        if errors.is_empty() {
            return 1.0;
        }
        errors.iter().sum::<f64>() / errors.len() as f64
    }

    pub fn max_q_error(&self) -> f64 {
        let errors = self.recent_errors.read();
        errors.iter().cloned().fold(1.0_f64, f64::max)
    }

    /// True if drifting or if window is full.
    pub fn should_retrain(&self) -> bool {
        self.is_drifting() || self.recent_errors.read().len() >= self.window_size
    }
}

/// Feedback loop that corrects cardinality estimates based on observed execution.
pub struct CardinalityFeedbackLoop {
    corrections: RwLock<HashMap<String, f64>>,
    observation_count: RwLock<HashMap<String, u64>>,
}

impl CardinalityFeedbackLoop {
    pub fn new() -> Self {
        Self {
            corrections: RwLock::new(HashMap::new()),
            observation_count: RwLock::new(HashMap::new()),
        }
    }

    /// Updates the running correction factor using exponential moving average (alpha=0.1).
    pub fn record_feedback(&self, table: &str, estimated: f64, actual: f64) {
        let new_factor = if estimated <= 0.0 {
            1.0
        } else {
            actual / estimated
        };
        let alpha = 0.1;
        let mut corrections = self.corrections.write();
        let mut counts = self.observation_count.write();
        let current = corrections.entry(table.to_string()).or_insert(1.0);
        *current = (1.0 - alpha) * *current + alpha * new_factor;
        *counts.entry(table.to_string()).or_insert(0) += 1;
    }

    /// Returns correction factor (1.0 if no data).
    pub fn get_correction(&self, table: &str) -> f64 {
        *self.corrections.read().get(table).unwrap_or(&1.0)
    }

    /// Applies correction factor to a raw estimate.
    pub fn corrected_estimate(&self, table: &str, raw_estimate: f64) -> f64 {
        raw_estimate * self.get_correction(table)
    }

    pub fn tables_with_feedback(&self) -> Vec<String> {
        self.corrections.read().keys().cloned().collect()
    }
}

// ---------------------------------------------------------------------------
// Runtime Re-optimization
// ---------------------------------------------------------------------------

/// Runtime re-optimizer that can trigger re-planning mid-execution
/// when actual cardinalities deviate significantly from estimates.
pub struct RuntimeReoptimizer {
    threshold: f64,
    check_interval_rows: usize,
    reopt_count: u32,
    max_reopts: u32,
}

impl RuntimeReoptimizer {
    pub fn new(threshold: f64, check_interval_rows: usize) -> Self {
        Self {
            threshold,
            check_interval_rows,
            reopt_count: 0,
            max_reopts: 3,
        }
    }

    /// Check if re-optimization should be triggered based on actual vs estimated rows.
    pub fn should_reoptimize(&self, estimated_rows: usize, actual_rows: usize) -> bool {
        if self.reopt_count >= self.max_reopts || estimated_rows == 0 {
            return false;
        }
        let ratio = actual_rows as f64 / estimated_rows as f64;
        ratio > self.threshold || ratio < (1.0 / self.threshold)
    }

    /// Record that a re-optimization occurred.
    pub fn record_reopt(&mut self) {
        self.reopt_count += 1;
    }

    /// Get the check interval in rows.
    pub fn check_interval(&self) -> usize {
        self.check_interval_rows
    }

    /// How many re-optimizations have occurred.
    pub fn reopt_count(&self) -> u32 {
        self.reopt_count
    }

    /// Whether more re-optimizations are allowed.
    pub fn can_reoptimize(&self) -> bool {
        self.reopt_count < self.max_reopts
    }
}

/// Adaptive parallelism controller that adjusts thread counts based on workload.
pub struct AdaptiveParallelism {
    min_threads: usize,
    max_threads: usize,
    current_threads: usize,
    cpu_utilization_history: Vec<f64>,
    max_history: usize,
}

impl AdaptiveParallelism {
    pub fn new(min_threads: usize, max_threads: usize) -> Self {
        Self {
            min_threads,
            max_threads,
            current_threads: min_threads,
            cpu_utilization_history: Vec::new(),
            max_history: 20,
        }
    }

    /// Record a CPU utilization sample (0.0 to 1.0).
    pub fn record_utilization(&mut self, utilization: f64) {
        if self.cpu_utilization_history.len() >= self.max_history {
            self.cpu_utilization_history.remove(0);
        }
        self.cpu_utilization_history
            .push(utilization.clamp(0.0, 1.0));
        self.adjust();
    }

    /// Get the current recommended thread count.
    pub fn recommended_threads(&self) -> usize {
        self.current_threads
    }

    fn adjust(&mut self) {
        if self.cpu_utilization_history.is_empty() {
            return;
        }
        let avg: f64 = self.cpu_utilization_history.iter().sum::<f64>()
            / self.cpu_utilization_history.len() as f64;

        if avg > 0.85 && self.current_threads > self.min_threads {
            // High CPU - reduce parallelism to avoid contention
            self.current_threads -= 1;
        } else if avg < 0.5 && self.current_threads < self.max_threads {
            // Low CPU - increase parallelism
            self.current_threads += 1;
        }
    }

    pub fn min_threads(&self) -> usize {
        self.min_threads
    }
    pub fn max_threads(&self) -> usize {
        self.max_threads
    }
}

// ---------------------------------------------------------------------------
// Phase 3: Workload Clustering + Pattern Detection + Auto-Indexing
// ---------------------------------------------------------------------------

/// Workload clusterer that groups similar queries for optimization.
pub struct WorkloadClusterer {
    clusters: Vec<QueryCluster>,
    similarity_threshold: f64,
}

/// A cluster of similar queries.
#[derive(Debug, Clone)]
pub struct QueryCluster {
    pub id: usize,
    pub centroid: Vec<f64>,
    pub query_count: usize,
    pub total_execution_time_us: u64,
    pub representative_sql: String,
}

impl WorkloadClusterer {
    pub fn new(similarity_threshold: f64) -> Self {
        Self {
            clusters: Vec::new(),
            similarity_threshold,
        }
    }

    /// Add a query to the appropriate cluster or create a new one.
    pub fn add_query(&mut self, features: &[f64], sql: &str, execution_time_us: u64) {
        let best_cluster = self.find_nearest_cluster(features);

        if let Some((idx, distance)) = best_cluster {
            if distance < self.similarity_threshold {
                let cluster = &mut self.clusters[idx];
                cluster.query_count += 1;
                cluster.total_execution_time_us += execution_time_us;
                // Update centroid (running average)
                for (i, &f) in features.iter().enumerate() {
                    if i < cluster.centroid.len() {
                        let n = cluster.query_count as f64;
                        cluster.centroid[i] = cluster.centroid[i] * (n - 1.0) / n + f / n;
                    }
                }
                return;
            }
        }

        // Create new cluster
        let id = self.clusters.len();
        self.clusters.push(QueryCluster {
            id,
            centroid: features.to_vec(),
            query_count: 1,
            total_execution_time_us: execution_time_us,
            representative_sql: sql.to_string(),
        });
    }

    fn find_nearest_cluster(&self, features: &[f64]) -> Option<(usize, f64)> {
        self.clusters
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let dist: f64 = c
                    .centroid
                    .iter()
                    .zip(features.iter())
                    .map(|(a, b)| (a - b).powi(2))
                    .sum::<f64>()
                    .sqrt();
                (i, dist)
            })
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    }

    /// Get all clusters sorted by total execution time (hottest first).
    pub fn hot_clusters(&self) -> Vec<&QueryCluster> {
        let mut sorted: Vec<_> = self.clusters.iter().collect();
        sorted.sort_by(|a, b| b.total_execution_time_us.cmp(&a.total_execution_time_us));
        sorted
    }

    pub fn num_clusters(&self) -> usize {
        self.clusters.len()
    }
}

/// Pattern detector that identifies recurring query shapes.
pub struct PatternDetector {
    patterns: HashMap<String, PatternStats>,
}

/// Statistics for a detected query pattern.
#[derive(Debug, Clone)]
pub struct PatternStats {
    pub pattern: String,
    pub count: usize,
    pub avg_execution_time_us: f64,
    pub tables: Vec<String>,
    pub has_join: bool,
    pub has_aggregation: bool,
}

impl PatternDetector {
    pub fn new() -> Self {
        Self {
            patterns: HashMap::new(),
        }
    }

    /// Extract a normalized pattern from a SQL query.
    pub fn extract_pattern(sql: &str) -> String {
        let upper = sql.to_uppercase();
        let mut pattern = String::new();

        if upper.contains("SELECT") {
            pattern.push_str("S");
        }
        if upper.contains("JOIN") {
            pattern.push_str("J");
        }
        if upper.contains("WHERE") {
            pattern.push_str("W");
        }
        if upper.contains("GROUP BY") {
            pattern.push_str("G");
        }
        if upper.contains("HAVING") {
            pattern.push_str("H");
        }
        if upper.contains("ORDER BY") {
            pattern.push_str("O");
        }
        if upper.contains("LIMIT") {
            pattern.push_str("L");
        }
        if upper.contains("UNION") || upper.contains("INTERSECT") || upper.contains("EXCEPT") {
            pattern.push_str("U");
        }
        if upper.contains("WITH") {
            pattern.push_str("C");
        } // CTE

        pattern
    }

    /// Record a query execution.
    pub fn record(&mut self, sql: &str, execution_time_us: u64) {
        let pattern = Self::extract_pattern(sql);
        let upper = sql.to_uppercase();

        let entry = self
            .patterns
            .entry(pattern.clone())
            .or_insert(PatternStats {
                pattern: pattern.clone(),
                count: 0,
                avg_execution_time_us: 0.0,
                tables: Vec::new(),
                has_join: upper.contains("JOIN"),
                has_aggregation: upper.contains("GROUP BY")
                    || upper.contains("SUM")
                    || upper.contains("COUNT")
                    || upper.contains("AVG"),
            });

        let n = entry.count as f64;
        entry.avg_execution_time_us =
            (entry.avg_execution_time_us * n + execution_time_us as f64) / (n + 1.0);
        entry.count += 1;
    }

    /// Get the most common patterns.
    pub fn top_patterns(&self, n: usize) -> Vec<&PatternStats> {
        let mut patterns: Vec<_> = self.patterns.values().collect();
        patterns.sort_by(|a, b| b.count.cmp(&a.count));
        patterns.truncate(n);
        patterns
    }

    pub fn num_patterns(&self) -> usize {
        self.patterns.len()
    }
}

/// Auto-indexer that recommends and tracks index creation.
pub struct AutoIndexer {
    recommendations: Vec<IndexRecommendation>,
    applied: Vec<String>,
    min_query_count: usize,
}

impl AutoIndexer {
    pub fn new(min_query_count: usize) -> Self {
        Self {
            recommendations: Vec::new(),
            applied: Vec::new(),
            min_query_count,
        }
    }

    /// Analyze a workload and generate index recommendations.
    pub fn analyze_workload(
        &mut self,
        patterns: &[&PatternStats],
        column_access: &HashMap<String, usize>,
    ) {
        self.recommendations.clear();

        for (column, &access_count) in column_access {
            if access_count >= self.min_query_count {
                let parts: Vec<&str> = column.splitn(2, '.').collect();
                if parts.len() == 2 {
                    self.recommendations.push(IndexRecommendation {
                        table_name: parts[0].to_string(),
                        columns: vec![parts[1].to_string()],
                        index_type: if patterns.iter().any(|p| p.has_join) {
                            IndexType::Hash
                        } else {
                            IndexType::BTree
                        },
                        estimated_benefit: (access_count as f64 / self.min_query_count as f64)
                            .min(10.0),
                        reason: format!("Column accessed {} times", access_count),
                    });
                }
            }
        }

        self.recommendations.sort_by(|a, b| {
            b.estimated_benefit
                .partial_cmp(&a.estimated_benefit)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    /// Mark a recommendation as applied.
    pub fn mark_applied(&mut self, table: &str, columns: &[String]) {
        let key = format!("{}:{}", table, columns.join(","));
        self.applied.push(key);
    }

    pub fn recommendations(&self) -> &[IndexRecommendation] {
        &self.recommendations
    }
    pub fn applied_count(&self) -> usize {
        self.applied.len()
    }
}

// ---------------------------------------------------------------------------
// Gradient-Based Cardinality Model
// ---------------------------------------------------------------------------

/// Metrics from the gradient cardinality model.
#[derive(Debug, Clone)]
pub struct ModelMetrics {
    /// Total predictions made
    pub predictions: u64,
    /// Mean absolute percentage error
    pub mean_error: f64,
    /// Maximum absolute percentage error observed
    pub max_error: f64,
    /// Number of training samples processed
    pub training_samples: u64,
}

/// Lightweight linear regression model for cardinality estimation trained via
/// online gradient descent.
pub struct GradientCardinalityModel {
    weights: RwLock<Vec<f64>>,
    bias: RwLock<f64>,
    learning_rate: f64,
    error_sum: RwLock<f64>,
    max_error: RwLock<f64>,
    training_samples: RwLock<u64>,
    predictions: RwLock<u64>,
}

impl GradientCardinalityModel {
    /// Create a new model with the given learning rate and feature count.
    pub fn new(learning_rate: f64, features: usize) -> Self {
        Self {
            weights: RwLock::new(vec![0.0; features]),
            bias: RwLock::new(0.0),
            learning_rate,
            error_sum: RwLock::new(0.0),
            max_error: RwLock::new(0.0),
            training_samples: RwLock::new(0),
            predictions: RwLock::new(0),
        }
    }

    /// Predict cardinality from a feature vector.
    pub fn predict(&self, features: &[f64]) -> f64 {
        let weights = self.weights.read();
        let bias = *self.bias.read();
        let raw: f64 = weights
            .iter()
            .zip(features.iter())
            .map(|(w, f)| w * f)
            .sum::<f64>()
            + bias;
        *self.predictions.write() += 1;
        raw.max(0.0)
    }

    /// Train the model on a single sample using gradient descent.
    pub fn train(&self, features: &[f64], actual: f64) {
        let predicted = {
            let weights = self.weights.read();
            let bias = *self.bias.read();
            weights
                .iter()
                .zip(features.iter())
                .map(|(w, f)| w * f)
                .sum::<f64>()
                + bias
        };
        let error = predicted - actual;

        // Update weights
        {
            let mut weights = self.weights.write();
            for (w, f) in weights.iter_mut().zip(features.iter()) {
                *w -= self.learning_rate * error * f;
            }
        }
        {
            let mut bias = self.bias.write();
            *bias -= self.learning_rate * error;
        }

        // Track error metrics
        let abs_pct_error = if actual.abs() > 1e-9 {
            (error.abs() / actual.abs()) * 100.0
        } else {
            error.abs() * 100.0
        };
        {
            let mut sum = self.error_sum.write();
            *sum += abs_pct_error;
        }
        {
            let mut max = self.max_error.write();
            if abs_pct_error > *max {
                *max = abs_pct_error;
            }
        }
        *self.training_samples.write() += 1;
    }

    /// Mean absolute percentage error across all training samples.
    pub fn accuracy(&self) -> f64 {
        let samples = *self.training_samples.read();
        if samples == 0 {
            return 0.0;
        }
        *self.error_sum.read() / samples as f64
    }

    /// Retrieve current model metrics.
    pub fn metrics(&self) -> ModelMetrics {
        ModelMetrics {
            predictions: *self.predictions.read(),
            mean_error: self.accuracy(),
            max_error: *self.max_error.read(),
            training_samples: *self.training_samples.read(),
        }
    }
}

// ---------------------------------------------------------------------------
// Plan Cost Predictor
// ---------------------------------------------------------------------------

/// Execution record for a plan.
#[derive(Debug, Clone)]
struct PlanRecord {
    cost_ema: f64,
    duration_ema_ms: f64,
    count: u64,
}

/// History-based plan cost predictor using exponential moving average.
pub struct PlanCostPredictor {
    records: RwLock<HashMap<u64, PlanRecord>>,
    alpha: f64,
}

impl PlanCostPredictor {
    /// Create a new predictor with default EMA smoothing factor.
    pub fn new() -> Self {
        Self {
            records: RwLock::new(HashMap::new()),
            alpha: 0.3,
        }
    }

    /// Record an execution of a plan.
    pub fn record_execution(&self, plan_hash: u64, cost: f64, duration_ms: u64) {
        let mut records = self.records.write();
        let entry = records.entry(plan_hash).or_insert(PlanRecord {
            cost_ema: cost,
            duration_ema_ms: duration_ms as f64,
            count: 0,
        });
        if entry.count > 0 {
            entry.cost_ema = self.alpha * cost + (1.0 - self.alpha) * entry.cost_ema;
            entry.duration_ema_ms =
                self.alpha * duration_ms as f64 + (1.0 - self.alpha) * entry.duration_ema_ms;
        }
        entry.count += 1;
    }

    /// Predict cost for a plan based on historical EMA.
    pub fn predict_cost(&self, plan_hash: u64) -> Option<f64> {
        self.records.read().get(&plan_hash).map(|r| r.cost_ema)
    }

    /// Find plans with cost within `threshold` ratio of the given plan's cost.
    pub fn similar_plans(&self, plan_hash: u64, threshold: f64) -> Vec<(u64, f64)> {
        let records = self.records.read();
        let base_cost = match records.get(&plan_hash) {
            Some(r) => r.cost_ema,
            None => return vec![],
        };
        records
            .iter()
            .filter(|(&h, r)| {
                h != plan_hash && {
                    let ratio = if base_cost > r.cost_ema {
                        r.cost_ema / base_cost
                    } else if r.cost_ema > 0.0 {
                        base_cost / r.cost_ema
                    } else {
                        0.0
                    };
                    ratio >= threshold
                }
            })
            .map(|(&h, r)| (h, r.cost_ema))
            .collect()
    }
}

impl Default for PlanCostPredictor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Automatic Index Recommender (filter-frequency based)
// ---------------------------------------------------------------------------

/// A recommendation produced by `IndexRecommender`.
#[derive(Debug, Clone)]
pub struct SmartIndexRecommendation {
    /// Table name
    pub table: String,
    /// Columns to index
    pub columns: Vec<String>,
    /// Estimated speedup factor
    pub estimated_speedup: f64,
    /// Number of queries that would benefit
    pub query_count: u64,
}

/// Recommends indexes based on filter column frequency across recorded queries.
pub struct IndexRecommender {
    /// column → frequency in filter predicates
    filter_freq: RwLock<HashMap<String, u64>>,
    /// column → frequency in any access
    access_freq: RwLock<HashMap<String, u64>>,
    total_queries: RwLock<u64>,
}

impl IndexRecommender {
    pub fn new() -> Self {
        Self {
            filter_freq: RwLock::new(HashMap::new()),
            access_freq: RwLock::new(HashMap::new()),
            total_queries: RwLock::new(0),
        }
    }

    /// Record a query's column access patterns.
    pub fn record_query(&self, columns_accessed: &[String], filter_columns: &[String]) {
        *self.total_queries.write() += 1;
        {
            let mut access = self.access_freq.write();
            for col in columns_accessed {
                *access.entry(col.clone()).or_insert(0) += 1;
            }
        }
        {
            let mut filters = self.filter_freq.write();
            for col in filter_columns {
                *filters.entry(col.clone()).or_insert(0) += 1;
            }
        }
    }

    /// Produce index recommendations sorted by estimated benefit.
    pub fn recommend(&self) -> Vec<SmartIndexRecommendation> {
        let filters = self.filter_freq.read();
        let total = *self.total_queries.read();
        if total == 0 {
            return vec![];
        }
        let mut recs: Vec<SmartIndexRecommendation> = filters
            .iter()
            .filter(|(_, &count)| count >= 2)
            .map(|(col, &count)| {
                let frequency_ratio = count as f64 / total as f64;
                SmartIndexRecommendation {
                    table: col.split('.').next().unwrap_or(col).to_string(),
                    columns: vec![col.clone()],
                    estimated_speedup: 1.0 + frequency_ratio * 9.0, // 1x–10x
                    query_count: count,
                }
            })
            .collect();
        recs.sort_by(|a, b| {
            b.estimated_speedup
                .partial_cmp(&a.estimated_speedup)
                .unwrap()
        });
        recs
    }
}

impl Default for IndexRecommender {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Query Plan A/B Tester
// ---------------------------------------------------------------------------

/// Identifies which plan variant is being tested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanVariant {
    PlanA,
    PlanB,
}

/// Statistics for a completed or in-progress experiment.
#[derive(Debug, Clone)]
pub struct ExperimentStats {
    pub plan_a_runs: u64,
    pub plan_b_runs: u64,
    pub plan_a_avg_ms: f64,
    pub plan_b_avg_ms: f64,
    /// Approximate p-value from Welch's t-test.
    pub p_value: f64,
}

/// An A/B test between two query plans.
pub struct PlanExperiment {
    name: String,
    plan_a_hash: u64,
    plan_b_hash: u64,
    plan_a_times: RwLock<Vec<f64>>,
    plan_b_times: RwLock<Vec<f64>>,
}

impl PlanExperiment {
    pub fn new(name: impl Into<String>, plan_a_hash: u64, plan_b_hash: u64) -> Self {
        Self {
            name: name.into(),
            plan_a_hash,
            plan_b_hash,
            plan_a_times: RwLock::new(Vec::new()),
            plan_b_times: RwLock::new(Vec::new()),
        }
    }

    /// Record a duration for a plan variant.
    pub fn record_result(&self, plan: PlanVariant, duration_ms: u64) {
        match plan {
            PlanVariant::PlanA => self.plan_a_times.write().push(duration_ms as f64),
            PlanVariant::PlanB => self.plan_b_times.write().push(duration_ms as f64),
        }
    }

    /// Return the statistically better plan, if significance threshold is met.
    pub fn winner(&self) -> Option<PlanVariant> {
        let stats = self.stats();
        if stats.plan_a_runs < 3 || stats.plan_b_runs < 3 {
            return None;
        }
        if stats.p_value > 0.05 {
            return None;
        }
        if stats.plan_a_avg_ms < stats.plan_b_avg_ms {
            Some(PlanVariant::PlanA)
        } else {
            Some(PlanVariant::PlanB)
        }
    }

    /// Compute experiment statistics.
    pub fn stats(&self) -> ExperimentStats {
        let a = self.plan_a_times.read();
        let b = self.plan_b_times.read();

        let mean_a = Self::mean(&a);
        let mean_b = Self::mean(&b);
        let p_value = Self::welch_t_test(&a, &b);

        ExperimentStats {
            plan_a_runs: a.len() as u64,
            plan_b_runs: b.len() as u64,
            plan_a_avg_ms: mean_a,
            plan_b_avg_ms: mean_b,
            p_value,
        }
    }

    /// Name of this experiment.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Plan A hash.
    pub fn plan_a_hash(&self) -> u64 {
        self.plan_a_hash
    }

    /// Plan B hash.
    pub fn plan_b_hash(&self) -> u64 {
        self.plan_b_hash
    }

    // --- helpers ---

    fn mean(values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.iter().sum::<f64>() / values.len() as f64
    }

    fn variance(values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        let m = Self::mean(values);
        values.iter().map(|v| (v - m).powi(2)).sum::<f64>() / (values.len() - 1) as f64
    }

    /// Welch's t-test approximation returning an approximate p-value.
    fn welch_t_test(a: &[f64], b: &[f64]) -> f64 {
        if a.len() < 2 || b.len() < 2 {
            return 1.0;
        }
        let var_a = Self::variance(a);
        let var_b = Self::variance(b);
        let n_a = a.len() as f64;
        let n_b = b.len() as f64;
        let se = (var_a / n_a + var_b / n_b).sqrt();
        if se < 1e-12 {
            // Zero variance: if means differ, it's perfectly significant
            let mean_diff = (Self::mean(a) - Self::mean(b)).abs();
            return if mean_diff > 1e-9 { 0.0 } else { 1.0 };
        }
        let t = (Self::mean(a) - Self::mean(b)).abs() / se;
        let _df = n_a + n_b - 2.0;
        // Approximate p-value using a simple sigmoid mapping of the t-statistic
        // This is a lightweight approximation suitable for an embedded engine.
        let p = (-0.7 * t + 1.5_f64).exp() / (1.0 + (-0.7 * t + 1.5_f64).exp());
        p.clamp(0.0, 1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_features_to_vector() {
        let features = QueryFeatures {
            tables: vec!["users".into(), "orders".into()],
            num_joins: 1,
            num_predicates: 2,
            num_aggregations: 1,
            has_order_by: true,
            has_group_by: true,
            limit: Some(100),
            predicate_columns: vec!["id".into()],
        };

        let vec = features.to_feature_vector();
        assert_eq!(vec.len(), QueryFeatures::NUM_FEATURES);
        assert_eq!(vec[0], 2.0); // 2 tables
        assert_eq!(vec[1], 1.0); // 1 join
    }

    #[test]
    fn test_workload_collector() {
        let collector = WorkloadCollector::new(100);

        let entry = WorkloadEntry {
            sql: "SELECT * FROM users".to_string(),
            duration: Duration::from_millis(50),
            rows_returned: 100,
            estimated_cardinality: 100,
            actual_cardinality: 95,
            join_strategy: None,
            timestamp: Instant::now(),
            features: QueryFeatures {
                tables: vec!["users".into()],
                ..Default::default()
            },
        };

        collector.record(entry);
        assert_eq!(collector.num_entries(), 1);

        let stats = collector.table_stats("users").unwrap();
        assert_eq!(stats.query_count, 1);
    }

    #[test]
    fn test_workload_collector_max_entries() {
        let collector = WorkloadCollector::new(5);

        for i in 0..10 {
            collector.record(WorkloadEntry {
                sql: format!("SELECT {}", i),
                duration: Duration::from_millis(1),
                rows_returned: 1,
                estimated_cardinality: 1,
                actual_cardinality: 1,
                join_strategy: None,
                timestamp: Instant::now(),
                features: QueryFeatures::default(),
            });
        }

        assert_eq!(collector.num_entries(), 5);
    }

    #[test]
    fn test_learned_cardinality_model() {
        let model = LearnedCardinalityModel::default_model();

        // Train on synthetic data
        for _ in 0..20 {
            let features = QueryFeatures {
                tables: vec!["t1".into()],
                num_predicates: 1,
                ..Default::default()
            };
            model.train(&features, 1000.0);
        }

        assert!(model.is_trained());
        assert!(model.samples_seen() >= 10);

        // Prediction should be in reasonable range
        let features = QueryFeatures {
            tables: vec!["t1".into()],
            num_predicates: 1,
            ..Default::default()
        };
        let pred = model.predict(&features);
        assert!(pred > 0.0);
    }

    #[test]
    fn test_adaptive_strategy_selector() {
        let collector = Arc::new(WorkloadCollector::with_default_capacity());
        let model = Arc::new(LearnedCardinalityModel::default_model());
        let selector = AdaptiveStrategySelector::new(collector, model);

        // Without training data, defaults to 1000 rows → HashJoin
        let left_features = QueryFeatures {
            tables: vec!["t1".into()],
            ..Default::default()
        };
        let right_features = QueryFeatures {
            tables: vec!["t2".into()],
            ..Default::default()
        };
        let strategy = selector.recommend_join_strategy(&left_features, &right_features);
        // Default estimated cardinality is 1000 which is < broadcast_threshold
        assert!(
            strategy == JoinStrategy::BroadcastHashJoin || strategy == JoinStrategy::HashJoin,
            "Strategy was {:?}",
            strategy
        );
    }

    #[test]
    fn test_ai_optimizer_integration() {
        let optimizer = AiOptimizer::new();

        // Record some queries
        for i in 0..20 {
            let features = QueryFeatures {
                tables: vec!["orders".into()],
                num_predicates: 1,
                ..Default::default()
            };
            optimizer.record_query(
                "SELECT * FROM orders WHERE id > 100",
                features,
                1000,
                950 + i,
                Duration::from_millis(50),
                950 + i,
            );
        }

        // Get recommendations
        let features = QueryFeatures {
            tables: vec!["orders".into()],
            num_predicates: 1,
            ..Default::default()
        };
        let rec = optimizer.get_recommendations(&features);
        assert!(rec.estimated_cardinality > 0);
        assert!(rec.parallelism >= 1);
        assert!(rec.memory_budget >= 1024 * 1024);
        assert!(rec.model_confidence > 0.0);
    }

    #[test]
    fn test_join_strategy_display() {
        assert_eq!(JoinStrategy::HashJoin.to_string(), "HashJoin");
        assert_eq!(
            JoinStrategy::BroadcastHashJoin.to_string(),
            "BroadcastHashJoin"
        );
    }

    #[test]
    fn test_frequent_patterns() {
        let collector = WorkloadCollector::new(100);

        for _ in 0..5 {
            collector.record(WorkloadEntry {
                sql: "SELECT * FROM users".to_string(),
                duration: Duration::from_millis(10),
                rows_returned: 10,
                estimated_cardinality: 10,
                actual_cardinality: 10,
                join_strategy: None,
                timestamp: Instant::now(),
                features: QueryFeatures {
                    tables: vec!["users".into()],
                    ..Default::default()
                },
            });
        }

        let patterns = collector.frequent_patterns(5);
        assert_eq!(patterns.len(), 1);
        assert_eq!(patterns[0].1, 5);
    }

    // -----------------------------------------------------------------------
    // New feature tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_bayesian_auto_tuner_suggest_config() {
        let tuner = BayesianAutoTuner::new().with_exploration_rate(0.0);

        // Without observations the default config is returned
        let cfg = tuner.suggest_config();
        assert_eq!(cfg, TuningConfig::default());

        // Record two observations; the better one should be chosen
        tuner.record_observation(TuningObservation {
            config: TuningConfig {
                batch_size: 4096,
                parallelism: 2,
                hash_table_load_factor: 0.6,
                sort_memory_fraction: 0.2,
            },
            query_duration: Duration::from_millis(200),
            rows_processed: 10_000,
            memory_used_bytes: 4 * 1024 * 1024,
            timestamp: Instant::now(),
        });

        tuner.record_observation(TuningObservation {
            config: TuningConfig {
                batch_size: 16384,
                parallelism: 8,
                hash_table_load_factor: 0.8,
                sort_memory_fraction: 0.3,
            },
            query_duration: Duration::from_millis(50),
            rows_processed: 50_000,
            memory_used_bytes: 8 * 1024 * 1024,
            timestamp: Instant::now(),
        });

        let suggested = tuner.suggest_config();
        // The second observation has much higher throughput → its config wins
        assert_eq!(suggested.batch_size, 16384);
        assert_eq!(suggested.parallelism, 8);
    }

    #[test]
    fn test_tuning_observation_throughput_score() {
        let obs_fast = TuningObservation {
            config: TuningConfig::default(),
            query_duration: Duration::from_millis(10),
            rows_processed: 100_000,
            memory_used_bytes: 2 * 1024 * 1024,
            timestamp: Instant::now(),
        };
        let obs_slow = TuningObservation {
            config: TuningConfig::default(),
            query_duration: Duration::from_secs(1),
            rows_processed: 100_000,
            memory_used_bytes: 2 * 1024 * 1024,
            timestamp: Instant::now(),
        };

        let score_fast = BayesianAutoTuner::throughput_score(&obs_fast);
        let score_slow = BayesianAutoTuner::throughput_score(&obs_slow);
        assert!(score_fast > score_slow, "Faster query should score higher");
        assert!(score_fast > 0.0);
    }

    #[test]
    fn test_index_recommendation_via_analyzer() {
        let collector = Arc::new(WorkloadCollector::new(1000));

        for _ in 0..10 {
            collector.record(WorkloadEntry {
                sql: "SELECT * FROM users WHERE email = 'x'".into(),
                duration: Duration::from_millis(5),
                rows_returned: 1,
                estimated_cardinality: 1,
                actual_cardinality: 1,
                join_strategy: None,
                timestamp: Instant::now(),
                features: QueryFeatures {
                    tables: vec!["users".into()],
                    num_predicates: 1,
                    predicate_columns: vec!["email".into()],
                    ..Default::default()
                },
            });
        }

        let analyzer = WorkloadAnalyzer::new(collector);
        let recs = analyzer.recommend_indexes(5);
        assert!(!recs.is_empty());
        assert_eq!(recs[0].table_name, "users");
        assert_eq!(recs[0].columns, vec!["email".to_string()]);
        assert!(recs[0].estimated_benefit > 1.0);
    }

    #[test]
    fn test_cost_model_validator_accuracy() {
        let validator = CostModelValidator::new();
        // Perfect estimates → error ratio 0
        validator.record_validation("q1", 100.0, 100.0);
        validator.record_validation("q2", 200.0, 200.0);
        assert!(validator.mean_error_ratio() < 1e-9);
        assert!(validator.is_calibrated(0.1));

        // Poor estimate
        validator.record_validation("q3", 500.0, 100.0);
        assert!(validator.mean_error_ratio() > 0.0);

        let worst = validator.worst_queries(1);
        assert_eq!(worst.len(), 1);
        assert_eq!(worst[0].query_hash, "q3");

        // p90 should be the large error
        let p90 = validator.p90_error_ratio();
        assert!(p90 > 1.0);
    }

    #[test]
    fn test_workload_summarizer() {
        let collector = WorkloadCollector::new(1000);
        for i in 0..20 {
            collector.record(WorkloadEntry {
                sql: format!("SELECT * FROM orders WHERE id = {}", i),
                duration: Duration::from_millis(10 + i as u64),
                rows_returned: 50,
                estimated_cardinality: 60,
                actual_cardinality: 50,
                join_strategy: None,
                timestamp: Instant::now(),
                features: QueryFeatures {
                    tables: vec!["orders".into()],
                    num_predicates: 1,
                    predicate_columns: vec!["id".into()],
                    ..Default::default()
                },
            });
        }

        let summary = WorkloadSummarizer::summarize(&collector);
        assert_eq!(summary.total_queries, 20);
        assert!(summary.avg_duration >= Duration::from_millis(10));
        assert!(summary.cardinality_accuracy > 0.5);

        let report = summary.format_report();
        assert!(report.contains("Total queries: 20"));
        assert!(report.contains("orders"));
    }

    #[test]
    fn test_workload_analyzer_materializations() {
        let collector = Arc::new(WorkloadCollector::new(1000));
        // Record a repeated pattern
        for _ in 0..5 {
            collector.record(WorkloadEntry {
                sql: "SELECT * FROM orders JOIN users ON orders.uid = users.id".into(),
                duration: Duration::from_millis(100),
                rows_returned: 500,
                estimated_cardinality: 500,
                actual_cardinality: 500,
                join_strategy: Some("HashJoin".into()),
                timestamp: Instant::now(),
                features: QueryFeatures {
                    tables: vec!["orders".into(), "users".into()],
                    num_joins: 1,
                    predicate_columns: vec!["uid".into(), "id".into()],
                    ..Default::default()
                },
            });
        }

        let analyzer = WorkloadAnalyzer::new(collector);
        let mats = analyzer.recommend_materializations(5);
        assert!(!mats.is_empty());
        assert!(mats[0].query_frequency >= 5);
        assert!(mats[0].estimated_speedup > 1.0);
        assert!(mats[0].source_tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_ab_tester_record_and_best_strategy() {
        let tester = PlanABTester::new(0.1);
        assert_eq!(tester.total_tests(), 0);
        assert!(tester.best_strategy("hash", "merge").is_none());

        tester.record_result(ABTestResult {
            plan_a_name: "hash".into(),
            plan_b_name: "merge".into(),
            plan_a_latency_ms: 10.0,
            plan_b_latency_ms: 20.0,
            winner: "hash".into(),
            speedup_pct: 50.0,
            sample_size: 1,
        });
        tester.record_result(ABTestResult {
            plan_a_name: "hash".into(),
            plan_b_name: "merge".into(),
            plan_a_latency_ms: 12.0,
            plan_b_latency_ms: 18.0,
            winner: "hash".into(),
            speedup_pct: 33.3,
            sample_size: 1,
        });
        assert_eq!(tester.total_tests(), 2);
        let best = tester.best_strategy("hash", "merge").unwrap();
        assert_eq!(best, "hash");

        // Query in reverse order should still work
        let best_rev = tester.best_strategy("merge", "hash").unwrap();
        assert_eq!(best_rev, "hash");
    }

    #[test]
    fn test_ab_tester_exploration() {
        let tester = PlanABTester::new(0.5);
        let mut explore_count = 0;
        for _ in 0..10 {
            if tester.should_explore() {
                explore_count += 1;
            }
        }
        // With rate 0.5, threshold = 2, so every 2nd call should explore => 5 out of 10
        assert_eq!(explore_count, 5);
    }

    #[test]
    fn test_model_drift_detection() {
        let detector = ModelDriftDetector::new(5, 2.0);
        assert!(!detector.is_drifting());
        assert_eq!(detector.mean_q_error(), 1.0);

        // Good predictions (q-error near 1.0)
        detector.record_prediction(100.0, 100.0);
        detector.record_prediction(200.0, 200.0);
        assert!(!detector.is_drifting());
        assert!((detector.mean_q_error() - 1.0).abs() < 0.01);

        // Bad predictions that cause drift
        detector.record_prediction(100.0, 1000.0); // q-error = 10
        detector.record_prediction(100.0, 500.0); // q-error = 5
        detector.record_prediction(100.0, 300.0); // q-error = 3
        assert!(detector.is_drifting());
        assert!(detector.max_q_error() >= 10.0);
    }

    #[test]
    fn test_model_drift_threshold() {
        let detector = ModelDriftDetector::new(3, 5.0);
        detector.record_prediction(100.0, 200.0); // q-error = 2
        detector.record_prediction(100.0, 300.0); // q-error = 3
        detector.record_prediction(100.0, 400.0); // q-error = 4
                                                  // Mean = 3.0, threshold = 5.0 => not drifting
        assert!(!detector.is_drifting());
        // But window is full => should retrain
        assert!(detector.should_retrain());
    }

    #[test]
    fn test_cardinality_feedback_loop() {
        let fb = CardinalityFeedbackLoop::new();
        assert_eq!(fb.get_correction("users"), 1.0);
        assert_eq!(fb.corrected_estimate("users", 100.0), 100.0);

        // Actual is 2x estimated repeatedly
        for _ in 0..20 {
            fb.record_feedback("users", 100.0, 200.0);
        }
        let correction = fb.get_correction("users");
        // EMA with alpha=0.1 converges toward 2.0
        assert!(correction > 1.5, "correction was {}", correction);
        assert!(correction <= 2.0, "correction was {}", correction);

        let corrected = fb.corrected_estimate("users", 100.0);
        assert!(corrected > 150.0);

        let tables = fb.tables_with_feedback();
        assert!(tables.contains(&"users".to_string()));
    }

    #[test]
    fn test_cardinality_correction_factor() {
        let fb = CardinalityFeedbackLoop::new();
        // Single observation: factor = 0.9 * 1.0 + 0.1 * (500/100) = 0.9 + 0.5 = 1.4
        fb.record_feedback("orders", 100.0, 500.0);
        let correction = fb.get_correction("orders");
        assert!(
            (correction - 1.4).abs() < 0.01,
            "correction was {}",
            correction
        );

        // Second observation: factor = 0.9 * 1.4 + 0.1 * (500/100) = 1.26 + 0.5 = 1.76
        fb.record_feedback("orders", 100.0, 500.0);
        let correction = fb.get_correction("orders");
        assert!(
            (correction - 1.76).abs() < 0.01,
            "correction was {}",
            correction
        );
    }

    // -----------------------------------------------------------------------
    // Runtime Re-optimizer tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_runtime_reoptimizer_trigger() {
        let mut reopt = RuntimeReoptimizer::new(3.0, 10000);
        // 10x overestimate should trigger
        assert!(reopt.should_reoptimize(100, 1000));
        // 2x is within threshold
        assert!(!reopt.should_reoptimize(100, 200));
        // Record reopt
        reopt.record_reopt();
        assert_eq!(reopt.reopt_count(), 1);
    }

    #[test]
    fn test_runtime_reoptimizer_max_reopts() {
        let mut reopt = RuntimeReoptimizer::new(2.0, 10000);
        reopt.record_reopt();
        reopt.record_reopt();
        reopt.record_reopt();
        assert!(!reopt.can_reoptimize());
        assert!(!reopt.should_reoptimize(100, 10000));
    }

    // -----------------------------------------------------------------------
    // Adaptive Parallelism tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_adaptive_parallelism_scale_up() {
        let mut ap = AdaptiveParallelism::new(2, 8);
        assert_eq!(ap.recommended_threads(), 2);
        // Low utilization should scale up
        for _ in 0..5 {
            ap.record_utilization(0.3);
        }
        assert!(ap.recommended_threads() > 2);
    }

    #[test]
    fn test_adaptive_parallelism_scale_down() {
        let mut ap = AdaptiveParallelism::new(2, 8);
        // Start at 2, so scale up first
        for _ in 0..5 {
            ap.record_utilization(0.3);
        }
        let threads_after_up = ap.recommended_threads();
        // High utilization should scale down (need enough samples to flush history)
        for _ in 0..20 {
            ap.record_utilization(0.95);
        }
        assert!(ap.recommended_threads() < threads_after_up);
    }

    // -----------------------------------------------------------------------
    // Workload Clustering tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_workload_clusterer() {
        let mut clusterer = WorkloadClusterer::new(5.0);
        clusterer.add_query(&[1.0, 2.0, 3.0], "SELECT * FROM users", 100);
        clusterer.add_query(&[1.1, 2.1, 3.1], "SELECT * FROM users WHERE id = 1", 150);
        clusterer.add_query(
            &[10.0, 20.0, 30.0],
            "SELECT * FROM orders JOIN products",
            500,
        );

        assert_eq!(clusterer.num_clusters(), 2); // Two distinct clusters
    }

    #[test]
    fn test_workload_clusterer_hot_clusters() {
        let mut clusterer = WorkloadClusterer::new(5.0);
        clusterer.add_query(&[1.0, 0.0], "fast query", 10);
        clusterer.add_query(&[100.0, 100.0], "slow query", 10000);

        let hot = clusterer.hot_clusters();
        assert_eq!(hot[0].total_execution_time_us, 10000);
    }

    // -----------------------------------------------------------------------
    // Pattern Detector tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_pattern_detector() {
        let mut detector = PatternDetector::new();
        detector.record("SELECT * FROM users WHERE id = 1", 100);
        detector.record("SELECT * FROM orders WHERE status = 'active'", 200);
        detector.record("SELECT * FROM users WHERE name = 'Alice'", 150);

        assert_eq!(detector.num_patterns(), 1); // Same pattern: SW
        let top = detector.top_patterns(1);
        assert_eq!(top[0].count, 3);
    }

    #[test]
    fn test_pattern_detector_different_patterns() {
        let mut detector = PatternDetector::new();
        detector.record("SELECT * FROM users", 100);
        detector.record("SELECT * FROM users WHERE id = 1 ORDER BY name", 200);
        detector.record(
            "SELECT * FROM users JOIN orders ON u.id = o.user_id GROUP BY u.name",
            300,
        );

        assert!(detector.num_patterns() >= 2); // Different shapes
    }

    // -----------------------------------------------------------------------
    // Auto-Indexer tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_auto_indexer() {
        let mut indexer = AutoIndexer::new(3);
        let patterns = vec![];
        let mut access = HashMap::new();
        access.insert("users.id".to_string(), 10);
        access.insert("users.name".to_string(), 5);
        access.insert("orders.date".to_string(), 1); // Below threshold

        indexer.analyze_workload(&patterns, &access);
        assert_eq!(indexer.recommendations().len(), 2); // users.id and users.name
        assert!(
            indexer.recommendations()[0].estimated_benefit
                >= indexer.recommendations()[1].estimated_benefit
        );
    }

    #[test]
    fn test_auto_indexer_mark_applied() {
        let mut indexer = AutoIndexer::new(1);
        indexer.mark_applied("users", &["id".to_string()]);
        assert_eq!(indexer.applied_count(), 1);
    }

    // -----------------------------------------------------------------------
    // GradientCardinalityModel tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_gradient_model_predict_untrained() {
        let model = GradientCardinalityModel::new(0.01, 3);
        let pred = model.predict(&[1.0, 2.0, 3.0]);
        assert_eq!(pred, 0.0); // all weights zero → prediction 0
    }

    #[test]
    fn test_gradient_model_train_and_converge() {
        let model = GradientCardinalityModel::new(0.01, 2);
        // Train on a simple pattern: actual = 3*x0 + 2*x1
        for _ in 0..500 {
            model.train(&[1.0, 1.0], 5.0);
            model.train(&[2.0, 0.0], 6.0);
            model.train(&[0.0, 3.0], 6.0);
        }
        let pred = model.predict(&[1.0, 1.0]);
        assert!((pred - 5.0).abs() < 0.5, "expected ~5.0, got {pred}");
    }

    #[test]
    fn test_gradient_model_metrics() {
        let model = GradientCardinalityModel::new(0.01, 2);
        model.train(&[1.0, 1.0], 10.0);
        model.train(&[2.0, 2.0], 20.0);
        let m = model.metrics();
        assert_eq!(m.training_samples, 2);
        assert!(m.mean_error >= 0.0);
        assert!(m.max_error >= m.mean_error);
    }

    #[test]
    fn test_gradient_model_accuracy_zero_when_no_training() {
        let model = GradientCardinalityModel::new(0.01, 2);
        assert_eq!(model.accuracy(), 0.0);
    }

    // -----------------------------------------------------------------------
    // PlanCostPredictor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_plan_cost_predictor_record_and_predict() {
        let predictor = PlanCostPredictor::new();
        assert!(predictor.predict_cost(42).is_none());
        predictor.record_execution(42, 100.0, 50);
        let cost = predictor.predict_cost(42).unwrap();
        assert!((cost - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_plan_cost_predictor_ema_update() {
        let predictor = PlanCostPredictor::new();
        predictor.record_execution(1, 100.0, 50);
        predictor.record_execution(1, 200.0, 80);
        let cost = predictor.predict_cost(1).unwrap();
        // EMA: 0.3*200 + 0.7*100 = 130
        assert!((cost - 130.0).abs() < 1e-9, "expected 130.0, got {cost}");
    }

    #[test]
    fn test_plan_cost_predictor_similar_plans() {
        let predictor = PlanCostPredictor::new();
        predictor.record_execution(1, 100.0, 50);
        predictor.record_execution(2, 95.0, 48);
        predictor.record_execution(3, 10.0, 5);
        let similar = predictor.similar_plans(1, 0.9);
        assert!(similar.iter().any(|(h, _)| *h == 2));
        assert!(!similar.iter().any(|(h, _)| *h == 3));
    }

    // -----------------------------------------------------------------------
    // IndexRecommender tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_index_recommender_no_queries() {
        let rec = IndexRecommender::new();
        assert!(rec.recommend().is_empty());
    }

    #[test]
    fn test_index_recommender_basic() {
        let rec = IndexRecommender::new();
        rec.record_query(
            &["users.id".into(), "users.name".into()],
            &["users.id".into()],
        );
        rec.record_query(
            &["users.id".into(), "orders.total".into()],
            &["users.id".into(), "orders.total".into()],
        );
        rec.record_query(&["orders.total".into()], &["orders.total".into()]);
        let recs = rec.recommend();
        // users.id appears in filters 2 times, orders.total 2 times
        assert_eq!(recs.len(), 2);
        assert!(recs[0].query_count >= 2);
    }

    #[test]
    fn test_index_recommender_filters_below_threshold() {
        let rec = IndexRecommender::new();
        rec.record_query(&["a".into()], &["a".into()]);
        // Only 1 occurrence of "a" in filters → below threshold of 2
        assert!(rec.recommend().is_empty());
    }

    // -----------------------------------------------------------------------
    // PlanExperiment tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_plan_experiment_no_winner_insufficient_data() {
        let exp = PlanExperiment::new("test", 1, 2);
        exp.record_result(PlanVariant::PlanA, 100);
        exp.record_result(PlanVariant::PlanB, 200);
        assert!(exp.winner().is_none()); // <3 runs each
    }

    #[test]
    fn test_plan_experiment_stats() {
        let exp = PlanExperiment::new("test", 1, 2);
        for _ in 0..5 {
            exp.record_result(PlanVariant::PlanA, 10);
            exp.record_result(PlanVariant::PlanB, 100);
        }
        let stats = exp.stats();
        assert_eq!(stats.plan_a_runs, 5);
        assert_eq!(stats.plan_b_runs, 5);
        assert!((stats.plan_a_avg_ms - 10.0).abs() < 1e-9);
        assert!((stats.plan_b_avg_ms - 100.0).abs() < 1e-9);
    }

    #[test]
    fn test_plan_experiment_clear_winner() {
        let exp = PlanExperiment::new("perf_test", 10, 20);
        // PlanA consistently faster
        for _ in 0..20 {
            exp.record_result(PlanVariant::PlanA, 10);
            exp.record_result(PlanVariant::PlanB, 500);
        }
        let winner = exp.winner();
        assert_eq!(winner, Some(PlanVariant::PlanA));
    }

    #[test]
    fn test_plan_experiment_no_winner_similar_plans() {
        let exp = PlanExperiment::new("similar", 1, 2);
        for _ in 0..10 {
            exp.record_result(PlanVariant::PlanA, 100);
            exp.record_result(PlanVariant::PlanB, 100);
        }
        // Identical results → p-value should be high → no winner
        assert!(exp.winner().is_none());
    }

    #[test]
    fn test_plan_experiment_accessors() {
        let exp = PlanExperiment::new("my_exp", 42, 84);
        assert_eq!(exp.name(), "my_exp");
        assert_eq!(exp.plan_a_hash(), 42);
        assert_eq!(exp.plan_b_hash(), 84);
    }

    // -----------------------------------------------------------------------
    // Cost model learning and runtime adaptation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cost_model_learner() {
        let mut learner = CostModelLearner::new();

        // Record observations
        learner.record_observation("HashJoin", 1000, 100.0);
        learner.record_observation("HashJoin", 2000, 200.0);
        learner.record_observation("HashJoin", 3000, 300.0);

        // Predict cost
        let predicted = learner.predict_cost("HashJoin", 1500);
        assert!(predicted > 0.0);
        assert!(predicted < 400.0);
    }

    #[test]
    fn test_cost_model_learner_retrain() {
        let mut learner = CostModelLearner::new();

        learner.record_observation("Scan", 100, 10.0);
        learner.record_observation("Scan", 200, 20.0);

        learner.retrain();

        let predicted = learner.predict_cost("Scan", 150);
        assert!((predicted - 15.0).abs() < 5.0);
    }

    #[test]
    fn test_runtime_advisor() {
        let advisor = RuntimeAdvisor::new();

        let stats = ExecutionStats {
            peak_memory: 2 * 1024 * 1024 * 1024, // 2GB
            cpu_utilization: 0.2,
            spill_count: 5,
            operator_timings: HashMap::new(),
        };

        let adaptations = advisor.advise(&stats);
        assert!(adaptations.len() > 0);
    }

    #[test]
    fn test_runtime_advisor_hash_join_oom() {
        let advisor = RuntimeAdvisor::new();

        let stats = ExecutionStats {
            peak_memory: 3 * 1024 * 1024 * 1024, // 3GB
            cpu_utilization: 0.5,
            spill_count: 0,
            operator_timings: HashMap::from([("HashJoin".to_string(), 1000.0)]),
        };

        let adaptations = advisor.advise(&stats);
        assert!(adaptations
            .iter()
            .any(|a| matches!(a, PlanAdaptation::SwitchJoinAlgorithm)));
    }

    #[test]
    fn test_plan_adaptation_types() {
        let adapt1 = PlanAdaptation::SwitchJoinAlgorithm;
        let adapt2 = PlanAdaptation::IncreaseParallelism;
        let adapt3 = PlanAdaptation::ReduceBatchSize;
        let adapt4 = PlanAdaptation::EnablePrefetch;

        assert!(matches!(adapt1, PlanAdaptation::SwitchJoinAlgorithm));
        assert!(matches!(adapt2, PlanAdaptation::IncreaseParallelism));
        assert!(matches!(adapt3, PlanAdaptation::ReduceBatchSize));
        assert!(matches!(adapt4, PlanAdaptation::EnablePrefetch));
    }
}

// ---------------------------------------------------------------------------
// Cost model learning and runtime adaptation implementation
// ---------------------------------------------------------------------------

/// Cost model learner using simple linear regression per operator type.
#[derive(Debug)]
pub struct CostModelLearner {
    /// Observations: operator_type -> (input_rows, actual_cost_ms)
    observations: HashMap<String, Vec<(usize, f64)>>,
    /// Learned models: operator_type -> (slope, intercept)
    models: HashMap<String, (f64, f64)>,
}

impl CostModelLearner {
    pub fn new() -> Self {
        Self {
            observations: HashMap::new(),
            models: HashMap::new(),
        }
    }

    /// Record an observation of operator execution.
    pub fn record_observation(
        &mut self,
        operator_type: &str,
        input_rows: usize,
        actual_cost_ms: f64,
    ) {
        self.observations
            .entry(operator_type.to_string())
            .or_insert_with(Vec::new)
            .push((input_rows, actual_cost_ms));
    }

    /// Predict cost for an operator given estimated input rows.
    pub fn predict_cost(&self, operator_type: &str, estimated_input_rows: usize) -> f64 {
        if let Some((slope, intercept)) = self.models.get(operator_type) {
            slope * estimated_input_rows as f64 + intercept
        } else {
            // No model available - return a default
            estimated_input_rows as f64 * 0.01 // 0.01ms per row default
        }
    }

    /// Retrain all models based on collected observations.
    pub fn retrain(&mut self) {
        for (operator_type, observations) in &self.observations {
            if observations.len() < 2 {
                continue; // Need at least 2 points for linear regression
            }

            // Simple linear regression: y = slope * x + intercept
            let n = observations.len() as f64;
            let sum_x: f64 = observations.iter().map(|(x, _)| *x as f64).sum();
            let sum_y: f64 = observations.iter().map(|(_, y)| *y).sum();
            let sum_xy: f64 = observations.iter().map(|(x, y)| *x as f64 * y).sum();
            let sum_x2: f64 = observations.iter().map(|(x, _)| (*x as f64).powi(2)).sum();

            let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x.powi(2));
            let intercept = (sum_y - slope * sum_x) / n;

            self.models
                .insert(operator_type.clone(), (slope, intercept));
        }
    }

    /// Get the number of operator types with learned models.
    pub fn model_count(&self) -> usize {
        self.models.len()
    }

    /// Get the number of observations for an operator type.
    pub fn observation_count(&self, operator_type: &str) -> usize {
        self.observations
            .get(operator_type)
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

impl Default for CostModelLearner {
    fn default() -> Self {
        Self::new()
    }
}

/// Plan adaptation suggestions.
#[derive(Debug, Clone, PartialEq)]
pub enum PlanAdaptation {
    /// Switch from hash join to sort-merge join
    SwitchJoinAlgorithm,
    /// Increase degree of parallelism
    IncreaseParallelism,
    /// Reduce batch size to lower memory usage
    ReduceBatchSize,
    /// Enable prefetching for sequential scans
    EnablePrefetch,
}

/// Execution statistics for runtime advice.
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    pub peak_memory: usize,                     // bytes
    pub cpu_utilization: f64,                   // 0.0 to 1.0
    pub spill_count: usize,                     // number of times data spilled to disk
    pub operator_timings: HashMap<String, f64>, // operator -> elapsed_ms
}

/// Runtime advisor that suggests plan adaptations.
#[derive(Debug)]
pub struct RuntimeAdvisor {
    // Configuration thresholds
    high_memory_threshold: usize,
    low_cpu_threshold: f64,
    spill_threshold: usize,
}

impl RuntimeAdvisor {
    pub fn new() -> Self {
        Self {
            high_memory_threshold: 2 * 1024 * 1024 * 1024, // 2GB
            low_cpu_threshold: 0.3,                        // 30%
            spill_threshold: 3,
        }
    }

    /// Analyze execution stats and provide adaptation suggestions.
    pub fn advise(&self, execution_stats: &ExecutionStats) -> Vec<PlanAdaptation> {
        let mut adaptations = Vec::new();

        // Check for hash join OOM
        if execution_stats.peak_memory > self.high_memory_threshold {
            if execution_stats.operator_timings.contains_key("HashJoin") {
                adaptations.push(PlanAdaptation::SwitchJoinAlgorithm);
            }
        }

        // Check for sort spilling
        if execution_stats.spill_count > self.spill_threshold {
            adaptations.push(PlanAdaptation::ReduceBatchSize);
        }

        // Check for CPU underutilization
        if execution_stats.cpu_utilization < self.low_cpu_threshold {
            adaptations.push(PlanAdaptation::IncreaseParallelism);
        }

        // Check for sequential scans
        if execution_stats.operator_timings.contains_key("TableScan")
            || execution_stats.operator_timings.contains_key("Scan")
        {
            adaptations.push(PlanAdaptation::EnablePrefetch);
        }

        adaptations
    }

    /// Set high memory threshold.
    pub fn with_high_memory_threshold(mut self, threshold: usize) -> Self {
        self.high_memory_threshold = threshold;
        self
    }

    /// Set low CPU utilization threshold.
    pub fn with_low_cpu_threshold(mut self, threshold: f64) -> Self {
        self.low_cpu_threshold = threshold;
        self
    }

    /// Set spill threshold.
    pub fn with_spill_threshold(mut self, threshold: usize) -> Self {
        self.spill_threshold = threshold;
        self
    }
}

impl Default for RuntimeAdvisor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Execution Telemetry (Per-Operator Metrics Collection)
// ---------------------------------------------------------------------------

/// Detailed per-operator execution metrics for feedback-driven optimization.
#[derive(Debug, Clone)]
pub struct OperatorTelemetry {
    /// Operator name (e.g. "HashJoin", "TableScan").
    pub operator: String,
    /// Estimated rows (from cost model).
    pub estimated_rows: usize,
    /// Actual rows produced.
    pub actual_rows: usize,
    /// Wall-clock execution time in microseconds.
    pub execution_us: u64,
    /// Peak memory usage in bytes.
    pub peak_memory_bytes: usize,
    /// Whether the operator spilled to disk.
    pub spilled: bool,
    /// Batch size used.
    pub batch_size: usize,
}

/// Collects execution telemetry across query runs and produces aggregate stats.
#[derive(Debug)]
pub struct TelemetryCollector {
    records: Vec<OperatorTelemetry>,
    max_records: usize,
}

impl TelemetryCollector {
    pub fn new(max_records: usize) -> Self {
        Self {
            records: Vec::new(),
            max_records,
        }
    }

    /// Record a new operator execution.
    pub fn record(&mut self, telemetry: OperatorTelemetry) {
        if self.records.len() >= self.max_records {
            self.records.remove(0);
        }
        self.records.push(telemetry);
    }

    /// Compute the average estimation error across all recorded operators.
    /// Returns the ratio `actual / estimated` (1.0 = perfect).
    pub fn estimation_accuracy(&self) -> f64 {
        if self.records.is_empty() {
            return 1.0;
        }
        let mut total_ratio = 0.0;
        let mut count = 0;
        for r in &self.records {
            if r.estimated_rows > 0 {
                total_ratio += r.actual_rows as f64 / r.estimated_rows as f64;
                count += 1;
            }
        }
        if count == 0 {
            1.0
        } else {
            total_ratio / count as f64
        }
    }

    /// Return the fraction of operators that spilled.
    pub fn spill_rate(&self) -> f64 {
        if self.records.is_empty() {
            return 0.0;
        }
        let spilled = self.records.iter().filter(|r| r.spilled).count();
        spilled as f64 / self.records.len() as f64
    }

    /// Average execution time per operator type.
    pub fn avg_time_by_operator(&self) -> HashMap<String, f64> {
        let mut sums: HashMap<String, (f64, usize)> = HashMap::new();
        for r in &self.records {
            let entry = sums.entry(r.operator.clone()).or_insert((0.0, 0));
            entry.0 += r.execution_us as f64;
            entry.1 += 1;
        }
        sums.into_iter()
            .map(|(k, (sum, count))| (k, sum / count as f64))
            .collect()
    }

    pub fn record_count(&self) -> usize {
        self.records.len()
    }
}

impl Default for TelemetryCollector {
    fn default() -> Self {
        Self::new(10_000)
    }
}

// ---------------------------------------------------------------------------
// Dynamic Batch Size Tuner
// ---------------------------------------------------------------------------

/// Dynamically adjusts query batch size based on memory pressure and spill rate.
#[derive(Debug)]
pub struct BatchSizeTuner {
    current_batch_size: usize,
    min_batch_size: usize,
    max_batch_size: usize,
    /// Target spill rate below which we can increase batch size.
    target_spill_rate: f64,
    /// Memory pressure threshold (0.0-1.0) above which we shrink.
    memory_pressure_threshold: f64,
    adjustments: u64,
}

impl BatchSizeTuner {
    pub fn new(initial_batch_size: usize) -> Self {
        Self {
            current_batch_size: initial_batch_size,
            min_batch_size: 256,
            max_batch_size: 65_536,
            target_spill_rate: 0.05,
            memory_pressure_threshold: 0.8,
            adjustments: 0,
        }
    }

    /// Tune batch size based on observed spill rate and memory pressure.
    /// Returns the recommended batch size.
    pub fn tune(&mut self, spill_rate: f64, memory_pressure: f64) -> usize {
        self.adjustments += 1;

        if memory_pressure > self.memory_pressure_threshold || spill_rate > self.target_spill_rate {
            // Shrink batch size by 25%
            self.current_batch_size = (self.current_batch_size * 3 / 4).max(self.min_batch_size);
        } else if memory_pressure < self.memory_pressure_threshold * 0.5
            && spill_rate < self.target_spill_rate * 0.5
        {
            // Grow batch size by 25%
            self.current_batch_size = (self.current_batch_size * 5 / 4).min(self.max_batch_size);
        }

        self.current_batch_size
    }

    pub fn current_batch_size(&self) -> usize {
        self.current_batch_size
    }

    pub fn adjustments(&self) -> u64 {
        self.adjustments
    }

    pub fn with_bounds(mut self, min: usize, max: usize) -> Self {
        self.min_batch_size = min;
        self.max_batch_size = max;
        self
    }
}

// ---------------------------------------------------------------------------
// Multi-Armed Bandit Join Selector
// ---------------------------------------------------------------------------

/// Uses Upper Confidence Bound (UCB1) to select the best join algorithm
/// for a given query shape based on observed performance.
#[derive(Debug)]
pub struct BanditJoinSelector {
    /// For each join strategy: (total_reward, times_selected)
    arms: HashMap<String, (f64, u64)>,
    total_selections: u64,
    exploration_factor: f64,
}

impl BanditJoinSelector {
    pub fn new() -> Self {
        let mut arms = HashMap::new();
        for strategy in &["HashJoin", "SortMergeJoin", "NestedLoopJoin"] {
            arms.insert(strategy.to_string(), (0.0, 0));
        }
        Self {
            arms,
            total_selections: 0,
            exploration_factor: 1.41, // sqrt(2)
        }
    }

    /// Select the best join algorithm using UCB1.
    pub fn select(&self) -> String {
        // If any arm hasn't been tried, pick it
        for (name, (_, count)) in &self.arms {
            if *count == 0 {
                return name.clone();
            }
        }

        let total = self.total_selections as f64;
        self.arms
            .iter()
            .map(|(name, (reward, count))| {
                let avg_reward = reward / *count as f64;
                let exploration = self.exploration_factor * (total.ln() / *count as f64).sqrt();
                (name.clone(), avg_reward + exploration)
            })
            .max_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(name, _)| name)
            .unwrap_or_else(|| "HashJoin".to_string())
    }

    /// Record the outcome of using a join strategy.
    /// `reward` should be inversely proportional to execution time
    /// (e.g. `1.0 / execution_time_ms`).
    pub fn record_outcome(&mut self, strategy: &str, reward: f64) {
        if let Some(arm) = self.arms.get_mut(strategy) {
            arm.0 += reward;
            arm.1 += 1;
        }
        self.total_selections += 1;
    }

    /// Return the best-performing strategy based on average reward.
    pub fn best_strategy(&self) -> Option<String> {
        self.arms
            .iter()
            .filter(|(_, (_, count))| *count > 0)
            .max_by(|a, b| {
                let avg_a = a.1 .0 / a.1 .1 as f64;
                let avg_b = b.1 .0 / b.1 .1 as f64;
                avg_a
                    .partial_cmp(&avg_b)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|(name, _)| name.clone())
    }

    pub fn total_selections(&self) -> u64 {
        self.total_selections
    }
}

impl Default for BanditJoinSelector {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Model Serialization for Persistence
// ---------------------------------------------------------------------------

/// Serializable snapshot of a learned cost model for persistence across restarts.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelSnapshot {
    /// Version for forward compatibility.
    pub version: u32,
    /// Learned cost model parameters: operator_type -> (slope, intercept).
    pub cost_models: HashMap<String, (f64, f64)>,
    /// Join strategy rewards: strategy -> (total_reward, count).
    pub join_rewards: HashMap<String, (f64, u64)>,
    /// Recommended batch size.
    pub batch_size: usize,
    /// Number of queries observed when this snapshot was taken.
    pub queries_observed: u64,
    /// Timestamp (epoch seconds).
    pub created_at: u64,
}

impl ModelSnapshot {
    /// Create a snapshot from current optimizer state.
    pub fn capture(
        cost_models: &HashMap<String, (f64, f64)>,
        join_selector: &BanditJoinSelector,
        batch_size: usize,
        queries_observed: u64,
    ) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            version: 1,
            cost_models: cost_models.clone(),
            join_rewards: join_selector.arms.clone(),
            batch_size,
            queries_observed,
            created_at: now,
        }
    }

    /// Serialize to JSON bytes.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
            .map_err(|e| BlazeError::execution(format!("Failed to serialize model: {e}")))
    }

    /// Deserialize from JSON bytes.
    pub fn from_json(data: &[u8]) -> Result<Self> {
        serde_json::from_slice(data)
            .map_err(|e| BlazeError::execution(format!("Failed to deserialize model: {e}")))
    }

    /// Save snapshot to a file.
    pub fn save_to(&self, path: &std::path::Path) -> Result<()> {
        let json = self.to_json()?;
        std::fs::write(path, json)
            .map_err(|e| BlazeError::execution(format!("Failed to save model: {e}")))?;
        Ok(())
    }

    /// Load snapshot from a file.
    pub fn load_from(path: &std::path::Path) -> Result<Self> {
        let data = std::fs::read(path)
            .map_err(|e| BlazeError::execution(format!("Failed to load model: {e}")))?;
        Self::from_json(&data)
    }
}

#[cfg(test)]
mod learned_optimizer_new_tests {
    use super::*;

    #[test]
    fn test_telemetry_collector() {
        let mut tc = TelemetryCollector::new(100);
        tc.record(OperatorTelemetry {
            operator: "HashJoin".into(),
            estimated_rows: 100,
            actual_rows: 120,
            execution_us: 5000,
            peak_memory_bytes: 1024,
            spilled: false,
            batch_size: 8192,
        });
        tc.record(OperatorTelemetry {
            operator: "TableScan".into(),
            estimated_rows: 1000,
            actual_rows: 1000,
            execution_us: 2000,
            peak_memory_bytes: 512,
            spilled: false,
            batch_size: 8192,
        });

        assert_eq!(tc.record_count(), 2);
        assert!(tc.estimation_accuracy() > 0.0);
        assert_eq!(tc.spill_rate(), 0.0);

        let avg = tc.avg_time_by_operator();
        assert_eq!(avg.len(), 2);
        assert_eq!(avg["HashJoin"], 5000.0);
    }

    #[test]
    fn test_telemetry_spill_rate() {
        let mut tc = TelemetryCollector::new(100);
        for i in 0..10 {
            tc.record(OperatorTelemetry {
                operator: "Agg".into(),
                estimated_rows: 100,
                actual_rows: 100,
                execution_us: 1000,
                peak_memory_bytes: 1024,
                spilled: i < 3, // 3 out of 10 spill
                batch_size: 8192,
            });
        }
        assert!((tc.spill_rate() - 0.3).abs() < 0.01);
    }

    #[test]
    fn test_batch_size_tuner_shrink() {
        let mut tuner = BatchSizeTuner::new(8192);
        // High memory pressure → shrink
        let new_size = tuner.tune(0.1, 0.9);
        assert!(new_size < 8192);
        assert_eq!(tuner.adjustments(), 1);
    }

    #[test]
    fn test_batch_size_tuner_grow() {
        let mut tuner = BatchSizeTuner::new(4096);
        // Low pressure → grow
        let new_size = tuner.tune(0.01, 0.2);
        assert!(new_size > 4096);
    }

    #[test]
    fn test_batch_size_tuner_bounds() {
        let mut tuner = BatchSizeTuner::new(300).with_bounds(256, 1024);
        // Shrink below min
        for _ in 0..20 {
            tuner.tune(0.5, 0.95);
        }
        assert!(tuner.current_batch_size() >= 256);
    }

    #[test]
    fn test_bandit_join_selector() {
        let mut selector = BanditJoinSelector::new();

        // First 3 selections should explore all arms
        let s1 = selector.select();
        selector.record_outcome(&s1, 1.0);
        let s2 = selector.select();
        selector.record_outcome(&s2, 0.5);
        let s3 = selector.select();
        selector.record_outcome(&s3, 0.1);

        assert_eq!(selector.total_selections(), 3);

        // After more observations, HashJoin should win if it got highest reward
        for _ in 0..10 {
            selector.record_outcome("HashJoin", 1.0);
            selector.record_outcome("SortMergeJoin", 0.3);
            selector.record_outcome("NestedLoopJoin", 0.1);
        }
        assert_eq!(selector.best_strategy(), Some("HashJoin".to_string()));
    }

    #[test]
    fn test_model_snapshot_roundtrip() {
        let mut cost_models = HashMap::new();
        cost_models.insert("HashJoin".to_string(), (0.5, 10.0));
        cost_models.insert("TableScan".to_string(), (0.1, 1.0));

        let selector = BanditJoinSelector::new();
        let snapshot = ModelSnapshot::capture(&cost_models, &selector, 8192, 500);

        let json = snapshot.to_json().unwrap();
        let restored = ModelSnapshot::from_json(&json).unwrap();
        assert_eq!(restored.version, 1);
        assert_eq!(restored.batch_size, 8192);
        assert_eq!(restored.queries_observed, 500);
        assert_eq!(restored.cost_models.len(), 2);
    }

    #[test]
    fn test_model_snapshot_file_io() {
        let tmp = tempfile::TempDir::new().unwrap();
        let path = tmp.path().join("model.json");

        let cost_models = HashMap::new();
        let selector = BanditJoinSelector::new();
        let snapshot = ModelSnapshot::capture(&cost_models, &selector, 4096, 100);

        snapshot.save_to(&path).unwrap();
        let loaded = ModelSnapshot::load_from(&path).unwrap();
        assert_eq!(loaded.batch_size, 4096);
    }
}
