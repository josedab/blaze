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
}
