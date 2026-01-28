//! Benchmark comparison and regression detection framework.
//!
//! This module provides a programmatic benchmark framework for running queries,
//! timing them, comparing results, and detecting performance regressions.

use crate::error::Result;
use crate::Connection;
use std::time::{Duration, Instant};

/// A single benchmark query definition.
#[derive(Debug, Clone)]
pub struct BenchmarkQuery {
    /// Name of the benchmark
    pub name: String,
    /// SQL query to execute
    pub sql: String,
}

impl BenchmarkQuery {
    pub fn new(name: impl Into<String>, sql: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            sql: sql.into(),
        }
    }
}

/// Result of a single benchmark run.
#[derive(Debug, Clone)]
pub struct BenchmarkResult {
    /// Name of the benchmark
    pub name: String,
    /// Elapsed time for query execution
    pub elapsed: Duration,
    /// Number of rows returned
    pub rows_returned: usize,
    /// Peak memory usage in bytes (if available)
    pub peak_memory_bytes: Option<usize>,
}

impl BenchmarkResult {
    pub fn elapsed_ms(&self) -> f64 {
        self.elapsed.as_secs_f64() * 1000.0
    }
}

/// Collection of benchmark queries.
#[derive(Debug, Clone, Default)]
pub struct BenchmarkSuite {
    queries: Vec<BenchmarkQuery>,
}

impl BenchmarkSuite {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a benchmark query to the suite.
    pub fn add(&mut self, query: BenchmarkQuery) {
        self.queries.push(query);
    }

    /// Add multiple benchmark queries at once.
    pub fn add_all(&mut self, queries: Vec<BenchmarkQuery>) {
        self.queries.extend(queries);
    }

    /// Get all benchmark queries.
    pub fn queries(&self) -> &[BenchmarkQuery] {
        &self.queries
    }

    /// Get number of benchmarks in the suite.
    pub fn len(&self) -> usize {
        self.queries.len()
    }

    /// Check if the suite is empty.
    pub fn is_empty(&self) -> bool {
        self.queries.is_empty()
    }
}

/// Runs benchmarks against a connection.
pub struct BenchmarkRunner {
    /// Number of warmup runs before timing (default: 1)
    pub warmup_runs: usize,
    /// Number of timed runs to average (default: 3)
    pub timed_runs: usize,
}

impl Default for BenchmarkRunner {
    fn default() -> Self {
        Self {
            warmup_runs: 1,
            timed_runs: 3,
        }
    }
}

impl BenchmarkRunner {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the number of warmup runs.
    pub fn with_warmup_runs(mut self, n: usize) -> Self {
        self.warmup_runs = n;
        self
    }

    /// Set the number of timed runs.
    pub fn with_timed_runs(mut self, n: usize) -> Self {
        self.timed_runs = n;
        self
    }

    /// Run a single benchmark and return the result.
    pub fn run_one(&self, conn: &Connection, query: &BenchmarkQuery) -> Result<BenchmarkResult> {
        // Warmup runs
        for _ in 0..self.warmup_runs {
            let _ = conn.query(&query.sql)?;
        }

        // Timed runs
        let mut total_elapsed = Duration::ZERO;
        let mut rows_returned = 0;

        for _ in 0..self.timed_runs {
            let start = Instant::now();
            let batches = conn.query(&query.sql)?;
            let elapsed = start.elapsed();

            total_elapsed += elapsed;
            rows_returned = batches.iter().map(|b| b.num_rows()).sum();
        }

        let avg_elapsed = total_elapsed / self.timed_runs as u32;

        Ok(BenchmarkResult {
            name: query.name.clone(),
            elapsed: avg_elapsed,
            rows_returned,
            peak_memory_bytes: None, // Memory tracking not implemented yet
        })
    }

    /// Run all benchmarks in a suite.
    pub fn run_suite(
        &self,
        conn: &Connection,
        suite: &BenchmarkSuite,
    ) -> Result<Vec<BenchmarkResult>> {
        let mut results = Vec::with_capacity(suite.len());
        for query in suite.queries() {
            results.push(self.run_one(conn, query)?);
        }
        Ok(results)
    }
}

/// Detected performance regression.
#[derive(Debug, Clone)]
pub struct Regression {
    /// Name of the benchmark
    pub name: String,
    /// Baseline elapsed time in ms
    pub baseline_ms: f64,
    /// Current elapsed time in ms
    pub current_ms: f64,
    /// Percentage slowdown (positive = slower)
    pub slowdown_percent: f64,
}

/// Detects performance regressions by comparing benchmark results.
pub struct RegressionDetector {
    /// Threshold for regression detection (default: 10.0 = 10% slowdown)
    pub threshold_percent: f64,
}

impl Default for RegressionDetector {
    fn default() -> Self {
        Self {
            threshold_percent: 10.0,
        }
    }
}

impl RegressionDetector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the regression threshold percentage.
    pub fn with_threshold(mut self, percent: f64) -> Self {
        self.threshold_percent = percent;
        self
    }

    /// Detect regressions by comparing two sets of benchmark results.
    pub fn detect(
        &self,
        baseline: &[BenchmarkResult],
        current: &[BenchmarkResult],
    ) -> Vec<Regression> {
        let mut regressions = Vec::new();

        // Build a map of baseline results by name
        let baseline_map: std::collections::HashMap<_, _> =
            baseline.iter().map(|r| (r.name.as_str(), r)).collect();

        for curr in current {
            if let Some(base) = baseline_map.get(curr.name.as_str()) {
                let baseline_ms = base.elapsed_ms();
                let current_ms = curr.elapsed_ms();

                if baseline_ms > 0.0 {
                    let slowdown = ((current_ms - baseline_ms) / baseline_ms) * 100.0;

                    if slowdown > self.threshold_percent {
                        regressions.push(Regression {
                            name: curr.name.clone(),
                            baseline_ms,
                            current_ms,
                            slowdown_percent: slowdown,
                        });
                    }
                }
            }
        }

        regressions
    }
}

/// Benchmark report generation.
pub struct BenchmarkReport {
    results: Vec<BenchmarkResult>,
}

impl BenchmarkReport {
    pub fn new(results: Vec<BenchmarkResult>) -> Self {
        Self { results }
    }

    /// Generate a JSON report.
    pub fn to_json(&self) -> String {
        let mut output = String::from("{\n");
        output.push_str("  \"benchmarks\": [\n");

        for (i, result) in self.results.iter().enumerate() {
            output.push_str("    {\n");
            output.push_str(&format!("      \"name\": \"{}\",\n", result.name));
            output.push_str(&format!(
                "      \"elapsed_ms\": {:.3},\n",
                result.elapsed_ms()
            ));
            output.push_str(&format!(
                "      \"rows_returned\": {}",
                result.rows_returned
            ));
            if let Some(mem) = result.peak_memory_bytes {
                output.push_str(&format!(",\n      \"peak_memory_bytes\": {}", mem));
            }
            output.push('\n');
            output.push_str("    }");
            if i < self.results.len() - 1 {
                output.push(',');
            }
            output.push('\n');
        }

        output.push_str("  ]\n");
        output.push_str("}\n");
        output
    }

    /// Generate a simple text summary.
    pub fn to_text(&self) -> String {
        let mut output = String::from("Benchmark Results\n");
        output.push_str("=================\n\n");

        for result in &self.results {
            output.push_str(&format!("{}\n", result.name));
            output.push_str(&format!("  Elapsed: {:.3} ms\n", result.elapsed_ms()));
            output.push_str(&format!("  Rows: {}\n", result.rows_returned));
            if let Some(mem) = result.peak_memory_bytes {
                output.push_str(&format!("  Memory: {} bytes\n", mem));
            }
            output.push('\n');
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_benchmark_suite_creation() {
        let mut suite = BenchmarkSuite::new();
        assert!(suite.is_empty());
        assert_eq!(suite.len(), 0);

        suite.add(BenchmarkQuery::new("test1", "SELECT 1"));
        suite.add(BenchmarkQuery::new("test2", "SELECT 2"));

        assert!(!suite.is_empty());
        assert_eq!(suite.len(), 2);
        assert_eq!(suite.queries()[0].name, "test1");
        assert_eq!(suite.queries()[1].name, "test2");
    }

    #[test]
    fn test_benchmark_suite_add_all() {
        let mut suite = BenchmarkSuite::new();
        let queries = vec![
            BenchmarkQuery::new("q1", "SELECT 1"),
            BenchmarkQuery::new("q2", "SELECT 2"),
            BenchmarkQuery::new("q3", "SELECT 3"),
        ];

        suite.add_all(queries);
        assert_eq!(suite.len(), 3);
    }

    #[test]
    fn test_run_benchmark() -> Result<()> {
        let conn = Connection::in_memory()?;

        let query = BenchmarkQuery::new("simple_query", "SELECT 1 as num");
        let runner = BenchmarkRunner::new()
            .with_warmup_runs(0)
            .with_timed_runs(1);

        let result = runner.run_one(&conn, &query)?;

        assert_eq!(result.name, "simple_query");
        assert!(result.elapsed.as_millis() >= 0);
        assert_eq!(result.rows_returned, 1);

        Ok(())
    }

    #[test]
    fn test_regression_detection() {
        let baseline = vec![
            BenchmarkResult {
                name: "query1".to_string(),
                elapsed: Duration::from_millis(100),
                rows_returned: 1000,
                peak_memory_bytes: None,
            },
            BenchmarkResult {
                name: "query2".to_string(),
                elapsed: Duration::from_millis(200),
                rows_returned: 2000,
                peak_memory_bytes: None,
            },
        ];

        let current = vec![
            BenchmarkResult {
                name: "query1".to_string(),
                elapsed: Duration::from_millis(125), // 25% slower
                rows_returned: 1000,
                peak_memory_bytes: None,
            },
            BenchmarkResult {
                name: "query2".to_string(),
                elapsed: Duration::from_millis(205), // 2.5% slower - under threshold
                rows_returned: 2000,
                peak_memory_bytes: None,
            },
        ];

        let detector = RegressionDetector::new().with_threshold(10.0);
        let regressions = detector.detect(&baseline, &current);

        assert_eq!(regressions.len(), 1);
        assert_eq!(regressions[0].name, "query1");
        assert!((regressions[0].slowdown_percent - 25.0).abs() < 0.1);
    }

    #[test]
    fn test_benchmark_report_json() {
        let results = vec![
            BenchmarkResult {
                name: "test1".to_string(),
                elapsed: Duration::from_millis(50),
                rows_returned: 100,
                peak_memory_bytes: Some(1024),
            },
            BenchmarkResult {
                name: "test2".to_string(),
                elapsed: Duration::from_millis(75),
                rows_returned: 200,
                peak_memory_bytes: None,
            },
        ];

        let report = BenchmarkReport::new(results);
        let json = report.to_json();

        assert!(json.contains("\"name\": \"test1\""));
        assert!(json.contains("\"name\": \"test2\""));
        assert!(json.contains("\"elapsed_ms\":"));
        assert!(json.contains("\"rows_returned\":"));
        assert!(json.contains("\"peak_memory_bytes\": 1024"));
    }

    #[test]
    fn test_benchmark_report_text() {
        let results = vec![BenchmarkResult {
            name: "simple_test".to_string(),
            elapsed: Duration::from_millis(100),
            rows_returned: 500,
            peak_memory_bytes: Some(2048),
        }];

        let report = BenchmarkReport::new(results);
        let text = report.to_text();

        assert!(text.contains("Benchmark Results"));
        assert!(text.contains("simple_test"));
        assert!(text.contains("Elapsed: 100"));
        assert!(text.contains("Rows: 500"));
        assert!(text.contains("Memory: 2048"));
    }
}
