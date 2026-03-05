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

        // Reset peak memory counter before timed runs
        let memory_manager = conn.memory_manager();
        memory_manager.reset_peak();

        // Timed runs
        let mut total_elapsed = Duration::ZERO;
        let mut rows_returned = 0;
        let mut max_peak_memory = 0usize;

        for _ in 0..self.timed_runs {
            memory_manager.reset_peak();
            let start = Instant::now();
            let batches = conn.query(&query.sql)?;
            let elapsed = start.elapsed();

            total_elapsed += elapsed;
            rows_returned = batches.iter().map(|b| b.num_rows()).sum();
            max_peak_memory = max_peak_memory.max(memory_manager.peak());
        }

        let avg_elapsed = total_elapsed / self.timed_runs as u32;

        Ok(BenchmarkResult {
            name: query.name.clone(),
            elapsed: avg_elapsed,
            rows_returned,
            peak_memory_bytes: Some(max_peak_memory),
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
    /// Type of regression detected
    pub kind: RegressionKind,
}

/// The kind of performance regression.
#[derive(Debug, Clone)]
pub enum RegressionKind {
    /// Query execution time regression
    Latency {
        /// Baseline elapsed time in ms
        baseline_ms: f64,
        /// Current elapsed time in ms
        current_ms: f64,
        /// Percentage slowdown (positive = slower)
        slowdown_percent: f64,
    },
    /// Peak memory usage regression
    Memory {
        /// Baseline peak memory in bytes
        baseline_bytes: usize,
        /// Current peak memory in bytes
        current_bytes: usize,
        /// Percentage increase (positive = more memory)
        increase_percent: f64,
    },
}

/// Detects performance regressions by comparing benchmark results.
pub struct RegressionDetector {
    /// Threshold for latency regression detection (default: 10.0 = 10% slowdown)
    pub threshold_percent: f64,
    /// Threshold for memory regression detection (default: 20.0 = 20% increase)
    pub memory_threshold_percent: f64,
}

impl Default for RegressionDetector {
    fn default() -> Self {
        Self {
            threshold_percent: 10.0,
            memory_threshold_percent: 20.0,
        }
    }
}

impl RegressionDetector {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the latency regression threshold percentage.
    pub fn with_threshold(mut self, percent: f64) -> Self {
        self.threshold_percent = percent;
        self
    }

    /// Set the memory regression threshold percentage.
    pub fn with_memory_threshold(mut self, percent: f64) -> Self {
        self.memory_threshold_percent = percent;
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
                // Check latency regression
                let baseline_ms = base.elapsed_ms();
                let current_ms = curr.elapsed_ms();

                if baseline_ms > 0.0 {
                    let slowdown = ((current_ms - baseline_ms) / baseline_ms) * 100.0;

                    if slowdown > self.threshold_percent {
                        regressions.push(Regression {
                            name: curr.name.clone(),
                            kind: RegressionKind::Latency {
                                baseline_ms,
                                current_ms,
                                slowdown_percent: slowdown,
                            },
                        });
                    }
                }

                // Check memory regression
                if let (Some(base_mem), Some(curr_mem)) =
                    (base.peak_memory_bytes, curr.peak_memory_bytes)
                {
                    if base_mem > 0 {
                        let increase =
                            ((curr_mem as f64 - base_mem as f64) / base_mem as f64) * 100.0;

                        if increase > self.memory_threshold_percent {
                            regressions.push(Regression {
                                name: curr.name.clone(),
                                kind: RegressionKind::Memory {
                                    baseline_bytes: base_mem,
                                    current_bytes: curr_mem,
                                    increase_percent: increase,
                                },
                            });
                        }
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
    metadata: Option<BenchmarkMetadata>,
}

/// Metadata for a benchmark run.
#[derive(Debug, Clone)]
pub struct BenchmarkMetadata {
    /// Engine name (e.g., "blaze")
    pub engine: String,
    /// Timestamp of the benchmark run
    pub timestamp: String,
    /// Scale factor used
    pub scale_factor: usize,
    /// Git commit hash (if available)
    pub git_commit: Option<String>,
}

impl BenchmarkReport {
    pub fn new(results: Vec<BenchmarkResult>) -> Self {
        Self {
            results,
            metadata: None,
        }
    }

    /// Attach metadata to the report.
    pub fn with_metadata(mut self, metadata: BenchmarkMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Generate a JSON report.
    pub fn to_json(&self) -> String {
        let mut output = String::from("{\n");

        if let Some(meta) = &self.metadata {
            output.push_str(&format!("  \"engine\": \"{}\",\n", meta.engine));
            output.push_str(&format!("  \"timestamp\": \"{}\",\n", meta.timestamp));
            output.push_str(&format!("  \"scale_factor\": {},\n", meta.scale_factor));
            if let Some(commit) = &meta.git_commit {
                output.push_str(&format!("  \"git_commit\": \"{}\",\n", commit));
            }
        }

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

    /// Generate a markdown comparison table.
    pub fn to_markdown(&self) -> String {
        let mut output = String::from("## Benchmark Results\n\n");
        output.push_str("| Query | Time (ms) | Rows |\n");
        output.push_str("|-------|-----------|------|\n");

        for result in &self.results {
            output.push_str(&format!(
                "| {} | {:.3} | {} |\n",
                result.name,
                result.elapsed_ms(),
                result.rows_returned,
            ));
        }

        output
    }

    /// Generate an HTML dashboard with Chart.js for historical benchmark trends.
    pub fn to_html_dashboard(&self) -> String {
        let mut html = String::new();
        html.push_str("<!DOCTYPE html>\n<html><head>\n");
        html.push_str("<title>Blaze TPC-H Benchmark Dashboard</title>\n");
        html.push_str("<script src=\"https://cdn.jsdelivr.net/npm/chart.js\"></script>\n");
        html.push_str("<style>\n");
        html.push_str("body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; ");
        html.push_str("margin: 0; padding: 20px; background: #f5f5f5; }\n");
        html.push_str("h1 { color: #333; text-align: center; }\n");
        html.push_str(".dashboard { max-width: 1200px; margin: 0 auto; }\n");
        html.push_str(
            ".card { background: white; border-radius: 8px; padding: 20px; margin: 16px 0; ",
        );
        html.push_str("box-shadow: 0 2px 4px rgba(0,0,0,0.1); }\n");
        html.push_str(".grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 16px; }\n");
        html.push_str(".stat { text-align: center; padding: 12px; }\n");
        html.push_str(".stat-value { font-size: 2em; font-weight: bold; color: #2563eb; }\n");
        html.push_str(".stat-label { color: #666; font-size: 0.9em; }\n");
        html.push_str("table { width: 100%; border-collapse: collapse; }\n");
        html.push_str(
            "th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #eee; }\n",
        );
        html.push_str("th { background: #f8f9fa; font-weight: 600; }\n");
        html.push_str(".pass { color: #16a34a; } .fail { color: #dc2626; }\n");
        html.push_str("</style>\n</head><body>\n");
        html.push_str("<div class=\"dashboard\">\n");
        html.push_str("<h1>Blaze TPC-H Benchmark Dashboard</h1>\n");

        // Summary statistics
        let total_queries = self.results.len();
        let avg_ms: f64 = if total_queries > 0 {
            self.results.iter().map(|r| r.elapsed_ms()).sum::<f64>() / total_queries as f64
        } else {
            0.0
        };
        let min_ms = self
            .results
            .iter()
            .map(|r| r.elapsed_ms())
            .fold(f64::INFINITY, f64::min);
        let max_ms = self
            .results
            .iter()
            .map(|r| r.elapsed_ms())
            .fold(0.0f64, f64::max);
        let total_rows: usize = self.results.iter().map(|r| r.rows_returned).sum();

        html.push_str("<div class=\"card\"><div class=\"grid\">\n");
        html.push_str(&format!(
            "<div class=\"stat\"><div class=\"stat-value\">{}</div><div class=\"stat-label\">Queries</div></div>\n",
            total_queries
        ));
        html.push_str(&format!(
            "<div class=\"stat\"><div class=\"stat-value\">{:.1}ms</div><div class=\"stat-label\">Avg Latency</div></div>\n",
            avg_ms
        ));
        html.push_str(&format!(
            "<div class=\"stat\"><div class=\"stat-value\">{:.1}ms</div><div class=\"stat-label\">Min Latency</div></div>\n",
            min_ms
        ));
        html.push_str(&format!(
            "<div class=\"stat\"><div class=\"stat-value\">{:.1}ms</div><div class=\"stat-label\">Max Latency</div></div>\n",
            max_ms
        ));
        html.push_str(&format!(
            "<div class=\"stat\"><div class=\"stat-value\">{}</div><div class=\"stat-label\">Total Rows</div></div>\n",
            total_rows
        ));
        html.push_str("</div></div>\n");

        // Chart
        html.push_str("<div class=\"card\">\n<h2>Query Latency (ms)</h2>\n");
        html.push_str("<canvas id=\"latencyChart\" height=\"300\"></canvas>\n");

        let labels: Vec<String> = self
            .results
            .iter()
            .map(|r| format!("\"{}\"", r.name))
            .collect();
        let data: Vec<String> = self
            .results
            .iter()
            .map(|r| format!("{:.2}", r.elapsed_ms()))
            .collect();

        html.push_str("<script>\n");
        html.push_str("new Chart(document.getElementById('latencyChart'), {\n");
        html.push_str("  type: 'bar',\n");
        html.push_str(&format!("  data: {{ labels: [{}],\n", labels.join(",")));
        html.push_str(&format!(
            "    datasets: [{{ label: 'Latency (ms)', data: [{}],\n",
            data.join(",")
        ));
        html.push_str("      backgroundColor: 'rgba(37, 99, 235, 0.6)', borderColor: 'rgba(37, 99, 235, 1)', borderWidth: 1 }]\n");
        html.push_str(
            "  },\n  options: { responsive: true, scales: { y: { beginAtZero: true } } }\n",
        );
        html.push_str("});\n</script>\n</div>\n");

        // Results table
        html.push_str("<div class=\"card\">\n<h2>Detailed Results</h2>\n<table>\n");
        html.push_str("<tr><th>Query</th><th>Latency (ms)</th><th>Rows</th></tr>\n");
        for result in &self.results {
            html.push_str(&format!(
                "<tr><td>{}</td><td>{:.2}</td><td>{}</td></tr>\n",
                result.name,
                result.elapsed_ms(),
                result.rows_returned,
            ));
        }
        html.push_str("</table>\n</div>\n");

        // Metadata
        if let Some(ref meta) = self.metadata {
            html.push_str("<div class=\"card\">\n<h2>Environment</h2>\n");
            html.push_str(&format!("<p>Engine: {}</p>\n", meta.engine));
            html.push_str(&format!("<p>Scale Factor: {}</p>\n", meta.scale_factor));
            html.push_str(&format!("<p>Timestamp: {}</p>\n", meta.timestamp));
            if let Some(ref commit) = meta.git_commit {
                html.push_str(&format!("<p>Git Commit: {}</p>\n", commit));
            }
            html.push_str("</div>\n");
        }

        html.push_str("</div>\n</body></html>");
        html
    }

    /// Generate a regression detection summary comparing against a baseline.
    pub fn regression_summary(
        &self,
        baseline: &[BenchmarkResult],
        threshold_pct: f64,
    ) -> RegressionReport {
        let detector = RegressionDetector::new().with_threshold(threshold_pct);
        let regressions = detector.detect(baseline, &self.results);

        RegressionReport {
            total_queries: self.results.len(),
            regressions,
            threshold_pct,
        }
    }
}

/// Regression detection summary report.
#[derive(Debug, Clone)]
pub struct RegressionReport {
    pub total_queries: usize,
    pub regressions: Vec<Regression>,
    pub threshold_pct: f64,
}

impl RegressionReport {
    /// Whether any regressions were detected.
    pub fn has_regressions(&self) -> bool {
        !self.regressions.is_empty()
    }

    pub fn to_markdown(&self) -> String {
        let mut md = String::new();
        md.push_str("## Benchmark Regression Report\n\n");
        md.push_str(&format!("Threshold: {:.0}%\n\n", self.threshold_pct));

        if self.has_regressions() {
            md.push_str(&format!(
                "**{} regression(s) detected** out of {} queries\n\n",
                self.regressions.len(),
                self.total_queries
            ));
            md.push_str("| Query | Type | Baseline | Current | Change |\n");
            md.push_str("|-------|------|----------|---------|--------|\n");
            for r in &self.regressions {
                match &r.kind {
                    RegressionKind::Latency {
                        baseline_ms,
                        current_ms,
                        slowdown_percent,
                    } => {
                        md.push_str(&format!(
                            "| {} | Latency | {:.2} ms | {:.2} ms | +{:.1}% |\n",
                            r.name, baseline_ms, current_ms, slowdown_percent
                        ));
                    }
                    RegressionKind::Memory {
                        baseline_bytes,
                        current_bytes,
                        increase_percent,
                    } => {
                        md.push_str(&format!(
                            "| {} | Memory | {} B | {} B | +{:.1}% |\n",
                            r.name, baseline_bytes, current_bytes, increase_percent
                        ));
                    }
                }
            }
        } else {
            md.push_str(&format!(
                "All {} queries within {:.0}% threshold.\n",
                self.total_queries, self.threshold_pct
            ));
        }

        md
    }
}

// ---------------------------------------------------------------------------
// TPC-H Benchmark Suite Builder
// ---------------------------------------------------------------------------

/// Pre-built TPC-H benchmark suite with all 22 queries (simplified).
pub fn tpch_benchmark_suite() -> BenchmarkSuite {
    let mut suite = BenchmarkSuite::new();
    suite.add_all(vec![
        BenchmarkQuery::new("Q1", "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, COUNT(*) AS count_order FROM lineitem GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus"),
        BenchmarkQuery::new("Q2", "SELECT s_suppkey, s_name, n_name FROM supplier JOIN nation ON s_nationkey = n_nationkey ORDER BY s_name LIMIT 10"),
        BenchmarkQuery::new("Q3", "SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY l_orderkey ORDER BY revenue DESC LIMIT 10"),
        BenchmarkQuery::new("Q4", "SELECT o_orderstatus, COUNT(*) AS order_count FROM orders GROUP BY o_orderstatus ORDER BY o_orderstatus"),
        BenchmarkQuery::new("Q5", "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey GROUP BY n_name ORDER BY revenue DESC"),
        BenchmarkQuery::new("Q6", "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"),
        BenchmarkQuery::new("Q7", "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS volume FROM lineitem JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey GROUP BY n_name ORDER BY n_name"),
        BenchmarkQuery::new("Q8", "SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS mkt_share FROM lineitem JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey GROUP BY n_name ORDER BY mkt_share DESC LIMIT 5"),
        BenchmarkQuery::new("Q9", "SELECT n_name AS nation, SUM(l_extendedprice * (1 - l_discount) - l_quantity * l_tax) AS profit FROM lineitem JOIN supplier ON l_suppkey = s_suppkey JOIN nation ON s_nationkey = n_nationkey GROUP BY n_name ORDER BY n_name"),
        BenchmarkQuery::new("Q10", "SELECT c_custkey, c_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON o_orderkey = l_orderkey WHERE l_returnflag = 'R' GROUP BY c_custkey, c_name ORDER BY revenue DESC LIMIT 20"),
        BenchmarkQuery::new("Q11", "SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value FROM partsupp GROUP BY ps_partkey ORDER BY value DESC LIMIT 20"),
        BenchmarkQuery::new("Q12", "SELECT l_linestatus, COUNT(*) AS order_count FROM lineitem JOIN orders ON l_orderkey = o_orderkey GROUP BY l_linestatus ORDER BY l_linestatus"),
        BenchmarkQuery::new("Q13", "SELECT c_custkey, COUNT(o_orderkey) AS c_count FROM customer LEFT JOIN orders ON c_custkey = o_custkey GROUP BY c_custkey ORDER BY c_count DESC LIMIT 20"),
        BenchmarkQuery::new("Q14", "SELECT SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem JOIN part ON l_partkey = p_partkey"),
        BenchmarkQuery::new("Q15", "SELECT s_suppkey, s_name, SUM(l_extendedprice * (1 - l_discount)) AS total_revenue FROM supplier JOIN lineitem ON s_suppkey = l_suppkey GROUP BY s_suppkey, s_name ORDER BY total_revenue DESC LIMIT 1"),
        BenchmarkQuery::new("Q16", "SELECT p_type, COUNT(*) AS supplier_cnt FROM partsupp JOIN part ON ps_partkey = p_partkey GROUP BY p_type ORDER BY supplier_cnt DESC LIMIT 10"),
        BenchmarkQuery::new("Q17", "SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly FROM lineitem WHERE l_quantity < 25"),
        BenchmarkQuery::new("Q18", "SELECT c_name, o_orderkey, SUM(l_quantity) AS total_qty FROM customer JOIN orders ON c_custkey = o_custkey JOIN lineitem ON o_orderkey = l_orderkey GROUP BY c_name, o_orderkey ORDER BY total_qty DESC LIMIT 10"),
        BenchmarkQuery::new("Q19", "SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue FROM lineitem WHERE l_quantity >= 1 AND l_quantity <= 30 AND l_discount BETWEEN 0.02 AND 0.09"),
        BenchmarkQuery::new("Q20", "SELECT s_name, s_suppkey FROM supplier JOIN nation ON s_nationkey = n_nationkey ORDER BY s_name LIMIT 20"),
        BenchmarkQuery::new("Q21", "SELECT s_name, COUNT(*) AS numwait FROM supplier JOIN lineitem ON s_suppkey = l_suppkey JOIN orders ON l_orderkey = o_orderkey GROUP BY s_name ORDER BY numwait DESC LIMIT 10"),
        BenchmarkQuery::new("Q22", "SELECT COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal FROM customer WHERE c_acctbal > 0"),
    ]);
    suite
}

/// Comparison entry for cross-engine benchmarks.
#[derive(Debug, Clone)]
pub struct EngineComparison {
    pub engine_name: String,
    pub query_name: String,
    pub elapsed_ms: f64,
    pub rows_returned: usize,
}

/// Generate a comparison report between engines.
pub fn engine_comparison_report(comparisons: &[EngineComparison]) -> String {
    let mut md = String::new();
    md.push_str("## Cross-Engine Comparison\n\n");
    md.push_str("| Query | Blaze (ms) | DuckDB (ms) | DataFusion (ms) | Blaze vs DuckDB |\n");
    md.push_str("|-------|-----------|------------|----------------|----------------|\n");

    // Group by query name
    let mut queries: std::collections::BTreeMap<String, Vec<&EngineComparison>> =
        std::collections::BTreeMap::new();
    for c in comparisons {
        queries.entry(c.query_name.clone()).or_default().push(c);
    }

    for (query, engines) in &queries {
        let blaze = engines
            .iter()
            .find(|e| e.engine_name == "blaze")
            .map(|e| e.elapsed_ms);
        let duckdb = engines
            .iter()
            .find(|e| e.engine_name == "duckdb")
            .map(|e| e.elapsed_ms);
        let datafusion = engines
            .iter()
            .find(|e| e.engine_name == "datafusion")
            .map(|e| e.elapsed_ms);

        let blaze_str = blaze
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| "-".to_string());
        let duckdb_str = duckdb
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| "-".to_string());
        let df_str = datafusion
            .map(|v| format!("{:.2}", v))
            .unwrap_or_else(|| "-".to_string());
        let ratio = match (blaze, duckdb) {
            (Some(b), Some(d)) if d > 0.0 => format!("{:.2}x", b / d),
            _ => "-".to_string(),
        };

        md.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            query, blaze_str, duckdb_str, df_str, ratio
        ));
    }

    md
}

/// Compare two benchmark reports and return a formatted comparison.
pub fn compare_reports(baseline: &[BenchmarkResult], current: &[BenchmarkResult]) -> String {
    let baseline_map: std::collections::HashMap<_, _> =
        baseline.iter().map(|r| (r.name.as_str(), r)).collect();

    let mut output = String::from("## Benchmark Comparison\n\n");
    output.push_str("| Query | Baseline (ms) | Current (ms) | Change |\n");
    output.push_str("|-------|--------------|-------------|--------|\n");

    for curr in current {
        if let Some(base) = baseline_map.get(curr.name.as_str()) {
            let baseline_ms = base.elapsed_ms();
            let current_ms = curr.elapsed_ms();
            let change = if baseline_ms > 0.0 {
                ((current_ms - baseline_ms) / baseline_ms) * 100.0
            } else {
                0.0
            };
            let indicator = if change > 5.0 {
                "🔴"
            } else if change < -5.0 {
                "🟢"
            } else {
                "⚪"
            };
            output.push_str(&format!(
                "| {} | {:.3} | {:.3} | {} {:.1}% |\n",
                curr.name, baseline_ms, current_ms, indicator, change,
            ));
        } else {
            output.push_str(&format!(
                "| {} | - | {:.3} | new |\n",
                curr.name,
                curr.elapsed_ms(),
            ));
        }
    }

    output
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
        match &regressions[0].kind {
            RegressionKind::Latency {
                slowdown_percent, ..
            } => {
                assert!((slowdown_percent - 25.0).abs() < 0.1);
            }
            _ => panic!("Expected latency regression"),
        }
    }

    #[test]
    fn test_memory_regression_detection() {
        let baseline = vec![BenchmarkResult {
            name: "query1".to_string(),
            elapsed: Duration::from_millis(100),
            rows_returned: 1000,
            peak_memory_bytes: Some(1_000_000),
        }];

        let current = vec![BenchmarkResult {
            name: "query1".to_string(),
            elapsed: Duration::from_millis(100), // same latency
            rows_returned: 1000,
            peak_memory_bytes: Some(1_500_000), // 50% more memory
        }];

        let detector = RegressionDetector::new()
            .with_threshold(10.0)
            .with_memory_threshold(20.0);
        let regressions = detector.detect(&baseline, &current);

        assert_eq!(regressions.len(), 1);
        assert_eq!(regressions[0].name, "query1");
        match &regressions[0].kind {
            RegressionKind::Memory {
                increase_percent, ..
            } => {
                assert!((increase_percent - 50.0).abs() < 0.1);
            }
            _ => panic!("Expected memory regression"),
        }
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

    #[test]
    fn test_html_dashboard_generation() {
        let results = vec![
            BenchmarkResult {
                name: "Q1".to_string(),
                elapsed: Duration::from_millis(50),
                rows_returned: 100,
                peak_memory_bytes: None,
            },
            BenchmarkResult {
                name: "Q2".to_string(),
                elapsed: Duration::from_millis(75),
                rows_returned: 200,
                peak_memory_bytes: None,
            },
        ];

        let report = BenchmarkReport::new(results);
        let html = report.to_html_dashboard();

        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Blaze TPC-H Benchmark Dashboard"));
        assert!(html.contains("chart.js"));
        assert!(html.contains("latencyChart"));
        assert!(html.contains("Q1"));
        assert!(html.contains("Q2"));
        assert!(html.contains("</html>"));
    }

    #[test]
    fn test_html_dashboard_with_metadata() {
        let results = vec![BenchmarkResult {
            name: "Q1".to_string(),
            elapsed: Duration::from_millis(42),
            rows_returned: 10,
            peak_memory_bytes: None,
        }];

        let meta = BenchmarkMetadata {
            engine: "blaze".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            scale_factor: 1,
            git_commit: Some("abc123".to_string()),
        };

        let report = BenchmarkReport::new(results).with_metadata(meta);
        let html = report.to_html_dashboard();

        assert!(html.contains("Engine: blaze"));
        assert!(html.contains("Scale Factor: 1"));
        assert!(html.contains("2024-01-01T00:00:00Z"));
        assert!(html.contains("abc123"));
    }

    #[test]
    fn test_html_dashboard_empty_results() {
        let report = BenchmarkReport::new(vec![]);
        let html = report.to_html_dashboard();

        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("</html>"));
        assert!(html.contains("0")); // 0 queries
    }

    #[test]
    fn test_regression_report_with_regressions() {
        let baseline = vec![
            BenchmarkResult {
                name: "Q1".to_string(),
                elapsed: Duration::from_millis(100),
                rows_returned: 100,
                peak_memory_bytes: None,
            },
            BenchmarkResult {
                name: "Q2".to_string(),
                elapsed: Duration::from_millis(200),
                rows_returned: 200,
                peak_memory_bytes: None,
            },
        ];

        let current = vec![
            BenchmarkResult {
                name: "Q1".to_string(),
                elapsed: Duration::from_millis(150), // 50% regression
                rows_returned: 100,
                peak_memory_bytes: None,
            },
            BenchmarkResult {
                name: "Q2".to_string(),
                elapsed: Duration::from_millis(202), // 1% - under threshold
                rows_returned: 200,
                peak_memory_bytes: None,
            },
        ];

        let report = BenchmarkReport::new(current);
        let regression = report.regression_summary(&baseline, 5.0);

        assert!(regression.has_regressions());
        assert_eq!(regression.regressions.len(), 1);
        assert_eq!(regression.regressions[0].name, "Q1");
        assert_eq!(regression.total_queries, 2);

        let md = regression.to_markdown();
        assert!(md.contains("Regression Report"));
        assert!(md.contains("1 regression(s) detected"));
        assert!(md.contains("Q1"));
        assert!(md.contains("5%"));
    }

    #[test]
    fn test_regression_report_no_regressions() {
        let baseline = vec![BenchmarkResult {
            name: "Q1".to_string(),
            elapsed: Duration::from_millis(100),
            rows_returned: 100,
            peak_memory_bytes: None,
        }];

        let current = vec![BenchmarkResult {
            name: "Q1".to_string(),
            elapsed: Duration::from_millis(102), // 2% - under threshold
            rows_returned: 100,
            peak_memory_bytes: None,
        }];

        let report = BenchmarkReport::new(current);
        let regression = report.regression_summary(&baseline, 5.0);

        assert!(!regression.has_regressions());

        let md = regression.to_markdown();
        assert!(md.contains("All 1 queries within 5% threshold"));
    }

    #[test]
    fn test_engine_comparison_report() {
        let comparisons = vec![
            EngineComparison {
                engine_name: "blaze".to_string(),
                query_name: "Q1".to_string(),
                elapsed_ms: 50.0,
                rows_returned: 100,
            },
            EngineComparison {
                engine_name: "duckdb".to_string(),
                query_name: "Q1".to_string(),
                elapsed_ms: 40.0,
                rows_returned: 100,
            },
            EngineComparison {
                engine_name: "datafusion".to_string(),
                query_name: "Q1".to_string(),
                elapsed_ms: 60.0,
                rows_returned: 100,
            },
            EngineComparison {
                engine_name: "blaze".to_string(),
                query_name: "Q2".to_string(),
                elapsed_ms: 30.0,
                rows_returned: 50,
            },
        ];

        let md = engine_comparison_report(&comparisons);

        assert!(md.contains("Cross-Engine Comparison"));
        assert!(md.contains("Q1"));
        assert!(md.contains("Q2"));
        assert!(md.contains("50.00"));
        assert!(md.contains("40.00"));
        assert!(md.contains("60.00"));
        assert!(md.contains("1.25x")); // 50/40
        assert!(md.contains("30.00"));
    }

    #[test]
    fn test_engine_comparison_missing_engines() {
        let comparisons = vec![EngineComparison {
            engine_name: "blaze".to_string(),
            query_name: "Q1".to_string(),
            elapsed_ms: 50.0,
            rows_returned: 100,
        }];

        let md = engine_comparison_report(&comparisons);

        assert!(md.contains("Q1"));
        assert!(md.contains("50.00"));
        assert!(md.contains("| - |")); // missing duckdb/datafusion
    }
}
