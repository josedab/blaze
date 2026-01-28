//! Query execution profiler with multiple output formats.
//!
//! Provides JSON, DOT (Graphviz), Mermaid, and text tree output formats
//! for visualizing query execution profiles from `ExecutionStats`.

use crate::planner::ExecutionStats;
use parking_lot::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Output format for profiler reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileFormat {
    /// Indented text tree (default)
    Text,
    /// JSON format for programmatic consumption
    Json,
    /// DOT format for Graphviz rendering
    Dot,
    /// Mermaid diagram format for Markdown embedding
    Mermaid,
}

/// A wrapper around ExecutionStats that provides multiple output formats.
#[derive(Debug)]
pub struct ProfileReport {
    stats: ExecutionStats,
}

impl ProfileReport {
    pub fn new(stats: ExecutionStats) -> Self {
        Self { stats }
    }

    pub fn stats(&self) -> &ExecutionStats {
        &self.stats
    }

    /// Render the profile in the specified format.
    pub fn render(&self, format: ProfileFormat) -> String {
        match format {
            ProfileFormat::Text => self.stats.format_tree(0),
            ProfileFormat::Json => self.to_json(),
            ProfileFormat::Dot => self.to_dot(),
            ProfileFormat::Mermaid => self.to_mermaid(),
        }
    }

    /// Render as HTML with visual execution plan tree.
    pub fn to_html(&self) -> String {
        let mut html = String::new();

        // HTML header
        html.push_str("<!DOCTYPE html>\n<html>\n<head>\n");
        html.push_str("<meta charset=\"utf-8\">\n");
        html.push_str("<title>Query Profile Report</title>\n");
        html.push_str("<style>\n");
        html.push_str(
            "body { font-family: Arial, sans-serif; margin: 20px; background: #f5f5f5; }\n",
        );
        html.push_str("h1 { color: #333; }\n");
        html.push_str(".operator { margin: 5px 0; padding: 10px; background: white; border-left: 4px solid #4CAF50; border-radius: 3px; }\n");
        html.push_str(".operator-name { font-weight: bold; color: #2196F3; }\n");
        html.push_str(".timing-bar { height: 20px; background: #4CAF50; margin: 5px 0; border-radius: 3px; display: inline-block; }\n");
        html.push_str(".stats { color: #666; font-size: 0.9em; }\n");
        html.push_str(".indent-1 { margin-left: 20px; }\n");
        html.push_str(".indent-2 { margin-left: 40px; }\n");
        html.push_str(".indent-3 { margin-left: 60px; }\n");
        html.push_str(".indent-4 { margin-left: 80px; }\n");
        html.push_str(".summary { background: white; padding: 15px; border-radius: 5px; margin-bottom: 20px; }\n");
        html.push_str(".summary table { width: 100%; border-collapse: collapse; }\n");
        html.push_str(".summary th { text-align: left; padding: 8px; background: #eee; }\n");
        html.push_str(".summary td { padding: 8px; border-bottom: 1px solid #eee; }\n");
        html.push_str("</style>\n");
        html.push_str("</head>\n<body>\n");

        // Title
        html.push_str("<h1>Query Execution Profile</h1>\n");

        // Summary statistics
        html.push_str("<div class=\"summary\">\n");
        html.push_str("<h2>Summary Statistics</h2>\n");
        html.push_str("<table>\n");
        html.push_str("<tr><th>Metric</th><th>Value</th></tr>\n");

        let total_ms = self.stats.elapsed_nanos as f64 / 1_000_000.0;
        html.push_str(&format!(
            "<tr><td>Total Time</td><td>{:.3} ms</td></tr>\n",
            total_ms
        ));
        html.push_str(&format!(
            "<tr><td>Rows Processed</td><td>{}</td></tr>\n",
            self.stats.rows_processed
        ));
        html.push_str(&format!(
            "<tr><td>Rows Output</td><td>{}</td></tr>\n",
            self.stats.rows_output
        ));
        html.push_str(&format!(
            "<tr><td>Batches</td><td>{}</td></tr>\n",
            self.stats.batches_processed
        ));
        html.push_str(&format!(
            "<tr><td>Peak Memory</td><td>{} bytes ({:.2} MB)</td></tr>\n",
            self.stats.peak_memory_bytes,
            self.stats.peak_memory_bytes as f64 / 1024.0 / 1024.0
        ));
        html.push_str("</table>\n");
        html.push_str("</div>\n");

        // Execution plan tree
        html.push_str("<h2>Execution Plan</h2>\n");
        let max_time = self.stats.total_elapsed_nanos() as f64;
        Self::render_operator_html(&self.stats, &mut html, 0, max_time);

        html.push_str("</body>\n</html>\n");
        html
    }

    fn render_operator_html(stats: &ExecutionStats, out: &mut String, depth: usize, max_time: f64) {
        let elapsed_ms = stats.elapsed_nanos as f64 / 1_000_000.0;
        let indent_class = format!("indent-{}", depth.min(4));

        out.push_str(&format!("<div class=\"operator {}\">\n", indent_class));
        out.push_str(&format!(
            "<div class=\"operator-name\">{}</div>\n",
            stats.operator_name
        ));

        // Timing bar (proportional to execution time)
        let bar_width = if max_time > 0.0 {
            ((stats.elapsed_nanos as f64 / max_time) * 300.0).max(10.0) as usize
        } else {
            10
        };
        out.push_str(&format!(
            "<div class=\"timing-bar\" style=\"width: {}px;\"></div> {:.3} ms\n",
            bar_width, elapsed_ms
        ));

        // Stats
        out.push_str("<div class=\"stats\">");
        out.push_str(&format!(
            "Rows: {} → {} | ",
            stats.rows_processed, stats.rows_output
        ));
        out.push_str(&format!("Batches: {} | ", stats.batches_processed));
        out.push_str(&format!("Memory: {} bytes", stats.peak_memory_bytes));

        if !stats.extra_metrics.is_empty() {
            out.push_str(" | ");
            for (name, value) in &stats.extra_metrics {
                out.push_str(&format!("{}: {} ", name, value));
            }
        }
        out.push_str("</div>\n");

        out.push_str("</div>\n");

        // Render children
        for child in &stats.children {
            Self::render_operator_html(child, out, depth + 1, max_time);
        }
    }

    /// Render as JSON.
    pub fn to_json(&self) -> String {
        let mut output = String::new();
        output.push_str("{\n");
        Self::stats_to_json(&self.stats, &mut output, 1);
        output.push_str("}\n");
        output
    }

    fn stats_to_json(stats: &ExecutionStats, out: &mut String, indent: usize) {
        let pad = "  ".repeat(indent);
        let elapsed_ms = stats.elapsed_nanos as f64 / 1_000_000.0;

        out.push_str(&format!(
            "{}\"operator\": \"{}\",\n",
            pad, stats.operator_name
        ));
        out.push_str(&format!("{}\"elapsed_ms\": {:.3},\n", pad, elapsed_ms));
        out.push_str(&format!("{}\"rows_in\": {},\n", pad, stats.rows_processed));
        out.push_str(&format!("{}\"rows_out\": {},\n", pad, stats.rows_output));
        out.push_str(&format!(
            "{}\"batches\": {},\n",
            pad, stats.batches_processed
        ));
        out.push_str(&format!(
            "{}\"peak_memory_bytes\": {},\n",
            pad, stats.peak_memory_bytes
        ));

        if !stats.extra_metrics.is_empty() {
            out.push_str(&format!("{}\"metrics\": {{\n", pad));
            for (i, (name, value)) in stats.extra_metrics.iter().enumerate() {
                let comma = if i < stats.extra_metrics.len() - 1 {
                    ","
                } else {
                    ""
                };
                out.push_str(&format!("{}  \"{}\": \"{}\"{}\n", pad, name, value, comma));
            }
            out.push_str(&format!("{}}},\n", pad));
        }

        out.push_str(&format!("{}\"children\": [", pad));
        if stats.children.is_empty() {
            out.push_str("]\n");
        } else {
            out.push('\n');
            for (i, child) in stats.children.iter().enumerate() {
                out.push_str(&format!("{}  {{\n", pad));
                Self::stats_to_json(child, out, indent + 2);
                let comma = if i < stats.children.len() - 1 {
                    ","
                } else {
                    ""
                };
                out.push_str(&format!("{}  }}{}\n", pad, comma));
            }
            out.push_str(&format!("{}]\n", pad));
        }
    }

    /// Render as DOT (Graphviz) format.
    pub fn to_dot(&self) -> String {
        let mut output = String::new();
        output.push_str("digraph QueryProfile {\n");
        output.push_str("  rankdir=TB;\n");
        output.push_str("  node [shape=record, style=filled, fillcolor=lightyellow];\n\n");

        let mut counter = 0;
        Self::stats_to_dot(&self.stats, &mut output, &mut counter, None);

        output.push_str("}\n");
        output
    }

    fn stats_to_dot(
        stats: &ExecutionStats,
        out: &mut String,
        counter: &mut usize,
        parent_id: Option<usize>,
    ) {
        let my_id = *counter;
        *counter += 1;

        let elapsed_ms = stats.elapsed_nanos as f64 / 1_000_000.0;
        let label = format!(
            "{}|time: {:.2}ms|rows: {} → {}",
            stats.operator_name, elapsed_ms, stats.rows_processed, stats.rows_output
        );
        out.push_str(&format!("  n{} [label=\"{}\"];\n", my_id, label));

        if let Some(pid) = parent_id {
            out.push_str(&format!("  n{} -> n{};\n", pid, my_id));
        }

        for child in &stats.children {
            Self::stats_to_dot(child, out, counter, Some(my_id));
        }
    }

    /// Render as Mermaid diagram format.
    pub fn to_mermaid(&self) -> String {
        let mut output = String::new();
        output.push_str("graph TD\n");

        let mut counter = 0;
        Self::stats_to_mermaid(&self.stats, &mut output, &mut counter, None);

        output
    }

    fn stats_to_mermaid(
        stats: &ExecutionStats,
        out: &mut String,
        counter: &mut usize,
        parent_id: Option<usize>,
    ) {
        let my_id = *counter;
        *counter += 1;

        let elapsed_ms = stats.elapsed_nanos as f64 / 1_000_000.0;
        let label = format!(
            "{}<br/>{:.2}ms | {} rows",
            stats.operator_name, elapsed_ms, stats.rows_output
        );
        out.push_str(&format!("  N{}[\"{}\"]\n", my_id, label));

        if let Some(pid) = parent_id {
            out.push_str(&format!("  N{} --> N{}\n", pid, my_id));
        }

        for child in &stats.children {
            Self::stats_to_mermaid(child, out, counter, Some(my_id));
        }
    }

    /// Get a flat list of all operators with their timings.
    pub fn flat_operators(&self) -> Vec<OperatorProfile> {
        let mut ops = Vec::new();
        Self::collect_operators(&self.stats, &mut ops, 0);
        ops
    }

    fn collect_operators(stats: &ExecutionStats, ops: &mut Vec<OperatorProfile>, depth: usize) {
        ops.push(OperatorProfile {
            name: stats.operator_name.clone(),
            depth,
            elapsed_ms: stats.elapsed_nanos as f64 / 1_000_000.0,
            rows_in: stats.rows_processed,
            rows_out: stats.rows_output,
            batches: stats.batches_processed,
            peak_memory_bytes: stats.peak_memory_bytes,
        });
        for child in &stats.children {
            Self::collect_operators(child, ops, depth + 1);
        }
    }
}

/// Flat representation of a single operator's profile.
#[derive(Debug, Clone)]
pub struct OperatorProfile {
    pub name: String,
    pub depth: usize,
    pub elapsed_ms: f64,
    pub rows_in: usize,
    pub rows_out: usize,
    pub batches: usize,
    pub peak_memory_bytes: usize,
}

// ---------------------------------------------------------------------------
// Production observability features
// ---------------------------------------------------------------------------

/// Production query profiler that tracks execution history.
pub struct QueryProfiler {
    history: RwLock<Vec<QueryProfileEntry>>,
    max_history: usize,
    slow_query_threshold_ms: f64,
}

/// A single recorded query profile entry.
#[derive(Debug, Clone)]
pub struct QueryProfileEntry {
    pub query_id: String,
    pub sql: String,
    pub total_elapsed_ms: f64,
    pub rows_returned: usize,
    pub peak_memory_bytes: usize,
    pub timestamp_epoch_ms: u64,
    pub is_slow: bool,
}

impl QueryProfiler {
    pub fn new(max_history: usize, slow_query_threshold_ms: f64) -> Self {
        Self {
            history: RwLock::new(Vec::new()),
            max_history,
            slow_query_threshold_ms,
        }
    }

    /// Record a completed query's execution statistics.
    pub fn record(&self, sql: &str, stats: &ExecutionStats) {
        let total_ms = stats.total_elapsed_nanos() as f64 / 1_000_000.0;
        let entry = QueryProfileEntry {
            query_id: Uuid::new_v4().to_string(),
            sql: sql.to_string(),
            total_elapsed_ms: total_ms,
            rows_returned: stats.rows_output,
            peak_memory_bytes: stats.peak_memory_bytes,
            timestamp_epoch_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            is_slow: total_ms > self.slow_query_threshold_ms,
        };

        let mut history = self.history.write();
        if history.len() >= self.max_history {
            history.remove(0);
        }
        history.push(entry);
    }

    /// Return all queries that exceeded the slow-query threshold.
    pub fn slow_queries(&self) -> Vec<QueryProfileEntry> {
        self.history
            .read()
            .iter()
            .filter(|e| e.is_slow)
            .cloned()
            .collect()
    }

    /// Average query duration across all recorded queries.
    pub fn avg_query_time_ms(&self) -> f64 {
        let history = self.history.read();
        if history.is_empty() {
            return 0.0;
        }
        let sum: f64 = history.iter().map(|e| e.total_elapsed_ms).sum();
        sum / history.len() as f64
    }

    /// 95th-percentile query latency.
    pub fn p95_query_time_ms(&self) -> f64 {
        let history = self.history.read();
        if history.is_empty() {
            return 0.0;
        }
        let mut times: Vec<f64> = history.iter().map(|e| e.total_elapsed_ms).collect();
        times.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((times.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
        times[idx.min(times.len() - 1)]
    }

    /// Total number of queries recorded.
    pub fn total_queries(&self) -> usize {
        self.history.read().len()
    }

    /// Clear all recorded history.
    pub fn clear(&self) {
        self.history.write().clear();
    }
}

// ---------------------------------------------------------------------------
// Optimization suggestions
// ---------------------------------------------------------------------------

/// An optimization suggestion based on query profile analysis.
#[derive(Debug, Clone)]
pub struct OptimizationSuggestion {
    /// Description of the optimization
    pub description: String,
    /// Estimated impact (High, Medium, Low)
    pub impact: String,
    /// Condition that triggered this suggestion
    pub condition: String,
}

impl OptimizationSuggestion {
    pub fn new(
        description: impl Into<String>,
        impact: impl Into<String>,
        condition: impl Into<String>,
    ) -> Self {
        Self {
            description: description.into(),
            impact: impact.into(),
            condition: condition.into(),
        }
    }
}

/// Analyze a profile report and generate optimization suggestions.
pub fn suggest_optimizations(profile: &ProfileReport) -> Vec<OptimizationSuggestion> {
    let mut suggestions = Vec::new();

    // Analyze the stats tree
    analyze_operator_for_suggestions(&profile.stats, &mut suggestions);

    suggestions
}

fn analyze_operator_for_suggestions(
    stats: &ExecutionStats,
    suggestions: &mut Vec<OptimizationSuggestion>,
) {
    let elapsed_ms = stats.elapsed_nanos as f64 / 1_000_000.0;

    // Full table scan without filters
    if stats.operator_name.contains("TableScan") || stats.operator_name.contains("Scan") {
        if stats.rows_processed > 100_000 && stats.rows_output == stats.rows_processed {
            suggestions.push(OptimizationSuggestion::new(
                "Add WHERE clause to filter rows earlier in the query plan",
                "High",
                format!("Full table scan processing {} rows", stats.rows_processed),
            ));
        }

        // High memory usage for scan
        if stats.peak_memory_bytes > 100 * 1024 * 1024 {
            suggestions.push(OptimizationSuggestion::new(
                "Consider using column pruning (SELECT specific columns instead of *)",
                "Medium",
                format!(
                    "Table scan using {} MB of memory",
                    stats.peak_memory_bytes / 1024 / 1024
                ),
            ));
        }
    }

    // Sort on large datasets without LIMIT
    if stats.operator_name.contains("Sort") {
        if stats.rows_processed > 100_000 {
            suggestions.push(OptimizationSuggestion::new(
                "Add LIMIT clause if you only need top N results - avoids sorting entire dataset",
                "High",
                format!("Sorting {} rows", stats.rows_processed),
            ));
        }

        // Slow sort
        if elapsed_ms > 1000.0 {
            suggestions.push(OptimizationSuggestion::new(
                "Consider creating an index on the sort column",
                "High",
                format!("Sort taking {:.0} ms", elapsed_ms),
            ));
        }
    }

    // Nested loop joins
    if stats.operator_name.contains("NestedLoop") || stats.operator_name.contains("CrossJoin") {
        suggestions.push(OptimizationSuggestion::new(
            "Nested loop join detected - add JOIN conditions or use hash join with indexes",
            "High",
            "Nested loop joins are inefficient for large datasets".to_string(),
        ));
    }

    // Hash join with high memory
    if stats.operator_name.contains("HashJoin") {
        if stats.peak_memory_bytes > 500 * 1024 * 1024 {
            suggestions.push(OptimizationSuggestion::new(
                "Hash join using high memory - consider reducing input size or using sort-merge join",
                "Medium",
                format!("Hash join using {} MB", stats.peak_memory_bytes / 1024 / 1024),
            ));
        }
    }

    // High memory operators in general
    if stats.peak_memory_bytes > 1024 * 1024 * 1024 {
        suggestions.push(OptimizationSuggestion::new(
            "Reduce batch size to lower memory consumption",
            "Medium",
            format!(
                "Operator using {} MB of memory",
                stats.peak_memory_bytes / 1024 / 1024
            ),
        ));
    }

    // Aggregate on many groups
    if stats.operator_name.contains("Aggregate") || stats.operator_name.contains("GroupBy") {
        if stats.rows_output > 100_000 {
            suggestions.push(OptimizationSuggestion::new(
                "Aggregation producing many groups - consider filtering or reducing cardinality",
                "Low",
                format!("Producing {} groups", stats.rows_output),
            ));
        }
    }

    // Recursively analyze children
    for child in &stats.children {
        analyze_operator_for_suggestions(child, suggestions);
    }
}

/// Analyzes query profiles to identify performance bottlenecks.
pub struct BottleneckAnalyzer;

/// A detected performance bottleneck.
#[derive(Debug, Clone)]
pub struct Bottleneck {
    pub operator_name: String,
    pub percentage_of_total: f64,
    pub elapsed_ms: f64,
    pub suggestion: String,
}

impl BottleneckAnalyzer {
    /// Analyze execution statistics and return operators consuming >20% of total time.
    pub fn analyze(stats: &ExecutionStats) -> Vec<Bottleneck> {
        let total_ns = stats.total_elapsed_nanos() as f64;
        if total_ns == 0.0 {
            return Vec::new();
        }

        let mut operators = Vec::new();
        Self::collect_operators(stats, &mut operators);

        operators
            .into_iter()
            .filter_map(|(name, elapsed_ns)| {
                let pct = (elapsed_ns as f64 / total_ns) * 100.0;
                if pct > 20.0 {
                    let suggestion = if name.starts_with("TableScan") {
                        "Consider adding filters or using partitioned data"
                    } else if name.starts_with("HashJoin") {
                        "Consider adding indexes or reducing join input size"
                    } else if name.starts_with("Sort") {
                        "Consider limiting result set or using pre-sorted data"
                    } else {
                        "Operator is a potential bottleneck"
                    };
                    Some(Bottleneck {
                        operator_name: name,
                        percentage_of_total: pct,
                        elapsed_ms: elapsed_ns as f64 / 1_000_000.0,
                        suggestion: suggestion.to_string(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    fn collect_operators(stats: &ExecutionStats, out: &mut Vec<(String, u64)>) {
        out.push((stats.operator_name.clone(), stats.elapsed_nanos));
        for child in &stats.children {
            Self::collect_operators(child, out);
        }
    }
}

/// Exports query profiles as OpenTelemetry-compatible trace spans.
pub struct OpenTelemetryExporter {
    service_name: String,
}

/// A single OpenTelemetry-compatible span.
#[derive(Debug, Clone)]
pub struct OtelSpan {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub operation_name: String,
    pub start_time_ns: u64,
    pub duration_ns: u64,
    pub attributes: Vec<(String, String)>,
}

impl OpenTelemetryExporter {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }

    /// Convert an ExecutionStats tree into a flat list of OtelSpans.
    pub fn export_spans(&self, stats: &ExecutionStats) -> Vec<OtelSpan> {
        let trace_id = Uuid::new_v4().to_string();
        let base_time_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let mut spans = Vec::new();
        self.collect_spans(stats, &trace_id, None, base_time_ns, &mut spans);
        spans
    }

    fn collect_spans(
        &self,
        stats: &ExecutionStats,
        trace_id: &str,
        parent_span_id: Option<&str>,
        start_time_ns: u64,
        spans: &mut Vec<OtelSpan>,
    ) {
        let span_id = Uuid::new_v4().to_string();

        let mut attributes = vec![
            ("service.name".to_string(), self.service_name.clone()),
            (
                "rows_processed".to_string(),
                stats.rows_processed.to_string(),
            ),
            ("rows_output".to_string(), stats.rows_output.to_string()),
            (
                "peak_memory_bytes".to_string(),
                stats.peak_memory_bytes.to_string(),
            ),
            (
                "batches_processed".to_string(),
                stats.batches_processed.to_string(),
            ),
        ];
        for (k, v) in &stats.extra_metrics {
            attributes.push((k.clone(), v.clone()));
        }

        spans.push(OtelSpan {
            trace_id: trace_id.to_string(),
            span_id: span_id.clone(),
            parent_span_id: parent_span_id.map(|s| s.to_string()),
            operation_name: stats.operator_name.clone(),
            start_time_ns,
            duration_ns: stats.elapsed_nanos,
            attributes,
        });

        let mut child_start = start_time_ns;
        for child in &stats.children {
            self.collect_spans(child, trace_id, Some(&span_id), child_start, spans);
            child_start += child.total_elapsed_nanos();
        }
    }
}

/// Prometheus-compatible metrics exporter.
#[derive(Debug, Clone, Default)]
pub struct PrometheusMetrics {
    pub query_count: u64,
    pub query_duration_sum_ms: f64,
    pub query_errors: u64,
    pub active_queries: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub memory_used_bytes: usize,
}

impl PrometheusMetrics {
    /// Record the outcome of a single query.
    pub fn record_query(&mut self, duration_ms: f64, error: bool) {
        self.query_count += 1;
        self.query_duration_sum_ms += duration_ms;
        if error {
            self.query_errors += 1;
        }
    }

    /// Format metrics in Prometheus text exposition format.
    pub fn format_exposition(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP blaze_queries_total Total queries executed\n");
        out.push_str("# TYPE blaze_queries_total counter\n");
        out.push_str(&format!("blaze_queries_total {}\n", self.query_count));
        out.push_str("# HELP blaze_query_duration_ms_sum Total query duration\n");
        out.push_str("# TYPE blaze_query_duration_ms_sum counter\n");
        out.push_str(&format!(
            "blaze_query_duration_ms_sum {:.3}\n",
            self.query_duration_sum_ms
        ));
        out.push_str("# HELP blaze_query_errors_total Total query errors\n");
        out.push_str("# TYPE blaze_query_errors_total counter\n");
        out.push_str(&format!("blaze_query_errors_total {}\n", self.query_errors));
        out.push_str("# HELP blaze_active_queries Currently active queries\n");
        out.push_str("# TYPE blaze_active_queries gauge\n");
        out.push_str(&format!("blaze_active_queries {}\n", self.active_queries));
        out.push_str("# HELP blaze_cache_hits Total cache hits\n");
        out.push_str("# TYPE blaze_cache_hits counter\n");
        out.push_str(&format!("blaze_cache_hits {}\n", self.cache_hits));
        out.push_str("# HELP blaze_cache_misses Total cache misses\n");
        out.push_str("# TYPE blaze_cache_misses counter\n");
        out.push_str(&format!("blaze_cache_misses {}\n", self.cache_misses));
        out.push_str("# HELP blaze_memory_used_bytes Current memory usage\n");
        out.push_str("# TYPE blaze_memory_used_bytes gauge\n");
        out.push_str(&format!(
            "blaze_memory_used_bytes {}\n",
            self.memory_used_bytes
        ));
        out
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Flamegraph Generator + Cardinality Annotator
// ---------------------------------------------------------------------------

/// Generates flamegraph-compatible folded stack output from profiling data.
pub struct FlamegraphGenerator;

impl FlamegraphGenerator {
    /// Generate folded stack format from operator profiles.
    /// Output format: "stack;frame duration_us" per line (for flamegraph.pl)
    pub fn generate_folded(operators: &[(String, u64)]) -> String {
        let mut output = String::new();
        let mut stack = String::new();
        for (name, duration_us) in operators {
            if !stack.is_empty() {
                stack.push(';');
            }
            stack.push_str(name);
            output.push_str(&format!("{} {}\n", stack, duration_us));
        }
        output
    }

    /// Generate a simple text-based flame chart.
    pub fn generate_text_chart(operators: &[(String, u64)], width: usize) -> String {
        if operators.is_empty() {
            return String::new();
        }
        let max_duration = operators.iter().map(|(_, d)| *d).max().unwrap_or(1);
        let mut chart = String::new();
        for (name, duration) in operators {
            let bar_width = (*duration as f64 / max_duration as f64 * width as f64) as usize;
            let bar = "█".repeat(bar_width.max(1));
            chart.push_str(&format!("{:>30} {} ({} μs)\n", name, bar, duration));
        }
        chart
    }
}

/// Annotates query plan nodes with actual vs estimated cardinality.
#[derive(Debug, Clone)]
pub struct CardinalityAnnotation {
    pub operator: String,
    pub estimated_rows: usize,
    pub actual_rows: usize,
    pub selectivity: f64,
}

impl CardinalityAnnotation {
    pub fn new(operator: &str, estimated: usize, actual: usize) -> Self {
        let selectivity = if estimated > 0 {
            actual as f64 / estimated as f64
        } else {
            0.0
        };
        Self {
            operator: operator.to_string(),
            estimated_rows: estimated,
            actual_rows: actual,
            selectivity,
        }
    }

    /// The Q-error (max of ratio and its inverse, always >= 1.0).
    pub fn q_error(&self) -> f64 {
        if self.estimated_rows == 0 || self.actual_rows == 0 {
            return f64::INFINITY;
        }
        let ratio = self.actual_rows as f64 / self.estimated_rows as f64;
        ratio.max(1.0 / ratio)
    }

    /// Whether the estimate was significantly off (q-error > threshold).
    pub fn is_misestimate(&self, threshold: f64) -> bool {
        self.q_error() > threshold
    }
}

/// Collects cardinality annotations across a query plan.
pub struct CardinalityAnnotator {
    annotations: Vec<CardinalityAnnotation>,
}

impl CardinalityAnnotator {
    pub fn new() -> Self {
        Self {
            annotations: Vec::new(),
        }
    }

    pub fn add(&mut self, operator: &str, estimated: usize, actual: usize) {
        self.annotations
            .push(CardinalityAnnotation::new(operator, estimated, actual));
    }

    /// Get all annotations.
    pub fn annotations(&self) -> &[CardinalityAnnotation] {
        &self.annotations
    }

    /// Get annotations where the estimate was significantly wrong.
    pub fn misestimates(&self, threshold: f64) -> Vec<&CardinalityAnnotation> {
        self.annotations
            .iter()
            .filter(|a| a.is_misestimate(threshold))
            .collect()
    }

    /// Average Q-error across all operators.
    pub fn avg_q_error(&self) -> f64 {
        let finite: Vec<f64> = self
            .annotations
            .iter()
            .map(|a| a.q_error())
            .filter(|q| q.is_finite())
            .collect();
        if finite.is_empty() {
            0.0
        } else {
            finite.iter().sum::<f64>() / finite.len() as f64
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 3: Slow Query Analyzer + Sampling Profiler
// ---------------------------------------------------------------------------

/// Analyzes slow queries and provides optimization recommendations.
pub struct SlowQueryAnalyzer {
    threshold_ms: u64,
    slow_queries: Vec<SlowQueryEntry>,
    max_entries: usize,
}

/// A recorded slow query with analysis.
#[derive(Debug, Clone)]
pub struct SlowQueryEntry {
    pub sql: String,
    pub execution_time_ms: u64,
    pub recommendations: Vec<String>,
}

impl SlowQueryAnalyzer {
    pub fn new(threshold_ms: u64, max_entries: usize) -> Self {
        Self {
            threshold_ms,
            slow_queries: Vec::new(),
            max_entries,
        }
    }

    /// Record a query execution and analyze if slow.
    pub fn record(&mut self, sql: &str, execution_time_ms: u64) {
        if execution_time_ms < self.threshold_ms {
            return;
        }

        let recommendations = Self::analyze_query(sql, execution_time_ms);

        if self.slow_queries.len() >= self.max_entries {
            self.slow_queries.remove(0);
        }

        self.slow_queries.push(SlowQueryEntry {
            sql: sql.to_string(),
            execution_time_ms,
            recommendations,
        });
    }

    fn analyze_query(sql: &str, _execution_time_ms: u64) -> Vec<String> {
        let upper = sql.to_uppercase();
        let mut recs = Vec::new();

        if upper.contains("SELECT *") {
            recs.push("Consider specifying columns instead of SELECT *".to_string());
        }
        if upper.contains("CROSS JOIN")
            || (upper.contains("FROM") && upper.contains(",") && !upper.contains("JOIN"))
        {
            recs.push(
                "Possible cross join detected - ensure join conditions are specified".to_string(),
            );
        }
        if !upper.contains("LIMIT") && !upper.contains("GROUP BY") {
            recs.push("Consider adding LIMIT to reduce result set size".to_string());
        }
        if upper.contains("LIKE '%") {
            recs.push("Leading wildcard in LIKE prevents index usage".to_string());
        }
        if upper.contains("OR") && upper.contains("WHERE") {
            recs.push(
                "OR conditions in WHERE may prevent index usage - consider UNION".to_string(),
            );
        }

        recs
    }

    /// Get all recorded slow queries.
    pub fn slow_queries(&self) -> &[SlowQueryEntry] {
        &self.slow_queries
    }

    /// Get the slowest query.
    pub fn slowest(&self) -> Option<&SlowQueryEntry> {
        self.slow_queries.iter().max_by_key(|q| q.execution_time_ms)
    }

    pub fn count(&self) -> usize {
        self.slow_queries.len()
    }
}

/// Sampling-based profiler for low-overhead continuous profiling.
pub struct SamplingProfiler {
    samples: Vec<ProfileSample>,
    sample_rate_hz: u32,
    is_active: bool,
    max_samples: usize,
}

/// A single profiling sample.
#[derive(Debug, Clone)]
pub struct ProfileSample {
    pub operator: String,
    pub timestamp_us: u64,
    pub memory_bytes: usize,
}

impl SamplingProfiler {
    pub fn new(sample_rate_hz: u32, max_samples: usize) -> Self {
        Self {
            samples: Vec::new(),
            sample_rate_hz,
            is_active: false,
            max_samples,
        }
    }

    pub fn start(&mut self) {
        self.is_active = true;
    }
    pub fn stop(&mut self) {
        self.is_active = false;
    }
    pub fn is_active(&self) -> bool {
        self.is_active
    }

    /// Record a sample.
    pub fn record(&mut self, operator: &str, timestamp_us: u64, memory_bytes: usize) {
        if !self.is_active {
            return;
        }
        if self.samples.len() >= self.max_samples {
            self.samples.remove(0);
        }
        self.samples.push(ProfileSample {
            operator: operator.to_string(),
            timestamp_us,
            memory_bytes,
        });
    }

    /// Get time spent per operator (aggregated).
    pub fn operator_time(&self) -> std::collections::HashMap<String, u64> {
        let mut times: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for sample in &self.samples {
            *times.entry(sample.operator.clone()).or_insert(0) += 1;
        }
        // Convert sample counts to estimated microseconds
        let interval_us = if self.sample_rate_hz > 0 {
            1_000_000 / self.sample_rate_hz as u64
        } else {
            0
        };
        for v in times.values_mut() {
            *v *= interval_us;
        }
        times
    }

    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate_hz
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_stats() -> ExecutionStats {
        let mut scan = ExecutionStats::new("TableScan[users]");
        scan.elapsed_nanos = 500_000; // 0.5ms
        scan.rows_processed = 0;
        scan.rows_output = 1000;
        scan.batches_processed = 1;
        scan.peak_memory_bytes = 8192;

        let mut filter = ExecutionStats::new("Filter[age > 21]");
        filter.elapsed_nanos = 200_000;
        filter.rows_processed = 1000;
        filter.rows_output = 450;
        filter.batches_processed = 1;
        filter.children.push(scan);

        let mut project = ExecutionStats::new("Projection[name, age]");
        project.elapsed_nanos = 100_000;
        project.rows_processed = 450;
        project.rows_output = 450;
        project.batches_processed = 1;
        project.children.push(filter);

        project
    }

    #[test]
    fn test_text_format() {
        let report = ProfileReport::new(sample_stats());
        let text = report.render(ProfileFormat::Text);
        assert!(text.contains("Projection"));
        assert!(text.contains("Filter"));
        assert!(text.contains("TableScan"));
    }

    #[test]
    fn test_json_format() {
        let report = ProfileReport::new(sample_stats());
        let json = report.render(ProfileFormat::Json);
        assert!(json.contains("\"operator\": \"Projection[name, age]\""));
        assert!(json.contains("\"rows_out\": 450"));
        assert!(json.contains("\"children\": ["));
    }

    #[test]
    fn test_dot_format() {
        let report = ProfileReport::new(sample_stats());
        let dot = report.render(ProfileFormat::Dot);
        assert!(dot.contains("digraph QueryProfile"));
        assert!(dot.contains("Projection"));
        assert!(dot.contains("->"));
        assert!(dot.contains("TableScan"));
    }

    #[test]
    fn test_mermaid_format() {
        let report = ProfileReport::new(sample_stats());
        let mermaid = report.render(ProfileFormat::Mermaid);
        assert!(mermaid.contains("graph TD"));
        assert!(mermaid.contains("-->"));
        assert!(mermaid.contains("Projection"));
    }

    #[test]
    fn test_flat_operators() {
        let report = ProfileReport::new(sample_stats());
        let ops = report.flat_operators();
        assert_eq!(ops.len(), 3);
        assert_eq!(ops[0].name, "Projection[name, age]");
        assert_eq!(ops[0].depth, 0);
        assert_eq!(ops[1].name, "Filter[age > 21]");
        assert_eq!(ops[1].depth, 1);
        assert_eq!(ops[2].name, "TableScan[users]");
        assert_eq!(ops[2].depth, 2);
    }

    #[test]
    fn test_profile_report_stats_access() {
        let stats = sample_stats();
        let total = stats.total_elapsed_nanos();
        let report = ProfileReport::new(stats);
        assert_eq!(report.stats().total_elapsed_nanos(), total);
    }

    #[test]
    fn test_query_profiler_record() {
        let profiler = QueryProfiler::new(100, 1.0);
        let stats = sample_stats();
        profiler.record("SELECT * FROM users", &stats);
        assert_eq!(profiler.total_queries(), 1);

        let history = profiler.history.read();
        assert_eq!(history[0].sql, "SELECT * FROM users");
        assert!(history[0].timestamp_epoch_ms > 0);
        assert!(!history[0].query_id.is_empty());
    }

    #[test]
    fn test_query_profiler_slow_queries() {
        let profiler = QueryProfiler::new(100, 0.1); // 0.1ms threshold
                                                     // sample_stats total = 0.8ms → should be slow
        profiler.record("SELECT * FROM users", &sample_stats());

        let mut fast = ExecutionStats::new("Projection");
        fast.elapsed_nanos = 1_000; // 0.001ms → fast
        fast.rows_output = 1;
        profiler.record("SELECT 1", &fast);

        assert_eq!(profiler.total_queries(), 2);
        let slow = profiler.slow_queries();
        assert_eq!(slow.len(), 1);
        assert_eq!(slow[0].sql, "SELECT * FROM users");
    }

    #[test]
    fn test_query_profiler_percentiles() {
        let profiler = QueryProfiler::new(100, 100.0);
        for i in 1..=100 {
            let mut s = ExecutionStats::new("Op");
            s.elapsed_nanos = i * 1_000_000; // i ms
            s.rows_output = 1;
            profiler.record("q", &s);
        }
        let avg = profiler.avg_query_time_ms();
        assert!((avg - 50.5).abs() < 0.01);
        let p95 = profiler.p95_query_time_ms();
        assert!((p95 - 95.0).abs() < 0.01);
    }

    #[test]
    fn test_bottleneck_analyzer() {
        let mut scan = ExecutionStats::new("TableScan[orders]");
        scan.elapsed_nanos = 8_000_000; // 8ms – dominant

        let mut join = ExecutionStats::new("HashJoin");
        join.elapsed_nanos = 1_000_000;
        join.children.push(scan);

        let mut project = ExecutionStats::new("Projection");
        project.elapsed_nanos = 1_000_000;
        project.children.push(join);

        let bottlenecks = BottleneckAnalyzer::analyze(&project);
        assert!(!bottlenecks.is_empty());
        let scan_b = bottlenecks
            .iter()
            .find(|b| b.operator_name.starts_with("TableScan"))
            .expect("TableScan should be a bottleneck");
        assert!(scan_b.percentage_of_total > 20.0);
        assert_eq!(
            scan_b.suggestion,
            "Consider adding filters or using partitioned data"
        );
    }

    #[test]
    fn test_otel_exporter_spans() {
        let exporter = OpenTelemetryExporter::new("blaze-test");
        let stats = sample_stats();
        let spans = exporter.export_spans(&stats);

        // Should have one span per operator (3 total)
        assert_eq!(spans.len(), 3);

        // All spans share the same trace_id
        let trace_id = &spans[0].trace_id;
        assert!(spans.iter().all(|s| s.trace_id == *trace_id));

        // Root span has no parent
        assert!(spans[0].parent_span_id.is_none());
        // Children have a parent
        assert!(spans[1].parent_span_id.is_some());
        assert!(spans[2].parent_span_id.is_some());

        // Service name appears in attributes
        assert!(spans[0]
            .attributes
            .iter()
            .any(|(k, v)| k == "service.name" && v == "blaze-test"));
    }

    #[test]
    fn test_prometheus_metrics_format() {
        let mut metrics = PrometheusMetrics::default();
        metrics.record_query(10.0, false);
        metrics.record_query(20.0, true);
        metrics.cache_hits = 5;
        metrics.cache_misses = 2;
        metrics.memory_used_bytes = 1024;

        let text = metrics.format_exposition();
        assert!(text.contains("blaze_queries_total 2"));
        assert!(text.contains("blaze_query_duration_ms_sum 30.000"));
        assert!(text.contains("blaze_query_errors_total 1"));
        assert!(text.contains("blaze_cache_hits 5"));
        assert!(text.contains("blaze_memory_used_bytes 1024"));
        assert!(text.contains("# TYPE blaze_queries_total counter"));
    }

    #[test]
    fn test_flamegraph_folded() {
        let ops = vec![
            ("Scan".to_string(), 1000u64),
            ("Filter".to_string(), 500),
            ("Project".to_string(), 200),
        ];
        let folded = FlamegraphGenerator::generate_folded(&ops);
        assert!(folded.contains("Scan 1000"));
        assert!(folded.contains("Scan;Filter 500"));
    }

    #[test]
    fn test_flamegraph_text_chart() {
        let ops = vec![("Scan".to_string(), 1000u64), ("Filter".to_string(), 500)];
        let chart = FlamegraphGenerator::generate_text_chart(&ops, 40);
        assert!(chart.contains("█"));
        assert!(chart.contains("Scan"));
    }

    #[test]
    fn test_cardinality_annotation() {
        let ann = CardinalityAnnotation::new("Scan", 1000, 5000);
        assert!((ann.q_error() - 5.0).abs() < 0.01);
        assert!(ann.is_misestimate(3.0));
        assert!(!ann.is_misestimate(10.0));
    }

    #[test]
    fn test_cardinality_annotator() {
        let mut annotator = CardinalityAnnotator::new();
        annotator.add("Scan", 1000, 1000);
        annotator.add("Filter", 500, 50);
        assert_eq!(annotator.annotations().len(), 2);
        assert_eq!(annotator.misestimates(5.0).len(), 1);
    }

    #[test]
    fn test_slow_query_analyzer() {
        let mut analyzer = SlowQueryAnalyzer::new(100, 10);
        analyzer.record("SELECT * FROM users", 200);
        analyzer.record("SELECT id FROM users", 50); // Not slow
        assert_eq!(analyzer.count(), 1);
        assert!(analyzer.slow_queries()[0].recommendations.len() > 0);
    }

    #[test]
    fn test_slow_query_recommendations() {
        let mut analyzer = SlowQueryAnalyzer::new(0, 10); // threshold=0 to capture all
        analyzer.record("SELECT * FROM users CROSS JOIN orders", 100);
        let recs = &analyzer.slow_queries()[0].recommendations;
        assert!(recs.iter().any(|r| r.contains("SELECT *")));
    }

    #[test]
    fn test_sampling_profiler() {
        let mut profiler = SamplingProfiler::new(1000, 100);
        profiler.start();
        profiler.record("Scan", 1000, 1024);
        profiler.record("Scan", 2000, 2048);
        profiler.record("Filter", 3000, 512);
        assert_eq!(profiler.sample_count(), 3);

        let times = profiler.operator_time();
        assert!(times.contains_key("Scan"));
        assert!(times.contains_key("Filter"));
    }

    #[test]
    fn test_sampling_profiler_inactive() {
        let mut profiler = SamplingProfiler::new(1000, 100);
        // Not started
        profiler.record("Scan", 1000, 1024);
        assert_eq!(profiler.sample_count(), 0);
    }

    #[test]
    fn test_html_generation() {
        use crate::planner::ExecutionStats;
        let stats = ExecutionStats {
            operator_name: "Scan".to_string(),
            elapsed_nanos: 1000000,
            rows_processed: 100,
            rows_output: 100,
            batches_processed: 1,
            peak_memory_bytes: 1024,
            bytes_spilled: 0,
            extra_metrics: vec![],
            children: vec![],
        };

        let report = ProfileReport::new(stats);
        let html = report.to_html();
        assert!(html.contains("<html>"));
        assert!(html.contains("Scan"));
        assert!(html.contains("1.000 ms"));
    }

    #[test]
    fn test_optimization_suggestions() {
        use crate::planner::ExecutionStats;

        // Full table scan
        let stats = ExecutionStats {
            operator_name: "TableScan".to_string(),
            elapsed_nanos: 10000000,
            rows_processed: 1000000,
            rows_output: 1000000,
            batches_processed: 10,
            peak_memory_bytes: 1024 * 1024 * 100,
            bytes_spilled: 0,
            extra_metrics: vec![],
            children: vec![],
        };

        let report = ProfileReport::new(stats);
        let suggestions = suggest_optimizations(&report);
        assert!(suggestions.len() > 0);
        assert!(suggestions.iter().any(|s| s.description.contains("WHERE")));
    }

    #[test]
    fn test_optimization_suggestion_sort() {
        use crate::planner::ExecutionStats;

        let stats = ExecutionStats {
            operator_name: "Sort".to_string(),
            elapsed_nanos: 5000000,
            rows_processed: 1000000,
            rows_output: 1000000,
            batches_processed: 10,
            peak_memory_bytes: 1024,
            bytes_spilled: 0,
            extra_metrics: vec![],
            children: vec![],
        };

        let report = ProfileReport::new(stats);
        let suggestions = suggest_optimizations(&report);
        assert!(suggestions.iter().any(|s| s.description.contains("LIMIT")));
    }
}
