//! Query execution profiler with multiple output formats.
//!
//! Provides JSON, DOT (Graphviz), Mermaid, and text tree output formats
//! for visualizing query execution profiles from `ExecutionStats`.

use crate::planner::ExecutionStats;

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
}
