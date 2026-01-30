//! AI-Powered Query Advisor for Blaze.
//!
//! Provides rule-based query analysis to detect anti-patterns, suggest index
//! opportunities, recommend optimizations, and produce human-readable
//! explanations of query plans.
//!
//! # Example
//! ```rust
//! use blaze::query_advisor::{QueryAdvisor, Advice, Severity};
//!
//! let advisor = QueryAdvisor::new();
//! let advices = advisor.analyze("SELECT * FROM orders WHERE status = 'pending'");
//!
//! for advice in &advices {
//!     println!("[{:?}] {}: {}", advice.severity, advice.category, advice.message);
//! }
//! ```

use std::collections::HashSet;

// ---------------------------------------------------------------------------
// Severity & Category
// ---------------------------------------------------------------------------

/// Severity level for advisor recommendations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Info,
    Warning,
    Critical,
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Info => write!(f, "INFO"),
            Severity::Warning => write!(f, "WARNING"),
            Severity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Categories of advice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdviceCategory {
    AntiPattern,
    IndexRecommendation,
    Performance,
    BestPractice,
    Security,
}

impl std::fmt::Display for AdviceCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdviceCategory::AntiPattern => write!(f, "Anti-Pattern"),
            AdviceCategory::IndexRecommendation => write!(f, "Index"),
            AdviceCategory::Performance => write!(f, "Performance"),
            AdviceCategory::BestPractice => write!(f, "Best Practice"),
            AdviceCategory::Security => write!(f, "Security"),
        }
    }
}

// ---------------------------------------------------------------------------
// Advice
// ---------------------------------------------------------------------------

/// A single piece of advice from the query advisor.
#[derive(Debug, Clone)]
pub struct Advice {
    pub severity: Severity,
    pub category: AdviceCategory,
    pub message: String,
    pub suggestion: Option<String>,
}

impl Advice {
    fn new(severity: Severity, category: AdviceCategory, message: impl Into<String>) -> Self {
        Self {
            severity,
            category,
            message: message.into(),
            suggestion: None,
        }
    }

    fn with_suggestion(mut self, suggestion: impl Into<String>) -> Self {
        self.suggestion = Some(suggestion.into());
        self
    }
}

impl std::fmt::Display for Advice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}: {}", self.severity, self.category, self.message)?;
        if let Some(ref s) = self.suggestion {
            write!(f, " → {}", s)?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Query Advisor
// ---------------------------------------------------------------------------

/// Analyzes SQL queries and provides optimization advice.
pub struct QueryAdvisor {
    rules: Vec<Box<dyn AnalysisRule + Send + Sync>>,
}

impl QueryAdvisor {
    /// Create a new advisor with all built-in rules.
    pub fn new() -> Self {
        Self {
            rules: vec![
                Box::new(SelectStarRule),
                Box::new(MissingWhereRule),
                Box::new(LeadingWildcardRule),
                Box::new(ImplicitCrossJoinRule),
                Box::new(NestedSubqueryRule),
                Box::new(FunctionOnIndexedColumnRule),
                Box::new(SelectDistinctRule),
                Box::new(UnionVsUnionAllRule),
                Box::new(OrderByWithoutLimitRule),
                Box::new(IndexCandidateRule),
            ],
        }
    }

    /// Analyze a SQL query and return a list of advice.
    pub fn analyze(&self, sql: &str) -> Vec<Advice> {
        let normalized = sql.to_uppercase();
        let mut advices = Vec::new();
        for rule in &self.rules {
            advices.extend(rule.check(sql, &normalized));
        }
        advices.sort_by(|a, b| b.severity.cmp(&a.severity));
        advices
    }

    /// Produce a human-readable explanation of what a SQL query does.
    pub fn explain_natural(&self, sql: &str) -> String {
        let normalized = sql.to_uppercase().replace('\n', " ");
        let mut parts = Vec::new();

        // Detect CTE
        if normalized.contains("WITH ") {
            parts.push(
                "This query uses a Common Table Expression (CTE) to define a temporary result set."
                    .to_string(),
            );
        }

        // Main operation
        if normalized.contains("SELECT") {
            if normalized.contains("SELECT *") {
                parts.push("It selects ALL columns".to_string());
            } else {
                let cols = extract_between(&normalized, "SELECT", "FROM");
                if let Some(cols) = cols {
                    let col_count = cols.split(',').count();
                    parts.push(format!("It selects {} column(s)/expression(s)", col_count));
                }
            }
        }

        // FROM
        if let Some(tables) = extract_tables(&normalized) {
            if tables.len() == 1 {
                parts.push(format!("from the {} table", tables[0]));
            } else {
                parts.push(format!(
                    "from {} tables: {}",
                    tables.len(),
                    tables.join(", ")
                ));
            }
        }

        // JOINs
        let join_count = normalized.matches("JOIN").count();
        if join_count > 0 {
            parts.push(format!("with {} join(s)", join_count));
        }

        // WHERE
        if normalized.contains("WHERE") {
            parts.push("applying filter conditions".to_string());
        }

        // GROUP BY
        if normalized.contains("GROUP BY") {
            parts.push("grouping results".to_string());
            if normalized.contains("HAVING") {
                parts.push("with a HAVING filter on the groups".to_string());
            }
        }

        // ORDER BY
        if normalized.contains("ORDER BY") {
            parts.push("sorted by specified column(s)".to_string());
        }

        // LIMIT
        if normalized.contains("LIMIT") {
            parts.push("with a row limit".to_string());
        }

        // Aggregates
        let aggs: Vec<&str> = ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX("]
            .iter()
            .filter(|a| normalized.contains(**a))
            .copied()
            .collect();
        if !aggs.is_empty() {
            parts.push(format!(
                "using aggregate function(s): {}",
                aggs.iter()
                    .map(|a| a.trim_end_matches('('))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }

        if parts.is_empty() {
            return "Unable to parse query structure.".to_string();
        }

        parts.join(", ") + "."
    }

    /// Get a summary report of all advice for a query.
    pub fn report(&self, sql: &str) -> String {
        let advices = self.analyze(sql);
        if advices.is_empty() {
            return "✓ No issues found. Query looks good!".to_string();
        }
        let mut lines = vec![format!("Found {} issue(s):", advices.len())];
        for (i, advice) in advices.iter().enumerate() {
            lines.push(format!("  {}. {}", i + 1, advice));
        }
        lines.join("\n")
    }
}

impl Default for QueryAdvisor {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Analysis Rule trait
// ---------------------------------------------------------------------------

trait AnalysisRule {
    fn check(&self, sql: &str, normalized: &str) -> Vec<Advice>;
}

// ---------------------------------------------------------------------------
// Built-in rules
// ---------------------------------------------------------------------------

struct SelectStarRule;
impl AnalysisRule for SelectStarRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        if normalized.contains("SELECT *") || normalized.contains("SELECT  *") {
            vec![Advice::new(
                Severity::Warning,
                AdviceCategory::AntiPattern,
                "SELECT * retrieves all columns, which may transfer unnecessary data",
            )
            .with_suggestion("List only the columns you need")]
        } else {
            vec![]
        }
    }
}

struct MissingWhereRule;
impl AnalysisRule for MissingWhereRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        let is_select = normalized.trim_start().starts_with("SELECT");
        let has_where = normalized.contains("WHERE");
        let has_join = normalized.contains("JOIN");
        let has_limit = normalized.contains("LIMIT");

        if is_select && !has_where && !has_join && !has_limit && normalized.contains("FROM") {
            vec![Advice::new(
                Severity::Warning,
                AdviceCategory::Performance,
                "Query has no WHERE clause and may perform a full table scan",
            )
            .with_suggestion("Add a WHERE clause to filter rows")]
        } else {
            vec![]
        }
    }
}

struct LeadingWildcardRule;
impl AnalysisRule for LeadingWildcardRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        if normalized.contains("LIKE '%") || normalized.contains("LIKE  '%") {
            vec![Advice::new(
                Severity::Warning,
                AdviceCategory::Performance,
                "LIKE with a leading wildcard (%) prevents index usage",
            )
            .with_suggestion("Consider full-text search or trailing wildcard patterns")]
        } else {
            vec![]
        }
    }
}

struct ImplicitCrossJoinRule;
impl AnalysisRule for ImplicitCrossJoinRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        // Detect comma-separated tables in FROM without explicit JOIN
        if let Some(from_clause) = extract_between(normalized, "FROM", "WHERE") {
            let tables: Vec<&str> = from_clause.split(',').collect();
            if tables.len() > 1 && !normalized.contains("JOIN") {
                return vec![Advice::new(
                    Severity::Warning,
                    AdviceCategory::AntiPattern,
                    "Implicit cross join detected (comma-separated tables without JOIN)",
                )
                .with_suggestion("Use explicit JOIN syntax for clarity")];
            }
        }
        vec![]
    }
}

struct NestedSubqueryRule;
impl AnalysisRule for NestedSubqueryRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        let subquery_count = normalized.matches("SELECT").count();
        if subquery_count > 2 {
            vec![Advice::new(
                Severity::Info,
                AdviceCategory::Performance,
                format!(
                    "Query contains {} nested SELECT statements which may impact readability",
                    subquery_count - 1
                ),
            )
            .with_suggestion("Consider using CTEs (WITH clause) for better readability")]
        } else {
            vec![]
        }
    }
}

struct FunctionOnIndexedColumnRule;
impl AnalysisRule for FunctionOnIndexedColumnRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        let patterns = [
            "UPPER(",
            "LOWER(",
            "TRIM(",
            "CAST(",
            "COALESCE(",
            "YEAR(",
            "MONTH(",
            "DAY(",
        ];
        let in_where = extract_between(normalized, "WHERE", "ORDER")
            .or_else(|| extract_between(normalized, "WHERE", "GROUP"));
        let in_where_str = in_where.as_deref().unwrap_or("");

        for pat in &patterns {
            if in_where_str.contains(pat) {
                return vec![Advice::new(
                    Severity::Warning,
                    AdviceCategory::Performance,
                    format!(
                        "Function {} in WHERE clause may prevent index usage",
                        pat.trim_end_matches('(')
                    ),
                )
                .with_suggestion(
                    "Consider computed/expression columns or restructure the condition",
                )];
            }
        }
        vec![]
    }
}

struct SelectDistinctRule;
impl AnalysisRule for SelectDistinctRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        if normalized.contains("SELECT DISTINCT") && normalized.contains("GROUP BY") {
            vec![Advice::new(
                Severity::Info,
                AdviceCategory::AntiPattern,
                "DISTINCT with GROUP BY is often redundant",
            )
            .with_suggestion("GROUP BY already produces distinct groups; remove DISTINCT")]
        } else {
            vec![]
        }
    }
}

struct UnionVsUnionAllRule;
impl AnalysisRule for UnionVsUnionAllRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        // UNION without ALL requires deduplication
        if normalized.contains("UNION") && !normalized.contains("UNION ALL") {
            vec![Advice::new(
                Severity::Info,
                AdviceCategory::Performance,
                "UNION performs deduplication; use UNION ALL if duplicates are acceptable",
            )
            .with_suggestion("Replace UNION with UNION ALL to avoid the sort/dedup cost")]
        } else {
            vec![]
        }
    }
}

struct OrderByWithoutLimitRule;
impl AnalysisRule for OrderByWithoutLimitRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        if normalized.contains("ORDER BY") && !normalized.contains("LIMIT") {
            vec![Advice::new(
                Severity::Info,
                AdviceCategory::Performance,
                "ORDER BY without LIMIT sorts the entire result set",
            )
            .with_suggestion("Add LIMIT if you only need the top/bottom N rows")]
        } else {
            vec![]
        }
    }
}

struct IndexCandidateRule;
impl AnalysisRule for IndexCandidateRule {
    fn check(&self, _sql: &str, normalized: &str) -> Vec<Advice> {
        let mut candidates = HashSet::new();

        // Columns in WHERE = comparisons
        if let Some(where_clause) = extract_between(normalized, "WHERE", "ORDER") {
            for part in where_clause.split("AND") {
                let part = part.trim();
                if part.contains('=') && !part.contains("!=") && !part.contains("<>") {
                    if let Some(col) = part.split('=').next() {
                        let col = col.trim();
                        if is_column_name(col) {
                            candidates.insert(col.to_string());
                        }
                    }
                }
            }
        }

        // Columns in ORDER BY
        if let Some(order_clause) = extract_after(normalized, "ORDER BY") {
            for part in order_clause.split(',') {
                let col = part.split_whitespace().next().unwrap_or("");
                if is_column_name(col) {
                    candidates.insert(col.to_string());
                }
            }
        }

        if candidates.is_empty() {
            return vec![];
        }

        let cols: Vec<String> = candidates.into_iter().collect();
        vec![Advice::new(
            Severity::Info,
            AdviceCategory::IndexRecommendation,
            format!("Consider indexing column(s): {}", cols.join(", ")),
        )]
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_between(s: &str, start: &str, end: &str) -> Option<String> {
    let start_idx = s.find(start)? + start.len();
    let rest = &s[start_idx..];
    let end_idx = rest.find(end).unwrap_or(rest.len());
    Some(rest[..end_idx].trim().to_string())
}

fn extract_after(s: &str, marker: &str) -> Option<String> {
    let idx = s.find(marker)? + marker.len();
    let rest = s[idx..].trim();
    // Take until LIMIT, ;, or end
    let end = rest
        .find("LIMIT")
        .or_else(|| rest.find(';'))
        .unwrap_or(rest.len());
    Some(rest[..end].trim().to_string())
}

fn extract_tables(normalized: &str) -> Option<Vec<String>> {
    let from_clause = extract_between(normalized, "FROM", "WHERE")
        .or_else(|| extract_between(normalized, "FROM", "GROUP"))
        .or_else(|| extract_between(normalized, "FROM", "ORDER"))
        .or_else(|| extract_between(normalized, "FROM", "LIMIT"))
        .or_else(|| extract_after(normalized, "FROM"))?;

    // Split by JOIN or comma
    let parts: Vec<&str> = from_clause
        .split(',')
        .flat_map(|s| s.split("JOIN"))
        .collect();

    let tables: Vec<String> = parts
        .iter()
        .filter_map(|p| {
            let tokens: Vec<&str> = p.split_whitespace().collect();
            tokens.first().map(|t| t.to_string())
        })
        .filter(|t| {
            !t.is_empty()
                && ![
                    "ON", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "OUTER", "NATURAL",
                ]
                .contains(&t.as_str())
        })
        .collect();

    if tables.is_empty() {
        None
    } else {
        Some(tables)
    }
}

fn is_column_name(s: &str) -> bool {
    !s.is_empty()
        && !s.contains('(')
        && !s.contains('\'')
        && !s.chars().next().unwrap_or(' ').is_ascii_digit()
        && s.chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '.')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_star_warning() {
        let advisor = QueryAdvisor::new();
        let advices = advisor.analyze("SELECT * FROM orders");
        assert!(advices
            .iter()
            .any(|a| a.category == AdviceCategory::AntiPattern));
    }

    #[test]
    fn test_no_where_clause() {
        let advisor = QueryAdvisor::new();
        let advices = advisor.analyze("SELECT id, name FROM users");
        assert!(advices
            .iter()
            .any(|a| a.category == AdviceCategory::Performance && a.message.contains("WHERE")));
    }

    #[test]
    fn test_leading_wildcard() {
        let advisor = QueryAdvisor::new();
        let advices = advisor.analyze("SELECT * FROM users WHERE name LIKE '%john%'");
        assert!(advices.iter().any(|a| a.message.contains("wildcard")));
    }

    #[test]
    fn test_clean_query_fewer_issues() {
        let advisor = QueryAdvisor::new();
        let advices =
            advisor.analyze("SELECT id, name FROM users WHERE status = 'active' LIMIT 10");
        // This well-formed query should have minimal issues
        let warnings = advices
            .iter()
            .filter(|a| a.severity >= Severity::Warning)
            .count();
        assert!(warnings == 0, "Expected no warnings, got: {:?}", advices);
    }

    #[test]
    fn test_union_vs_union_all() {
        let advisor = QueryAdvisor::new();
        let advices = advisor.analyze("SELECT id FROM a UNION SELECT id FROM b");
        assert!(advices.iter().any(|a| a.message.contains("UNION ALL")));
    }

    #[test]
    fn test_order_without_limit() {
        let advisor = QueryAdvisor::new();
        let advices = advisor.analyze("SELECT id FROM users WHERE active = 1 ORDER BY created_at");
        assert!(advices.iter().any(|a| a.message.contains("ORDER BY")));
    }

    #[test]
    fn test_index_recommendation() {
        let advisor = QueryAdvisor::new();
        let advices = advisor
            .analyze("SELECT name FROM users WHERE status = 'active' ORDER BY created_at LIMIT 10");
        assert!(advices
            .iter()
            .any(|a| a.category == AdviceCategory::IndexRecommendation));
    }

    #[test]
    fn test_natural_language_explain() {
        let advisor = QueryAdvisor::new();
        let explanation = advisor.explain_natural(
            "SELECT name, COUNT(*) FROM orders WHERE status = 'pending' GROUP BY name ORDER BY name LIMIT 10",
        );
        assert!(explanation.contains("column"));
        assert!(explanation.contains("filter"));
        assert!(explanation.contains("group"));
    }

    #[test]
    fn test_report_clean_query() {
        let advisor = QueryAdvisor::new();
        let report = advisor.report("SELECT id FROM users WHERE id = 1 LIMIT 1");
        // Minimal issues expected
        assert!(!report.is_empty());
    }

    #[test]
    fn test_implicit_cross_join() {
        let advisor = QueryAdvisor::new();
        let advices =
            advisor.analyze("SELECT * FROM users, orders WHERE users.id = orders.user_id");
        assert!(advices.iter().any(|a| a.message.contains("cross join")));
    }

    #[test]
    fn test_function_in_where() {
        let advisor = QueryAdvisor::new();
        let advices =
            advisor.analyze("SELECT id FROM users WHERE UPPER(name) = 'JOHN' ORDER BY id LIMIT 1");
        assert!(advices.iter().any(|a| a.message.contains("UPPER")));
    }

    #[test]
    fn test_severity_ordering() {
        assert!(Severity::Critical > Severity::Warning);
        assert!(Severity::Warning > Severity::Info);
    }

    #[test]
    fn test_advice_display() {
        let advice = Advice::new(Severity::Warning, AdviceCategory::Performance, "Test msg")
            .with_suggestion("Fix it");
        let display = format!("{}", advice);
        assert!(display.contains("WARNING"));
        assert!(display.contains("Test msg"));
        assert!(display.contains("Fix it"));
    }
}
