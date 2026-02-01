//! SQL Language Server Protocol support for Blaze.
//!
//! This module provides LSP-compatible diagnostics, completions, hover,
//! and go-to-definition for SQL queries backed by the Blaze parser and catalog.

use std::collections::HashMap;

use parking_lot::RwLock;

use crate::sql::parser::Parser;
use crate::types::Schema;

// ---------------------------------------------------------------------------
// LSP-compatible types
// ---------------------------------------------------------------------------

/// A zero-indexed position in a text document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub line: u32,
    pub character: u32,
}

/// A range in a text document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Range {
    pub start: Position,
    pub end: Position,
}

/// Diagnostic severity levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticSeverity {
    Error,
    Warning,
    Info,
    Hint,
}

/// A diagnostic message produced by analysing a SQL document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostic {
    pub range: Range,
    pub severity: DiagnosticSeverity,
    pub message: String,
    pub source: String,
}

/// Kind of completion item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionItemKind {
    Table,
    Column,
    Keyword,
    Function,
}

/// A completion suggestion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletionItem {
    pub label: String,
    pub kind: CompletionItemKind,
    pub detail: Option<String>,
    pub insert_text: Option<String>,
}

/// Hover information shown when the cursor rests on a token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoverInfo {
    pub contents: String,
    pub range: Option<Range>,
}

// ---------------------------------------------------------------------------
// Completion context
// ---------------------------------------------------------------------------

/// The syntactic context at the cursor position, used to drive completions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionContext {
    AfterSelect,
    AfterFrom,
    AfterWhere,
    AfterJoin,
    AfterDot(String),
    AfterOrderBy,
    AfterGroupBy,
    General,
}

// ---------------------------------------------------------------------------
// SqlAnalyzer – stateless helpers
// ---------------------------------------------------------------------------

/// Stateless utility functions for analysing SQL text.
pub struct SqlAnalyzer;

impl SqlAnalyzer {
    /// Extract table names referenced in `FROM` and `JOIN` clauses.
    pub fn extract_table_names(sql: &str) -> Vec<String> {
        let upper = sql.to_uppercase();
        let tokens: Vec<&str> = sql.split_whitespace().collect();
        let upper_tokens: Vec<&str> = upper.split_whitespace().collect();
        let mut names = Vec::new();

        for (i, tok) in upper_tokens.iter().enumerate() {
            if (*tok == "FROM" || *tok == "JOIN") && i + 1 < tokens.len() {
                let name =
                    tokens[i + 1].trim_end_matches(|c: char| c == ',' || c == ';' || c == ')');
                if !name.is_empty()
                    && name
                        .chars()
                        .next()
                        .map_or(false, |c| c.is_alphabetic() || c == '_')
                {
                    names.push(name.to_string());
                }
            }
        }
        names
    }

    /// Return the word under the cursor.
    pub fn word_at_position(sql: &str, position: &Position) -> Option<String> {
        let line = sql.lines().nth(position.line as usize)?;
        let col = position.character as usize;
        if col > line.len() {
            return None;
        }

        let bytes = line.as_bytes();
        let mut start = col;
        while start > 0 && (bytes[start - 1] as char).is_alphanumeric()
            || (start > 0 && bytes[start - 1] == b'_')
        {
            start -= 1;
        }
        let mut end = col;
        while end < bytes.len() && ((bytes[end] as char).is_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        if start == end {
            return None;
        }
        Some(line[start..end].to_string())
    }

    /// Determine the completion context at the given cursor position.
    pub fn context_at_position(sql: &str, position: &Position) -> CompletionContext {
        // Collect all text up to the cursor.
        let mut text_before = String::new();
        for (i, line) in sql.lines().enumerate() {
            if (i as u32) < position.line {
                text_before.push_str(line);
                text_before.push(' ');
            } else if i as u32 == position.line {
                let end = (position.character as usize).min(line.len());
                text_before.push_str(&line[..end]);
            }
        }

        let upper = text_before.to_uppercase();
        let trimmed = upper.trim_end();

        // Check for dot-qualification (e.g. "t.")
        if trimmed.ends_with('.') {
            let before_dot = trimmed[..trimmed.len() - 1].trim();
            if let Some(table) = before_dot.rsplit_once(|c: char| c.is_whitespace() || c == ',') {
                return CompletionContext::AfterDot(table.1.to_lowercase());
            }
            return CompletionContext::AfterDot(before_dot.to_lowercase());
        }

        // Walk backwards through whitespace-separated tokens.
        let tokens: Vec<&str> = trimmed.split_whitespace().collect();
        if let Some(&last) = tokens.last() {
            match last {
                "SELECT" | "DISTINCT" => return CompletionContext::AfterSelect,
                "FROM" => return CompletionContext::AfterFrom,
                "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "OUTER" => {
                    return CompletionContext::AfterJoin
                }
                "WHERE" | "AND" | "OR" | "ON" | "HAVING" => return CompletionContext::AfterWhere,
                "BY" => {
                    if tokens.len() >= 2 {
                        match tokens[tokens.len() - 2] {
                            "ORDER" => return CompletionContext::AfterOrderBy,
                            "GROUP" => return CompletionContext::AfterGroupBy,
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }

        CompletionContext::General
    }
}

// ---------------------------------------------------------------------------
// SqlLanguageService
// ---------------------------------------------------------------------------

/// SQL language service providing diagnostics, completions, hover, and
/// go-to-definition backed by the Blaze parser and catalog metadata.
pub struct SqlLanguageService {
    schemas: RwLock<HashMap<String, Schema>>,
    keywords: Vec<String>,
    functions: Vec<String>,
}

impl SqlLanguageService {
    /// Create a new service pre-populated with SQL keywords and Blaze functions.
    pub fn new() -> Self {
        let keywords = vec![
            "SELECT",
            "FROM",
            "WHERE",
            "JOIN",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "OUTER",
            "CROSS",
            "ON",
            "GROUP",
            "BY",
            "ORDER",
            "ASC",
            "DESC",
            "HAVING",
            "LIMIT",
            "OFFSET",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "ALL",
            "DISTINCT",
            "AS",
            "AND",
            "OR",
            "NOT",
            "IN",
            "BETWEEN",
            "LIKE",
            "IS",
            "NULL",
            "TRUE",
            "FALSE",
            "CASE",
            "WHEN",
            "THEN",
            "ELSE",
            "END",
            "EXISTS",
            "INSERT",
            "INTO",
            "VALUES",
            "UPDATE",
            "SET",
            "DELETE",
            "CREATE",
            "TABLE",
            "DROP",
            "WITH",
            "RECURSIVE",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        let functions = vec![
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "UPPER",
            "LOWER",
            "ABS",
            "COALESCE",
            "CONCAT",
            "TRIM",
            "LENGTH",
            "SUBSTRING",
            "CAST",
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "LAG",
            "LEAD",
            "NTILE",
            "FIRST_VALUE",
            "LAST_VALUE",
        ]
        .into_iter()
        .map(String::from)
        .collect();

        Self {
            schemas: RwLock::new(HashMap::new()),
            keywords,
            functions,
        }
    }

    /// Register a table schema so that its columns appear in completions and
    /// hover information.
    pub fn register_table(&self, name: impl Into<String>, schema: Schema) {
        self.schemas.write().insert(name.into(), schema);
    }

    // ---- diagnostics -------------------------------------------------------

    /// Parse `sql` with the Blaze parser and return diagnostics for any errors.
    pub fn diagnostics(&self, sql: &str) -> Vec<Diagnostic> {
        match Parser::parse(sql) {
            Ok(_) => Vec::new(),
            Err(e) => {
                let end_col = sql.lines().last().map_or(0, |l| l.len() as u32);
                let end_line = sql.lines().count().saturating_sub(1) as u32;
                vec![Diagnostic {
                    range: Range {
                        start: Position {
                            line: 0,
                            character: 0,
                        },
                        end: Position {
                            line: end_line,
                            character: end_col,
                        },
                    },
                    severity: DiagnosticSeverity::Error,
                    message: e.to_string(),
                    source: "blaze-sql".to_string(),
                }]
            }
        }
    }

    // ---- completions -------------------------------------------------------

    /// Return context-aware completion items for the given cursor position.
    pub fn complete(&self, sql: &str, position: Position) -> Vec<CompletionItem> {
        let ctx = SqlAnalyzer::context_at_position(sql, &position);
        match ctx {
            CompletionContext::AfterFrom | CompletionContext::AfterJoin => self.table_completions(),
            CompletionContext::AfterSelect
            | CompletionContext::AfterWhere
            | CompletionContext::AfterOrderBy
            | CompletionContext::AfterGroupBy => {
                let mut items = self.column_completions_from_sql(sql);
                items.extend(self.function_completions());
                items
            }
            CompletionContext::AfterDot(ref table) => self.column_completions_for_table(table),
            CompletionContext::General => self.keyword_completions(),
        }
    }

    // ---- hover -------------------------------------------------------------

    /// Provide hover information for the token at the given position.
    pub fn hover(&self, sql: &str, position: Position) -> Option<HoverInfo> {
        let word = SqlAnalyzer::word_at_position(sql, &position)?;
        let upper = word.to_uppercase();

        // Check tables
        let schemas = self.schemas.read();
        if let Some(schema) = schemas
            .get(&word)
            .or_else(|| schemas.get(&word.to_lowercase()))
        {
            let mut md = format!(
                "**Table** `{}`\n\n| Column | Type | Nullable |\n|---|---|---|\n",
                word
            );
            for field in schema.fields() {
                md.push_str(&format!(
                    "| {} | {:?} | {} |\n",
                    field.name(),
                    field.data_type(),
                    if field.is_nullable() { "YES" } else { "NO" }
                ));
            }
            return Some(HoverInfo {
                contents: md,
                range: None,
            });
        }
        drop(schemas);

        // Check functions
        if self.functions.iter().any(|f| f == &upper) {
            let desc = function_description(&upper);
            return Some(HoverInfo {
                contents: format!("**Function** `{}`\n\n{}", upper, desc),
                range: None,
            });
        }

        // Check keywords
        if self.keywords.iter().any(|k| k == &upper) {
            let desc = keyword_description(&upper);
            return Some(HoverInfo {
                contents: format!("**Keyword** `{}`\n\n{}", upper, desc),
                range: None,
            });
        }

        None
    }

    // ---- go-to-definition --------------------------------------------------

    /// Find the definition site of a CTE or table alias in the SQL text.
    pub fn find_definition(&self, sql: &str, position: Position) -> Option<Range> {
        let word = SqlAnalyzer::word_at_position(sql, &position)?;
        let upper_word = word.to_uppercase();

        for (line_idx, line) in sql.lines().enumerate() {
            let upper_line = line.to_uppercase();

            // CTE: WITH name AS
            if let Some(pos) = upper_line.find(&format!("{} AS", upper_word)) {
                // Only match if preceded by WITH or comma (start of CTE)
                let prefix = upper_line[..pos].trim();
                if prefix.is_empty() || prefix.ends_with("WITH") || prefix.ends_with(',') {
                    return Some(Range {
                        start: Position {
                            line: line_idx as u32,
                            character: pos as u32,
                        },
                        end: Position {
                            line: line_idx as u32,
                            character: (pos + word.len()) as u32,
                        },
                    });
                }
            }

            // Table alias: FROM table alias  /  JOIN table alias
            // Look for patterns like "FROM <table> <word>" or "JOIN <table> <word>"
            let tokens: Vec<&str> = line.split_whitespace().collect();
            let upper_tokens: Vec<String> = tokens.iter().map(|t| t.to_uppercase()).collect();
            for (i, tok) in upper_tokens.iter().enumerate() {
                if (*tok == "FROM" || *tok == "JOIN") && i + 2 < upper_tokens.len() {
                    let alias_candidate =
                        upper_tokens[i + 2].trim_end_matches(|c: char| c == ',' || c == ')');
                    if alias_candidate == upper_word {
                        // Find the byte position of the alias in the line
                        if let Some(alias_pos) = line.rfind(tokens[i + 2]) {
                            return Some(Range {
                                start: Position {
                                    line: line_idx as u32,
                                    character: alias_pos as u32,
                                },
                                end: Position {
                                    line: line_idx as u32,
                                    character: (alias_pos + tokens[i + 2].len()) as u32,
                                },
                            });
                        }
                    }
                }
            }
        }
        None
    }

    // ---- internal helpers --------------------------------------------------

    fn table_completions(&self) -> Vec<CompletionItem> {
        self.schemas
            .read()
            .keys()
            .map(|name| CompletionItem {
                label: name.clone(),
                kind: CompletionItemKind::Table,
                detail: Some("Table".to_string()),
                insert_text: Some(name.clone()),
            })
            .collect()
    }

    fn column_completions_for_table(&self, table: &str) -> Vec<CompletionItem> {
        let schemas = self.schemas.read();
        let schema = match schemas.get(table) {
            Some(s) => s,
            None => return Vec::new(),
        };
        schema
            .fields()
            .iter()
            .map(|f| CompletionItem {
                label: f.name().to_string(),
                kind: CompletionItemKind::Column,
                detail: Some(format!("{:?}", f.data_type())),
                insert_text: Some(f.name().to_string()),
            })
            .collect()
    }

    fn column_completions_from_sql(&self, sql: &str) -> Vec<CompletionItem> {
        let tables = SqlAnalyzer::extract_table_names(sql);
        let schemas = self.schemas.read();
        let mut items = Vec::new();

        if tables.is_empty() {
            // No explicit tables yet – suggest columns from all registered tables.
            for (table, schema) in schemas.iter() {
                for f in schema.fields() {
                    items.push(CompletionItem {
                        label: f.name().to_string(),
                        kind: CompletionItemKind::Column,
                        detail: Some(format!("{} ({:?})", table, f.data_type())),
                        insert_text: Some(f.name().to_string()),
                    });
                }
            }
        } else {
            for table in &tables {
                if let Some(schema) = schemas.get(table) {
                    for f in schema.fields() {
                        items.push(CompletionItem {
                            label: f.name().to_string(),
                            kind: CompletionItemKind::Column,
                            detail: Some(format!("{} ({:?})", table, f.data_type())),
                            insert_text: Some(f.name().to_string()),
                        });
                    }
                }
            }
        }
        items
    }

    fn keyword_completions(&self) -> Vec<CompletionItem> {
        self.keywords
            .iter()
            .map(|kw| CompletionItem {
                label: kw.clone(),
                kind: CompletionItemKind::Keyword,
                detail: Some("Keyword".to_string()),
                insert_text: Some(kw.clone()),
            })
            .collect()
    }

    fn function_completions(&self) -> Vec<CompletionItem> {
        self.functions
            .iter()
            .map(|f| CompletionItem {
                label: f.clone(),
                kind: CompletionItemKind::Function,
                detail: Some(function_description(f).to_string()),
                insert_text: Some(format!("{}()", f)),
            })
            .collect()
    }
}

impl Default for SqlLanguageService {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Descriptions
// ---------------------------------------------------------------------------

fn function_description(name: &str) -> &'static str {
    match name {
        "COUNT" => "COUNT(expr) – number of non-null values",
        "SUM" => "SUM(expr) – sum of numeric values",
        "AVG" => "AVG(expr) – average of numeric values",
        "MIN" => "MIN(expr) – minimum value",
        "MAX" => "MAX(expr) – maximum value",
        "UPPER" => "UPPER(str) – convert to uppercase",
        "LOWER" => "LOWER(str) – convert to lowercase",
        "ABS" => "ABS(num) – absolute value",
        "COALESCE" => "COALESCE(a, b, ...) – first non-null argument",
        "CONCAT" => "CONCAT(a, b, ...) – concatenate strings",
        "TRIM" => "TRIM(str) – remove leading/trailing whitespace",
        "LENGTH" => "LENGTH(str) – string length",
        "SUBSTRING" => "SUBSTRING(str, start, len) – extract substring",
        "CAST" => "CAST(expr AS type) – type conversion",
        "ROW_NUMBER" => "ROW_NUMBER() – sequential row number within partition",
        "RANK" => "RANK() – rank with gaps for ties",
        "DENSE_RANK" => "DENSE_RANK() – rank without gaps",
        "LAG" => "LAG(expr, offset, default) – access previous row",
        "LEAD" => "LEAD(expr, offset, default) – access next row",
        "NTILE" => "NTILE(n) – divide rows into n buckets",
        "FIRST_VALUE" => "FIRST_VALUE(expr) – first value in window frame",
        "LAST_VALUE" => "LAST_VALUE(expr) – last value in window frame",
        _ => "SQL function",
    }
}

fn keyword_description(name: &str) -> &'static str {
    match name {
        "SELECT" => "Retrieve rows and columns from tables",
        "FROM" => "Specify source tables for a query",
        "WHERE" => "Filter rows by condition",
        "JOIN" => "Combine rows from two or more tables",
        "GROUP" => "Group rows sharing common values",
        "ORDER" => "Sort result set",
        "HAVING" => "Filter groups by aggregate condition",
        "LIMIT" => "Restrict number of returned rows",
        "UNION" => "Combine results of multiple queries",
        "WITH" => "Define common table expressions (CTEs)",
        "INSERT" => "Add rows to a table",
        "UPDATE" => "Modify existing rows",
        "DELETE" => "Remove rows from a table",
        "CREATE" => "Create a new database object",
        "DROP" => "Remove a database object",
        "CASE" => "Conditional expression",
        "DISTINCT" => "Remove duplicate rows",
        _ => "SQL keyword",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};

    fn sample_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("email", DataType::Utf8, true),
        ])
    }

    fn service_with_table() -> SqlLanguageService {
        let svc = SqlLanguageService::new();
        svc.register_table("users", sample_schema());
        svc
    }

    #[test]
    fn test_diagnostics_valid_sql() {
        let svc = SqlLanguageService::new();
        let diags = svc.diagnostics("SELECT 1");
        assert!(diags.is_empty(), "valid SQL should produce no diagnostics");
    }

    #[test]
    fn test_diagnostics_invalid_sql() {
        let svc = SqlLanguageService::new();
        let diags = svc.diagnostics("SELECTT 1");
        assert!(!diags.is_empty(), "invalid SQL should produce diagnostics");
        assert_eq!(diags[0].severity, DiagnosticSeverity::Error);
        assert_eq!(diags[0].source, "blaze-sql");
    }

    #[test]
    fn test_completion_after_select() {
        let svc = service_with_table();
        let items = svc.complete(
            "SELECT ",
            Position {
                line: 0,
                character: 7,
            },
        );
        // Should contain columns from `users` and functions
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"id"), "should suggest column 'id'");
        assert!(labels.contains(&"COUNT"), "should suggest function COUNT");
    }

    #[test]
    fn test_completion_after_from() {
        let svc = service_with_table();
        let items = svc.complete(
            "SELECT * FROM ",
            Position {
                line: 0,
                character: 14,
            },
        );
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"users"), "should suggest table 'users'");
        assert!(
            items.iter().all(|i| i.kind == CompletionItemKind::Table),
            "all items should be tables"
        );
    }

    #[test]
    fn test_completion_keywords() {
        let svc = SqlLanguageService::new();
        let items = svc.complete(
            "",
            Position {
                line: 0,
                character: 0,
            },
        );
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"SELECT"), "should contain SELECT keyword");
        assert!(labels.contains(&"FROM"), "should contain FROM keyword");
    }

    #[test]
    fn test_hover_table() {
        let svc = service_with_table();
        let info = svc.hover(
            "SELECT * FROM users",
            Position {
                line: 0,
                character: 15,
            },
        );
        assert!(info.is_some(), "should have hover info for table");
        let contents = info.unwrap().contents;
        assert!(contents.contains("**Table**"), "should show table heading");
        assert!(contents.contains("id"), "should list 'id' column");
        assert!(contents.contains("name"), "should list 'name' column");
    }

    #[test]
    fn test_hover_function() {
        let svc = SqlLanguageService::new();
        let info = svc.hover(
            "SELECT COUNT(*)",
            Position {
                line: 0,
                character: 8,
            },
        );
        assert!(info.is_some(), "should have hover info for function");
        let contents = info.unwrap().contents;
        assert!(
            contents.contains("**Function**"),
            "should show function heading"
        );
        assert!(contents.contains("COUNT"), "should mention COUNT");
    }

    #[test]
    fn test_word_at_position() {
        let sql = "SELECT id FROM users";
        let word = SqlAnalyzer::word_at_position(
            sql,
            &Position {
                line: 0,
                character: 8,
            },
        );
        assert_eq!(word, Some("id".to_string()));

        let word = SqlAnalyzer::word_at_position(
            sql,
            &Position {
                line: 0,
                character: 16,
            },
        );
        assert_eq!(word, Some("users".to_string()));
    }

    #[test]
    fn test_context_detection() {
        let ctx = SqlAnalyzer::context_at_position(
            "SELECT * FROM ",
            &Position {
                line: 0,
                character: 14,
            },
        );
        assert_eq!(ctx, CompletionContext::AfterFrom);

        let ctx = SqlAnalyzer::context_at_position(
            "SELECT ",
            &Position {
                line: 0,
                character: 7,
            },
        );
        assert_eq!(ctx, CompletionContext::AfterSelect);

        let ctx = SqlAnalyzer::context_at_position(
            "SELECT * FROM t ORDER BY ",
            &Position {
                line: 0,
                character: 25,
            },
        );
        assert_eq!(ctx, CompletionContext::AfterOrderBy);
    }

    #[test]
    fn test_register_table() {
        let svc = SqlLanguageService::new();
        let items = svc.complete(
            "SELECT * FROM ",
            Position {
                line: 0,
                character: 14,
            },
        );
        assert!(items.is_empty(), "no tables registered yet");

        svc.register_table(
            "orders",
            Schema::new(vec![Field::new("order_id", DataType::Int64, false)]),
        );

        let items = svc.complete(
            "SELECT * FROM ",
            Position {
                line: 0,
                character: 14,
            },
        );
        let labels: Vec<&str> = items.iter().map(|i| i.label.as_str()).collect();
        assert!(labels.contains(&"orders"), "registered table should appear");
    }
}
