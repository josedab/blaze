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
// Signature Help Provider (next-gen)
// ---------------------------------------------------------------------------

/// Detailed parameter information for function signatures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionParameterInfo {
    pub name: String,
    pub type_name: String,
    pub description: String,
    pub optional: bool,
}

/// Full function signature returned by the signature help provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionSignature {
    pub name: String,
    pub description: String,
    pub parameters: Vec<FunctionParameterInfo>,
    pub return_type: String,
}

/// Provides signature help for Blaze's built-in SQL functions.
pub struct SignatureHelpProvider {
    signatures: HashMap<String, FunctionSignature>,
}

impl SignatureHelpProvider {
    pub fn new() -> Self {
        let mut signatures = HashMap::new();

        signatures.insert("COUNT".to_string(), FunctionSignature {
            name: "COUNT".to_string(),
            description: "Returns the number of rows matching the query.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "expression".to_string(),
                type_name: "Any".to_string(),
                description: "Column or expression to count. Use * for all rows.".to_string(),
                optional: false,
            }],
            return_type: "Int64".to_string(),
        });
        signatures.insert("SUM".to_string(), FunctionSignature {
            name: "SUM".to_string(),
            description: "Returns the sum of a numeric column.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "expression".to_string(),
                type_name: "Numeric".to_string(),
                description: "Numeric column or expression to sum.".to_string(),
                optional: false,
            }],
            return_type: "Numeric".to_string(),
        });
        signatures.insert("AVG".to_string(), FunctionSignature {
            name: "AVG".to_string(),
            description: "Returns the average of a numeric column.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "expression".to_string(),
                type_name: "Numeric".to_string(),
                description: "Numeric column or expression to average.".to_string(),
                optional: false,
            }],
            return_type: "Float64".to_string(),
        });
        signatures.insert("MIN".to_string(), FunctionSignature {
            name: "MIN".to_string(),
            description: "Returns the minimum value in a column.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "expression".to_string(),
                type_name: "Any".to_string(),
                description: "Column or expression to find the minimum of.".to_string(),
                optional: false,
            }],
            return_type: "Same as input".to_string(),
        });
        signatures.insert("MAX".to_string(), FunctionSignature {
            name: "MAX".to_string(),
            description: "Returns the maximum value in a column.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "expression".to_string(),
                type_name: "Any".to_string(),
                description: "Column or expression to find the maximum of.".to_string(),
                optional: false,
            }],
            return_type: "Same as input".to_string(),
        });
        signatures.insert("UPPER".to_string(), FunctionSignature {
            name: "UPPER".to_string(),
            description: "Converts a string to uppercase.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "string".to_string(),
                type_name: "Utf8".to_string(),
                description: "String expression to convert.".to_string(),
                optional: false,
            }],
            return_type: "Utf8".to_string(),
        });
        signatures.insert("LOWER".to_string(), FunctionSignature {
            name: "LOWER".to_string(),
            description: "Converts a string to lowercase.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "string".to_string(),
                type_name: "Utf8".to_string(),
                description: "String expression to convert.".to_string(),
                optional: false,
            }],
            return_type: "Utf8".to_string(),
        });
        signatures.insert("LENGTH".to_string(), FunctionSignature {
            name: "LENGTH".to_string(),
            description: "Returns the length of a string.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "string".to_string(),
                type_name: "Utf8".to_string(),
                description: "String expression to measure.".to_string(),
                optional: false,
            }],
            return_type: "Int64".to_string(),
        });
        signatures.insert("COALESCE".to_string(), FunctionSignature {
            name: "COALESCE".to_string(),
            description: "Returns the first non-null argument.".to_string(),
            parameters: vec![
                FunctionParameterInfo {
                    name: "expr1".to_string(),
                    type_name: "Any".to_string(),
                    description: "First expression to evaluate.".to_string(),
                    optional: false,
                },
                FunctionParameterInfo {
                    name: "expr2".to_string(),
                    type_name: "Any".to_string(),
                    description: "Second expression (fallback).".to_string(),
                    optional: false,
                },
                FunctionParameterInfo {
                    name: "exprN".to_string(),
                    type_name: "Any".to_string(),
                    description: "Additional fallback expressions.".to_string(),
                    optional: true,
                },
            ],
            return_type: "Any".to_string(),
        });
        signatures.insert("ABS".to_string(), FunctionSignature {
            name: "ABS".to_string(),
            description: "Returns the absolute value of a number.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "value".to_string(),
                type_name: "Numeric".to_string(),
                description: "Numeric expression to take the absolute value of.".to_string(),
                optional: false,
            }],
            return_type: "Numeric".to_string(),
        });
        signatures.insert("CONCAT".to_string(), FunctionSignature {
            name: "CONCAT".to_string(),
            description: "Concatenates two or more strings.".to_string(),
            parameters: vec![
                FunctionParameterInfo {
                    name: "string1".to_string(),
                    type_name: "Utf8".to_string(),
                    description: "First string.".to_string(),
                    optional: false,
                },
                FunctionParameterInfo {
                    name: "string2".to_string(),
                    type_name: "Utf8".to_string(),
                    description: "Second string.".to_string(),
                    optional: false,
                },
                FunctionParameterInfo {
                    name: "stringN".to_string(),
                    type_name: "Utf8".to_string(),
                    description: "Additional strings to concatenate.".to_string(),
                    optional: true,
                },
            ],
            return_type: "Utf8".to_string(),
        });
        signatures.insert("TRIM".to_string(), FunctionSignature {
            name: "TRIM".to_string(),
            description: "Removes leading and trailing whitespace from a string.".to_string(),
            parameters: vec![FunctionParameterInfo {
                name: "string".to_string(),
                type_name: "Utf8".to_string(),
                description: "String expression to trim.".to_string(),
                optional: false,
            }],
            return_type: "Utf8".to_string(),
        });

        Self { signatures }
    }

    /// Returns the signature for a built-in function (case-insensitive lookup).
    pub fn get_signature(&self, function_name: &str) -> Option<FunctionSignature> {
        self.signatures.get(&function_name.to_uppercase()).cloned()
    }
}

// ---------------------------------------------------------------------------
// Code Action Provider (next-gen)
// ---------------------------------------------------------------------------

/// The kind of a code action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NextGenCodeActionKind {
    QuickFix,
    Refactor,
    OptimizationHint,
}

/// A code action suggested by the provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NextGenCodeAction {
    pub title: String,
    pub kind: NextGenCodeActionKind,
    pub edit: Option<String>,
}

/// Analyzes SQL text and suggests code actions.
pub struct CodeActionProvider;

impl CodeActionProvider {
    pub fn new() -> Self {
        Self
    }

    /// Returns code actions applicable to the given SQL within the specified range.
    pub fn get_actions(&self, sql: &str, _range: Range) -> Vec<NextGenCodeAction> {
        let upper = sql.to_uppercase();
        let mut actions = Vec::new();

        if upper.contains("SELECT *") || upper.contains("SELECT  *") {
            actions.push(NextGenCodeAction {
                title: "Replace SELECT * with explicit column list".to_string(),
                kind: NextGenCodeActionKind::Refactor,
                edit: Some("SELECT col1, col2, ... FROM".to_string()),
            });
        }

        if self.has_missing_aliases(&upper) {
            actions.push(NextGenCodeAction {
                title: "Add table aliases for readability".to_string(),
                kind: NextGenCodeActionKind::QuickFix,
                edit: None,
            });
        }

        if self.has_implicit_cross_join(&upper) {
            actions.push(NextGenCodeAction {
                title: "Convert implicit cross join to explicit JOIN".to_string(),
                kind: NextGenCodeActionKind::OptimizationHint,
                edit: Some("Use explicit JOIN ... ON syntax instead of comma-separated tables".to_string()),
            });
        }

        actions
    }

    fn has_missing_aliases(&self, upper_sql: &str) -> bool {
        if let Some(from_pos) = upper_sql.find("FROM ") {
            let after_from = &upper_sql[from_pos + 5..];
            let end = Self::find_clause_end(after_from);
            let from_clause = &after_from[..end];
            let tables: Vec<&str> = from_clause.split(',').collect();
            if tables.len() > 1 && !from_clause.contains(" AS ") {
                return true;
            }
        }
        false
    }

    fn has_implicit_cross_join(&self, upper_sql: &str) -> bool {
        if let Some(from_pos) = upper_sql.find("FROM ") {
            let after_from = &upper_sql[from_pos + 5..];
            let end = Self::find_clause_end(after_from);
            let from_clause = &after_from[..end];
            let tables: Vec<&str> = from_clause.split(',').collect();
            if tables.len() > 1 && !upper_sql.contains(" JOIN ") {
                return true;
            }
        }
        false
    }

    /// Finds the end of a FROM clause by looking for the next SQL keyword boundary.
    fn find_clause_end(s: &str) -> usize {
        let keywords = [" WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING "];
        keywords.iter()
            .filter_map(|kw| s.find(kw))
            .min()
            .unwrap_or(s.len())
    }
}

// ---------------------------------------------------------------------------
// Performance Analyzer (next-gen)
// ---------------------------------------------------------------------------

/// Severity of a performance hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HintSeverity {
    Info,
    Warning,
    Critical,
}

/// A performance hint produced by the analyzer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PerformanceHint {
    pub message: String,
    pub severity: HintSeverity,
    pub suggestion: String,
}

/// Analyzes SQL queries for potential performance issues.
pub struct PerformanceAnalyzer;

impl PerformanceAnalyzer {
    pub fn new() -> Self {
        Self
    }

    /// Analyzes the SQL statement and returns performance hints.
    pub fn analyze(&self, sql: &str) -> Vec<PerformanceHint> {
        let upper = sql.to_uppercase();
        let mut hints = Vec::new();

        if upper.starts_with("SELECT") && !upper.contains("WHERE") && !upper.contains("LIMIT") {
            hints.push(PerformanceHint {
                message: "Query performs a full table scan without filtering.".to_string(),
                severity: HintSeverity::Warning,
                suggestion: "Add a WHERE clause or LIMIT to reduce the result set.".to_string(),
            });
        }

        if self.has_cartesian_product(&upper) {
            hints.push(PerformanceHint {
                message: "Potential Cartesian product detected.".to_string(),
                severity: HintSeverity::Critical,
                suggestion: "Use explicit JOIN with ON conditions instead of comma-separated tables.".to_string(),
            });
        }

        if upper.starts_with("DELETE") && !upper.contains("WHERE") {
            hints.push(PerformanceHint {
                message: "DELETE without WHERE will remove all rows.".to_string(),
                severity: HintSeverity::Critical,
                suggestion: "Add a WHERE clause to target specific rows.".to_string(),
            });
        }

        if upper.starts_with("UPDATE") && !upper.contains("WHERE") {
            hints.push(PerformanceHint {
                message: "UPDATE without WHERE will modify all rows.".to_string(),
                severity: HintSeverity::Critical,
                suggestion: "Add a WHERE clause to target specific rows.".to_string(),
            });
        }

        if upper.contains("LIKE '%") || upper.contains("LIKE  '%") {
            hints.push(PerformanceHint {
                message: "LIKE with leading wildcard prevents index usage.".to_string(),
                severity: HintSeverity::Warning,
                suggestion: "Consider full-text search or restructure the query to avoid leading wildcards.".to_string(),
            });
        }

        if self.has_or_convertible_to_in(&upper) {
            hints.push(PerformanceHint {
                message: "Multiple OR conditions on the same column could be simplified.".to_string(),
                severity: HintSeverity::Info,
                suggestion: "Replace multiple OR conditions with an IN clause for clarity and potential optimization.".to_string(),
            });
        }

        hints
    }

    fn has_cartesian_product(&self, upper_sql: &str) -> bool {
        if let Some(from_pos) = upper_sql.find("FROM ") {
            let after_from = &upper_sql[from_pos + 5..];
            let end = Self::find_clause_end(after_from);
            let from_clause = &after_from[..end];
            let tables: Vec<&str> = from_clause.split(',').collect();
            if tables.len() > 1 && !upper_sql.contains(" JOIN ") && !upper_sql.contains(" WHERE ") {
                return true;
            }
        }
        false
    }

    /// Finds the end of a FROM clause by looking for the next SQL keyword boundary.
    fn find_clause_end(s: &str) -> usize {
        let keywords = [" WHERE ", " GROUP ", " ORDER ", " LIMIT ", " HAVING "];
        keywords.iter()
            .filter_map(|kw| s.find(kw))
            .min()
            .unwrap_or(s.len())
    }

    fn has_or_convertible_to_in(&self, upper_sql: &str) -> bool {
        if let Some(where_pos) = upper_sql.find(" WHERE ") {
            let where_clause = &upper_sql[where_pos + 7..];
            let parts: Vec<&str> = where_clause.split(" OR ").collect();
            if parts.len() >= 2 {
                if let (Some(col1), Some(col2)) = (
                    self.extract_column_from_eq(parts[0]),
                    self.extract_column_from_eq(parts[1]),
                ) {
                    return col1 == col2;
                }
            }
        }
        false
    }

    fn extract_column_from_eq<'a>(&self, expr: &'a str) -> Option<&'a str> {
        let trimmed = expr.trim();
        if let Some(eq_pos) = trimmed.find('=') {
            let col = trimmed[..eq_pos].trim();
            if !col.is_empty() {
                return Some(col);
            }
        }
        None
    }
}

// ---------------------------------------------------------------------------
// Query Explain Provider (next-gen)
// ---------------------------------------------------------------------------

/// Output of a query explain operation.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainOutput {
    pub plan_text: String,
    pub estimated_rows: u64,
    pub estimated_cost: f64,
    pub warnings: Vec<String>,
}

/// Provides simplified query explain / estimation for SQL queries.
pub struct QueryExplainProvider;

impl QueryExplainProvider {
    pub fn new() -> Self {
        Self
    }

    /// Produces a simplified explain output for the given SQL.
    pub fn explain(&self, sql: &str) -> crate::error::Result<ExplainOutput> {
        let _stmt = Parser::parse(sql).map_err(|e| {
            crate::error::BlazeError::analysis(format!("Parse error: {e}"))
        })?;

        let upper = sql.to_uppercase();

        let join_count = upper.matches(" JOIN ").count();
        let where_conditions = self.count_where_conditions(&upper);
        let has_group_by = upper.contains("GROUP BY");
        let has_order_by = upper.contains("ORDER BY");
        let has_subquery = upper.matches("SELECT").count() > 1;

        let base_rows: u64 = 1000;
        let estimated_rows = if where_conditions > 0 {
            base_rows / (where_conditions as u64 + 1)
        } else {
            base_rows
        };

        let mut cost = 1.0;
        cost *= 2.0_f64.powi(join_count as i32);
        if has_group_by {
            cost *= 1.5;
        }
        if has_order_by {
            cost *= 1.3;
        }
        if has_subquery {
            cost *= 2.0;
        }

        let estimated_cost = cost * estimated_rows as f64;

        let mut plan_lines = Vec::new();
        plan_lines.push("QueryPlan".to_string());
        if has_order_by {
            plan_lines.push(format!("  Sort (est. cost: {:.1})", estimated_cost * 0.2));
        }
        if has_group_by {
            plan_lines.push(format!("  HashAggregate (est. cost: {:.1})", estimated_cost * 0.3));
        }
        for i in 0..join_count {
            plan_lines.push(format!("  HashJoin #{} (est. cost: {:.1})", i + 1, estimated_cost * 0.2));
        }
        if where_conditions > 0 {
            plan_lines.push(format!("  Filter ({} conditions)", where_conditions));
        }
        plan_lines.push(format!("  Scan (est. rows: {})", estimated_rows));

        let mut warnings = Vec::new();
        if join_count >= 3 {
            warnings.push("High number of JOINs may impact performance.".to_string());
        }
        if where_conditions == 0 && upper.starts_with("SELECT") {
            warnings.push("No WHERE clause — full table scan expected.".to_string());
        }
        if has_subquery {
            warnings.push("Subquery detected — consider using a CTE or JOIN.".to_string());
        }

        Ok(ExplainOutput {
            plan_text: plan_lines.join("\n"),
            estimated_rows,
            estimated_cost,
            warnings,
        })
    }

    fn count_where_conditions(&self, upper_sql: &str) -> usize {
        if let Some(where_pos) = upper_sql.find(" WHERE ") {
            let after_where = &upper_sql[where_pos + 7..];
            let keywords = [" GROUP ", " ORDER ", " LIMIT ", " HAVING "];
            let end = keywords.iter()
                .filter_map(|kw| after_where.find(kw))
                .min()
                .unwrap_or(after_where.len());
            let where_clause = &after_where[..end];
            let ands = where_clause.matches(" AND ").count();
            let ors = where_clause.matches(" OR ").count();
            1 + ands + ors
        } else {
            0
        }
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

    // -----------------------------------------------------------------------
    // Signature Help Provider tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_signature_help_known_function() {
        let provider = SignatureHelpProvider::new();
        let sig = provider.get_signature("COUNT").unwrap();
        assert_eq!(sig.name, "COUNT");
        assert_eq!(sig.return_type, "Int64");
        assert!(!sig.parameters.is_empty());
    }

    #[test]
    fn test_signature_help_case_insensitive() {
        let provider = SignatureHelpProvider::new();
        let sig = provider.get_signature("avg").unwrap();
        assert_eq!(sig.name, "AVG");
        assert_eq!(sig.return_type, "Float64");
    }

    #[test]
    fn test_signature_help_unknown_function() {
        let provider = SignatureHelpProvider::new();
        assert!(provider.get_signature("NONEXISTENT").is_none());
    }

    #[test]
    fn test_signature_help_all_builtins() {
        let provider = SignatureHelpProvider::new();
        let expected = [
            "COUNT", "SUM", "AVG", "MIN", "MAX",
            "UPPER", "LOWER", "LENGTH", "COALESCE", "ABS", "CONCAT", "TRIM",
        ];
        for name in &expected {
            assert!(provider.get_signature(name).is_some(), "missing signature for {name}");
        }
    }

    #[test]
    fn test_signature_help_coalesce_optional_param() {
        let provider = SignatureHelpProvider::new();
        let sig = provider.get_signature("COALESCE").unwrap();
        assert!(sig.parameters.len() >= 3);
        assert!(sig.parameters.last().unwrap().optional);
    }

    // -----------------------------------------------------------------------
    // Code Action Provider tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_code_action_select_star() {
        let provider = CodeActionProvider::new();
        let range = Range {
            start: Position { line: 0, character: 0 },
            end: Position { line: 0, character: 20 },
        };
        let actions = provider.get_actions("SELECT * FROM users", range);
        assert!(actions.iter().any(|a| a.title.contains("SELECT *")));
    }

    #[test]
    fn test_code_action_implicit_cross_join() {
        let provider = CodeActionProvider::new();
        let range = Range {
            start: Position { line: 0, character: 0 },
            end: Position { line: 0, character: 40 },
        };
        let actions = provider.get_actions("SELECT id FROM orders, products", range);
        assert!(actions.iter().any(|a| a.kind == NextGenCodeActionKind::OptimizationHint));
    }

    #[test]
    fn test_code_action_missing_aliases() {
        let provider = CodeActionProvider::new();
        let range = Range {
            start: Position { line: 0, character: 0 },
            end: Position { line: 0, character: 50 },
        };
        let actions = provider.get_actions(
            "SELECT id FROM orders, products WHERE orders.id = 1",
            range,
        );
        assert!(actions.iter().any(|a| a.title.contains("aliases")));
    }

    #[test]
    fn test_code_action_clean_query() {
        let provider = CodeActionProvider::new();
        let range = Range {
            start: Position { line: 0, character: 0 },
            end: Position { line: 0, character: 30 },
        };
        let actions = provider.get_actions("SELECT id FROM users WHERE id = 1", range);
        assert!(actions.is_empty());
    }

    // -----------------------------------------------------------------------
    // Performance Analyzer tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_perf_full_table_scan() {
        let analyzer = PerformanceAnalyzer::new();
        let hints = analyzer.analyze("SELECT id FROM users");
        assert!(hints.iter().any(|h| h.severity == HintSeverity::Warning
            && h.message.contains("full table scan")));
    }

    #[test]
    fn test_perf_cartesian_product() {
        let analyzer = PerformanceAnalyzer::new();
        let hints = analyzer.analyze("SELECT a.id FROM orders, products");
        assert!(hints.iter().any(|h| h.severity == HintSeverity::Critical
            && h.message.contains("Cartesian")));
    }

    #[test]
    fn test_perf_delete_without_where() {
        let analyzer = PerformanceAnalyzer::new();
        let hints = analyzer.analyze("DELETE FROM users");
        assert!(hints.iter().any(|h| h.severity == HintSeverity::Critical
            && h.message.contains("DELETE without WHERE")));
    }

    #[test]
    fn test_perf_update_without_where() {
        let analyzer = PerformanceAnalyzer::new();
        let hints = analyzer.analyze("UPDATE users SET name = 'x'");
        assert!(hints.iter().any(|h| h.severity == HintSeverity::Critical
            && h.message.contains("UPDATE without WHERE")));
    }

    #[test]
    fn test_perf_leading_wildcard() {
        let analyzer = PerformanceAnalyzer::new();
        let hints = analyzer.analyze("SELECT id FROM users WHERE name LIKE '%abc'");
        assert!(hints.iter().any(|h| h.message.contains("leading wildcard")));
    }

    #[test]
    fn test_perf_or_to_in() {
        let analyzer = PerformanceAnalyzer::new();
        let hints = analyzer.analyze(
            "SELECT id FROM users WHERE STATUS = 1 OR STATUS = 2 OR STATUS = 3",
        );
        assert!(hints.iter().any(|h| h.severity == HintSeverity::Info
            && h.message.contains("OR")));
    }

    // -----------------------------------------------------------------------
    // Query Explain Provider tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_explain_simple_select() {
        let provider = QueryExplainProvider::new();
        let output = provider.explain("SELECT id FROM users").unwrap();
        assert!(output.plan_text.contains("Scan"));
        assert!(output.estimated_rows > 0);
        assert!(output.estimated_cost > 0.0);
    }

    #[test]
    fn test_explain_with_join() {
        let provider = QueryExplainProvider::new();
        let output = provider
            .explain("SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id")
            .unwrap();
        assert!(output.plan_text.contains("HashJoin"));
        assert!(output.estimated_cost > 1000.0);
    }

    #[test]
    fn test_explain_complex_query_warnings() {
        let provider = QueryExplainProvider::new();
        let output = provider.explain(
            "SELECT a.id FROM t1 a JOIN t2 b ON a.id = b.id \
             JOIN t3 c ON b.id = c.id JOIN t4 d ON c.id = d.id",
        ).unwrap();
        assert!(output.warnings.iter().any(|w| w.contains("JOINs")));
    }

    #[test]
    fn test_explain_invalid_sql() {
        let provider = QueryExplainProvider::new();
        assert!(provider.explain("SELECTT broken").is_err());
    }
}
