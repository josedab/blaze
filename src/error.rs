//! Error types for the Blaze query engine.
#![deny(clippy::unwrap_used)]
//!
//! This module provides a comprehensive error type hierarchy for all
//! operations in the query engine, from SQL parsing to execution.

use std::fmt;
use strsim::jaro_winkler;
use thiserror::Error;

/// The primary error type for Blaze operations.
#[derive(Error, Debug)]
pub enum BlazeError {
    /// SQL parsing error
    #[error("SQL parse error: {message}")]
    Parse {
        message: String,
        location: Option<Location>,
    },

    /// SQL binding/analysis error
    #[error("Analysis error: {message}")]
    Analysis { message: String },

    /// Query planning error
    #[error("Planning error: {message}")]
    Plan { message: String },

    /// Query optimization error
    #[error("Optimization error: {message}")]
    Optimization { message: String },

    /// Query execution error
    #[error("Execution error: {message}")]
    Execution { message: String },

    /// Type error (type mismatch, unsupported type, etc.)
    #[error("Type error: {message}")]
    Type { message: String },

    /// Schema error (column not found, table not found, etc.)
    #[error("Schema error: {message}")]
    Schema { message: String },

    /// Catalog error (database not found, etc.)
    #[error("Catalog error: {message}")]
    Catalog { message: String },

    /// I/O error
    #[error("I/O error: {source}")]
    Io {
        #[from]
        source: std::io::Error,
    },

    /// Arrow error
    #[error("Arrow error: {message}")]
    Arrow { message: String },

    /// Parquet error
    #[error("Parquet error: {message}")]
    Parquet { message: String },

    /// Memory allocation/management error
    #[error("Memory error: {message}")]
    Memory { message: String },

    /// Resource limit exceeded
    #[error("Resource limit exceeded: {message}")]
    ResourceExhausted { message: String },

    /// Operation cancelled
    #[error("Operation cancelled")]
    Cancelled,

    /// Internal error (bug in the engine)
    #[error("Internal error: {message}")]
    Internal { message: String },

    /// Not implemented
    #[error("Not implemented: {feature}")]
    NotImplemented { feature: String },

    /// Invalid argument
    #[error("Invalid argument: {message}")]
    InvalidArgument { message: String },
}

/// Location in SQL source text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    /// Line number (1-indexed)
    pub line: usize,
    /// Column number (1-indexed)
    pub column: usize,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}, column {}", self.line, self.column)
    }
}

impl BlazeError {
    /// Create a parse error with location information.
    pub fn parse(message: impl Into<String>) -> Self {
        Self::Parse {
            message: message.into(),
            location: None,
        }
    }

    /// Create a parse error with location.
    pub fn parse_at(message: impl Into<String>, line: usize, column: usize) -> Self {
        Self::Parse {
            message: message.into(),
            location: Some(Location { line, column }),
        }
    }

    /// Create an analysis error.
    pub fn analysis(message: impl Into<String>) -> Self {
        Self::Analysis {
            message: message.into(),
        }
    }

    /// Create a planning error.
    pub fn plan(message: impl Into<String>) -> Self {
        Self::Plan {
            message: message.into(),
        }
    }

    /// Create an optimization error.
    pub fn optimization(message: impl Into<String>) -> Self {
        Self::Optimization {
            message: message.into(),
        }
    }

    /// Create an execution error.
    pub fn execution(message: impl Into<String>) -> Self {
        Self::Execution {
            message: message.into(),
        }
    }

    /// Create a type error.
    pub fn type_error(message: impl Into<String>) -> Self {
        Self::Type {
            message: message.into(),
        }
    }

    /// Create a schema error.
    pub fn schema(message: impl Into<String>) -> Self {
        Self::Schema {
            message: message.into(),
        }
    }

    /// Create a catalog error.
    pub fn catalog(message: impl Into<String>) -> Self {
        Self::Catalog {
            message: message.into(),
        }
    }

    /// Create an internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
        }
    }

    /// Create a not implemented error.
    pub fn not_implemented(feature: impl Into<String>) -> Self {
        Self::NotImplemented {
            feature: feature.into(),
        }
    }

    /// Create an invalid argument error.
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument {
            message: message.into(),
        }
    }

    /// Create a memory error.
    pub fn memory(message: impl Into<String>) -> Self {
        Self::Memory {
            message: message.into(),
        }
    }

    /// Create a resource exhausted error.
    pub fn resource_exhausted(message: impl Into<String>) -> Self {
        Self::ResourceExhausted {
            message: message.into(),
        }
    }

    /// Create a schema error with suggestions for similar names.
    ///
    /// This is useful when a column or table is not found, to suggest
    /// similar names that the user might have meant.
    pub fn schema_with_suggestions(
        not_found: &str,
        available: &[String],
        entity_type: &str,
    ) -> Self {
        let suggestions = find_similar_names(not_found, available, 3);
        let mut message = format!("{} '{}' not found", entity_type, not_found);

        if !suggestions.is_empty() {
            message.push_str(". Did you mean: ");
            message.push_str(&suggestions.join(", "));
            message.push('?');
        }

        if !available.is_empty() && available.len() <= 10 {
            message.push_str(&format!(" Available: {}", available.join(", ")));
        }

        Self::Schema { message }
    }

    /// Create a catalog error with suggestions for similar table names.
    pub fn catalog_with_suggestions(not_found: &str, available: &[String]) -> Self {
        let suggestions = find_similar_names(not_found, available, 3);
        let mut message = format!("Table '{}' not found", not_found);

        if !suggestions.is_empty() {
            message.push_str(". Did you mean: ");
            message.push_str(&suggestions.join(", "));
            message.push('?');
        }

        Self::Catalog { message }
    }

    /// Create an analysis error with suggestions for similar function names.
    pub fn function_not_found(not_found: &str, available: &[String]) -> Self {
        let suggestions = find_similar_names(not_found, available, 3);
        let mut message = format!("Function '{}' not found", not_found);

        if !suggestions.is_empty() {
            message.push_str(". Did you mean: ");
            message.push_str(&suggestions.join(", "));
            message.push('?');
        }

        Self::Analysis { message }
    }
}

/// Find similar names using Jaro-Winkler distance.
///
/// Returns up to `max_suggestions` names that are similar to `target`,
/// sorted by similarity (most similar first).
pub fn find_similar_names(
    target: &str,
    candidates: &[String],
    max_suggestions: usize,
) -> Vec<String> {
    const MIN_SIMILARITY: f64 = 0.7; // Threshold for considering a match

    let target_lower = target.to_lowercase();

    let mut scored: Vec<(f64, &String)> = candidates
        .iter()
        .map(|c| {
            let similarity = jaro_winkler(&target_lower, &c.to_lowercase());
            (similarity, c)
        })
        .filter(|(score, _)| *score >= MIN_SIMILARITY)
        .collect();

    // Sort by similarity (highest first)
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    scored
        .into_iter()
        .take(max_suggestions)
        .map(|(_, name)| name.clone())
        .collect()
}

impl From<arrow::error::ArrowError> for BlazeError {
    fn from(err: arrow::error::ArrowError) -> Self {
        Self::Arrow {
            message: err.to_string(),
        }
    }
}

impl From<parquet::errors::ParquetError> for BlazeError {
    fn from(err: parquet::errors::ParquetError) -> Self {
        Self::Parquet {
            message: err.to_string(),
        }
    }
}

impl From<sqlparser::parser::ParserError> for BlazeError {
    fn from(err: sqlparser::parser::ParserError) -> Self {
        Self::Parse {
            message: err.to_string(),
            location: None,
        }
    }
}

/// Result type alias for Blaze operations.
pub type Result<T> = std::result::Result<T, BlazeError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = BlazeError::parse("unexpected token");
        assert_eq!(err.to_string(), "SQL parse error: unexpected token");
    }

    #[test]
    fn test_error_with_location() {
        let err = BlazeError::parse_at("unexpected token", 10, 5);
        if let BlazeError::Parse { location, .. } = &err {
            assert_eq!(location.as_ref().unwrap().to_string(), "line 10, column 5");
        }
    }

    #[test]
    fn test_find_similar_names() {
        let candidates = vec![
            "user_id".to_string(),
            "user_name".to_string(),
            "email".to_string(),
            "created_at".to_string(),
        ];

        // Should find "user_id" when searching for "user_d"
        let suggestions = find_similar_names("user_d", &candidates, 3);
        assert!(!suggestions.is_empty());
        assert!(suggestions.contains(&"user_id".to_string()));

        // Should find "email" when searching for "emal"
        let suggestions = find_similar_names("emal", &candidates, 3);
        assert!(!suggestions.is_empty());
        assert!(suggestions.contains(&"email".to_string()));

        // Should not find anything for completely different name
        let suggestions = find_similar_names("xyz123", &candidates, 3);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_schema_error_with_suggestions() {
        let available = vec!["id".to_string(), "name".to_string(), "email".to_string()];

        let err = BlazeError::schema_with_suggestions("nam", &available, "Column");
        let msg = err.to_string();

        assert!(msg.contains("Column 'nam' not found"));
        assert!(msg.contains("Did you mean"));
        assert!(msg.contains("name"));
    }

    // --- Additional error coverage tests ---

    #[test]
    fn test_error_resource_exhausted() {
        let err = BlazeError::resource_exhausted("Out of memory");
        let msg = err.to_string();
        assert!(msg.contains("Resource limit exceeded"));
        assert!(msg.contains("Out of memory"));
    }

    #[test]
    fn test_error_cancelled() {
        let err = BlazeError::Cancelled;
        assert_eq!(err.to_string(), "Operation cancelled");
    }

    #[test]
    fn test_error_memory() {
        let err = BlazeError::memory("allocation failed");
        let msg = err.to_string();
        assert!(msg.contains("Memory error"));
        assert!(msg.contains("allocation failed"));
    }

    #[test]
    fn test_error_not_implemented() {
        let err = BlazeError::not_implemented("Feature X");
        let msg = err.to_string();
        assert!(msg.contains("Not implemented"));
        assert!(msg.contains("Feature X"));
    }

    #[test]
    fn test_error_invalid_argument() {
        let err = BlazeError::invalid_argument("bad value");
        let msg = err.to_string();
        assert!(msg.contains("Invalid argument"));
    }

    #[test]
    fn test_error_type_error() {
        let err = BlazeError::type_error("cannot cast");
        let msg = err.to_string();
        assert!(msg.contains("Type error"));
    }

    #[test]
    fn test_error_plan() {
        let err = BlazeError::plan("bad plan");
        assert!(err.to_string().contains("Planning error"));
    }

    #[test]
    fn test_error_optimization() {
        let err = BlazeError::optimization("loop detected");
        assert!(err.to_string().contains("Optimization error"));
    }

    #[test]
    fn test_error_execution() {
        let err = BlazeError::execution("runtime failure");
        assert!(err.to_string().contains("Execution error"));
    }

    #[test]
    fn test_error_internal() {
        let err = BlazeError::internal("bug");
        assert!(err.to_string().contains("Internal error"));
    }

    #[test]
    fn test_error_catalog() {
        let err = BlazeError::catalog("db not found");
        assert!(err.to_string().contains("Catalog error"));
    }

    #[test]
    fn test_error_schema() {
        let err = BlazeError::schema("column missing");
        assert!(err.to_string().contains("Schema error"));
    }

    // --- Arrow/Parquet error conversions ---

    #[test]
    fn test_arrow_error_conversion() {
        let arrow_err =
            arrow::error::ArrowError::InvalidArgumentError("bad arg".to_string());
        let blaze_err: BlazeError = arrow_err.into();
        let msg = blaze_err.to_string();
        assert!(msg.contains("Arrow error"));
        assert!(msg.contains("bad arg"));
    }

    #[test]
    fn test_parquet_error_conversion() {
        let parquet_err = parquet::errors::ParquetError::General("bad file".to_string());
        let blaze_err: BlazeError = parquet_err.into();
        let msg = blaze_err.to_string();
        assert!(msg.contains("Parquet error"));
        assert!(msg.contains("bad file"));
    }

    #[test]
    fn test_io_error_conversion() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
        let blaze_err: BlazeError = io_err.into();
        assert!(blaze_err.to_string().contains("I/O error"));
    }

    #[test]
    fn test_sqlparser_error_conversion() {
        let parser_err =
            sqlparser::parser::ParserError::ParserError("syntax error".to_string());
        let blaze_err: BlazeError = parser_err.into();
        assert!(blaze_err.to_string().contains("SQL parse error"));
    }

    // --- find_similar_names edge cases ---

    #[test]
    fn test_find_similar_names_empty_candidates() {
        let suggestions = find_similar_names("abc", &[], 3);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn test_find_similar_names_exact_match() {
        let candidates = vec!["user_id".to_string()];
        let suggestions = find_similar_names("user_id", &candidates, 3);
        assert!(!suggestions.is_empty());
        assert!(suggestions.contains(&"user_id".to_string()));
    }

    #[test]
    fn test_find_similar_names_single_char() {
        let candidates = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let suggestions = find_similar_names("a", &candidates, 3);
        // Single-char input may or may not find matches depending on threshold
        // Just verify it doesn't panic
        assert!(suggestions.len() <= 3);
    }

    #[test]
    fn test_find_similar_names_unicode() {
        let candidates = vec!["ñame".to_string(), "naïve".to_string()];
        let suggestions = find_similar_names("name", &candidates, 3);
        // Unicode names should not cause panics
        assert!(suggestions.len() <= 3);
    }

    #[test]
    fn test_find_similar_names_max_suggestions() {
        let candidates: Vec<String> = (0..20).map(|i| format!("col_{}", i)).collect();
        let suggestions = find_similar_names("col_", &candidates, 2);
        assert!(suggestions.len() <= 2);
    }

    // --- catalog_with_suggestions ---

    #[test]
    fn test_catalog_with_suggestions() {
        let available = vec!["users".to_string(), "orders".to_string()];
        let err = BlazeError::catalog_with_suggestions("user", &available);
        let msg = err.to_string();
        assert!(msg.contains("Table 'user' not found"));
        assert!(msg.contains("Did you mean"));
        assert!(msg.contains("users"));
    }

    #[test]
    fn test_catalog_with_suggestions_no_match() {
        let available = vec!["users".to_string()];
        let err = BlazeError::catalog_with_suggestions("xyzzy", &available);
        let msg = err.to_string();
        assert!(msg.contains("Table 'xyzzy' not found"));
        // No similar names should be suggested
        assert!(!msg.contains("Did you mean"));
    }

    // --- function_not_found ---

    #[test]
    fn test_function_not_found() {
        let available = vec![
            "COUNT".to_string(),
            "SUM".to_string(),
            "AVG".to_string(),
        ];
        let err = BlazeError::function_not_found("CONT", &available);
        let msg = err.to_string();
        assert!(msg.contains("Function 'CONT' not found"));
        assert!(msg.contains("Did you mean"));
        assert!(msg.contains("COUNT"));
    }

    #[test]
    fn test_function_not_found_no_candidates() {
        let err = BlazeError::function_not_found("MYFUNC", &[]);
        let msg = err.to_string();
        assert!(msg.contains("Function 'MYFUNC' not found"));
        assert!(!msg.contains("Did you mean"));
    }

    // --- schema_with_suggestions edge cases ---

    #[test]
    fn test_schema_with_suggestions_many_available() {
        let available: Vec<String> = (0..20).map(|i| format!("field_{}", i)).collect();
        let err = BlazeError::schema_with_suggestions("field_", &available, "Column");
        let msg = err.to_string();
        assert!(msg.contains("Column 'field_' not found"));
        // More than 10 available, so "Available:" list should not be shown
        assert!(!msg.contains("Available:"));
    }

    #[test]
    fn test_schema_with_suggestions_few_available() {
        let available = vec!["id".to_string(), "name".to_string()];
        let err = BlazeError::schema_with_suggestions("idd", &available, "Column");
        let msg = err.to_string();
        assert!(msg.contains("Available:"));
    }

    #[test]
    fn test_location_display() {
        let loc = Location { line: 1, column: 5 };
        assert_eq!(format!("{}", loc), "line 1, column 5");
    }

    #[test]
    fn test_parse_at_stores_location() {
        let err = BlazeError::parse_at("unexpected", 3, 7);
        if let BlazeError::Parse { location, .. } = &err {
            let loc = location.as_ref().unwrap();
            assert_eq!(loc.line, 3);
            assert_eq!(loc.column, 7);
        } else {
            panic!("Expected Parse error");
        }
    }

    #[test]
    fn test_error_debug() {
        let err = BlazeError::execution("test");
        let debug = format!("{:?}", err);
        assert!(debug.contains("Execution"));
    }
}
