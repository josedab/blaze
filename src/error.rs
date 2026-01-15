//! Error types for the Blaze query engine.
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
    Parse { message: String, location: Option<Location> },

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
    pub fn catalog_with_suggestions(
        not_found: &str,
        available: &[String],
    ) -> Self {
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
    pub fn function_not_found(
        not_found: &str,
        available: &[String],
    ) -> Self {
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
pub fn find_similar_names(target: &str, candidates: &[String], max_suggestions: usize) -> Vec<String> {
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
        let available = vec![
            "id".to_string(),
            "name".to_string(),
            "email".to_string(),
        ];

        let err = BlazeError::schema_with_suggestions("nam", &available, "Column");
        let msg = err.to_string();

        assert!(msg.contains("Column 'nam' not found"));
        assert!(msg.contains("Did you mean"));
        assert!(msg.contains("name"));
    }
}
