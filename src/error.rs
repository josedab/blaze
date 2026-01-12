//! Error types for the Blaze query engine.
//!
//! This module provides a comprehensive error type hierarchy for all
//! operations in the query engine, from SQL parsing to execution.

use std::fmt;
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
}
