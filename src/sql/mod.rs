//! SQL parsing and analysis for Blaze.
//!
//! This module provides SQL parsing using sqlparser-rs and converts
//! the parsed AST into Blaze's internal representations.

pub mod parser;

pub use parser::{Parser, Statement};
