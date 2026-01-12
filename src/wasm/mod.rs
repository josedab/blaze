//! WebAssembly Target Module
//!
//! This module provides WebAssembly-compatible bindings for the Blaze query engine,
//! enabling use in browsers and other WASM runtimes.
//!
//! # Features
//!
//! - **Browser SQL**: Execute SQL queries directly in the browser
//! - **Memory Tables**: In-memory data storage for WASM environment
//! - **JSON I/O**: JSON-based data import/export for JavaScript interop
//! - **Arrow IPC**: Arrow IPC format support for efficient data transfer
//!
//! # Usage
//!
//! ```javascript
//! // In JavaScript/TypeScript
//! import init, { BlazeWasm } from 'blaze-wasm';
//!
//! await init();
//! const db = new BlazeWasm();
//!
//! db.execute("CREATE TABLE users (id INT, name VARCHAR)");
//! db.loadJson("users", '[{"id": 1, "name": "Alice"}]');
//! const results = db.query("SELECT * FROM users");
//! ```

mod bindings;
mod memory;
mod serialization;

pub use bindings::{WasmConnection, WasmQueryResult, WasmError};
pub use memory::{WasmMemoryManager, WasmBuffer};
pub use serialization::{JsonSerializer, ArrowIpcSerializer};

use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use crate::error::{BlazeError, Result};

/// Configuration for the WASM runtime.
#[derive(Debug, Clone)]
pub struct WasmConfig {
    /// Maximum memory limit (in bytes)
    pub max_memory: usize,
    /// Maximum query result size (in bytes)
    pub max_result_size: usize,
    /// Enable query logging
    pub enable_logging: bool,
    /// Use streaming results for large queries
    pub streaming_threshold: usize,
}

impl Default for WasmConfig {
    fn default() -> Self {
        Self {
            max_memory: 256 * 1024 * 1024, // 256 MB
            max_result_size: 64 * 1024 * 1024, // 64 MB
            enable_logging: false,
            streaming_threshold: 10_000, // 10K rows
        }
    }
}

impl WasmConfig {
    /// Create a new WASM configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum memory limit.
    pub fn with_max_memory(mut self, bytes: usize) -> Self {
        self.max_memory = bytes;
        self
    }

    /// Set maximum result size.
    pub fn with_max_result_size(mut self, bytes: usize) -> Self {
        self.max_result_size = bytes;
        self
    }

    /// Enable or disable logging.
    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.enable_logging = enabled;
        self
    }

    /// Set streaming threshold.
    pub fn with_streaming_threshold(mut self, rows: usize) -> Self {
        self.streaming_threshold = rows;
        self
    }
}

/// Result format for WASM queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultFormat {
    /// JSON array of objects
    Json,
    /// Arrow IPC stream format
    ArrowIpc,
    /// CSV format
    Csv,
    /// Column-oriented JSON (more compact)
    ColumnarJson,
}

impl ResultFormat {
    /// Get the MIME type for this format.
    pub fn mime_type(&self) -> &'static str {
        match self {
            ResultFormat::Json => "application/json",
            ResultFormat::ArrowIpc => "application/vnd.apache.arrow.stream",
            ResultFormat::Csv => "text/csv",
            ResultFormat::ColumnarJson => "application/json",
        }
    }
}

/// Query execution options for WASM.
#[derive(Debug, Clone)]
pub struct WasmQueryOptions {
    /// Output format
    pub format: ResultFormat,
    /// Maximum rows to return
    pub max_rows: Option<usize>,
    /// Include schema information
    pub include_schema: bool,
    /// Pretty print output (for JSON)
    pub pretty: bool,
}

impl Default for WasmQueryOptions {
    fn default() -> Self {
        Self {
            format: ResultFormat::Json,
            max_rows: None,
            include_schema: true,
            pretty: false,
        }
    }
}

impl WasmQueryOptions {
    /// Create new query options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the output format.
    pub fn with_format(mut self, format: ResultFormat) -> Self {
        self.format = format;
        self
    }

    /// Set maximum rows.
    pub fn with_max_rows(mut self, rows: usize) -> Self {
        self.max_rows = Some(rows);
        self
    }

    /// Include or exclude schema.
    pub fn with_schema(mut self, include: bool) -> Self {
        self.include_schema = include;
        self
    }

    /// Enable pretty printing.
    pub fn with_pretty(mut self, pretty: bool) -> Self {
        self.pretty = pretty;
        self
    }
}

/// Statistics about WASM runtime.
#[derive(Debug, Clone, Default)]
pub struct WasmStats {
    /// Number of queries executed
    pub queries_executed: u64,
    /// Total query time in milliseconds
    pub total_query_time_ms: u64,
    /// Current memory usage
    pub memory_used: usize,
    /// Peak memory usage
    pub peak_memory: usize,
    /// Number of tables
    pub table_count: usize,
    /// Total rows across all tables
    pub total_rows: usize,
}

impl WasmStats {
    /// Get average query time.
    pub fn avg_query_time_ms(&self) -> f64 {
        if self.queries_executed == 0 {
            0.0
        } else {
            self.total_query_time_ms as f64 / self.queries_executed as f64
        }
    }

    /// Get memory usage percentage.
    pub fn memory_usage_percent(&self, max_memory: usize) -> f64 {
        if max_memory == 0 {
            0.0
        } else {
            (self.memory_used as f64 / max_memory as f64) * 100.0
        }
    }
}

/// Check if running in WASM environment.
pub fn is_wasm() -> bool {
    cfg!(target_arch = "wasm32")
}

/// Get the WASM target triple.
pub fn wasm_target() -> Option<&'static str> {
    if cfg!(target_arch = "wasm32") {
        if cfg!(target_os = "unknown") {
            Some("wasm32-unknown-unknown")
        } else if cfg!(target_os = "wasi") {
            Some("wasm32-wasi")
        } else {
            Some("wasm32")
        }
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wasm_config_default() {
        let config = WasmConfig::default();
        assert_eq!(config.max_memory, 256 * 1024 * 1024);
        assert!(!config.enable_logging);
    }

    #[test]
    fn test_wasm_config_builder() {
        let config = WasmConfig::new()
            .with_max_memory(128 * 1024 * 1024)
            .with_max_result_size(32 * 1024 * 1024)
            .with_logging(true)
            .with_streaming_threshold(5000);

        assert_eq!(config.max_memory, 128 * 1024 * 1024);
        assert_eq!(config.max_result_size, 32 * 1024 * 1024);
        assert!(config.enable_logging);
        assert_eq!(config.streaming_threshold, 5000);
    }

    #[test]
    fn test_result_format() {
        assert_eq!(ResultFormat::Json.mime_type(), "application/json");
        assert_eq!(
            ResultFormat::ArrowIpc.mime_type(),
            "application/vnd.apache.arrow.stream"
        );
        assert_eq!(ResultFormat::Csv.mime_type(), "text/csv");
    }

    #[test]
    fn test_wasm_query_options() {
        let options = WasmQueryOptions::new()
            .with_format(ResultFormat::Csv)
            .with_max_rows(100)
            .with_schema(false)
            .with_pretty(true);

        assert_eq!(options.format, ResultFormat::Csv);
        assert_eq!(options.max_rows, Some(100));
        assert!(!options.include_schema);
        assert!(options.pretty);
    }

    #[test]
    fn test_wasm_stats() {
        let stats = WasmStats {
            queries_executed: 10,
            total_query_time_ms: 500,
            memory_used: 50 * 1024 * 1024,
            peak_memory: 100 * 1024 * 1024,
            table_count: 5,
            total_rows: 10000,
        };

        assert_eq!(stats.avg_query_time_ms(), 50.0);
        assert!((stats.memory_usage_percent(100 * 1024 * 1024) - 50.0).abs() < 0.1);
    }

    #[test]
    fn test_wasm_target() {
        // In native tests, this should return None
        #[cfg(not(target_arch = "wasm32"))]
        {
            assert!(!is_wasm());
            assert!(wasm_target().is_none());
        }
    }
}
