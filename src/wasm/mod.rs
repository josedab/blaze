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
pub mod http_reader;
pub mod indexeddb;
mod memory;
mod serialization;
pub mod vfs;
pub mod worker;

pub use bindings::{WasmConnection, WasmError, WasmQueryResult};
pub use memory::{WasmBuffer, WasmMemoryManager};
pub use serialization::{ArrowIpcSerializer, JsonSerializer};
pub use worker::{QueryTaskId, QueryTaskStatus, WasmOptConfig, WorkerQueryManager};

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
            max_memory: 256 * 1024 * 1024,     // 256 MB
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

/// Build profile for WASM optimization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WasmBuildProfile {
    /// Full build with all features (~2-4MB)
    Full,
    /// Core-only build: parser + executor + memory storage (~500KB target)
    Core,
    /// Minimal build: parser + basic execution (~300KB target)
    Minimal,
    /// Custom build with selected features
    Custom { features: Vec<String> },
}

impl WasmBuildProfile {
    /// Estimated binary size in bytes for this build profile.
    pub fn estimated_size_bytes(&self) -> usize {
        match self {
            WasmBuildProfile::Full => 3 * 1024 * 1024, // ~3MB
            WasmBuildProfile::Core => 500 * 1024,      // ~500KB
            WasmBuildProfile::Minimal => 300 * 1024,   // ~300KB
            WasmBuildProfile::Custom { features } => {
                // Base size + per-feature estimate
                200 * 1024 + features.len() * 50 * 1024
            }
        }
    }

    /// List of features included in this build profile.
    pub fn included_features(&self) -> Vec<&'static str> {
        match self {
            WasmBuildProfile::Full => vec![
                "sql_parser",
                "executor",
                "memory_storage",
                "json_output",
                "csv_support",
                "parquet_support",
                "window_functions",
                "aggregations",
                "joins",
                "cte_support",
            ],
            WasmBuildProfile::Core => {
                vec!["sql_parser", "executor", "memory_storage", "json_output"]
            }
            WasmBuildProfile::Minimal => vec!["sql_parser", "executor"],
            WasmBuildProfile::Custom { .. } => vec![],
        }
    }
}

/// Tracks WASM binary size and warns about regressions.
#[derive(Debug, Clone)]
pub struct WasmSizeTracker {
    /// Uncompressed binary size in bytes.
    pub uncompressed_bytes: usize,
    /// Gzip-compressed size in bytes.
    pub gzip_bytes: usize,
    /// Brotli-compressed size in bytes.
    pub brotli_bytes: usize,
    /// Size budget in bytes.
    pub budget_bytes: usize,
}

impl WasmSizeTracker {
    /// Create a new size tracker with the given budget.
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            uncompressed_bytes: 0,
            gzip_bytes: 0,
            brotli_bytes: 0,
            budget_bytes,
        }
    }

    /// Record observed binary sizes.
    pub fn record_size(&mut self, uncompressed: usize, gzip: usize, brotli: usize) {
        self.uncompressed_bytes = uncompressed;
        self.gzip_bytes = gzip;
        self.brotli_bytes = brotli;
    }

    /// Check if the uncompressed size is within budget.
    pub fn is_within_budget(&self) -> bool {
        self.uncompressed_bytes <= self.budget_bytes
    }

    /// Percentage of budget used (based on uncompressed size).
    pub fn budget_utilization(&self) -> f64 {
        if self.budget_bytes == 0 {
            return 0.0;
        }
        (self.uncompressed_bytes as f64 / self.budget_bytes as f64) * 100.0
    }

    /// Human-readable size report.
    pub fn format_report(&self) -> String {
        format!(
            "WASM Size Report:\n  Uncompressed: {:.1} KB\n  Gzip: {:.1} KB\n  Brotli: {:.1} KB\n  Budget: {:.1} KB ({:.1}% used){}",
            self.uncompressed_bytes as f64 / 1024.0,
            self.gzip_bytes as f64 / 1024.0,
            self.brotli_bytes as f64 / 1024.0,
            self.budget_bytes as f64 / 1024.0,
            self.budget_utilization(),
            if self.is_within_budget() { "" } else { "\n  WARNING: Over budget!" },
        )
    }
}

/// Feature gate for WASM builds to control included functionality.
#[derive(Debug, Clone, Default)]
pub struct WasmFeatureGate {
    enabled_features: std::collections::HashSet<String>,
}

impl WasmFeatureGate {
    /// Create a new empty feature gate.
    pub fn new() -> Self {
        Self {
            enabled_features: std::collections::HashSet::new(),
        }
    }

    /// Create a feature gate with core SQL features only.
    pub fn core() -> Self {
        let mut gate = Self::new();
        for feature in &["sql_parser", "executor", "memory_storage", "json_output"] {
            gate.enable(*feature);
        }
        gate
    }

    /// Create a feature gate with all features enabled.
    pub fn full() -> Self {
        let mut gate = Self::core();
        for feature in &[
            "csv_support",
            "parquet_support",
            "window_functions",
            "aggregations",
            "joins",
            "cte_support",
        ] {
            gate.enable(*feature);
        }
        gate
    }

    /// Enable a feature.
    pub fn enable(&mut self, feature: impl Into<String>) {
        self.enabled_features.insert(feature.into());
    }

    /// Disable a feature.
    pub fn disable(&mut self, feature: &str) {
        self.enabled_features.remove(feature);
    }

    /// Check if a feature is enabled.
    pub fn is_enabled(&self, feature: &str) -> bool {
        self.enabled_features.contains(feature)
    }

    /// Number of enabled features.
    pub fn enabled_count(&self) -> usize {
        self.enabled_features.len()
    }

    /// Estimated size impact in bytes based on enabled features.
    pub fn estimated_size_impact(&self) -> usize {
        // Base size + per-feature overhead
        200 * 1024 + self.enabled_features.len() * 50 * 1024
    }
}

/// Lazy loader for WASM extension modules.
#[derive(Debug, Default)]
pub struct WasmLazyLoader {
    loaded_modules: std::collections::HashSet<String>,
    load_times_ms: std::collections::HashMap<String, u64>,
}

impl WasmLazyLoader {
    /// Create a new lazy loader.
    pub fn new() -> Self {
        Self {
            loaded_modules: std::collections::HashSet::new(),
            load_times_ms: std::collections::HashMap::new(),
        }
    }

    /// Record loading of a module with its load time.
    pub fn load_module(&mut self, name: impl Into<String>, load_time_ms: u64) {
        let name = name.into();
        self.loaded_modules.insert(name.clone());
        self.load_times_ms.insert(name, load_time_ms);
    }

    /// Check if a module is loaded.
    pub fn is_loaded(&self, name: &str) -> bool {
        self.loaded_modules.contains(name)
    }

    /// List all loaded modules (sorted for deterministic output).
    pub fn loaded_modules(&self) -> Vec<String> {
        let mut modules: Vec<String> = self.loaded_modules.iter().cloned().collect();
        modules.sort();
        modules
    }

    /// Total load time across all modules.
    pub fn total_load_time_ms(&self) -> u64 {
        self.load_times_ms.values().sum()
    }

    /// Get load time for a specific module.
    pub fn module_load_time(&self, name: &str) -> Option<u64> {
        self.load_times_ms.get(name).copied()
    }
}

/// Startup performance metrics for WASM deployment.
#[derive(Debug, Clone, Default)]
pub struct WasmStartupMetrics {
    /// Time to instantiate the WASM module in milliseconds.
    pub instantiation_ms: u64,
    /// Time to compile the WASM module in milliseconds.
    pub compilation_ms: u64,
    /// Time to initialize the engine in milliseconds.
    pub initialization_ms: u64,
    /// Time to execute the first query in milliseconds.
    pub first_query_ms: u64,
}

impl WasmStartupMetrics {
    /// Total startup time in milliseconds.
    pub fn total_startup_ms(&self) -> u64 {
        self.instantiation_ms + self.compilation_ms + self.initialization_ms + self.first_query_ms
    }

    /// Check if total startup time is within the target.
    pub fn is_within_target(&self, target_ms: u64) -> bool {
        self.total_startup_ms() <= target_ms
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Custom Panic Handler + Tree-Shaking Analysis
// ---------------------------------------------------------------------------

/// Custom WASM panic handler that formats errors for browser console.
pub struct WasmPanicHandler;

impl WasmPanicHandler {
    /// Install the custom panic handler (noop in non-WASM builds).
    pub fn install() {
        // In actual WASM build, this would call:
        // std::panic::set_hook(Box::new(|info| { ... }));
    }

    /// Format a panic message for WASM environments.
    pub fn format_panic(info: &str) -> String {
        format!("[Blaze WASM Error] {}", info)
    }

    /// Format a panic with location information.
    pub fn format_panic_with_location(info: &str, file: &str, line: u32) -> String {
        format!("[Blaze WASM Error] {} (at {}:{})", info, file, line)
    }
}

/// Tree-shaking analyzer that identifies unused code paths in WASM builds.
#[derive(Debug)]
pub struct TreeShakingAnalyzer {
    used_features: Vec<String>,
    all_features: Vec<WasmFeatureInfo>,
}

/// Information about a shakeable feature.
#[derive(Debug, Clone)]
pub struct WasmFeatureInfo {
    pub name: String,
    pub estimated_size_bytes: usize,
    pub dependencies: Vec<String>,
    pub is_required: bool,
}

impl TreeShakingAnalyzer {
    pub fn new() -> Self {
        let all_features = vec![
            WasmFeatureInfo {
                name: "sql_parser".into(),
                estimated_size_bytes: 50_000,
                dependencies: vec![],
                is_required: true,
            },
            WasmFeatureInfo {
                name: "query_planner".into(),
                estimated_size_bytes: 30_000,
                dependencies: vec!["sql_parser".into()],
                is_required: true,
            },
            WasmFeatureInfo {
                name: "executor".into(),
                estimated_size_bytes: 40_000,
                dependencies: vec!["query_planner".into()],
                is_required: true,
            },
            WasmFeatureInfo {
                name: "csv_reader".into(),
                estimated_size_bytes: 15_000,
                dependencies: vec![],
                is_required: false,
            },
            WasmFeatureInfo {
                name: "json_output".into(),
                estimated_size_bytes: 10_000,
                dependencies: vec![],
                is_required: false,
            },
            WasmFeatureInfo {
                name: "arrow_ipc".into(),
                estimated_size_bytes: 25_000,
                dependencies: vec![],
                is_required: false,
            },
            WasmFeatureInfo {
                name: "window_functions".into(),
                estimated_size_bytes: 20_000,
                dependencies: vec!["executor".into()],
                is_required: false,
            },
        ];
        Self {
            used_features: Vec::new(),
            all_features,
        }
    }

    /// Mark a feature as used.
    pub fn mark_used(&mut self, feature: &str) {
        if !self.used_features.contains(&feature.to_string()) {
            self.used_features.push(feature.to_string());
            // Also mark dependencies
            let deps: Vec<String> = self
                .all_features
                .iter()
                .filter(|f| f.name == feature)
                .flat_map(|f| f.dependencies.clone())
                .collect();
            for dep in deps {
                self.mark_used(&dep);
            }
        }
    }

    /// Get features that can be removed (not used and not required).
    pub fn removable_features(&self) -> Vec<&WasmFeatureInfo> {
        self.all_features
            .iter()
            .filter(|f| !f.is_required && !self.used_features.contains(&f.name))
            .collect()
    }

    /// Estimated size savings from tree-shaking.
    pub fn estimated_savings(&self) -> usize {
        self.removable_features()
            .iter()
            .map(|f| f.estimated_size_bytes)
            .sum()
    }

    /// Total estimated size after tree-shaking.
    pub fn estimated_size(&self) -> usize {
        let total: usize = self
            .all_features
            .iter()
            .map(|f| f.estimated_size_bytes)
            .sum();
        total - self.estimated_savings()
    }
}

// ---------------------------------------------------------------------------
// Phase 3: Code Splitting + Size Regression
// ---------------------------------------------------------------------------

/// WASM code splitter that divides the module into loadable chunks.
#[derive(Debug)]
pub struct WasmCodeSplitter {
    chunks: Vec<WasmChunk>,
}

/// A loadable code chunk.
#[derive(Debug, Clone)]
pub struct WasmChunk {
    pub name: String,
    pub features: Vec<String>,
    pub estimated_size_bytes: usize,
    pub is_core: bool,
}

impl WasmCodeSplitter {
    pub fn new() -> Self {
        Self {
            chunks: vec![
                WasmChunk {
                    name: "core".into(),
                    features: vec![
                        "sql_parser".into(),
                        "query_planner".into(),
                        "executor".into(),
                    ],
                    estimated_size_bytes: 120_000,
                    is_core: true,
                },
                WasmChunk {
                    name: "io".into(),
                    features: vec![
                        "csv_reader".into(),
                        "json_output".into(),
                        "arrow_ipc".into(),
                    ],
                    estimated_size_bytes: 50_000,
                    is_core: false,
                },
                WasmChunk {
                    name: "advanced".into(),
                    features: vec!["window_functions".into()],
                    estimated_size_bytes: 20_000,
                    is_core: false,
                },
            ],
        }
    }

    /// Get chunks needed for a set of features.
    pub fn required_chunks(&self, features: &[String]) -> Vec<&WasmChunk> {
        self.chunks
            .iter()
            .filter(|chunk| chunk.is_core || chunk.features.iter().any(|f| features.contains(f)))
            .collect()
    }

    /// Total size of required chunks.
    pub fn total_size(&self, features: &[String]) -> usize {
        self.required_chunks(features)
            .iter()
            .map(|c| c.estimated_size_bytes)
            .sum()
    }

    pub fn all_chunks(&self) -> &[WasmChunk] {
        &self.chunks
    }
}

/// CI size regression checker for WASM builds.
#[derive(Debug)]
pub struct WasmSizeRegression {
    budget_bytes: usize,
    previous_size: Option<usize>,
    current_size: usize,
}

impl WasmSizeRegression {
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            budget_bytes,
            previous_size: None,
            current_size: 0,
        }
    }

    /// Set the previous build size for comparison.
    pub fn set_previous_size(&mut self, size: usize) {
        self.previous_size = Some(size);
    }

    /// Set the current build size.
    pub fn set_current_size(&mut self, size: usize) {
        self.current_size = size;
    }

    /// Check if current size is within budget.
    pub fn is_within_budget(&self) -> bool {
        self.current_size <= self.budget_bytes
    }

    /// Size change from previous build (positive = growth).
    pub fn size_delta(&self) -> Option<i64> {
        self.previous_size
            .map(|prev| self.current_size as i64 - prev as i64)
    }

    /// Percentage change from previous build.
    pub fn size_delta_pct(&self) -> Option<f64> {
        self.previous_size.map(|prev| {
            if prev == 0 {
                0.0
            } else {
                (self.current_size as f64 - prev as f64) / prev as f64 * 100.0
            }
        })
    }

    /// Generate a CI report string.
    pub fn report(&self) -> String {
        let mut report = format!("WASM Size Report\n");
        report.push_str(&format!(
            "  Current: {} bytes ({:.1} KB)\n",
            self.current_size,
            self.current_size as f64 / 1024.0
        ));
        report.push_str(&format!(
            "  Budget:  {} bytes ({:.1} KB)\n",
            self.budget_bytes,
            self.budget_bytes as f64 / 1024.0
        ));
        if let Some(delta) = self.size_delta() {
            let pct = self.size_delta_pct().unwrap_or(0.0);
            report.push_str(&format!("  Delta:   {:+} bytes ({:+.1}%)\n", delta, pct));
        }
        report.push_str(&format!(
            "  Status:  {}",
            if self.is_within_budget() {
                "PASS ✅"
            } else {
                "FAIL ❌"
            }
        ));
        report
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

    #[test]
    fn test_wasm_build_profile_sizes() {
        assert_eq!(
            WasmBuildProfile::Full.estimated_size_bytes(),
            3 * 1024 * 1024
        );
        assert_eq!(WasmBuildProfile::Core.estimated_size_bytes(), 500 * 1024);
        assert_eq!(WasmBuildProfile::Minimal.estimated_size_bytes(), 300 * 1024);

        let custom = WasmBuildProfile::Custom {
            features: vec!["a".into(), "b".into()],
        };
        assert_eq!(custom.estimated_size_bytes(), 200 * 1024 + 2 * 50 * 1024);

        assert_eq!(WasmBuildProfile::Full.included_features().len(), 10);
        assert_eq!(WasmBuildProfile::Core.included_features().len(), 4);
        assert_eq!(WasmBuildProfile::Minimal.included_features().len(), 2);
    }

    #[test]
    fn test_wasm_size_tracker_budget() {
        let mut tracker = WasmSizeTracker::new(1_000_000);
        tracker.record_size(800_000, 300_000, 250_000);
        assert!(tracker.is_within_budget());
        assert!((tracker.budget_utilization() - 80.0).abs() < 0.1);

        tracker.record_size(1_200_000, 400_000, 350_000);
        assert!(!tracker.is_within_budget());
        assert!((tracker.budget_utilization() - 120.0).abs() < 0.1);
    }

    #[test]
    fn test_wasm_size_tracker_report() {
        let mut tracker = WasmSizeTracker::new(1_000_000);
        tracker.record_size(500_000, 200_000, 150_000);
        let report = tracker.format_report();
        assert!(report.contains("WASM Size Report:"));
        assert!(report.contains("Uncompressed:"));
        assert!(report.contains("Gzip:"));
        assert!(report.contains("Brotli:"));
        assert!(!report.contains("WARNING"));

        tracker.record_size(1_500_000, 500_000, 400_000);
        let report = tracker.format_report();
        assert!(report.contains("WARNING: Over budget!"));
    }

    #[test]
    fn test_wasm_feature_gate_core() {
        let gate = WasmFeatureGate::core();
        assert_eq!(gate.enabled_count(), 4);
        assert!(gate.is_enabled("sql_parser"));
        assert!(gate.is_enabled("executor"));
        assert!(gate.is_enabled("memory_storage"));
        assert!(gate.is_enabled("json_output"));
        assert!(!gate.is_enabled("parquet_support"));
    }

    #[test]
    fn test_wasm_feature_gate_full() {
        let gate = WasmFeatureGate::full();
        assert_eq!(gate.enabled_count(), 10);
        assert!(gate.is_enabled("sql_parser"));
        assert!(gate.is_enabled("csv_support"));
        assert!(gate.is_enabled("window_functions"));
        assert!(gate.is_enabled("cte_support"));
    }

    #[test]
    fn test_wasm_feature_gate_custom() {
        let mut gate = WasmFeatureGate::new();
        assert_eq!(gate.enabled_count(), 0);

        gate.enable("sql_parser");
        gate.enable("executor");
        assert_eq!(gate.enabled_count(), 2);
        assert!(gate.is_enabled("sql_parser"));

        gate.disable("executor");
        assert_eq!(gate.enabled_count(), 1);
        assert!(!gate.is_enabled("executor"));

        assert!(gate.estimated_size_impact() > 0);
    }

    #[test]
    fn test_wasm_lazy_loader() {
        let mut loader = WasmLazyLoader::new();
        assert!(!loader.is_loaded("csv"));
        assert_eq!(loader.total_load_time_ms(), 0);

        loader.load_module("csv", 15);
        loader.load_module("parquet", 25);
        assert!(loader.is_loaded("csv"));
        assert!(loader.is_loaded("parquet"));
        assert!(!loader.is_loaded("json"));
        assert_eq!(loader.total_load_time_ms(), 40);
        assert_eq!(loader.module_load_time("csv"), Some(15));
        assert_eq!(loader.module_load_time("json"), None);
        assert_eq!(loader.loaded_modules().len(), 2);
    }

    #[test]
    fn test_wasm_startup_metrics() {
        let metrics = WasmStartupMetrics {
            instantiation_ms: 10,
            compilation_ms: 50,
            initialization_ms: 20,
            first_query_ms: 30,
        };
        assert_eq!(metrics.total_startup_ms(), 110);
        assert!(metrics.is_within_target(200));
        assert!(!metrics.is_within_target(50));

        let default_metrics = WasmStartupMetrics::default();
        assert_eq!(default_metrics.total_startup_ms(), 0);
        assert!(default_metrics.is_within_target(0));
    }

    #[test]
    fn test_wasm_panic_handler_format() {
        let msg = WasmPanicHandler::format_panic("test error");
        assert!(msg.contains("Blaze WASM Error"));
        assert!(msg.contains("test error"));
    }

    #[test]
    fn test_wasm_panic_handler_with_location() {
        let msg = WasmPanicHandler::format_panic_with_location("err", "main.rs", 42);
        assert!(msg.contains("main.rs:42"));
    }

    #[test]
    fn test_tree_shaking_analyzer() {
        let mut analyzer = TreeShakingAnalyzer::new();
        analyzer.mark_used("csv_reader");
        let removable = analyzer.removable_features();
        assert!(!removable.iter().any(|f| f.name == "csv_reader"));
        assert!(analyzer.estimated_savings() > 0);
    }

    #[test]
    fn test_tree_shaking_dependencies() {
        let mut analyzer = TreeShakingAnalyzer::new();
        analyzer.mark_used("window_functions");
        // Should also mark executor as used (dependency)
        assert!(analyzer.used_features.contains(&"executor".to_string()));
    }

    #[test]
    fn test_code_splitter_core_always_included() {
        let splitter = WasmCodeSplitter::new();
        let chunks = splitter.required_chunks(&[]);
        assert!(chunks.iter().any(|c| c.name == "core"));
    }

    #[test]
    fn test_code_splitter_feature_chunks() {
        let splitter = WasmCodeSplitter::new();
        let chunks = splitter.required_chunks(&["csv_reader".to_string()]);
        assert!(chunks.iter().any(|c| c.name == "io"));
    }

    #[test]
    fn test_size_regression_within_budget() {
        let mut reg = WasmSizeRegression::new(500_000);
        reg.set_current_size(400_000);
        assert!(reg.is_within_budget());
    }

    #[test]
    fn test_size_regression_over_budget() {
        let mut reg = WasmSizeRegression::new(500_000);
        reg.set_current_size(600_000);
        assert!(!reg.is_within_budget());
    }

    #[test]
    fn test_size_regression_delta() {
        let mut reg = WasmSizeRegression::new(500_000);
        reg.set_previous_size(400_000);
        reg.set_current_size(420_000);
        assert_eq!(reg.size_delta(), Some(20_000));
        assert!((reg.size_delta_pct().unwrap() - 5.0).abs() < 0.1);
    }

    #[test]
    fn test_size_regression_report() {
        let mut reg = WasmSizeRegression::new(500_000);
        reg.set_previous_size(400_000);
        reg.set_current_size(420_000);
        let report = reg.report();
        assert!(report.contains("PASS"));
        assert!(report.contains("420000"));
    }
}
