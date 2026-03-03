//! Shared constants for the Blaze query engine.
//!
//! Centralizes default values and limits to avoid magic numbers across modules.

// ---------------------------------------------------------------------------
// Batch & Row Processing
// ---------------------------------------------------------------------------

/// Default number of rows per processing batch across all operators and storage readers.
pub const DEFAULT_BATCH_SIZE: usize = 8192;

/// Minimum allowed batch size.
pub const MIN_BATCH_SIZE: usize = 1;

/// Maximum allowed batch size (16 million rows).
pub const MAX_BATCH_SIZE: usize = 16_777_216;

/// Target number of rows per partition when partitioning data for parallel execution.
pub const DEFAULT_ROWS_PER_PARTITION: usize = 100_000;

// ---------------------------------------------------------------------------
// Query Limits
// ---------------------------------------------------------------------------

/// Maximum number of rows allowed in a cross-join result to prevent accidental
/// cartesian product explosions.
pub const MAX_CROSS_JOIN_ROWS: usize = 10_000_000;

/// Maximum query nesting depth (CTEs, subqueries) to prevent stack overflow.
pub const MAX_PLAN_DEPTH: usize = 128;

// ---------------------------------------------------------------------------
// Cache Defaults
// ---------------------------------------------------------------------------

/// Default maximum number of entries in the query result cache.
pub const DEFAULT_CACHE_MAX_ENTRIES: usize = 256;

/// Default memory budget for the query result cache (256 MB).
pub const DEFAULT_CACHE_MEMORY_BYTES: usize = 256 * 1024 * 1024;

/// Default time-to-live for cached query results (5 minutes).
pub const DEFAULT_CACHE_TTL_SECS: u64 = 300;

// ---------------------------------------------------------------------------
// Parallel Execution
// ---------------------------------------------------------------------------

/// Default target partition size for parallel execution (64 MB).
pub const DEFAULT_TARGET_PARTITION_SIZE: usize = 64 * 1024 * 1024;

/// Default memory limit per parallel task (256 MB).
pub const DEFAULT_TASK_MEMORY_LIMIT: usize = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Resource Governor
// ---------------------------------------------------------------------------

/// Default per-tenant memory limit (1 GB).
pub const DEFAULT_TENANT_MEMORY_BYTES: usize = 1024 * 1024 * 1024;

/// Default CPU time cap per query (30 seconds).
pub const DEFAULT_CPU_TIME_MS: u64 = 30_000;

/// Default maximum concurrent queries per tenant.
pub const DEFAULT_MAX_CONCURRENT_QUERIES: usize = 16;

/// Default maximum rows in a result set.
pub const DEFAULT_MAX_RESULT_ROWS: usize = 1_000_000;

/// Default query wall-clock timeout (60 seconds).
pub const DEFAULT_QUERY_TIMEOUT_MS: u64 = 60_000;

// ---------------------------------------------------------------------------
// Storage
// ---------------------------------------------------------------------------

/// Default number of rows to sample for CSV schema inference.
pub const DEFAULT_SCHEMA_INFER_ROWS: usize = 1000;

/// Maximum row-group bytes for Parquet budget checks (256 MB).
pub const MAX_ROW_GROUP_BYTES: usize = 256 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Cost Model
// ---------------------------------------------------------------------------

/// Default page size for cost estimation.
pub const DEFAULT_PAGE_SIZE: usize = 8192;

/// Default average tuple width in bytes for cost estimation.
pub const DEFAULT_AVG_TUPLE_WIDTH: usize = 100;

// ---------------------------------------------------------------------------
// Ingestion
// ---------------------------------------------------------------------------

/// Default target row count per compacted batch in the ingestion pipeline.
pub const DEFAULT_COMPACTION_TARGET_ROWS: usize = 8192;

// ---------------------------------------------------------------------------
// Visualization
// ---------------------------------------------------------------------------

/// Default chart width in pixels for SVG chart rendering.
pub const DEFAULT_CHART_WIDTH: usize = 600;
