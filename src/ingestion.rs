//! Real-Time Ingestion Pipeline
//!
//! This module provides a write-ahead log backed streaming ingestion system
//! with micro-batching and auto-compaction for the Blaze query engine.
//!
//! # Features
//!
//! - **Micro-Batching**: Accumulates incoming records into buffered batches
//! - **Auto-Flush**: Configurable triggers for buffer size, byte limit, and time interval
//! - **Compaction**: Merges small flushed batches into larger, optimal-sized segments
//! - **Statistics**: Tracks ingestion throughput, flush counts, and compaction metrics
//!
//! # Example
//!
//! ```rust,no_run
//! use blaze::ingestion::{IngestionPipeline, IngestionConfig};
//! use blaze::types::{Schema, Field, DataType};
//!
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//!     Field::new("value", DataType::Float64, true),
//! ]);
//!
//! let config = IngestionConfig::default();
//! let pipeline = IngestionPipeline::new("my_table", schema, config);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::compute::concat_batches;
use arrow::record_batch::RecordBatch;
use parking_lot::{Mutex, RwLock};

use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// Default target row count per compacted batch.
const DEFAULT_COMPACTION_TARGET_ROWS: usize = 8192;

// ---------------------------------------------------------------------------
// IngestionConfig
// ---------------------------------------------------------------------------

/// Configuration for the ingestion pipeline.
#[derive(Debug, Clone)]
pub struct IngestionConfig {
    /// Maximum rows in the buffer before auto-flush (default: 10,000).
    pub max_buffer_size: usize,
    /// Maximum bytes in the buffer before auto-flush (default: 64 MB).
    pub max_buffer_bytes: usize,
    /// Auto-flush interval in milliseconds (default: 1000).
    pub flush_interval_ms: u64,
    /// Whether to enable write-ahead log for durability (default: true).
    pub enable_wal: bool,
    /// Number of flushes before triggering compaction (default: 10).
    pub compaction_threshold: usize,
}

impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 10_000,
            max_buffer_bytes: 64 * 1024 * 1024, // 64 MB
            flush_interval_ms: 1000,
            enable_wal: true,
            compaction_threshold: 10,
        }
    }
}

// ---------------------------------------------------------------------------
// FlushTrigger / FlushResult
// ---------------------------------------------------------------------------

/// Reason a flush was triggered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushTrigger {
    /// Buffer reached `max_buffer_size`.
    BufferFull,
    /// Buffer reached `max_buffer_bytes`.
    ByteLimit,
    /// `flush_interval_ms` elapsed since last flush.
    TimerExpired,
    /// User explicitly called `flush()`.
    Manual,
    /// Pipeline is shutting down.
    Shutdown,
}

/// Result of a flush operation.
#[derive(Debug, Clone)]
pub struct FlushResult {
    /// Monotonically increasing flush identifier.
    pub flush_id: u64,
    /// Number of rows flushed.
    pub rows_flushed: usize,
    /// Approximate bytes flushed.
    pub bytes_flushed: usize,
    /// Why the flush was triggered.
    pub trigger: FlushTrigger,
    /// Wall-clock duration of the flush.
    pub duration: Duration,
}

// ---------------------------------------------------------------------------
// IngestionStats
// ---------------------------------------------------------------------------

/// Statistics for the ingestion pipeline.
#[derive(Debug, Clone)]
pub struct IngestionStats {
    /// Total rows ingested since pipeline creation.
    pub total_rows_ingested: u64,
    /// Total bytes ingested since pipeline creation.
    pub total_bytes_ingested: u64,
    /// Total number of flushes performed.
    pub total_flushes: u64,
    /// Total number of compactions performed.
    pub total_compactions: u64,
    /// Current rows sitting in the buffer.
    pub buffer_rows: usize,
    /// Current approximate bytes in the buffer.
    pub buffer_bytes: usize,
    /// Timestamp of the most recent flush.
    pub last_flush_time: Option<Instant>,
    /// Average flush duration in microseconds.
    pub avg_flush_duration_us: u64,
}

impl Default for IngestionStats {
    fn default() -> Self {
        Self {
            total_rows_ingested: 0,
            total_bytes_ingested: 0,
            total_flushes: 0,
            total_compactions: 0,
            buffer_rows: 0,
            buffer_bytes: 0,
            last_flush_time: None,
            avg_flush_duration_us: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// MicroBatchBuffer
// ---------------------------------------------------------------------------

/// A micro-batch buffer that accumulates records before flushing.
pub struct MicroBatchBuffer {
    schema: Schema,
    pending: Vec<RecordBatch>,
    row_count: usize,
    byte_count: usize,
    created_at: Instant,
}

impl MicroBatchBuffer {
    fn new(schema: Schema) -> Self {
        Self {
            schema,
            pending: Vec::new(),
            row_count: 0,
            byte_count: 0,
            created_at: Instant::now(),
        }
    }

    /// Approximate byte size of a RecordBatch.
    fn batch_byte_size(batch: &RecordBatch) -> usize {
        batch
            .columns()
            .iter()
            .map(|c| c.get_array_memory_size())
            .sum()
    }

    fn push(&mut self, batch: RecordBatch) {
        let rows = batch.num_rows();
        let bytes = Self::batch_byte_size(&batch);
        self.pending.push(batch);
        self.row_count += rows;
        self.byte_count += bytes;
    }

    fn drain(&mut self) -> (Vec<RecordBatch>, usize, usize) {
        let batches = std::mem::take(&mut self.pending);
        let rows = self.row_count;
        let bytes = self.byte_count;
        self.row_count = 0;
        self.byte_count = 0;
        self.created_at = Instant::now();
        (batches, rows, bytes)
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }
}

// ---------------------------------------------------------------------------
// CompactedSegment / CompactionManager
// ---------------------------------------------------------------------------

/// Segment metadata produced by compaction.
#[derive(Debug, Clone)]
pub struct CompactedSegment {
    /// Unique segment identifier.
    pub segment_id: u64,
    /// Number of rows in this segment.
    pub row_count: usize,
    /// Approximate byte size of this segment.
    pub byte_size: usize,
    /// When the segment was created.
    pub created_at: Instant,
    /// Flush identifiers that contributed to this segment.
    pub source_flush_ids: Vec<u64>,
}

/// Manages compaction of flushed micro-batches into larger segments.
pub struct CompactionManager {
    config: IngestionConfig,
    segments: RwLock<Vec<CompactedSegment>>,
    pending_compaction: AtomicU64,
}

impl CompactionManager {
    /// Create a new compaction manager.
    pub fn new(config: IngestionConfig) -> Self {
        Self {
            config,
            segments: RwLock::new(Vec::new()),
            pending_compaction: AtomicU64::new(0),
        }
    }

    /// Merge a vector of small `RecordBatch`es into fewer, larger batches.
    ///
    /// The target is approximately [`DEFAULT_COMPACTION_TARGET_ROWS`] rows per
    /// output batch. Uses `arrow::compute::concat_batches` under the hood.
    pub fn compact_batches(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        if batches.is_empty() {
            return Ok(Vec::new());
        }

        let arrow_schema = batches[0].schema();
        let mut result = Vec::new();
        let mut group: Vec<RecordBatch> = Vec::new();
        let mut group_rows: usize = 0;

        for batch in batches {
            let rows = batch.num_rows();
            group.push(batch);
            group_rows += rows;

            if group_rows >= DEFAULT_COMPACTION_TARGET_ROWS {
                let merged = concat_batches(&arrow_schema, &group)
                    .map_err(|e| BlazeError::execution(format!("Compaction concat failed: {e}")))?;
                result.push(merged);
                group = Vec::new();
                group_rows = 0;
            }
        }

        // Flush remaining group
        if !group.is_empty() {
            let merged = concat_batches(&arrow_schema, &group)
                .map_err(|e| BlazeError::execution(format!("Compaction concat failed: {e}")))?;
            result.push(merged);
        }

        self.pending_compaction.store(0, Ordering::Release);
        Ok(result)
    }

    /// Check whether the number of pending flushes exceeds the compaction threshold.
    pub fn needs_compaction(&self) -> bool {
        self.pending_compaction.load(Ordering::Acquire) >= self.config.compaction_threshold as u64
    }

    /// Increment the pending compaction counter (called after each flush).
    pub(crate) fn record_flush(&self) {
        self.pending_compaction.fetch_add(1, Ordering::AcqRel);
    }

    /// Return a snapshot of all compacted segments.
    pub fn segments(&self) -> Vec<CompactedSegment> {
        self.segments.read().clone()
    }

    /// Record a new compacted segment.
    pub(crate) fn add_segment(&self, segment: CompactedSegment) {
        self.segments.write().push(segment);
    }
}

// ---------------------------------------------------------------------------
// IngestionPipeline
// ---------------------------------------------------------------------------

/// The ingestion pipeline for a single table.
///
/// Accepts incoming data, buffers it in micro-batches, flushes to an internal
/// store, and supports compaction of small batches into larger segments.
pub struct IngestionPipeline {
    #[allow(dead_code)]
    table_name: String,
    config: IngestionConfig,
    buffer: Mutex<MicroBatchBuffer>,
    flushed_batches: RwLock<Vec<RecordBatch>>,
    stats: RwLock<IngestionStats>,
    sequence: AtomicU64,
    compaction_manager: CompactionManager,
}

impl IngestionPipeline {
    /// Create a new ingestion pipeline for the given table.
    pub fn new(table_name: impl Into<String>, schema: Schema, config: IngestionConfig) -> Self {
        let compaction_manager = CompactionManager::new(config.clone());
        Self {
            table_name: table_name.into(),
            config: config.clone(),
            buffer: Mutex::new(MicroBatchBuffer::new(schema)),
            flushed_batches: RwLock::new(Vec::new()),
            stats: RwLock::new(IngestionStats::default()),
            sequence: AtomicU64::new(0),
            compaction_manager,
        }
    }

    /// Ingest a `RecordBatch` into the buffer.
    pub fn ingest(&self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let rows = batch.num_rows() as u64;
        let bytes = MicroBatchBuffer::batch_byte_size(&batch) as u64;

        {
            let mut buf = self.buffer.lock();
            buf.push(batch);
        }

        // Update cumulative stats
        {
            let mut stats = self.stats.write();
            stats.total_rows_ingested += rows;
            stats.total_bytes_ingested += bytes;
        }

        // Auto-flush if trigger fires
        if let Some(trigger) = self.should_flush() {
            self.flush_with_trigger(trigger)?;
        }

        Ok(())
    }

    /// Ingest rows provided as vectors of `ScalarValue`.
    ///
    /// Each inner `Vec` represents a single row whose length must match the
    /// schema field count. Values are converted column-wise into Arrow arrays.
    pub fn ingest_rows(&self, rows: Vec<Vec<crate::types::ScalarValue>>) -> Result<()> {
        use crate::types::DataType;
        use arrow::array::{Float64Array, Int64Array, StringArray};

        if rows.is_empty() {
            return Ok(());
        }

        let schema = {
            let buf = self.buffer.lock();
            buf.schema.clone()
        };

        let num_fields = schema.len();
        if rows[0].len() != num_fields {
            return Err(BlazeError::execution(format!(
                "Row has {} values but schema has {} fields",
                rows[0].len(),
                num_fields
            )));
        }

        let arrow_schema = Arc::new(schema.to_arrow());
        let mut columns: Vec<arrow::array::ArrayRef> = Vec::with_capacity(num_fields);

        for col_idx in 0..num_fields {
            let field = schema
                .field(col_idx)
                .ok_or_else(|| BlazeError::execution(format!("Invalid field index: {col_idx}")))?;
            let col_values: Vec<&crate::types::ScalarValue> =
                rows.iter().map(|r| &r[col_idx]).collect();

            let array: arrow::array::ArrayRef = match field.data_type() {
                DataType::Int64 => {
                    let arr: Int64Array = col_values
                        .iter()
                        .map(|v| match v {
                            crate::types::ScalarValue::Int64(val) => *val,
                            _ => None,
                        })
                        .collect();
                    Arc::new(arr)
                }
                DataType::Float64 => {
                    let arr: Float64Array = col_values
                        .iter()
                        .map(|v| match v {
                            crate::types::ScalarValue::Float64(val) => *val,
                            _ => None,
                        })
                        .collect();
                    Arc::new(arr)
                }
                DataType::Utf8 => {
                    let arr: StringArray = col_values
                        .iter()
                        .map(|v| match v {
                            crate::types::ScalarValue::Utf8(val) => val.as_deref(),
                            _ => None,
                        })
                        .collect();
                    Arc::new(arr)
                }
                dt => {
                    return Err(BlazeError::not_implemented(format!(
                        "ingest_rows does not yet support column type {dt:?}"
                    )));
                }
            };
            columns.push(array);
        }

        let batch = RecordBatch::try_new(arrow_schema, columns).map_err(|e| {
            BlazeError::execution(format!("Failed to build RecordBatch from rows: {e}"))
        })?;

        self.ingest(batch)
    }

    /// Manually flush the buffer, returning the flush result.
    pub fn flush(&self) -> Result<FlushResult> {
        self.flush_with_trigger(FlushTrigger::Manual)
    }

    /// Check whether an automatic flush should be triggered.
    pub fn should_flush(&self) -> Option<FlushTrigger> {
        let buf = self.buffer.lock();
        if buf.is_empty() {
            return None;
        }
        if buf.row_count >= self.config.max_buffer_size {
            return Some(FlushTrigger::BufferFull);
        }
        if buf.byte_count >= self.config.max_buffer_bytes {
            return Some(FlushTrigger::ByteLimit);
        }
        if buf.created_at.elapsed().as_millis() as u64 >= self.config.flush_interval_ms {
            return Some(FlushTrigger::TimerExpired);
        }
        None
    }

    /// Compact all flushed batches into larger segments, returning the result.
    pub fn compact(&self) -> Result<Vec<RecordBatch>> {
        let old_batches = {
            let mut flushed = self.flushed_batches.write();
            std::mem::take(&mut *flushed)
        };

        if old_batches.is_empty() {
            return Ok(Vec::new());
        }

        let compacted = self.compaction_manager.compact_batches(old_batches)?;

        // Record segment metadata
        let seg_id = self.sequence.fetch_add(1, Ordering::AcqRel);
        let row_count: usize = compacted.iter().map(|b| b.num_rows()).sum();
        let byte_size: usize = compacted
            .iter()
            .map(MicroBatchBuffer::batch_byte_size)
            .sum();
        self.compaction_manager.add_segment(CompactedSegment {
            segment_id: seg_id,
            row_count,
            byte_size,
            created_at: Instant::now(),
            source_flush_ids: Vec::new(),
        });

        // Put compacted batches back
        {
            let mut flushed = self.flushed_batches.write();
            *flushed = compacted.clone();
        }

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_compactions += 1;
        }

        Ok(compacted)
    }

    /// Get a snapshot of all data (buffered + flushed).
    pub fn snapshot(&self) -> Vec<RecordBatch> {
        let mut result = Vec::new();

        // Flushed batches first
        {
            let flushed = self.flushed_batches.read();
            result.extend(flushed.iter().cloned());
        }

        // Then buffered (pending) batches
        {
            let buf = self.buffer.lock();
            result.extend(buf.pending.iter().cloned());
        }

        result
    }

    /// Get current pipeline statistics.
    pub fn stats(&self) -> IngestionStats {
        let mut s = self.stats.read().clone();
        // Overlay live buffer stats
        let buf = self.buffer.lock();
        s.buffer_rows = buf.row_count;
        s.buffer_bytes = buf.byte_count;
        s
    }

    /// Clear all data (buffer + flushed batches + statistics).
    pub fn reset(&self) {
        {
            let mut buf = self.buffer.lock();
            buf.pending.clear();
            buf.row_count = 0;
            buf.byte_count = 0;
            buf.created_at = Instant::now();
        }
        self.flushed_batches.write().clear();
        *self.stats.write() = IngestionStats::default();
        self.sequence.store(0, Ordering::Release);
    }

    // -- private helpers ----------------------------------------------------

    fn flush_with_trigger(&self, trigger: FlushTrigger) -> Result<FlushResult> {
        let start = Instant::now();

        let (batches, rows, bytes) = {
            let mut buf = self.buffer.lock();
            if buf.is_empty() {
                return Ok(FlushResult {
                    flush_id: self.sequence.load(Ordering::Acquire),
                    rows_flushed: 0,
                    bytes_flushed: 0,
                    trigger,
                    duration: Duration::ZERO,
                });
            }
            buf.drain()
        };

        // Append to flushed store
        {
            let mut flushed = self.flushed_batches.write();
            flushed.extend(batches);
        }

        let flush_id = self.sequence.fetch_add(1, Ordering::AcqRel);
        let duration = start.elapsed();

        // Update stats
        {
            let mut stats = self.stats.write();
            stats.total_flushes += 1;
            stats.last_flush_time = Some(Instant::now());
            // Running average of flush duration
            let prev_total_us = stats.avg_flush_duration_us * (stats.total_flushes - 1);
            stats.avg_flush_duration_us =
                (prev_total_us + duration.as_micros() as u64) / stats.total_flushes;
        }

        self.compaction_manager.record_flush();

        Ok(FlushResult {
            flush_id,
            rows_flushed: rows,
            bytes_flushed: bytes,
            trigger,
            duration,
        })
    }
}

// ---------------------------------------------------------------------------
// SourceConnector Framework
// ---------------------------------------------------------------------------

/// Configuration for a source connector.
#[derive(Debug, Clone)]
pub struct SourceConnectorConfig {
    /// Maximum rows per polled batch.
    pub batch_size: usize,
    /// Interval in milliseconds between poll attempts.
    pub poll_interval_ms: u64,
}

impl Default for SourceConnectorConfig {
    fn default() -> Self {
        Self {
            batch_size: 1024,
            poll_interval_ms: 100,
        }
    }
}

/// Trait for pluggable source connectors that produce `RecordBatch`es.
pub trait SourceConnector: Send + Sync {
    /// Human-readable name of this connector.
    fn name(&self) -> &str;
    /// Schema of the data produced by this connector.
    fn schema(&self) -> &Schema;
    /// Poll the next batch from the source. Returns `Ok(None)` when exhausted.
    fn poll_batch(&mut self) -> Result<Option<RecordBatch>>;
}

/// A CSV source connector that reads a CSV file in batches.
pub struct CsvSourceConnector {
    path: String,
    schema: Schema,
    config: SourceConnectorConfig,
    exhausted: bool,
}

impl CsvSourceConnector {
    /// Create a new CSV source connector.
    pub fn new(path: impl Into<String>, schema: Schema, config: SourceConnectorConfig) -> Self {
        Self {
            path: path.into(),
            schema,
            config,
            exhausted: false,
        }
    }
}

impl SourceConnector for CsvSourceConnector {
    fn name(&self) -> &str {
        "csv"
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        // In a real implementation this would read `self.config.batch_size` rows
        // from the CSV file at `self.path`. For now, mark as exhausted.
        self.exhausted = true;
        Err(BlazeError::not_implemented(format!(
            "CsvSourceConnector::poll_batch not yet wired to file I/O for '{}'",
            self.path
        )))
    }
}

/// A newline-delimited JSON (NDJSON) source connector.
pub struct JsonSourceConnector {
    path: String,
    schema: Schema,
    exhausted: bool,
}

impl JsonSourceConnector {
    /// Create a new NDJSON source connector.
    pub fn new(path: impl Into<String>, schema: Schema) -> Self {
        Self {
            path: path.into(),
            schema,
            exhausted: false,
        }
    }
}

impl SourceConnector for JsonSourceConnector {
    fn name(&self) -> &str {
        "json"
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn poll_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.exhausted {
            return Ok(None);
        }
        self.exhausted = true;
        Err(BlazeError::not_implemented(format!(
            "JsonSourceConnector::poll_batch not yet wired to file I/O for '{}'",
            self.path
        )))
    }
}

/// Registry of named source connectors.
pub struct ConnectorRegistry {
    connectors: std::collections::HashMap<String, Box<dyn SourceConnector>>,
}

impl ConnectorRegistry {
    /// Create an empty connector registry.
    pub fn new() -> Self {
        Self {
            connectors: std::collections::HashMap::new(),
        }
    }

    /// Register a connector under the given name.
    pub fn register(&mut self, name: impl Into<String>, connector: Box<dyn SourceConnector>) {
        self.connectors.insert(name.into(), connector);
    }

    /// Get a mutable reference to a connector by name.
    pub fn get(&mut self, name: &str) -> Option<&mut Box<dyn SourceConnector>> {
        self.connectors.get_mut(name)
    }

    /// List all registered connector names.
    pub fn list(&self) -> Vec<&str> {
        self.connectors.keys().map(|k| k.as_str()).collect()
    }
}

// ---------------------------------------------------------------------------
// Schema Evolution
// ---------------------------------------------------------------------------

/// Type of schema evolution change detected between two schemas.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvolutionType {
    /// A new column was added.
    AddColumn,
    /// An existing column was dropped.
    DropColumn,
    /// A column type was widened (e.g. Int32 → Int64).
    WidenType,
    /// Schemas are fully compatible (no changes or only safe additions).
    Compatible,
    /// Schemas are incompatible (e.g. narrowing type, changing nullability unsafely).
    Incompatible,
}

/// A single detected schema change.
#[derive(Debug, Clone)]
pub struct SchemaChange {
    /// Column name affected by the change.
    pub column: String,
    /// Kind of change.
    pub change_type: EvolutionType,
}

/// Manages forward-compatible schema evolution.
#[derive(Debug, Clone)]
pub struct SchemaEvolution {
    schema: Schema,
}

impl SchemaEvolution {
    /// Create a new `SchemaEvolution` tracker with the given initial schema.
    pub fn new(initial_schema: Schema) -> Self {
        Self {
            schema: initial_schema,
        }
    }

    /// Current schema.
    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Attempt to evolve the schema to `new_schema`.
    ///
    /// Returns an updated `SchemaEvolution` if the new schema is compatible
    /// (additions and safe widenings only). Returns an error otherwise.
    pub fn evolve(&self, new_schema: &Schema) -> Result<SchemaEvolution> {
        if !Self::is_compatible(&self.schema, new_schema) {
            return Err(BlazeError::analysis(
                "Schema evolution failed: incompatible changes detected",
            ));
        }
        Ok(SchemaEvolution {
            schema: new_schema.clone(),
        })
    }

    /// Check whether evolving from `old` to `new` is compatible.
    ///
    /// Compatible means: columns may be added, types may be widened, but
    /// existing columns must not be removed or narrowed.
    pub fn is_compatible(old: &Schema, new: &Schema) -> bool {
        let changes = Self::detect_changes(old, new);
        !changes.iter().any(|c| {
            matches!(
                c.change_type,
                EvolutionType::DropColumn | EvolutionType::Incompatible
            )
        })
    }

    /// Detect individual column-level changes between two schemas.
    pub fn detect_changes(old: &Schema, new: &Schema) -> Vec<SchemaChange> {
        let mut changes = Vec::new();

        // Check existing columns in old schema
        for field in old.fields() {
            match new.field_by_name(field.name()) {
                Some(new_field) => {
                    if field.data_type() != new_field.data_type() {
                        if Self::is_widening(field.data_type(), new_field.data_type()) {
                            changes.push(SchemaChange {
                                column: field.name().to_string(),
                                change_type: EvolutionType::WidenType,
                            });
                        } else {
                            changes.push(SchemaChange {
                                column: field.name().to_string(),
                                change_type: EvolutionType::Incompatible,
                            });
                        }
                    }
                }
                None => {
                    changes.push(SchemaChange {
                        column: field.name().to_string(),
                        change_type: EvolutionType::DropColumn,
                    });
                }
            }
        }

        // Check for new columns
        for field in new.fields() {
            if old.field_by_name(field.name()).is_none() {
                changes.push(SchemaChange {
                    column: field.name().to_string(),
                    change_type: EvolutionType::AddColumn,
                });
            }
        }

        changes
    }

    /// Check if changing from `from` to `to` is a safe widening.
    fn is_widening(from: &crate::types::DataType, to: &crate::types::DataType) -> bool {
        use crate::types::DataType;
        matches!(
            (from, to),
            (DataType::Int8, DataType::Int16)
                | (DataType::Int8, DataType::Int32)
                | (DataType::Int8, DataType::Int64)
                | (DataType::Int16, DataType::Int32)
                | (DataType::Int16, DataType::Int64)
                | (DataType::Int32, DataType::Int64)
                | (DataType::Float32, DataType::Float64)
                | (DataType::Utf8, DataType::LargeUtf8)
        )
    }
}

// ---------------------------------------------------------------------------
// Dead Letter Queue
// ---------------------------------------------------------------------------

/// A record that failed processing.
#[derive(Debug, Clone)]
pub struct FailedRecord {
    /// Raw data that failed.
    pub data: Vec<u8>,
    /// Error description.
    pub error: String,
    /// Timestamp (epoch millis) when the failure occurred.
    pub timestamp: u64,
    /// Name of the source that produced this record.
    pub source: String,
}

/// Aggregate statistics for a `DeadLetterQueue`.
#[derive(Debug, Clone, Default)]
pub struct DlqStats {
    /// Total failures ever pushed.
    pub total_failures: u64,
    /// Records retried (placeholder for future retry logic).
    pub retried: u64,
    /// Records discarded due to capacity eviction.
    pub discarded: u64,
}

/// A bounded dead-letter queue for records that failed ingestion.
pub struct DeadLetterQueue {
    max_size: usize,
    records: Vec<FailedRecord>,
    stats: DlqStats,
}

impl DeadLetterQueue {
    /// Create a new dead-letter queue with the given maximum capacity.
    pub fn new(max_size: usize) -> Self {
        Self {
            max_size,
            records: Vec::new(),
            stats: DlqStats::default(),
        }
    }

    /// Push a failed record. Evicts the oldest record if the queue is full.
    pub fn push(&mut self, record: FailedRecord) {
        self.stats.total_failures += 1;
        if self.records.len() >= self.max_size {
            self.records.remove(0);
            self.stats.discarded += 1;
        }
        self.records.push(record);
    }

    /// Drain all records from the queue.
    pub fn drain(&mut self) -> Vec<FailedRecord> {
        std::mem::take(&mut self.records)
    }

    /// Number of records currently in the queue.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Return a snapshot of the DLQ statistics.
    pub fn stats(&self) -> DlqStats {
        self.stats.clone()
    }
}

// ---------------------------------------------------------------------------
// Transformation Pipeline
// ---------------------------------------------------------------------------

/// A single transformation step in the pipeline.
#[derive(Debug, Clone)]
pub enum TransformStep {
    /// Filter rows where `column` satisfies `predicate` (kept as a string expression).
    Filter { column: String, predicate: String },
    /// Rename a column.
    Rename { old_name: String, new_name: String },
    /// Cast a column to a new data type.
    Cast {
        column: String,
        target_type: crate::types::DataType,
    },
    /// Drop a column from the batch.
    Drop { column: String },
}

/// A builder-style pipeline of transformations applied to `RecordBatch`es.
pub struct TransformPipeline {
    steps: Vec<TransformStep>,
}

impl TransformPipeline {
    /// Create an empty transformation pipeline.
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    /// Add a filter step.
    pub fn add_filter(mut self, column: impl Into<String>, predicate: impl Into<String>) -> Self {
        self.steps.push(TransformStep::Filter {
            column: column.into(),
            predicate: predicate.into(),
        });
        self
    }

    /// Add a column rename step.
    pub fn add_rename(mut self, old_name: impl Into<String>, new_name: impl Into<String>) -> Self {
        self.steps.push(TransformStep::Rename {
            old_name: old_name.into(),
            new_name: new_name.into(),
        });
        self
    }

    /// Add a type cast step.
    pub fn add_cast(
        mut self,
        column: impl Into<String>,
        target_type: crate::types::DataType,
    ) -> Self {
        self.steps.push(TransformStep::Cast {
            column: column.into(),
            target_type,
        });
        self
    }

    /// Add a column drop step.
    pub fn add_drop(mut self, column: impl Into<String>) -> Self {
        self.steps.push(TransformStep::Drop {
            column: column.into(),
        });
        self
    }

    /// Return the list of steps in this pipeline.
    pub fn steps(&self) -> &[TransformStep] {
        &self.steps
    }

    /// Apply all transformation steps to the given batch.
    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut current = batch.clone();
        for step in &self.steps {
            current = self.apply_step(&current, step)?;
        }
        Ok(current)
    }

    fn apply_step(&self, batch: &RecordBatch, step: &TransformStep) -> Result<RecordBatch> {
        match step {
            TransformStep::Rename { old_name, new_name } => {
                let schema = batch.schema();
                let new_fields: Vec<arrow::datatypes::Field> = schema
                    .fields()
                    .iter()
                    .map(|f| {
                        if f.name() == old_name {
                            arrow::datatypes::Field::new(
                                new_name,
                                f.data_type().clone(),
                                f.is_nullable(),
                            )
                        } else {
                            f.as_ref().clone()
                        }
                    })
                    .collect();
                let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
                RecordBatch::try_new(new_schema, batch.columns().to_vec())
                    .map_err(|e| BlazeError::execution(format!("Rename failed: {e}")))
            }
            TransformStep::Drop { column } => {
                let schema = batch.schema();
                let idx = schema.index_of(column).map_err(|_| {
                    BlazeError::analysis(format!("Column '{column}' not found for drop"))
                })?;
                let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
                let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
                for (i, f) in schema.fields().iter().enumerate() {
                    if i != idx {
                        fields.push(f.as_ref().clone());
                        columns.push(batch.column(i).clone());
                    }
                }
                let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
                RecordBatch::try_new(new_schema, columns)
                    .map_err(|e| BlazeError::execution(format!("Drop column failed: {e}")))
            }
            TransformStep::Cast {
                column,
                target_type,
            } => {
                let schema = batch.schema();
                let idx = schema.index_of(column).map_err(|_| {
                    BlazeError::analysis(format!("Column '{column}' not found for cast"))
                })?;
                let arrow_target = target_type.to_arrow();
                let casted =
                    arrow::compute::cast(batch.column(idx), &arrow_target).map_err(|e| {
                        BlazeError::execution(format!("Cast failed for column '{column}': {e}"))
                    })?;
                let mut fields: Vec<arrow::datatypes::Field> = Vec::new();
                let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();
                for (i, f) in schema.fields().iter().enumerate() {
                    if i == idx {
                        fields.push(arrow::datatypes::Field::new(
                            f.name(),
                            arrow_target.clone(),
                            f.is_nullable(),
                        ));
                        columns.push(casted.clone());
                    } else {
                        fields.push(f.as_ref().clone());
                        columns.push(batch.column(i).clone());
                    }
                }
                let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
                RecordBatch::try_new(new_schema, columns)
                    .map_err(|e| BlazeError::execution(format!("Cast rebuild failed: {e}")))
            }
            TransformStep::Filter { column, predicate } => {
                let schema = batch.schema();
                let idx = schema.index_of(column).map_err(|_| {
                    BlazeError::analysis(format!("Column '{column}' not found for filter"))
                })?;
                let col = batch.column(idx);

                // Support simple "is_not_null" predicate
                if predicate == "is_not_null" {
                    let boolean_mask = arrow::compute::is_not_null(col).map_err(|e| {
                        BlazeError::execution(format!("Filter is_not_null failed: {e}"))
                    })?;
                    let filtered = arrow::compute::filter_record_batch(batch, &boolean_mask)
                        .map_err(|e| BlazeError::execution(format!("Filter apply failed: {e}")))?;
                    return Ok(filtered);
                }

                Err(BlazeError::not_implemented(format!(
                    "Filter predicate '{predicate}' is not yet supported"
                )))
            }
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
    use arrow::array::{Float64Array, Int64Array};
    use std::sync::Arc;

    /// Helper: create a Blaze schema with (id: Int64, value: Float64).
    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ])
    }

    /// Helper: create an Arrow RecordBatch matching `test_schema`.
    fn test_batch(ids: Vec<i64>, values: Vec<f64>) -> RecordBatch {
        let schema = test_schema();
        let arrow_schema = Arc::new(schema.to_arrow());
        let id_array = Arc::new(Int64Array::from(ids)) as arrow::array::ArrayRef;
        let val_array = Arc::new(Float64Array::from(values)) as arrow::array::ArrayRef;
        RecordBatch::try_new(arrow_schema, vec![id_array, val_array]).unwrap()
    }

    #[test]
    fn test_ingestion_config_defaults() {
        let config = IngestionConfig::default();
        assert_eq!(config.max_buffer_size, 10_000);
        assert_eq!(config.max_buffer_bytes, 64 * 1024 * 1024);
        assert_eq!(config.flush_interval_ms, 1000);
        assert!(config.enable_wal);
        assert_eq!(config.compaction_threshold, 10);
    }

    #[test]
    fn test_ingest_single_batch() {
        let pipeline =
            IngestionPipeline::new("test_table", test_schema(), IngestionConfig::default());

        let batch = test_batch(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        pipeline.ingest(batch).unwrap();

        let stats = pipeline.stats();
        assert_eq!(stats.total_rows_ingested, 3);
        assert_eq!(stats.buffer_rows, 3);
    }

    #[test]
    fn test_ingest_and_flush() {
        let pipeline =
            IngestionPipeline::new("test_table", test_schema(), IngestionConfig::default());

        let batch = test_batch(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        pipeline.ingest(batch).unwrap();

        let result = pipeline.flush().unwrap();
        assert_eq!(result.rows_flushed, 3);
        assert_eq!(result.trigger, FlushTrigger::Manual);
        assert!(result.bytes_flushed > 0);

        let stats = pipeline.stats();
        assert_eq!(stats.total_flushes, 1);
        assert_eq!(stats.buffer_rows, 0);
    }

    #[test]
    fn test_auto_flush_trigger_buffer_full() {
        let config = IngestionConfig {
            max_buffer_size: 5, // very small threshold
            ..Default::default()
        };
        let pipeline = IngestionPipeline::new("test_table", test_schema(), config);

        // Ingest 6 rows — should trigger auto-flush after crossing threshold
        let batch = test_batch(vec![1, 2, 3, 4, 5, 6], vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        pipeline.ingest(batch).unwrap();

        let stats = pipeline.stats();
        // Auto-flush should have moved data out of the buffer
        assert!(stats.total_flushes >= 1);
    }

    #[test]
    fn test_compaction_merges_batches() {
        let config = IngestionConfig {
            max_buffer_size: 100_000, // prevent auto-flush
            ..Default::default()
        };
        let pipeline = IngestionPipeline::new("test_table", test_schema(), config);

        // Ingest and flush several small batches
        for i in 0..5 {
            let batch = test_batch(vec![i], vec![i as f64]);
            pipeline.ingest(batch).unwrap();
            pipeline.flush().unwrap();
        }

        // Now compact
        let compacted = pipeline.compact().unwrap();
        assert!(!compacted.is_empty());
        let total_rows: usize = compacted.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        let stats = pipeline.stats();
        assert_eq!(stats.total_compactions, 1);
    }

    #[test]
    fn test_ingestion_stats_tracking() {
        let pipeline =
            IngestionPipeline::new("test_table", test_schema(), IngestionConfig::default());

        let batch1 = test_batch(vec![1, 2], vec![1.0, 2.0]);
        let batch2 = test_batch(vec![3, 4, 5], vec![3.0, 4.0, 5.0]);

        pipeline.ingest(batch1).unwrap();
        pipeline.ingest(batch2).unwrap();

        let stats = pipeline.stats();
        assert_eq!(stats.total_rows_ingested, 5);
        assert!(stats.total_bytes_ingested > 0);
        assert_eq!(stats.buffer_rows, 5);

        pipeline.flush().unwrap();
        let stats = pipeline.stats();
        assert_eq!(stats.total_flushes, 1);
        assert!(stats.last_flush_time.is_some());
        assert_eq!(stats.buffer_rows, 0);
    }

    #[test]
    fn test_pipeline_snapshot() {
        let config = IngestionConfig {
            max_buffer_size: 100_000,
            ..Default::default()
        };
        let pipeline = IngestionPipeline::new("test_table", test_schema(), config);

        // Flush one batch to flushed store
        let batch1 = test_batch(vec![1, 2], vec![1.0, 2.0]);
        pipeline.ingest(batch1).unwrap();
        pipeline.flush().unwrap();

        // Leave another batch in the buffer
        let batch2 = test_batch(vec![3, 4], vec![3.0, 4.0]);
        pipeline.ingest(batch2).unwrap();

        let snapshot = pipeline.snapshot();
        let total_rows: usize = snapshot.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4); // 2 flushed + 2 buffered
    }

    #[test]
    fn test_flush_result() {
        let pipeline =
            IngestionPipeline::new("test_table", test_schema(), IngestionConfig::default());

        // Flush on empty buffer returns zero rows
        let result = pipeline.flush().unwrap();
        assert_eq!(result.rows_flushed, 0);
        assert_eq!(result.bytes_flushed, 0);

        // Ingest then flush
        let batch = test_batch(vec![10, 20], vec![1.5, 2.5]);
        pipeline.ingest(batch).unwrap();

        let result = pipeline.flush().unwrap();
        assert_eq!(result.rows_flushed, 2);
        assert!(result.bytes_flushed > 0);
        assert_eq!(result.trigger, FlushTrigger::Manual);
    }

    #[test]
    fn test_pipeline_reset() {
        let pipeline =
            IngestionPipeline::new("test_table", test_schema(), IngestionConfig::default());

        let batch = test_batch(vec![1, 2, 3], vec![1.0, 2.0, 3.0]);
        pipeline.ingest(batch).unwrap();
        pipeline.flush().unwrap();

        pipeline.reset();

        let stats = pipeline.stats();
        assert_eq!(stats.total_rows_ingested, 0);
        assert_eq!(stats.total_flushes, 0);
        assert_eq!(stats.buffer_rows, 0);
        assert!(pipeline.snapshot().is_empty());
    }

    // -----------------------------------------------------------------------
    // Source Connector tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_source_connector_config_defaults() {
        let config = SourceConnectorConfig::default();
        assert_eq!(config.batch_size, 1024);
        assert_eq!(config.poll_interval_ms, 100);
    }

    #[test]
    fn test_csv_source_connector_basic() {
        let schema = test_schema();
        let config = SourceConnectorConfig {
            batch_size: 512,
            poll_interval_ms: 50,
        };
        let connector = CsvSourceConnector::new("/tmp/test.csv", schema, config);
        assert_eq!(connector.name(), "csv");
        assert_eq!(connector.schema().len(), 2);
    }

    #[test]
    fn test_json_source_connector_basic() {
        let schema = test_schema();
        let connector = JsonSourceConnector::new("/tmp/test.ndjson", schema);
        assert_eq!(connector.name(), "json");
        assert_eq!(connector.schema().len(), 2);
    }

    #[test]
    fn test_connector_registry() {
        let schema = test_schema();
        let mut registry = ConnectorRegistry::new();
        assert!(registry.list().is_empty());

        registry.register(
            "csv_source",
            Box::new(CsvSourceConnector::new(
                "/tmp/a.csv",
                schema.clone(),
                SourceConnectorConfig::default(),
            )),
        );
        registry.register(
            "json_source",
            Box::new(JsonSourceConnector::new("/tmp/b.ndjson", schema)),
        );

        assert_eq!(registry.list().len(), 2);
        assert!(registry.get("csv_source").is_some());
        assert!(registry.get("missing").is_none());
    }

    // -----------------------------------------------------------------------
    // Schema Evolution tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_schema_evolution_compatible_add_column() {
        let old = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let new = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        assert!(SchemaEvolution::is_compatible(&old, &new));

        let evo = SchemaEvolution::new(old);
        let evolved = evo.evolve(&new).unwrap();
        assert_eq!(evolved.schema().len(), 2);
    }

    #[test]
    fn test_schema_evolution_incompatible_drop_column() {
        let old = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);
        let new = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        assert!(!SchemaEvolution::is_compatible(&old, &new));

        let evo = SchemaEvolution::new(old);
        assert!(evo.evolve(&new).is_err());
    }

    #[test]
    fn test_schema_evolution_detect_changes() {
        let old = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("dropped", DataType::Utf8, true),
        ]);
        let new = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("added", DataType::Float64, true),
        ]);
        let changes = SchemaEvolution::detect_changes(&old, &new);
        assert_eq!(changes.len(), 3); // widen id, drop "dropped", add "added"

        let widen = changes.iter().find(|c| c.column == "id").unwrap();
        assert_eq!(widen.change_type, EvolutionType::WidenType);
        let drop = changes.iter().find(|c| c.column == "dropped").unwrap();
        assert_eq!(drop.change_type, EvolutionType::DropColumn);
        let add = changes.iter().find(|c| c.column == "added").unwrap();
        assert_eq!(add.change_type, EvolutionType::AddColumn);
    }

    // -----------------------------------------------------------------------
    // Dead Letter Queue tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_dlq_push_and_drain() {
        let mut dlq = DeadLetterQueue::new(10);
        assert!(dlq.is_empty());

        dlq.push(FailedRecord {
            data: vec![1, 2, 3],
            error: "parse error".into(),
            timestamp: 1000,
            source: "csv".into(),
        });
        assert_eq!(dlq.len(), 1);
        assert!(!dlq.is_empty());

        let drained = dlq.drain();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].error, "parse error");
        assert!(dlq.is_empty());
    }

    #[test]
    fn test_dlq_eviction() {
        let mut dlq = DeadLetterQueue::new(2);
        for i in 0..3 {
            dlq.push(FailedRecord {
                data: vec![i],
                error: format!("err{i}"),
                timestamp: i as u64,
                source: "test".into(),
            });
        }
        assert_eq!(dlq.len(), 2);
        let stats = dlq.stats();
        assert_eq!(stats.total_failures, 3);
        assert_eq!(stats.discarded, 1);

        // Oldest record (i=0) should have been evicted
        let records = dlq.drain();
        assert_eq!(records[0].error, "err1");
        assert_eq!(records[1].error, "err2");
    }

    // -----------------------------------------------------------------------
    // Transformation Pipeline tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_transform_pipeline_rename() {
        let batch = test_batch(vec![1, 2], vec![1.0, 2.0]);
        let pipeline = TransformPipeline::new().add_rename("id", "user_id");
        let result = pipeline.apply(&batch).unwrap();
        assert_eq!(result.schema().field(0).name(), "user_id");
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_transform_pipeline_drop() {
        let batch = test_batch(vec![1, 2], vec![1.0, 2.0]);
        let pipeline = TransformPipeline::new().add_drop("value");
        let result = pipeline.apply(&batch).unwrap();
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "id");
    }

    #[test]
    fn test_transform_pipeline_cast() {
        let batch = test_batch(vec![1, 2], vec![1.0, 2.0]);
        let pipeline = TransformPipeline::new().add_cast("id", DataType::Float64);
        let result = pipeline.apply(&batch).unwrap();
        assert_eq!(
            *result.schema().field(0).data_type(),
            arrow::datatypes::DataType::Float64
        );
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_transform_pipeline_chained() {
        let batch = test_batch(vec![1, 2, 3], vec![10.0, 20.0, 30.0]);
        let pipeline = TransformPipeline::new()
            .add_rename("id", "row_id")
            .add_drop("value");
        let result = pipeline.apply(&batch).unwrap();
        assert_eq!(result.num_columns(), 1);
        assert_eq!(result.schema().field(0).name(), "row_id");
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_transform_pipeline_steps() {
        let pipeline = TransformPipeline::new()
            .add_filter("x", "is_not_null")
            .add_rename("a", "b")
            .add_cast("c", DataType::Int64)
            .add_drop("d");
        assert_eq!(pipeline.steps().len(), 4);
    }
}
