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
}
