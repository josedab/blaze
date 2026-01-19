//! Persistent on-disk columnar storage engine for Blaze.
//!
//! This module provides a durable storage layer that persists Arrow RecordBatches
//! to disk using a page-based format with write-ahead logging (WAL) for crash
//! recovery. Data is buffered in memory via an LRU-managed buffer pool and
//! flushed to Parquet files on demand.
//!
//! # Architecture
//!
//! ```text
//! Insert → WAL append → In-memory batches → Flush → Parquet files
//!                              ↑
//!                     BufferPool (LRU cache)
//! ```
//!
//! # Example
//!
//! ```no_run
//! use blaze::storage::persistent_table;
//! use blaze::types::{Schema, Field, DataType};
//!
//! let schema = Schema::new(vec![
//!     Field::new("id", DataType::Int64, false),
//! ]);
//! let table = persistent_table("/tmp/my_table", schema).unwrap();
//! ```

use std::any::Any;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use lru::LruCache;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::catalog::{ColumnStatistics, TableProvider, TableStatistics, TableType};
use crate::error::{BlazeError, Result};
use crate::planner::PhysicalExpr;
use crate::types::Schema;

// ---------------------------------------------------------------------------
// Page-based storage
// ---------------------------------------------------------------------------

/// The type of a storage page.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PageType {
    /// Page holding row data.
    Data,
    /// Page holding index entries.
    Index,
    /// Page holding catalog / schema metadata.
    Meta,
    /// Unused page available for reuse.
    Free,
}

/// A fixed-size page in the storage layer.
#[derive(Debug, Clone)]
pub struct Page {
    /// Unique identifier for this page.
    pub page_id: u64,
    /// Raw byte content of the page.
    pub data: Vec<u8>,
    /// Logical type of the page.
    pub page_type: PageType,
}

/// LRU-managed buffer pool for caching pages in memory.
#[derive(Debug)]
pub struct BufferPool {
    cache: Mutex<LruCache<u64, Page>>,
    capacity: usize,
}

impl BufferPool {
    /// Create a new buffer pool with the given page capacity.
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::new(1).unwrap());
        Self {
            cache: Mutex::new(LruCache::new(cap)),
            capacity,
        }
    }

    /// Retrieve a page from the cache, promoting it in the LRU order.
    pub fn get_page(&self, page_id: u64) -> Option<Page> {
        self.cache.lock().unwrap().get(&page_id).cloned()
    }

    /// Insert or update a page in the cache.
    ///
    /// If the cache is at capacity the least-recently-used page is evicted.
    pub fn put_page(&self, page: Page) {
        self.cache.lock().unwrap().put(page.page_id, page);
    }

    /// Flush (clear) all pages from the buffer pool.
    pub fn flush(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Return the configured capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Return the number of pages currently cached.
    pub fn len(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Return `true` if the cache contains no pages.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// Write-Ahead Log (WAL)
// ---------------------------------------------------------------------------

/// The kind of operation recorded in a WAL entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalOperation {
    /// Row data was inserted.
    Insert,
    /// Row data was deleted.
    Delete,
    /// A checkpoint was taken — all prior entries are durable on disk.
    Checkpoint,
}

/// A single entry in the write-ahead log.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Log sequence number – monotonically increasing.
    pub lsn: u64,
    /// The operation type.
    pub operation: WalOperation,
    /// Name of the affected table.
    pub table_name: String,
    /// Operation payload encoded as base64 Arrow IPC bytes (empty for checkpoints).
    pub data: Option<String>,
}

/// Append-only write-ahead log backed by a file on disk.
///
/// Each entry is serialised as a single JSON line prefixed with its byte length
/// (length-prefixed framing) so that partial writes can be detected during
/// recovery.
#[derive(Debug)]
pub struct WriteAheadLog {
    path: PathBuf,
    next_lsn: AtomicU64,
}

impl WriteAheadLog {
    /// Open (or create) a WAL file at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists.
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Determine the next LSN by scanning existing entries.
        let next_lsn = if path.exists() {
            let entries = Self::read_entries(&path)?;
            entries.last().map_or(1, |e| e.lsn + 1)
        } else {
            // Create the file.
            File::create(&path)?;
            1
        };

        Ok(Self {
            path,
            next_lsn: AtomicU64::new(next_lsn),
        })
    }

    /// Append a new entry to the log and return its LSN.
    pub fn append(
        &self,
        operation: WalOperation,
        table_name: &str,
        data: Option<String>,
    ) -> Result<u64> {
        let lsn = self.next_lsn.fetch_add(1, Ordering::SeqCst);

        let entry = WalEntry {
            lsn,
            operation,
            table_name: table_name.to_string(),
            data,
        };

        let json = serde_json::to_string(&entry)
            .map_err(|e| BlazeError::execution(format!("WAL serialization failed: {e}")))?;

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        // Length-prefixed frame: "<len>\n<json>\n"
        writeln!(file, "{}", json.len())?;
        writeln!(file, "{json}")?;
        file.flush()?;

        Ok(lsn)
    }

    /// Recover all valid entries from the WAL file.
    ///
    /// Partial or corrupted trailing entries are silently skipped so that
    /// recovery is tolerant of crashes mid-write.
    pub fn recover(&self) -> Result<Vec<WalEntry>> {
        Self::read_entries(&self.path)
    }

    /// Write a checkpoint entry and truncate the WAL.
    ///
    /// After a checkpoint all preceding entries are no longer needed because
    /// the data has been flushed to durable Parquet files.
    pub fn checkpoint(&self, table_name: &str) -> Result<u64> {
        let lsn = self.append(WalOperation::Checkpoint, table_name, None)?;
        // Truncate the WAL — the data is now durable.
        File::create(&self.path)?;
        self.next_lsn.store(lsn + 1, Ordering::SeqCst);
        Ok(lsn)
    }

    // -- internal helpers ---------------------------------------------------

    fn read_entries(path: &Path) -> Result<Vec<WalEntry>> {
        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();
        let mut lines = reader.lines();

        while let Some(Ok(len_line)) = lines.next() {
            let len_line = len_line.trim().to_string();
            if len_line.is_empty() {
                continue;
            }
            let expected_len: usize = match len_line.parse() {
                Ok(v) => v,
                Err(_) => break, // corrupted frame — stop recovery
            };

            if let Some(Ok(json_line)) = lines.next() {
                if json_line.len() != expected_len {
                    break; // partial write
                }
                match serde_json::from_str::<WalEntry>(&json_line) {
                    Ok(entry) => entries.push(entry),
                    Err(_) => break,
                }
            } else {
                break;
            }
        }

        Ok(entries)
    }
}

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Configuration for the persistent storage engine.
#[derive(Debug, Clone)]
pub struct PersistentConfig {
    /// Maximum number of pages in the buffer pool.
    pub buffer_pool_size: usize,
    /// Directory for WAL files.
    pub wal_dir: PathBuf,
    /// Directory for data (Parquet) files.
    pub data_dir: PathBuf,
    /// Number of inserted rows before an automatic flush.
    pub flush_threshold: usize,
}

impl Default for PersistentConfig {
    fn default() -> Self {
        Self {
            buffer_pool_size: 1024,
            wal_dir: PathBuf::from("wal"),
            data_dir: PathBuf::from("data"),
            flush_threshold: 10_000,
        }
    }
}

impl PersistentConfig {
    /// Set the buffer pool capacity.
    pub fn with_buffer_pool_size(mut self, size: usize) -> Self {
        self.buffer_pool_size = size;
        self
    }

    /// Set the WAL directory.
    pub fn with_wal_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.wal_dir = dir.into();
        self
    }

    /// Set the data directory.
    pub fn with_data_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.data_dir = dir.into();
        self
    }

    /// Set the flush threshold (rows).
    pub fn with_flush_threshold(mut self, threshold: usize) -> Self {
        self.flush_threshold = threshold;
        self
    }
}

// ---------------------------------------------------------------------------
// PersistentTable
// ---------------------------------------------------------------------------

/// A durable, on-disk table backed by Parquet files with WAL-based recovery.
///
/// Data inserted into the table is first appended to the write-ahead log and
/// held in memory. Calling [`PersistentTable::flush`] writes the buffered
/// batches to Parquet files and checkpoints the WAL.
#[derive(Debug)]
pub struct PersistentTable {
    /// Root directory for this table.
    path: PathBuf,
    /// Table schema.
    schema: Schema,
    /// Shared buffer pool for page caching.
    buffer_pool: Arc<BufferPool>,
    /// Write-ahead log.
    wal: Arc<Mutex<WriteAheadLog>>,
    /// In-memory record batches (mirrors MemoryTable).
    batches: RwLock<Vec<RecordBatch>>,
}

impl PersistentTable {
    /// Create a new persistent table at `path` with the given schema.
    ///
    /// The table directory and WAL file are created on disk.
    pub fn create(path: impl AsRef<Path>, schema: Schema) -> Result<Self> {
        Self::create_with_config(path, schema, PersistentConfig::default())
    }

    /// Create a new persistent table with explicit configuration.
    pub fn create_with_config(
        path: impl AsRef<Path>,
        schema: Schema,
        config: PersistentConfig,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let data_dir = path.join(&config.data_dir);
        let wal_path = path.join(&config.wal_dir).join("wal.log");

        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(wal_path.parent().unwrap())?;

        // Persist the schema as JSON for later reopening.
        let schema_path = path.join("schema.json");
        let arrow_schema = schema.to_arrow();
        let schema_json = serde_json::to_string_pretty(&arrow_schema)
            .map_err(|e| BlazeError::execution(format!("Failed to serialize schema: {e}")))?;
        fs::write(&schema_path, schema_json)?;

        let wal = WriteAheadLog::open(&wal_path)?;
        let buffer_pool = BufferPool::new(config.buffer_pool_size);

        Ok(Self {
            path,
            schema,
            buffer_pool: Arc::new(buffer_pool),
            wal: Arc::new(Mutex::new(wal)),
            batches: RwLock::new(Vec::new()),
        })
    }

    /// Open an existing persistent table, replaying the WAL to recover state.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(path, PersistentConfig::default())
    }

    /// Open an existing persistent table with explicit configuration.
    pub fn open_with_config(path: impl AsRef<Path>, config: PersistentConfig) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Read persisted schema.
        let schema_path = path.join("schema.json");
        let schema_json = fs::read_to_string(&schema_path)
            .map_err(|e| BlazeError::execution(format!("Failed to read schema: {e}")))?;
        let arrow_schema: arrow::datatypes::Schema = serde_json::from_str(&schema_json)
            .map_err(|e| BlazeError::execution(format!("Failed to deserialize schema: {e}")))?;
        let schema = Schema::from_arrow(&arrow_schema)?;

        // Load existing Parquet data files.
        let data_dir = path.join(&config.data_dir);
        let mut batches = Vec::new();
        if data_dir.exists() {
            let mut parquet_files: Vec<_> = fs::read_dir(&data_dir)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| p.extension().map_or(false, |ext| ext == "parquet"))
                .collect();
            parquet_files.sort();

            for pf in parquet_files {
                let file = File::open(&pf)?;
                let reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?
                        .build()?;
                for batch_result in reader {
                    batches.push(batch_result?);
                }
            }
        }

        // Replay WAL.
        let wal_path = path.join(&config.wal_dir).join("wal.log");
        let wal = WriteAheadLog::open(&wal_path)?;
        let wal_entries = wal.recover()?;

        for entry in &wal_entries {
            if entry.operation == WalOperation::Insert {
                if let Some(ref encoded) = entry.data {
                    if let Ok(batch) = Self::decode_batch(encoded, &arrow_schema) {
                        batches.push(batch);
                    }
                }
            }
        }

        let buffer_pool = BufferPool::new(config.buffer_pool_size);

        Ok(Self {
            path,
            schema,
            buffer_pool: Arc::new(buffer_pool),
            wal: Arc::new(Mutex::new(wal)),
            batches: RwLock::new(batches),
        })
    }

    /// Flush in-memory batches to Parquet files and checkpoint the WAL.
    pub fn flush(&self) -> Result<()> {
        let batches = self.batches.read().clone();
        if batches.is_empty() {
            return Ok(());
        }

        let data_dir = self.path.join("data");
        fs::create_dir_all(&data_dir)?;

        let file_name = format!("data_{}.parquet", chrono::Utc::now().timestamp_millis());
        let file_path = data_dir.join(file_name);

        crate::storage::write_parquet(&file_path, &batches)?;

        // Checkpoint the WAL now that data is durable.
        let table_name = self
            .path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        self.wal.lock().unwrap().checkpoint(&table_name)?;

        Ok(())
    }

    /// Get the table directory path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Get the number of in-memory rows.
    pub fn num_rows(&self) -> usize {
        self.batches.read().iter().map(|b| b.num_rows()).sum()
    }

    /// Get a reference to the buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    // -- internal helpers ---------------------------------------------------

    /// Encode a RecordBatch to base64-encoded Arrow IPC bytes.
    fn encode_batch(batch: &RecordBatch) -> Result<String> {
        let mut buf = Vec::new();
        {
            let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        Ok(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            &buf,
        ))
    }

    /// Decode a RecordBatch from base64-encoded Arrow IPC bytes.
    fn decode_batch(encoded: &str, arrow_schema: &arrow::datatypes::Schema) -> Result<RecordBatch> {
        let bytes = base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
            .map_err(|e| BlazeError::execution(format!("Base64 decode failed: {e}")))?;

        let cursor = std::io::Cursor::new(bytes);
        let mut reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)?;
        let batch = reader
            .next()
            .ok_or_else(|| BlazeError::execution("Empty IPC stream"))?
            .map_err(|e| BlazeError::Arrow {
                message: e.to_string(),
            })?;

        // Verify schema compatibility.
        if batch.schema().fields() != arrow_schema.fields() {
            return Err(BlazeError::schema("Schema mismatch during WAL replay"));
        }
        let _ = arrow_schema; // used above
        Ok(batch)
    }
}

// ---------------------------------------------------------------------------
// TableProvider implementation (mirrors MemoryTable)
// ---------------------------------------------------------------------------

impl TableProvider for PersistentTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn statistics(&self) -> Option<TableStatistics> {
        let batches = self.batches.read();
        let num_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let total_bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();

        let num_columns = self.schema.len();
        let mut null_counts: Vec<usize> = vec![0; num_columns];

        for batch in batches.iter() {
            for (col_idx, col) in batch.columns().iter().enumerate() {
                null_counts[col_idx] += col.null_count();
            }
        }

        let column_statistics: Vec<ColumnStatistics> = null_counts
            .into_iter()
            .map(|null_count| ColumnStatistics {
                null_count: Some(null_count),
                distinct_count: None,
                min_value: None,
                max_value: None,
            })
            .collect();

        Some(TableStatistics {
            num_rows: Some(num_rows),
            total_byte_size: Some(total_bytes),
            column_statistics,
        })
    }

    fn scan(
        &self,
        projection: Option<&[usize]>,
        _filters: &[()],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self.batches.read();
        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch in batches.iter() {
            let projected = match projection {
                Some(indices) => {
                    let columns: Vec<_> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| batch.schema().field(i).clone())
                        .collect();
                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                }
                None => batch.clone(),
            };

            if let Some(limit) = limit {
                let remaining = limit - rows_collected;
                if remaining == 0 {
                    break;
                }
                if projected.num_rows() <= remaining {
                    rows_collected += projected.num_rows();
                    result.push(projected);
                } else {
                    result.push(projected.slice(0, remaining));
                    break;
                }
            } else {
                result.push(projected);
            }
        }

        Ok(result)
    }

    fn supports_filter_pushdown(&self) -> bool {
        true
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn scan_with_filters(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        let batches = self.batches.read();
        let mut result = Vec::new();
        let mut rows_collected = 0;

        for batch in batches.iter() {
            // Apply filters before projection for correct column indices.
            let filtered_batch = if filters.is_empty() {
                batch.clone()
            } else {
                let mut current_batch = batch.clone();
                for filter in filters {
                    if current_batch.num_rows() == 0 {
                        break;
                    }
                    let filter_array = filter.evaluate(&current_batch)?;
                    let filter_array = filter_array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| BlazeError::type_error("Filter must return boolean"))?;
                    current_batch = filter_record_batch(&current_batch, filter_array)?;
                }
                current_batch
            };

            if filtered_batch.num_rows() == 0 {
                continue;
            }

            let projected = match projection {
                Some(indices) => {
                    let columns: Vec<_> = indices
                        .iter()
                        .map(|&i| filtered_batch.column(i).clone())
                        .collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| filtered_batch.schema().field(i).clone())
                        .collect();
                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns)?
                }
                None => filtered_batch,
            };

            if let Some(limit) = limit {
                let remaining = limit - rows_collected;
                if remaining == 0 {
                    break;
                }
                if projected.num_rows() <= remaining {
                    rows_collected += projected.num_rows();
                    result.push(projected);
                } else {
                    result.push(projected.slice(0, remaining));
                    break;
                }
            } else {
                result.push(projected);
            }
        }

        Ok(result)
    }

    fn insert(&self, batches: Vec<RecordBatch>) -> Result<usize> {
        let mut count = 0;
        let table_name = self
            .path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        for batch in &batches {
            let encoded = Self::encode_batch(batch)?;
            self.wal
                .lock()
                .unwrap()
                .append(WalOperation::Insert, &table_name, Some(encoded))?;
            count += batch.num_rows();
        }

        self.batches.write().extend(batches);
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// Helper function
// ---------------------------------------------------------------------------

/// Create a new persistent table at `path` with the given schema.
///
/// This is a convenience wrapper that returns an `Arc<dyn TableProvider>`.
pub fn persistent_table(path: impl AsRef<Path>, schema: Schema) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(PersistentTable::create(path, schema)?))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};
    use arrow::array::Int64Array;
    use tempfile::TempDir;

    fn test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ])
    }

    fn create_test_batch(ids: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, false),
        ]));
        let id_col = Arc::new(Int64Array::from(ids));
        let val_col = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(schema, vec![id_col, val_col]).unwrap()
    }

    #[test]
    fn test_create_and_open() {
        let tmp = TempDir::new().unwrap();
        let table_path = tmp.path().join("test_table");

        // Create table and insert data.
        {
            let table = PersistentTable::create(&table_path, test_schema()).unwrap();
            let batch = create_test_batch(vec![1, 2, 3], vec![10, 20, 30]);
            table.insert(vec![batch]).unwrap();
            table.flush().unwrap();
        }

        // Reopen and verify data survived.
        {
            let table = PersistentTable::open(&table_path).unwrap();
            let results = table.scan(None, &[], None).unwrap();
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 3);

            let id_col = results[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(id_col.value(0), 1);
            assert_eq!(id_col.value(2), 3);
        }
    }

    #[test]
    fn test_wal_recovery() {
        let tmp = TempDir::new().unwrap();
        let table_path = tmp.path().join("wal_table");

        // Create table, insert data, but do NOT flush (simulate crash).
        {
            let table = PersistentTable::create(&table_path, test_schema()).unwrap();
            let batch = create_test_batch(vec![10, 20], vec![100, 200]);
            table.insert(vec![batch]).unwrap();
            // No flush — data only in WAL.
        }

        // Reopen — WAL replay should recover the data.
        {
            let table = PersistentTable::open(&table_path).unwrap();
            let results = table.scan(None, &[], None).unwrap();
            let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total_rows, 2);

            let val_col = results[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(val_col.value(0), 100);
            assert_eq!(val_col.value(1), 200);
        }
    }

    #[test]
    fn test_buffer_pool_lru() {
        let pool = BufferPool::new(2);

        let p1 = Page {
            page_id: 1,
            data: vec![1],
            page_type: PageType::Data,
        };
        let p2 = Page {
            page_id: 2,
            data: vec![2],
            page_type: PageType::Data,
        };
        let p3 = Page {
            page_id: 3,
            data: vec![3],
            page_type: PageType::Index,
        };

        pool.put_page(p1);
        pool.put_page(p2);
        assert_eq!(pool.len(), 2);

        // Access page 1 to make page 2 the LRU candidate.
        assert!(pool.get_page(1).is_some());

        // Inserting page 3 should evict page 2.
        pool.put_page(p3);
        assert_eq!(pool.len(), 2);
        assert!(
            pool.get_page(2).is_none(),
            "page 2 should have been evicted"
        );
        assert!(pool.get_page(1).is_some());
        assert!(pool.get_page(3).is_some());
    }

    #[test]
    fn test_persistent_table_scan() {
        let tmp = TempDir::new().unwrap();
        let table_path = tmp.path().join("scan_table");

        let table = PersistentTable::create(&table_path, test_schema()).unwrap();
        let batch = create_test_batch(vec![1, 2, 3, 4, 5], vec![10, 20, 30, 40, 50]);
        table.insert(vec![batch]).unwrap();

        // Projection: only column 0 ("id").
        let projected = table.scan(Some(&[0]), &[], None).unwrap();
        assert_eq!(projected[0].num_columns(), 1);
        assert_eq!(projected[0].num_rows(), 5);

        // Limit: first 3 rows.
        let limited = table.scan(None, &[], Some(3)).unwrap();
        let total: usize = limited.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3);

        // Projection + limit combined.
        let both = table.scan(Some(&[1]), &[], Some(2)).unwrap();
        let total: usize = both.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 2);
        assert_eq!(both[0].num_columns(), 1);
    }
}
