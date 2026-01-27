//! Persistent Storage with Write-Ahead Logging and Snapshots
//!
//! This module provides durability for the Blaze query engine through:
//! - **Write-Ahead Log (WAL)**: Records all mutations for crash recovery
//! - **Snapshots**: Periodic full table state persistence using Arrow IPC
//! - **PersistentDatabase**: Combines WAL and snapshots for full persistence
//!
//! # WAL Format
//!
//! Each line in the WAL file follows the format: `SEQ|TYPE|DATA`
//! - `SEQ`: Monotonically increasing sequence number
//! - `TYPE`: One of `CREATE_TABLE`, `DROP_TABLE`, `INSERT`, `CHECKPOINT`
//! - `DATA`: JSON-encoded payload (INSERT data uses base64-encoded Arrow IPC)
//!
//! # Snapshot Format
//!
//! Snapshots are stored as Arrow IPC stream files in a directory structure:
//! `{snapshot_dir}/{table_name}.arrow`

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Cursor, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

use base64::engine::general_purpose::STANDARD as BASE64_ENGINE;
use base64::Engine;

use crate::error::{BlazeError, Result};
use crate::types::Schema;

// ---------------------------------------------------------------------------
// WAL Entry
// ---------------------------------------------------------------------------

/// Represents a single mutation recorded in the write-ahead log.
#[derive(Debug, Clone)]
pub enum WalEntry {
    /// A new table was created with the given schema.
    CreateTable {
        /// Table name
        name: String,
        /// Table schema serialized as Arrow JSON
        schema: Schema,
    },
    /// A table was dropped.
    DropTable {
        /// Table name
        name: String,
    },
    /// A batch of rows was inserted into a table.
    InsertBatch {
        /// Target table name
        table: String,
        /// Arrow IPC-encoded batch data
        batch_data: Vec<u8>,
    },
    /// A checkpoint marker indicating snapshots are consistent up to this point.
    Checkpoint {
        /// Unix timestamp (seconds since epoch) when the checkpoint was taken
        timestamp: u64,
    },
}

// ---------------------------------------------------------------------------
// Write-Ahead Log
// ---------------------------------------------------------------------------

/// Write-Ahead Log for durability.
///
/// The WAL records every mutation in a sequential, append-only file so that
/// the database state can be recovered after a crash.
pub struct WriteAheadLog {
    /// Path to the WAL file
    path: PathBuf,
    /// Buffered writer for appending entries
    writer: Option<BufWriter<File>>,
    /// Next sequence number to assign
    sequence: u64,
}

impl WriteAheadLog {
    /// Create a new WAL file at the given path, overwriting any existing file.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        let file = File::create(&path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            path,
            writer: Some(writer),
            sequence: 0,
        })
    }

    /// Open an existing WAL file for appending, or create it if it doesn't exist.
    ///
    /// This reads through the existing entries to determine the current
    /// sequence number, then opens the file in append mode.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Determine current sequence by reading existing entries
        let sequence = if path.exists() {
            let file = File::open(&path)?;
            let reader = BufReader::new(file);
            let mut max_seq: u64 = 0;

            for line in reader.lines() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok((seq, _)) = Self::deserialize_entry(&line) {
                    if seq > max_seq {
                        max_seq = seq;
                    }
                }
            }
            max_seq
        } else {
            0
        };

        // Open for appending
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            path,
            writer: Some(writer),
            sequence,
        })
    }

    /// Append a new entry to the WAL and return the assigned sequence number.
    pub fn append(&mut self, entry: &WalEntry) -> Result<u64> {
        self.sequence += 1;
        let seq = self.sequence;

        let serialized = Self::serialize_entry(entry)?;
        let line = format!("{}|{}\n", seq, serialized);

        if let Some(ref mut writer) = self.writer {
            writer.write_all(line.as_bytes())?;
        } else {
            return Err(BlazeError::execution("WAL writer is not open"));
        }

        Ok(seq)
    }

    /// Replay all entries in the WAL, returning them in sequence order.
    pub fn replay(&self) -> Result<Vec<(u64, WalEntry)>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match Self::deserialize_entry(trimmed) {
                Ok(entry) => entries.push(entry),
                Err(e) => {
                    // Log warning but continue - partial WAL lines can happen
                    // during crash recovery
                    tracing::warn!("Skipping corrupt WAL entry: {}", e);
                }
            }
        }

        // Sort by sequence number to ensure correct ordering
        entries.sort_by_key(|(seq, _)| *seq);
        Ok(entries)
    }

    /// Flush all buffered writes to disk.
    pub fn flush(&mut self) -> Result<()> {
        if let Some(ref mut writer) = self.writer {
            writer.flush()?;
        }
        Ok(())
    }

    /// Truncate the WAL file, discarding all entries.
    ///
    /// This is typically called after a successful checkpoint, since all
    /// mutations are captured in the snapshot.
    pub fn truncate(&mut self) -> Result<()> {
        // Close the current writer
        self.writer = None;

        // Truncate by recreating the file
        let file = File::create(&self.path)?;
        self.writer = Some(BufWriter::new(file));
        self.sequence = 0;

        Ok(())
    }

    /// Serialize a WAL entry into the `TYPE|DATA` portion of a WAL line.
    fn serialize_entry(entry: &WalEntry) -> Result<String> {
        match entry {
            WalEntry::CreateTable { name, schema } => {
                let arrow_schema = schema.to_arrow();
                let schema_json = serde_json::to_string(&arrow_schema).map_err(|e| {
                    BlazeError::execution(format!("Failed to serialize schema: {}", e))
                })?;
                let data = serde_json::json!({
                    "name": name,
                    "schema": schema_json,
                });
                Ok(format!(
                    "CREATE_TABLE|{}",
                    serde_json::to_string(&data).map_err(|e| {
                        BlazeError::execution(format!(
                            "Failed to serialize CREATE_TABLE entry: {}",
                            e
                        ))
                    })?
                ))
            }
            WalEntry::DropTable { name } => {
                let data = serde_json::json!({ "name": name });
                Ok(format!(
                    "DROP_TABLE|{}",
                    serde_json::to_string(&data).map_err(|e| {
                        BlazeError::execution(format!(
                            "Failed to serialize DROP_TABLE entry: {}",
                            e
                        ))
                    })?
                ))
            }
            WalEntry::InsertBatch { table, batch_data } => {
                let encoded = BASE64_ENGINE.encode(batch_data);
                let data = serde_json::json!({
                    "table": table,
                    "batch_data": encoded,
                });
                Ok(format!(
                    "INSERT|{}",
                    serde_json::to_string(&data).map_err(|e| {
                        BlazeError::execution(format!("Failed to serialize INSERT entry: {}", e))
                    })?
                ))
            }
            WalEntry::Checkpoint { timestamp } => {
                let data = serde_json::json!({ "timestamp": timestamp });
                Ok(format!(
                    "CHECKPOINT|{}",
                    serde_json::to_string(&data).map_err(|e| {
                        BlazeError::execution(format!(
                            "Failed to serialize CHECKPOINT entry: {}",
                            e
                        ))
                    })?
                ))
            }
        }
    }

    /// Deserialize a full WAL line (`SEQ|TYPE|DATA`) into a sequence number and entry.
    fn deserialize_entry(line: &str) -> Result<(u64, WalEntry)> {
        // Format: SEQ|TYPE|DATA
        let first_pipe = line
            .find('|')
            .ok_or_else(|| BlazeError::execution("Invalid WAL line: missing sequence separator"))?;

        let seq_str = &line[..first_pipe];
        let rest = &line[first_pipe + 1..];

        let seq: u64 = seq_str
            .parse()
            .map_err(|e| BlazeError::execution(format!("Invalid WAL sequence number: {}", e)))?;

        let second_pipe = rest
            .find('|')
            .ok_or_else(|| BlazeError::execution("Invalid WAL line: missing type separator"))?;

        let entry_type = &rest[..second_pipe];
        let data_str = &rest[second_pipe + 1..];

        let entry = match entry_type {
            "CREATE_TABLE" => {
                let data: serde_json::Value = serde_json::from_str(data_str).map_err(|e| {
                    BlazeError::execution(format!("Failed to parse CREATE_TABLE data: {}", e))
                })?;

                let name = data["name"]
                    .as_str()
                    .ok_or_else(|| BlazeError::execution("Missing 'name' in CREATE_TABLE"))?
                    .to_string();

                let schema_json_str = data["schema"]
                    .as_str()
                    .ok_or_else(|| BlazeError::execution("Missing 'schema' in CREATE_TABLE"))?;

                let arrow_schema: arrow::datatypes::Schema = serde_json::from_str(schema_json_str)
                    .map_err(|e| {
                        BlazeError::execution(format!("Failed to deserialize Arrow schema: {}", e))
                    })?;

                let schema = Schema::from_arrow(&arrow_schema)?;

                WalEntry::CreateTable { name, schema }
            }
            "DROP_TABLE" => {
                let data: serde_json::Value = serde_json::from_str(data_str).map_err(|e| {
                    BlazeError::execution(format!("Failed to parse DROP_TABLE data: {}", e))
                })?;

                let name = data["name"]
                    .as_str()
                    .ok_or_else(|| BlazeError::execution("Missing 'name' in DROP_TABLE"))?
                    .to_string();

                WalEntry::DropTable { name }
            }
            "INSERT" => {
                let data: serde_json::Value = serde_json::from_str(data_str).map_err(|e| {
                    BlazeError::execution(format!("Failed to parse INSERT data: {}", e))
                })?;

                let table = data["table"]
                    .as_str()
                    .ok_or_else(|| BlazeError::execution("Missing 'table' in INSERT"))?
                    .to_string();

                let encoded = data["batch_data"]
                    .as_str()
                    .ok_or_else(|| BlazeError::execution("Missing 'batch_data' in INSERT"))?;

                let batch_data = BASE64_ENGINE.decode(encoded).map_err(|e| {
                    BlazeError::execution(format!("Failed to decode base64 batch data: {}", e))
                })?;

                WalEntry::InsertBatch { table, batch_data }
            }
            "CHECKPOINT" => {
                let data: serde_json::Value = serde_json::from_str(data_str).map_err(|e| {
                    BlazeError::execution(format!("Failed to parse CHECKPOINT data: {}", e))
                })?;

                let timestamp = data["timestamp"]
                    .as_u64()
                    .ok_or_else(|| BlazeError::execution("Missing 'timestamp' in CHECKPOINT"))?;

                WalEntry::Checkpoint { timestamp }
            }
            other => {
                return Err(BlazeError::execution(format!(
                    "Unknown WAL entry type: {}",
                    other
                )));
            }
        };

        Ok((seq, entry))
    }
}

// ---------------------------------------------------------------------------
// Snapshot Manager
// ---------------------------------------------------------------------------

/// Snapshot manager for periodic table state persistence.
///
/// Snapshots store the full state of each table as Arrow IPC stream files,
/// enabling fast recovery without replaying the entire WAL.
pub struct SnapshotManager {
    /// Directory where snapshot files are stored
    snapshot_dir: PathBuf,
}

impl SnapshotManager {
    /// Create a new snapshot manager, creating the directory if needed.
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let snapshot_dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&snapshot_dir)?;
        Ok(Self { snapshot_dir })
    }

    /// Save a table's data as an Arrow IPC stream file.
    ///
    /// The file is written to `{snapshot_dir}/{name}.arrow`.
    pub fn save_table(&self, name: &str, batches: &[RecordBatch]) -> Result<()> {
        if batches.is_empty() {
            // Remove the snapshot file if the table is empty
            let path = self.table_path(name);
            if path.exists() {
                fs::remove_file(&path)?;
            }
            return Ok(());
        }

        let path = self.table_path(name);
        let file = File::create(&path)?;
        let mut writer = StreamWriter::try_new(file, &batches[0].schema())?;

        for batch in batches {
            writer.write(batch)?;
        }

        writer.finish()?;
        Ok(())
    }

    /// Load a table from its snapshot file.
    ///
    /// Returns `None` if no snapshot exists for the given table name.
    /// Returns the Arrow schema and all record batches on success.
    pub fn load_table(
        &self,
        name: &str,
    ) -> Result<Option<(arrow::datatypes::Schema, Vec<RecordBatch>)>> {
        let path = self.table_path(name);
        if !path.exists() {
            return Ok(None);
        }

        let file = File::open(&path)?;
        let reader = StreamReader::try_new(file, None)?;
        let schema = reader.schema().as_ref().clone();

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch = batch_result?;
            batches.push(batch);
        }

        Ok(Some((schema, batches)))
    }

    /// Save all tables in a single operation.
    pub fn save_all_tables(&self, tables: &HashMap<String, Vec<RecordBatch>>) -> Result<()> {
        for (name, batches) in tables {
            self.save_table(name, batches)?;
        }
        Ok(())
    }

    /// List all tables that have snapshots.
    pub fn list_tables(&self) -> Result<Vec<String>> {
        let mut tables = Vec::new();

        if !self.snapshot_dir.exists() {
            return Ok(tables);
        }

        for entry in fs::read_dir(&self.snapshot_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("arrow") {
                if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
                    tables.push(stem.to_string());
                }
            }
        }

        tables.sort();
        Ok(tables)
    }

    /// Remove a table's snapshot file.
    pub fn remove_table(&self, name: &str) -> Result<()> {
        let path = self.table_path(name);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    /// Get the file path for a table snapshot.
    fn table_path(&self, name: &str) -> PathBuf {
        self.snapshot_dir.join(format!("{}.arrow", name))
    }
}

// ---------------------------------------------------------------------------
// Persistent Database
// ---------------------------------------------------------------------------

/// Persistent database combining WAL and snapshots.
///
/// `PersistentDatabase` coordinates the WAL and snapshot manager to provide
/// full durability for the Blaze query engine. The recovery strategy is:
///
/// 1. Load the latest snapshots (full table state)
/// 2. Replay the WAL from the beginning to apply any mutations made after the
///    last checkpoint
///
/// Checkpoints save all table state to snapshots and truncate the WAL.
pub struct PersistentDatabase {
    /// Write-ahead log for recording mutations
    wal: WriteAheadLog,
    /// Snapshot manager for full table persistence
    snapshot_manager: SnapshotManager,
    /// Root database directory
    db_path: PathBuf,
}

impl PersistentDatabase {
    /// Open a persistent database at the given path.
    ///
    /// Creates the directory structure if it doesn't exist. The layout is:
    /// ```text
    /// {path}/
    ///   wal.log          - Write-ahead log
    ///   snapshots/       - Table snapshot directory
    ///     table1.arrow
    ///     table2.arrow
    /// ```
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let db_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&db_path)?;

        let wal_path = db_path.join("wal.log");
        let snapshot_dir = db_path.join("snapshots");

        let wal = WriteAheadLog::open(&wal_path)?;
        let snapshot_manager = SnapshotManager::new(&snapshot_dir)?;

        Ok(Self {
            wal,
            snapshot_manager,
            db_path,
        })
    }

    /// Log a CREATE TABLE operation to the WAL.
    pub fn log_create_table(&mut self, name: &str, schema: &Schema) -> Result<()> {
        let entry = WalEntry::CreateTable {
            name: name.to_string(),
            schema: schema.clone(),
        };
        self.wal.append(&entry)?;
        self.wal.flush()?;
        Ok(())
    }

    /// Log a DROP TABLE operation to the WAL.
    pub fn log_drop_table(&mut self, name: &str) -> Result<()> {
        let entry = WalEntry::DropTable {
            name: name.to_string(),
        };
        self.wal.append(&entry)?;
        self.wal.flush()?;
        Ok(())
    }

    /// Log an INSERT operation to the WAL.
    ///
    /// The record batch is serialized to Arrow IPC format and base64-encoded
    /// for storage in the text-based WAL.
    pub fn log_insert(&mut self, table: &str, batch: &RecordBatch) -> Result<()> {
        let batch_data = Self::serialize_batch(batch)?;
        let entry = WalEntry::InsertBatch {
            table: table.to_string(),
            batch_data,
        };
        self.wal.append(&entry)?;
        self.wal.flush()?;
        Ok(())
    }

    /// Create a checkpoint by saving all tables to snapshots and truncating the WAL.
    ///
    /// After a successful checkpoint, the WAL is cleared since all state is
    /// captured in the snapshots.
    pub fn checkpoint(&mut self, tables: &HashMap<String, Vec<RecordBatch>>) -> Result<()> {
        // Save all table data to snapshots
        self.snapshot_manager.save_all_tables(tables)?;

        // Write checkpoint marker before truncating
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = WalEntry::Checkpoint { timestamp };
        self.wal.append(&entry)?;
        self.wal.flush()?;

        // Truncate the WAL since snapshots are now up to date
        self.wal.truncate()?;

        Ok(())
    }

    /// Recover WAL entries for replay.
    ///
    /// Returns all WAL entries in sequence order. The caller should apply
    /// these entries on top of the snapshot state to reconstruct the
    /// current database state.
    pub fn recover(&self) -> Result<Vec<WalEntry>> {
        let entries = self.wal.replay()?;
        Ok(entries.into_iter().map(|(_, entry)| entry).collect())
    }

    /// Load all table snapshots.
    ///
    /// Returns a map from table name to (Arrow schema, record batches).
    pub fn load_snapshot_tables(
        &self,
    ) -> Result<HashMap<String, (arrow::datatypes::Schema, Vec<RecordBatch>)>> {
        let mut tables = HashMap::new();
        let table_names = self.snapshot_manager.list_tables()?;

        for name in table_names {
            if let Some(data) = self.snapshot_manager.load_table(&name)? {
                tables.insert(name, data);
            }
        }

        Ok(tables)
    }

    /// Get the database path.
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Serialize a RecordBatch to Arrow IPC format bytes.
    fn serialize_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
            writer.write(batch)?;
            writer.finish()?;
        }
        Ok(buf)
    }

    /// Deserialize a RecordBatch from Arrow IPC format bytes.
    pub fn deserialize_batch(data: &[u8]) -> Result<Vec<RecordBatch>> {
        let cursor = Cursor::new(data);
        let reader = StreamReader::try_new(cursor, None)?;
        let mut batches = Vec::new();

        for batch_result in reader {
            let batch = batch_result?;
            batches.push(batch);
        }

        Ok(batches)
    }
}

// ---------------------------------------------------------------------------
// Columnar Storage Engine
// ---------------------------------------------------------------------------

/// Magic bytes for the Blaze columnar file format.
#[allow(dead_code)]
const BLAZE_FORMAT_MAGIC: &[u8; 4] = b"BLZC";
const BLAZE_FORMAT_VERSION: u32 = 1;

/// Compression codecs available for column chunks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionCodec {
    None,
    Dictionary,
    RunLengthEncoding,
    DeltaEncoding,
    Snappy,
}

impl CompressionCodec {
    /// Compress data using the selected codec.
    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Dictionary => {
                // Simple dictionary encoding: collect unique values, emit index table + indices.
                // Format: [num_entries:u32][entry_len:u32][entry_bytes...]*[index:u32]*
                let mut dict: Vec<Vec<u8>> = Vec::new();
                let mut indices: Vec<u32> = Vec::new();
                // Treat input as length-prefixed values (4-byte LE length + bytes)
                let mut cursor = 0;
                while cursor + 4 <= data.len() {
                    let val_len =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + val_len > data.len() {
                        break;
                    }
                    let val = &data[cursor..cursor + val_len];
                    cursor += val_len;
                    let idx = if let Some(pos) = dict.iter().position(|v| v == val) {
                        pos as u32
                    } else {
                        let pos = dict.len() as u32;
                        dict.push(val.to_vec());
                        pos
                    };
                    indices.push(idx);
                }
                let mut out = Vec::new();
                out.extend_from_slice(&(dict.len() as u32).to_le_bytes());
                for entry in &dict {
                    out.extend_from_slice(&(entry.len() as u32).to_le_bytes());
                    out.extend_from_slice(entry);
                }
                for idx in &indices {
                    out.extend_from_slice(&idx.to_le_bytes());
                }
                Ok(out)
            }
            CompressionCodec::RunLengthEncoding => {
                // RLE: [value_byte][count:u32] runs
                if data.is_empty() {
                    return Ok(Vec::new());
                }
                let mut out = Vec::new();
                let mut current = data[0];
                let mut count: u32 = 1;
                for &b in &data[1..] {
                    if b == current && count < u32::MAX {
                        count += 1;
                    } else {
                        out.push(current);
                        out.extend_from_slice(&count.to_le_bytes());
                        current = b;
                        count = 1;
                    }
                }
                out.push(current);
                out.extend_from_slice(&count.to_le_bytes());
                Ok(out)
            }
            CompressionCodec::DeltaEncoding => {
                // Delta encoding for 4-byte LE integers
                if data.len() < 4 {
                    return Ok(data.to_vec());
                }
                let mut out = Vec::new();
                let num_values = data.len() / 4;
                // Write first value as-is
                let first = i32::from_le_bytes(data[0..4].try_into().unwrap());
                out.extend_from_slice(&first.to_le_bytes());
                let mut prev = first;
                for i in 1..num_values {
                    let offset = i * 4;
                    let val = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    let delta = val.wrapping_sub(prev);
                    out.extend_from_slice(&delta.to_le_bytes());
                    prev = val;
                }
                Ok(out)
            }
            CompressionCodec::Snappy => {
                // Lightweight compression: store original with a length prefix
                let mut out = Vec::with_capacity(4 + data.len());
                out.extend_from_slice(&(data.len() as u32).to_le_bytes());
                // Simple byte-packing (real snappy would use LZ77-style)
                out.extend_from_slice(data);
                Ok(out)
            }
        }
    }

    /// Decompress data back to the original form.
    pub fn decompress(&self, data: &[u8], uncompressed_size: usize) -> Result<Vec<u8>> {
        match self {
            CompressionCodec::None => Ok(data.to_vec()),
            CompressionCodec::Dictionary => {
                if data.len() < 4 {
                    return Err(BlazeError::execution("Invalid dictionary compressed data"));
                }
                let num_entries = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                let mut cursor = 4;
                let mut dict: Vec<Vec<u8>> = Vec::with_capacity(num_entries);
                for _ in 0..num_entries {
                    if cursor + 4 > data.len() {
                        return Err(BlazeError::execution("Truncated dictionary data"));
                    }
                    let entry_len =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if cursor + entry_len > data.len() {
                        return Err(BlazeError::execution("Truncated dictionary entry"));
                    }
                    dict.push(data[cursor..cursor + entry_len].to_vec());
                    cursor += entry_len;
                }
                let mut out = Vec::with_capacity(uncompressed_size);
                while cursor + 4 <= data.len() {
                    let idx =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    if idx >= dict.len() {
                        return Err(BlazeError::execution("Invalid dictionary index"));
                    }
                    out.extend_from_slice(&(dict[idx].len() as u32).to_le_bytes());
                    out.extend_from_slice(&dict[idx]);
                }
                Ok(out)
            }
            CompressionCodec::RunLengthEncoding => {
                if data.is_empty() {
                    return Ok(Vec::new());
                }
                let mut out = Vec::with_capacity(uncompressed_size);
                let mut cursor = 0;
                while cursor + 5 <= data.len() {
                    let value = data[cursor];
                    cursor += 1;
                    let count =
                        u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
                    cursor += 4;
                    out.extend(std::iter::repeat_n(value, count));
                }
                Ok(out)
            }
            CompressionCodec::DeltaEncoding => {
                if data.len() < 4 {
                    return Ok(data.to_vec());
                }
                let mut out = Vec::with_capacity(uncompressed_size);
                let num_values = data.len() / 4;
                let first = i32::from_le_bytes(data[0..4].try_into().unwrap());
                out.extend_from_slice(&first.to_le_bytes());
                let mut prev = first;
                for i in 1..num_values {
                    let offset = i * 4;
                    let delta = i32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
                    let val = prev.wrapping_add(delta);
                    out.extend_from_slice(&val.to_le_bytes());
                    prev = val;
                }
                Ok(out)
            }
            CompressionCodec::Snappy => {
                if data.len() < 4 {
                    return Err(BlazeError::execution("Invalid snappy compressed data"));
                }
                let original_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
                if data.len() < 4 + original_len {
                    return Err(BlazeError::execution("Truncated snappy data"));
                }
                Ok(data[4..4 + original_len].to_vec())
            }
        }
    }

    /// Human-readable name for the codec.
    pub fn name(&self) -> &str {
        match self {
            CompressionCodec::None => "none",
            CompressionCodec::Dictionary => "dictionary",
            CompressionCodec::RunLengthEncoding => "rle",
            CompressionCodec::DeltaEncoding => "delta",
            CompressionCodec::Snappy => "snappy",
        }
    }
}

/// Zone map (min/max statistics) for a column chunk, enabling predicate pushdown.
#[derive(Debug, Clone)]
pub struct ZoneMap {
    pub column_name: String,
    pub min_value: Option<Vec<u8>>,
    pub max_value: Option<Vec<u8>>,
    pub null_count: u64,
    pub row_count: u64,
    pub has_nulls: bool,
}

impl ZoneMap {
    pub fn new(column_name: impl Into<String>, row_count: u64) -> Self {
        Self {
            column_name: column_name.into(),
            min_value: None,
            max_value: None,
            null_count: 0,
            row_count,
            has_nulls: false,
        }
    }

    pub fn with_bounds(mut self, min: Vec<u8>, max: Vec<u8>) -> Self {
        self.min_value = Some(min);
        self.max_value = Some(max);
        self
    }

    /// Returns true if the zone map range might contain the given value.
    pub fn might_contain_value(&self, value: &[u8]) -> bool {
        match (&self.min_value, &self.max_value) {
            (Some(min), Some(max)) => value >= min.as_slice() && value <= max.as_slice(),
            _ => true, // No bounds means we can't exclude anything
        }
    }

    /// Returns true if the zone map range might overlap with [min, max].
    pub fn might_contain_range(&self, min: &[u8], max: &[u8]) -> bool {
        match (&self.min_value, &self.max_value) {
            (Some(zone_min), Some(zone_max)) => {
                max >= zone_min.as_slice() && min <= zone_max.as_slice()
            }
            _ => true,
        }
    }
}

/// Bloom filter for column chunks, supporting fast point-lookup exclusion.
#[derive(Debug, Clone)]
pub struct ColumnChunkBloomFilter {
    bits: Vec<u8>,
    num_hashes: usize,
    num_items: usize,
}

impl ColumnChunkBloomFilter {
    /// Create a new bloom filter sized for `expected_items` with false-positive probability `fpp`.
    pub fn new(expected_items: usize, fpp: f64) -> Self {
        let expected_items = expected_items.max(1);
        let fpp = fpp.max(0.0001);
        let num_bits = (-(expected_items as f64) * fpp.ln() / (2.0_f64.ln().powi(2)))
            .ceil()
            .max(8.0) as usize;
        let num_hashes = ((num_bits as f64 / expected_items as f64) * 2.0_f64.ln())
            .ceil()
            .max(1.0) as usize;
        let num_bytes = num_bits.div_ceil(8);
        Self {
            bits: vec![0u8; num_bytes],
            num_hashes,
            num_items: 0,
        }
    }

    /// Insert a value into the bloom filter.
    pub fn insert(&mut self, value: &[u8]) {
        let num_bits = self.bits.len() * 8;
        for i in 0..self.num_hashes {
            let hash = self.hash(value, i);
            let bit_pos = hash % num_bits;
            self.bits[bit_pos / 8] |= 1 << (bit_pos % 8);
        }
        self.num_items += 1;
    }

    /// Returns true if the value might be in the set (false positives possible).
    pub fn might_contain(&self, value: &[u8]) -> bool {
        let num_bits = self.bits.len() * 8;
        for i in 0..self.num_hashes {
            let hash = self.hash(value, i);
            let bit_pos = hash % num_bits;
            if self.bits[bit_pos / 8] & (1 << (bit_pos % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Size of the serialized bloom filter in bytes.
    pub fn serialized_size(&self) -> usize {
        // 4 bytes num_hashes + 4 bytes num_items + 4 bytes bits_len + bits
        12 + self.bits.len()
    }

    /// Simple hash function using FNV-1a with seed mixing.
    fn hash(&self, value: &[u8], seed: usize) -> usize {
        let mut h: u64 = 0xcbf29ce484222325_u64.wrapping_add(seed as u64 * 0x100000001b3);
        for &b in value {
            h ^= b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        h as usize
    }
}

/// Column chunk metadata in the file format.
#[derive(Debug, Clone)]
pub struct ColumnChunkMeta {
    pub column_name: String,
    pub compression: CompressionCodec,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub offset: u64,
    pub zone_map: ZoneMap,
    pub bloom_filter: Option<ColumnChunkBloomFilter>,
}

/// Row group metadata.
#[derive(Debug, Clone)]
pub struct RowGroupMeta {
    pub row_count: u64,
    pub column_chunks: Vec<ColumnChunkMeta>,
}

/// File-level metadata for the Blaze columnar format.
#[derive(Debug, Clone)]
pub struct BlazeFileMetadata {
    pub version: u32,
    pub schema: Vec<(String, String)>,
    pub row_groups: Vec<RowGroupMeta>,
    pub total_rows: u64,
    pub created_at: u64,
}

impl Default for BlazeFileMetadata {
    fn default() -> Self {
        Self::new()
    }
}

impl BlazeFileMetadata {
    pub fn new() -> Self {
        let created_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            version: BLAZE_FORMAT_VERSION,
            schema: Vec::new(),
            row_groups: Vec::new(),
            total_rows: 0,
            created_at,
        }
    }

    pub fn total_compressed_size(&self) -> u64 {
        self.row_groups
            .iter()
            .flat_map(|rg| rg.column_chunks.iter())
            .map(|cc| cc.compressed_size)
            .sum()
    }

    pub fn total_uncompressed_size(&self) -> u64 {
        self.row_groups
            .iter()
            .flat_map(|rg| rg.column_chunks.iter())
            .map(|cc| cc.uncompressed_size)
            .sum()
    }

    pub fn compression_ratio(&self) -> f64 {
        let compressed = self.total_compressed_size();
        let uncompressed = self.total_uncompressed_size();
        if compressed == 0 {
            1.0
        } else {
            uncompressed as f64 / compressed as f64
        }
    }
}

/// In-memory representation of a Blaze columnar file.
#[derive(Debug)]
pub struct BlazeFileData {
    pub metadata: BlazeFileMetadata,
    pub data: Vec<Vec<u8>>,
}

/// Writer for the Blaze native columnar format.
pub struct ColumnarWriter {
    compression: CompressionCodec,
    row_group_size: usize,
    enable_bloom_filters: bool,
}

impl Default for ColumnarWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ColumnarWriter {
    pub fn new() -> Self {
        Self {
            compression: CompressionCodec::None,
            row_group_size: 64 * 1024,
            enable_bloom_filters: false,
        }
    }

    pub fn with_compression(mut self, codec: CompressionCodec) -> Self {
        self.compression = codec;
        self
    }

    pub fn with_row_group_size(mut self, size: usize) -> Self {
        self.row_group_size = size.max(1);
        self
    }

    pub fn with_bloom_filters(mut self, enable: bool) -> Self {
        self.enable_bloom_filters = enable;
        self
    }

    /// Write record batches into the Blaze native columnar format.
    pub fn write_batches(&self, batches: &[RecordBatch]) -> Result<BlazeFileData> {
        if batches.is_empty() {
            return Ok(BlazeFileData {
                metadata: BlazeFileMetadata::new(),
                data: Vec::new(),
            });
        }

        let schema = batches[0].schema();
        let mut metadata = BlazeFileMetadata::new();
        metadata.schema = schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), format!("{}", f.data_type())))
            .collect();

        let mut all_data: Vec<Vec<u8>> = Vec::new();
        let mut global_offset: u64 = 0;

        // Serialize each batch as a row group
        for batch in batches {
            let row_count = batch.num_rows() as u64;
            metadata.total_rows += row_count;

            let mut column_chunks = Vec::new();

            for col_idx in 0..batch.num_columns() {
                let field = schema.field(col_idx);
                let column = batch.column(col_idx);

                // Serialize column data via Arrow IPC into bytes
                let col_batch = RecordBatch::try_new(
                    std::sync::Arc::new(arrow::datatypes::Schema::new(vec![field.clone()])),
                    vec![column.clone()],
                )
                .map_err(|e| {
                    BlazeError::execution(format!("Failed to create column batch: {e}"))
                })?;

                let mut buf = Vec::new();
                {
                    let mut writer =
                        StreamWriter::try_new(&mut buf, &col_batch.schema()).map_err(|e| {
                            BlazeError::execution(format!("Failed to create IPC writer: {e}"))
                        })?;
                    writer.write(&col_batch).map_err(|e| {
                        BlazeError::execution(format!("Failed to write column IPC: {e}"))
                    })?;
                    writer.finish().map_err(|e| {
                        BlazeError::execution(format!("Failed to finish IPC writer: {e}"))
                    })?;
                }

                let uncompressed_size = buf.len() as u64;
                let compressed = self.compression.compress(&buf)?;
                let compressed_size = compressed.len() as u64;

                // Build zone map from raw column bytes
                let mut zone_map = ZoneMap::new(field.name().clone(), row_count);
                zone_map.null_count = column.null_count() as u64;
                zone_map.has_nulls = column.null_count() > 0;

                // Compute min/max from the serialized bytes (use IPC bytes as proxy)
                if buf.len() >= 8 {
                    let min_bytes = buf[..8.min(buf.len())].to_vec();
                    let max_bytes = buf[buf.len().saturating_sub(8)..].to_vec();
                    if min_bytes <= max_bytes {
                        zone_map = zone_map.with_bounds(min_bytes, max_bytes);
                    } else {
                        zone_map = zone_map.with_bounds(max_bytes, min_bytes);
                    }
                }

                // Optionally build bloom filter
                let bloom_filter = if self.enable_bloom_filters {
                    let mut bf = ColumnChunkBloomFilter::new(batch.num_rows().max(1), 0.01);
                    // Insert IPC-serialized chunks as bloom entries
                    let chunk_size = 8;
                    let mut pos = 0;
                    while pos + chunk_size <= buf.len() {
                        bf.insert(&buf[pos..pos + chunk_size]);
                        pos += chunk_size;
                    }
                    if pos < buf.len() {
                        bf.insert(&buf[pos..]);
                    }
                    Some(bf)
                } else {
                    None
                };

                column_chunks.push(ColumnChunkMeta {
                    column_name: field.name().clone(),
                    compression: self.compression,
                    compressed_size,
                    uncompressed_size,
                    offset: global_offset,
                    zone_map,
                    bloom_filter,
                });

                all_data.push(compressed);
                global_offset += compressed_size;
            }

            metadata.row_groups.push(RowGroupMeta {
                row_count,
                column_chunks,
            });
        }

        Ok(BlazeFileData {
            metadata,
            data: all_data,
        })
    }
}

/// Reader for the Blaze native columnar format.
pub struct ColumnarReader;

impl ColumnarReader {
    /// Read all record batches from a BlazeFileData.
    pub fn read(file_data: &BlazeFileData) -> Result<Vec<RecordBatch>> {
        Self::read_with_filter(file_data, None, None)
    }

    /// Read with optional column pruning and zone map filtering.
    pub fn read_with_filter(
        file_data: &BlazeFileData,
        columns: Option<&[String]>,
        zone_map_filter: Option<&dyn Fn(&ZoneMap) -> bool>,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        let mut data_idx = 0;

        for rg in &file_data.metadata.row_groups {
            // (data_index, column_chunk_meta) for selected columns
            let mut selected_columns: Vec<(usize, &ColumnChunkMeta)> = Vec::new();
            let rg_base_idx = data_idx;

            for (col_idx, cc) in rg.column_chunks.iter().enumerate() {
                let this_data_idx = rg_base_idx + col_idx;

                // Column pruning
                if let Some(cols) = columns {
                    if !cols.contains(&cc.column_name) {
                        continue;
                    }
                }

                // Zone map filtering
                if let Some(ref filter) = zone_map_filter {
                    if !filter(&cc.zone_map) {
                        continue;
                    }
                }

                selected_columns.push((this_data_idx, cc));
            }

            // Advance data_idx past all columns in this row group
            data_idx += rg.column_chunks.len();

            if selected_columns.is_empty() {
                continue;
            }

            // Read and decompress each selected column chunk
            let mut col_batches: Vec<RecordBatch> = Vec::new();
            for (di, cc) in &selected_columns {
                let compressed_data = &file_data.data[*di];
                let decompressed = cc
                    .compression
                    .decompress(compressed_data, cc.uncompressed_size as usize)?;

                let cursor = Cursor::new(&decompressed);
                let reader = StreamReader::try_new(cursor, None).map_err(|e| {
                    BlazeError::execution(format!("Failed to read column IPC: {e}"))
                })?;
                for batch_result in reader {
                    let batch = batch_result.map_err(|e| {
                        BlazeError::execution(format!("Failed to read IPC batch: {e}"))
                    })?;
                    col_batches.push(batch);
                }
            }

            // Merge column batches into a single batch
            if !col_batches.is_empty() {
                let mut fields = Vec::new();
                let mut arrays = Vec::new();
                for cb in &col_batches {
                    for i in 0..cb.num_columns() {
                        fields.push(cb.schema().field(i).clone());
                        arrays.push(cb.column(i).clone());
                    }
                }
                let merged_schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
                let merged_batch = RecordBatch::try_new(merged_schema, arrays).map_err(|e| {
                    BlazeError::execution(format!("Failed to merge column batches: {e}"))
                })?;
                batches.push(merged_batch);
            }
        }

        Ok(batches)
    }

    /// Access file metadata.
    pub fn metadata(file_data: &BlazeFileData) -> &BlazeFileMetadata {
        &file_data.metadata
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Schema as ArrowSchema;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::types::{DataType, Field};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));

        let id_array = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec![
            Some("Alice"),
            Some("Bob"),
            Some("Charlie"),
        ]));

        RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    #[test]
    fn test_wal_new_and_append() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let mut wal = WriteAheadLog::new(&wal_path).unwrap();

        let entry = WalEntry::CreateTable {
            name: "users".to_string(),
            schema: create_test_schema(),
        };

        let seq = wal.append(&entry).unwrap();
        assert_eq!(seq, 1);
        wal.flush().unwrap();

        // Replay and verify
        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, 1);

        if let WalEntry::CreateTable { name, schema } = &entries[0].1 {
            assert_eq!(name, "users");
            assert_eq!(schema.len(), 2);
        } else {
            panic!("Expected CreateTable entry");
        }
    }

    #[test]
    fn test_wal_multiple_entries() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let mut wal = WriteAheadLog::new(&wal_path).unwrap();

        // Create table
        let entry1 = WalEntry::CreateTable {
            name: "users".to_string(),
            schema: create_test_schema(),
        };
        wal.append(&entry1).unwrap();

        // Insert batch
        let batch = create_test_batch();
        let batch_data = PersistentDatabase::serialize_batch(&batch).unwrap();
        let entry2 = WalEntry::InsertBatch {
            table: "users".to_string(),
            batch_data,
        };
        wal.append(&entry2).unwrap();

        // Drop table
        let entry3 = WalEntry::DropTable {
            name: "users".to_string(),
        };
        wal.append(&entry3).unwrap();

        wal.flush().unwrap();

        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].0, 1);
        assert_eq!(entries[1].0, 2);
        assert_eq!(entries[2].0, 3);
    }

    #[test]
    fn test_wal_open_existing() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        // Create and write
        {
            let mut wal = WriteAheadLog::new(&wal_path).unwrap();
            let entry = WalEntry::CreateTable {
                name: "t1".to_string(),
                schema: create_test_schema(),
            };
            wal.append(&entry).unwrap();
            wal.flush().unwrap();
        }

        // Open existing and append
        {
            let mut wal = WriteAheadLog::open(&wal_path).unwrap();
            let entry = WalEntry::DropTable {
                name: "t1".to_string(),
            };
            let seq = wal.append(&entry).unwrap();
            assert_eq!(seq, 2);
            wal.flush().unwrap();
        }

        // Verify both entries
        let wal = WriteAheadLog::open(&wal_path).unwrap();
        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn test_wal_truncate() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let mut wal = WriteAheadLog::new(&wal_path).unwrap();
        let entry = WalEntry::CreateTable {
            name: "t1".to_string(),
            schema: create_test_schema(),
        };
        wal.append(&entry).unwrap();
        wal.flush().unwrap();

        // Truncate
        wal.truncate().unwrap();

        let entries = wal.replay().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_snapshot_save_and_load() {
        let tmp = TempDir::new().unwrap();
        let snap_dir = tmp.path().join("snapshots");

        let manager = SnapshotManager::new(&snap_dir).unwrap();
        let batch = create_test_batch();

        manager.save_table("users", &[batch.clone()]).unwrap();

        let loaded = manager.load_table("users").unwrap();
        assert!(loaded.is_some());

        let (schema, batches) = loaded.unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[test]
    fn test_snapshot_list_tables() {
        let tmp = TempDir::new().unwrap();
        let snap_dir = tmp.path().join("snapshots");

        let manager = SnapshotManager::new(&snap_dir).unwrap();
        let batch = create_test_batch();

        manager.save_table("users", &[batch.clone()]).unwrap();
        manager.save_table("orders", &[batch.clone()]).unwrap();

        let tables = manager.list_tables().unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.contains(&"users".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_snapshot_remove_table() {
        let tmp = TempDir::new().unwrap();
        let snap_dir = tmp.path().join("snapshots");

        let manager = SnapshotManager::new(&snap_dir).unwrap();
        let batch = create_test_batch();

        manager.save_table("users", &[batch]).unwrap();
        assert!(manager.load_table("users").unwrap().is_some());

        manager.remove_table("users").unwrap();
        assert!(manager.load_table("users").unwrap().is_none());
    }

    #[test]
    fn test_snapshot_load_nonexistent() {
        let tmp = TempDir::new().unwrap();
        let snap_dir = tmp.path().join("snapshots");

        let manager = SnapshotManager::new(&snap_dir).unwrap();
        let loaded = manager.load_table("nonexistent").unwrap();
        assert!(loaded.is_none());
    }

    #[test]
    fn test_persistent_database_open() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("testdb");

        let _db = PersistentDatabase::open(&db_path).unwrap();

        // Verify directory structure was created
        assert!(db_path.exists());
        assert!(db_path.join("wal.log").exists());
        assert!(db_path.join("snapshots").exists());
    }

    #[test]
    fn test_persistent_database_log_and_recover() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("testdb");

        let schema = create_test_schema();
        let batch = create_test_batch();

        // Write operations
        {
            let mut db = PersistentDatabase::open(&db_path).unwrap();
            db.log_create_table("users", &schema).unwrap();
            db.log_insert("users", &batch).unwrap();
        }

        // Recover
        {
            let db = PersistentDatabase::open(&db_path).unwrap();
            let entries = db.recover().unwrap();
            assert_eq!(entries.len(), 2);

            match &entries[0] {
                WalEntry::CreateTable { name, .. } => assert_eq!(name, "users"),
                _ => panic!("Expected CreateTable"),
            }

            match &entries[1] {
                WalEntry::InsertBatch { table, batch_data } => {
                    assert_eq!(table, "users");
                    let batches = PersistentDatabase::deserialize_batch(batch_data).unwrap();
                    assert_eq!(batches.len(), 1);
                    assert_eq!(batches[0].num_rows(), 3);
                }
                _ => panic!("Expected InsertBatch"),
            }
        }
    }

    #[test]
    fn test_persistent_database_checkpoint() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("testdb");

        let schema = create_test_schema();
        let batch = create_test_batch();

        {
            let mut db = PersistentDatabase::open(&db_path).unwrap();
            db.log_create_table("users", &schema).unwrap();
            db.log_insert("users", &batch).unwrap();

            // Checkpoint
            let mut tables = HashMap::new();
            tables.insert("users".to_string(), vec![batch.clone()]);
            db.checkpoint(&tables).unwrap();
        }

        // After checkpoint, WAL should be empty
        {
            let db = PersistentDatabase::open(&db_path).unwrap();
            let entries = db.recover().unwrap();
            assert!(entries.is_empty());

            // But snapshots should be available
            let snapshot_tables = db.load_snapshot_tables().unwrap();
            assert_eq!(snapshot_tables.len(), 1);
            assert!(snapshot_tables.contains_key("users"));
            let (_, batches) = &snapshot_tables["users"];
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 3);
        }
    }

    #[test]
    fn test_persistent_database_drop_table() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("testdb");

        let schema = create_test_schema();

        let mut db = PersistentDatabase::open(&db_path).unwrap();
        db.log_create_table("users", &schema).unwrap();
        db.log_drop_table("users").unwrap();

        let entries = db.recover().unwrap();
        assert_eq!(entries.len(), 2);

        match &entries[1] {
            WalEntry::DropTable { name } => assert_eq!(name, "users"),
            _ => panic!("Expected DropTable"),
        }
    }

    #[test]
    fn test_batch_serialization_roundtrip() {
        let batch = create_test_batch();
        let data = PersistentDatabase::serialize_batch(&batch).unwrap();
        let batches = PersistentDatabase::deserialize_batch(&data).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
        assert_eq!(batches[0].num_columns(), 2);

        let id_col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 1);
        assert_eq!(id_col.value(1), 2);
        assert_eq!(id_col.value(2), 3);

        let name_col = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");
        assert_eq!(name_col.value(2), "Charlie");
    }

    #[test]
    fn test_wal_checkpoint_entry() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let mut wal = WriteAheadLog::new(&wal_path).unwrap();

        let entry = WalEntry::Checkpoint {
            timestamp: 1700000000,
        };
        wal.append(&entry).unwrap();
        wal.flush().unwrap();

        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 1);

        if let WalEntry::Checkpoint { timestamp } = &entries[0].1 {
            assert_eq!(*timestamp, 1700000000);
        } else {
            panic!("Expected Checkpoint entry");
        }
    }

    #[test]
    fn test_snapshot_empty_table() {
        let tmp = TempDir::new().unwrap();
        let snap_dir = tmp.path().join("snapshots");

        let manager = SnapshotManager::new(&snap_dir).unwrap();

        // Saving an empty table should not create a file
        manager.save_table("empty", &[]).unwrap();
        assert!(manager.load_table("empty").unwrap().is_none());
    }

    #[test]
    fn test_persistent_database_full_workflow() {
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("testdb");

        let schema = create_test_schema();
        let batch = create_test_batch();

        // Phase 1: Create table, insert data, checkpoint
        {
            let mut db = PersistentDatabase::open(&db_path).unwrap();
            db.log_create_table("users", &schema).unwrap();
            db.log_insert("users", &batch).unwrap();

            let mut tables = HashMap::new();
            tables.insert("users".to_string(), vec![batch.clone()]);
            db.checkpoint(&tables).unwrap();
        }

        // Phase 2: More inserts after checkpoint
        {
            let mut db = PersistentDatabase::open(&db_path).unwrap();

            // Snapshot should exist
            let snap = db.load_snapshot_tables().unwrap();
            assert_eq!(snap.len(), 1);

            // WAL should be empty after checkpoint
            let entries = db.recover().unwrap();
            assert!(entries.is_empty());

            // Add more data
            db.log_insert("users", &batch).unwrap();
        }

        // Phase 3: Recover and verify
        {
            let db = PersistentDatabase::open(&db_path).unwrap();

            // Snapshot still has original data
            let snap = db.load_snapshot_tables().unwrap();
            assert_eq!(snap["users"].1.len(), 1);

            // WAL has the post-checkpoint insert
            let entries = db.recover().unwrap();
            assert_eq!(entries.len(), 1);

            match &entries[0] {
                WalEntry::InsertBatch { table, .. } => assert_eq!(table, "users"),
                _ => panic!("Expected InsertBatch"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Columnar Storage Engine Tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_compression_roundtrip() {
        let codecs = [
            CompressionCodec::None,
            CompressionCodec::RunLengthEncoding,
            CompressionCodec::Snappy,
        ];
        let original = vec![0u8, 0, 0, 1, 1, 1, 2, 2, 3, 3, 3, 3];
        for codec in &codecs {
            let compressed = codec.compress(&original).unwrap();
            let decompressed = codec.decompress(&compressed, original.len()).unwrap();
            assert_eq!(
                decompressed,
                original,
                "Roundtrip failed for codec {}",
                codec.name()
            );
        }
    }

    #[test]
    fn test_delta_encoding_roundtrip() {
        let values: Vec<i32> = vec![10, 12, 15, 20, 28];
        let mut data = Vec::new();
        for v in &values {
            data.extend_from_slice(&v.to_le_bytes());
        }
        let codec = CompressionCodec::DeltaEncoding;
        let compressed = codec.compress(&data).unwrap();
        let decompressed = codec.decompress(&compressed, data.len()).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_zone_map_filtering() {
        let zm = ZoneMap::new("col1", 100).with_bounds(vec![10], vec![50]);

        // Value within range
        assert!(zm.might_contain_value(&[25]));
        // Value at boundaries
        assert!(zm.might_contain_value(&[10]));
        assert!(zm.might_contain_value(&[50]));
        // Value below range
        assert!(!zm.might_contain_value(&[5]));
        // Value above range
        assert!(!zm.might_contain_value(&[60]));

        // Range overlap
        assert!(zm.might_contain_range(&[20], &[30]));
        assert!(zm.might_contain_range(&[1], &[15]));
        // Range entirely below
        assert!(!zm.might_contain_range(&[1], &[9]));
        // Range entirely above
        assert!(!zm.might_contain_range(&[51], &[100]));
    }

    #[test]
    fn test_bloom_filter_membership() {
        let mut bf = ColumnChunkBloomFilter::new(100, 0.01);

        bf.insert(b"hello");
        bf.insert(b"world");
        bf.insert(b"blaze");

        // Inserted values must be found
        assert!(bf.might_contain(b"hello"));
        assert!(bf.might_contain(b"world"));
        assert!(bf.might_contain(b"blaze"));

        // Values never inserted are very unlikely to match
        // (testing several to reduce flakiness from false positives)
        let mut false_positives = 0;
        for i in 0..100 {
            let val = format!("nonexistent_{}", i);
            if bf.might_contain(val.as_bytes()) {
                false_positives += 1;
            }
        }
        assert!(
            false_positives < 10,
            "Too many false positives: {false_positives}/100"
        );

        assert!(bf.serialized_size() > 0);
    }

    #[test]
    fn test_columnar_writer_reader_roundtrip() {
        let batch = create_test_batch();

        let writer = ColumnarWriter::new()
            .with_compression(CompressionCodec::None)
            .with_bloom_filters(true);

        let file_data = writer.write_batches(&[batch.clone()]).unwrap();

        // Verify metadata
        let meta = ColumnarReader::metadata(&file_data);
        assert_eq!(meta.version, BLAZE_FORMAT_VERSION);
        assert_eq!(meta.total_rows, 3);
        assert_eq!(meta.schema.len(), 2);
        assert_eq!(meta.row_groups.len(), 1);
        assert_eq!(meta.row_groups[0].column_chunks.len(), 2);

        // Read back
        let read_batches = ColumnarReader::read(&file_data).unwrap();
        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_rows(), 3);
        assert_eq!(read_batches[0].num_columns(), 2);
    }

    #[test]
    fn test_columnar_metadata_statistics() {
        let batch = create_test_batch();

        let writer = ColumnarWriter::new().with_compression(CompressionCodec::Snappy);
        let file_data = writer.write_batches(&[batch]).unwrap();

        let meta = &file_data.metadata;
        assert_eq!(meta.total_rows, 3);
        assert!(meta.total_compressed_size() > 0);
        assert!(meta.total_uncompressed_size() > 0);
        assert!(meta.compression_ratio() > 0.0);

        // Each row group has zone maps
        for rg in &meta.row_groups {
            for cc in &rg.column_chunks {
                assert_eq!(cc.zone_map.row_count, 3);
                assert!(cc.zone_map.min_value.is_some());
                assert!(cc.zone_map.max_value.is_some());
            }
        }
    }

    #[test]
    fn test_columnar_reader_column_pruning() {
        let batch = create_test_batch();

        let writer = ColumnarWriter::new();
        let file_data = writer.write_batches(&[batch]).unwrap();

        // Read only the "id" column
        let columns = vec!["id".to_string()];
        let read_batches =
            ColumnarReader::read_with_filter(&file_data, Some(&columns), None).unwrap();

        assert_eq!(read_batches.len(), 1);
        assert_eq!(read_batches[0].num_columns(), 1);
        assert_eq!(read_batches[0].schema().field(0).name(), "id");
    }
}
