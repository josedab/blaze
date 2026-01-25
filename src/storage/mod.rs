//! Storage layer for Blaze.
//!
//! This module provides readers and writers for various data formats
//! including CSV, Parquet, JSON, Arrow IPC, and Delta Lake.

mod csv;
mod delta;
mod memory;
mod object_store;
mod parquet;
mod persistent;

pub use self::csv::{write_csv, CsvOptions, CsvTable};
pub use self::delta::{
    DeltaTable, DeltaTableOptions, DeltaVersionInfo, DeltaWriteMode, DeltaWriteResult,
};
pub use self::object_store::{
    CachingObjectStore, LocalFileSystemStore, ObjectMeta, ObjectPath, ObjectStoreProvider,
    ObjectStoreRegistry, ObjectStoreTable,
};
pub use self::parquet::{write_parquet, write_parquet_with_options, ParquetOptions, ParquetTable};
pub use memory::MemoryTable;
pub use persistent::{
    persistent_table, BufferPool, Page, PageType, PersistentConfig, PersistentTable, WalEntry,
    WalOperation, WriteAheadLog,
};

use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::catalog::TableProvider;
use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// Read a file and return a table provider.
pub fn read_file(path: impl AsRef<Path>) -> Result<Arc<dyn TableProvider>> {
    let path = path.as_ref();

    // Check if it's a Delta table (directory with _delta_log)
    if path.is_dir() && path.join("_delta_log").exists() {
        return Ok(Arc::new(DeltaTable::open(path)?));
    }

    let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");

    match extension.to_lowercase().as_str() {
        "csv" | "tsv" => Ok(Arc::new(CsvTable::open(path)?)),
        "parquet" | "pq" => Ok(Arc::new(ParquetTable::open(path)?)),
        _ => Err(BlazeError::invalid_argument(format!(
            "Unsupported file format: {}",
            extension
        ))),
    }
}

/// Read a CSV file.
pub fn read_csv(path: impl AsRef<Path>) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(CsvTable::open(path)?))
}

/// Read a Parquet file.
pub fn read_parquet(path: impl AsRef<Path>) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(ParquetTable::open(path)?))
}

/// Create an in-memory table from record batches.
pub fn memory_table(batches: Vec<RecordBatch>) -> Result<Arc<dyn TableProvider>> {
    if batches.is_empty() {
        return Err(BlazeError::invalid_argument("Empty batches"));
    }
    let schema = Schema::from_arrow(batches[0].schema().as_ref())?;
    Ok(Arc::new(MemoryTable::new(schema, batches)))
}

/// Read a Delta Lake table.
pub fn read_delta(path: impl AsRef<Path>) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(DeltaTable::open(path)?))
}

/// Read a Delta Lake table at a specific version (time travel).
pub fn read_delta_version(path: impl AsRef<Path>, version: i64) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(DeltaTable::open_at_version(path, version)?))
}

/// Read a Delta Lake table as of a specific timestamp (time travel).
pub fn read_delta_timestamp(
    path: impl AsRef<Path>,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(DeltaTable::open_at_timestamp(path, timestamp)?))
}
