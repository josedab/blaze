//! Storage layer for Blaze.
//!
//! This module provides readers and writers for various data formats
//! including CSV, Parquet, JSON, and Arrow IPC.

mod memory;
mod csv;
mod parquet;

pub use self::csv::{CsvTable, CsvOptions, write_csv};
pub use self::parquet::{ParquetTable, ParquetOptions, write_parquet, write_parquet_with_options};
pub use memory::MemoryTable;

use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::catalog::TableProvider;
use crate::error::{BlazeError, Result};
use crate::types::Schema;

/// Read a file and return a table provider.
pub fn read_file(path: impl AsRef<Path>) -> Result<Arc<dyn TableProvider>> {
    let path = path.as_ref();
    let extension = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");

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
