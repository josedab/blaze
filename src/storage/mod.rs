//! Storage layer for Blaze.
//!
//! This module provides readers and writers for various data formats
//! including CSV, Parquet, JSON, Arrow IPC, and Delta Lake.

mod csv;
#[cfg(feature = "lakehouse")]
mod delta;
mod memory;
mod object_store;
mod parquet;
mod persistent;
pub mod type_inference;
pub mod view;

pub use self::csv::{write_csv, CsvOptions, CsvTable};
#[cfg(feature = "lakehouse")]
pub use self::delta::{
    DeltaTable, DeltaTableOptions, DeltaVersionInfo, DeltaWriteMode, DeltaWriteResult,
};
pub use self::object_store::{
    CachingObjectStore, LocalFileSystemStore, ObjectMeta, ObjectPath, ObjectStoreProvider,
    ObjectStoreRegistry, ObjectStoreTable,
};
pub use self::parquet::{write_parquet, write_parquet_with_options, ParquetOptions, ParquetTable};
pub use self::type_inference::{
    infer_csv_schema, infer_json_schema, merge_schemas, InferenceConfig,
};
pub use memory::MemoryTable;
pub use persistent::{
    persistent_table, BufferPool, Page, PageType, PersistentConfig, PersistentTable, WalEntry,
    WalOperation, WriteAheadLog,
};
pub use view::ViewTable;

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
    #[cfg(feature = "lakehouse")]
    if path.is_dir() && path.join("_delta_log").exists() {
        return Ok(Arc::new(DeltaTable::open(path)?));
    }

    let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");

    match extension.to_lowercase().as_str() {
        "csv" | "tsv" => Ok(Arc::new(CsvTable::open(path)?)),
        "parquet" | "pq" => Ok(Arc::new(ParquetTable::open(path)?)),
        "json" | "jsonl" | "ndjson" => read_json_inferred(path),
        _ => Err(BlazeError::invalid_argument(format!(
            "Unsupported file format: {}",
            extension
        ))),
    }
}

/// Read a CSV file with automatic type inference and delimiter detection.
pub fn read_csv_inferred(
    path: impl AsRef<Path>,
    config: Option<InferenceConfig>,
) -> Result<Arc<dyn TableProvider>> {
    let config = config.unwrap_or_default();
    let (schema, delimiter) = infer_csv_schema(path.as_ref(), &config)?;
    let blaze_schema = Schema::from_arrow(&schema)?;
    let options = CsvOptions {
        delimiter,
        has_header: true,
        batch_size: 8192,
        schema_infer_max_records: config.sample_rows,
    };
    Ok(Arc::new(CsvTable::with_schema(path, blaze_schema, options)))
}

/// Read a newline-delimited JSON file with automatic type inference.
pub fn read_json_inferred(path: impl AsRef<Path>) -> Result<Arc<dyn TableProvider>> {
    let config = InferenceConfig::default();
    let schema = infer_json_schema(path.as_ref(), &config)?;

    // Read all lines and build record batches
    let file = std::fs::File::open(path.as_ref())?;
    let reader = std::io::BufReader::new(file);
    let decoder = arrow::json::ReaderBuilder::new(schema.clone()).build(reader)?;

    let mut batches = Vec::new();
    for batch_result in decoder {
        batches.push(batch_result?);
    }

    if batches.is_empty() {
        let blaze_schema = Schema::from_arrow(&schema)?;
        return Ok(Arc::new(MemoryTable::new(blaze_schema, vec![])));
    }

    let blaze_schema = Schema::from_arrow(&schema)?;
    Ok(Arc::new(MemoryTable::new(blaze_schema, batches)))
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
#[cfg(feature = "lakehouse")]
pub fn read_delta(path: impl AsRef<Path>) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(DeltaTable::open(path)?))
}

/// Read a Delta Lake table at a specific version (time travel).
#[cfg(feature = "lakehouse")]
pub fn read_delta_version(path: impl AsRef<Path>, version: i64) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(DeltaTable::open_at_version(path, version)?))
}

/// Read a Delta Lake table as of a specific timestamp (time travel).
#[cfg(feature = "lakehouse")]
pub fn read_delta_timestamp(
    path: impl AsRef<Path>,
    timestamp: chrono::DateTime<chrono::Utc>,
) -> Result<Arc<dyn TableProvider>> {
    Ok(Arc::new(DeltaTable::open_at_timestamp(path, timestamp)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::io::Write;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int64, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("valid batch")
    }

    #[test]
    fn test_read_file_csv() {
        let provider = read_file("data/customers.csv").unwrap();
        assert!(!provider.schema().fields().is_empty());
    }

    #[test]
    fn test_read_file_parquet() {
        let provider = read_file("data/customers.parquet").unwrap();
        assert!(!provider.schema().fields().is_empty());
    }

    #[test]
    fn test_read_file_unsupported_extension() {
        let err = read_file("data/file.xyz").unwrap_err();
        assert!(err.to_string().contains("Unsupported file format"));
    }

    #[test]
    fn test_read_file_nonexistent() {
        let err = read_file("data/does_not_exist.csv").unwrap_err();
        // Should get an I/O or format-related error
        let msg = err.to_string();
        assert!(!msg.is_empty());
    }

    #[test]
    fn test_read_csv() {
        let provider = read_csv("data/customers.csv").unwrap();
        assert!(!provider.schema().fields().is_empty());
    }

    #[test]
    fn test_read_parquet() {
        let provider = read_parquet("data/customers.parquet").unwrap();
        assert!(!provider.schema().fields().is_empty());
    }

    #[test]
    fn test_read_csv_inferred() {
        let provider = read_csv_inferred("data/customers.csv", None).unwrap();
        assert!(!provider.schema().fields().is_empty());
    }

    #[test]
    fn test_read_csv_inferred_nonexistent() {
        let err = read_csv_inferred("data/missing.csv", None).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn test_memory_table_valid() {
        let batch = make_test_batch();
        let provider = memory_table(vec![batch]).unwrap();
        assert_eq!(provider.schema().fields().len(), 2);
    }

    #[test]
    fn test_memory_table_empty_batches_error() {
        let err = memory_table(vec![]).unwrap_err();
        assert!(err.to_string().contains("Empty batches"));
    }

    #[test]
    fn test_read_csv_inferred_with_custom_config() {
        let config = InferenceConfig {
            sample_rows: 10,
            ..InferenceConfig::default()
        };
        let provider = read_csv_inferred("data/customers.csv", Some(config)).unwrap();
        assert!(!provider.schema().fields().is_empty());
    }

    #[test]
    fn test_read_json_inferred_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.json");
        std::fs::File::create(&path).unwrap();
        // Empty JSON file should either succeed with empty table or return error
        let result = read_json_inferred(&path);
        // Both outcomes are acceptable; we just verify no panic
        drop(result);
    }

    #[test]
    fn test_read_csv_inferred_tsv() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.tsv");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "id\tname").unwrap();
        writeln!(f, "1\tAlice").unwrap();
        writeln!(f, "2\tBob").unwrap();
        drop(f);

        let provider = read_csv_inferred(&path, None).unwrap();
        assert!(!provider.schema().fields().is_empty());
    }
}
