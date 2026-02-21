//! Output formatting for CLI and query results.
//!
//! This module provides different output formats for query results:
//! - Table (default, pretty-printed)
//! - CSV
//! - JSON
//! - Arrow IPC

use std::fs::File;
use std::io::{self, Write};
use std::path::Path;

use arrow::csv::WriterBuilder as CsvWriterBuilder;
use arrow::ipc::writer::FileWriter as IpcWriter;
use arrow::json::LineDelimitedWriter;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::print_batches;

use crate::error::{BlazeError, Result};

/// Output format for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Pretty-printed table (default)
    #[default]
    Table,
    /// Comma-separated values
    Csv,
    /// JSON Lines (newline-delimited JSON)
    Json,
    /// Arrow IPC format
    Arrow,
}

impl OutputFormat {
    /// Parse output format from string.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "table" => Ok(Self::Table),
            "csv" => Ok(Self::Csv),
            "json" | "jsonl" | "ndjson" => Ok(Self::Json),
            "arrow" | "ipc" => Ok(Self::Arrow),
            _ => Err(BlazeError::invalid_argument(format!(
                "Unknown output format: '{}'. Valid formats: table, csv, json, arrow",
                s
            ))),
        }
    }
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        OutputFormat::from_str(s).map_err(|e| e.to_string())
    }
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Table => write!(f, "table"),
            Self::Csv => write!(f, "csv"),
            Self::Json => write!(f, "json"),
            Self::Arrow => write!(f, "arrow"),
        }
    }
}

/// Output writer for query results.
pub struct OutputWriter {
    format: OutputFormat,
    output: OutputTarget,
}

enum OutputTarget {
    Stdout,
    File(File),
}

impl OutputWriter {
    /// Create a new output writer to stdout.
    pub fn stdout(format: OutputFormat) -> Self {
        Self {
            format,
            output: OutputTarget::Stdout,
        }
    }

    /// Create a new output writer to a file.
    pub fn file(format: OutputFormat, path: impl AsRef<Path>) -> Result<Self> {
        let file = File::create(path.as_ref())?;
        Ok(Self {
            format,
            output: OutputTarget::File(file),
        })
    }

    /// Write record batches to the output.
    pub fn write_batches(&mut self, batches: &[RecordBatch]) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        match self.format {
            OutputFormat::Table => self.write_table(batches),
            OutputFormat::Csv => self.write_csv(batches),
            OutputFormat::Json => self.write_json(batches),
            OutputFormat::Arrow => self.write_arrow(batches),
        }
    }

    fn write_table(&mut self, batches: &[RecordBatch]) -> Result<()> {
        match &mut self.output {
            OutputTarget::Stdout => {
                print_batches(batches)?;
            }
            OutputTarget::File(file) => {
                // For file output, write a simple text representation
                let output = arrow::util::pretty::pretty_format_batches(batches)?;
                writeln!(file, "{}", output)?;
            }
        }
        Ok(())
    }

    fn write_csv(&mut self, batches: &[RecordBatch]) -> Result<()> {
        let _schema = batches[0].schema();

        match &mut self.output {
            OutputTarget::Stdout => {
                let mut writer = CsvWriterBuilder::new()
                    .with_header(true)
                    .build(io::stdout());
                for batch in batches {
                    writer.write(batch)?;
                }
            }
            OutputTarget::File(file) => {
                let mut writer = CsvWriterBuilder::new().with_header(true).build(file);
                for batch in batches {
                    writer.write(batch)?;
                }
            }
        }
        Ok(())
    }

    fn write_json(&mut self, batches: &[RecordBatch]) -> Result<()> {
        match &mut self.output {
            OutputTarget::Stdout => {
                let stdout = io::stdout();
                let mut writer = LineDelimitedWriter::new(stdout.lock());
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            OutputTarget::File(file) => {
                let mut writer = LineDelimitedWriter::new(file);
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
        }
        Ok(())
    }

    fn write_arrow(&mut self, batches: &[RecordBatch]) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        let schema = batches[0].schema();

        match &mut self.output {
            OutputTarget::Stdout => {
                let stdout = io::stdout();
                let mut writer = IpcWriter::try_new(stdout.lock(), &schema)?;
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            OutputTarget::File(file) => {
                let mut writer = IpcWriter::try_new(file, &schema)?;
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
        }
        Ok(())
    }
}

/// Format query results as a string in the specified format.
pub fn format_batches(batches: &[RecordBatch], format: OutputFormat) -> Result<String> {
    if batches.is_empty() {
        return Ok(String::new());
    }

    match format {
        OutputFormat::Table => {
            let output = arrow::util::pretty::pretty_format_batches(batches)?;
            Ok(output.to_string())
        }
        OutputFormat::Csv => {
            let mut buf = Vec::new();
            {
                let mut writer = CsvWriterBuilder::new().with_header(true).build(&mut buf);
                for batch in batches {
                    writer.write(batch)?;
                }
            }
            Ok(String::from_utf8(buf).map_err(|e| BlazeError::internal(e.to_string()))?)
        }
        OutputFormat::Json => {
            let mut buf = Vec::new();
            {
                let mut writer = LineDelimitedWriter::new(&mut buf);
                for batch in batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            Ok(String::from_utf8(buf).map_err(|e| BlazeError::internal(e.to_string()))?)
        }
        OutputFormat::Arrow => Err(BlazeError::invalid_argument(
            "Arrow IPC format cannot be converted to string",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_format_parsing() {
        assert_eq!(
            OutputFormat::from_str("table").unwrap(),
            OutputFormat::Table
        );
        assert_eq!(OutputFormat::from_str("CSV").unwrap(), OutputFormat::Csv);
        assert_eq!(OutputFormat::from_str("json").unwrap(), OutputFormat::Json);
        assert_eq!(
            OutputFormat::from_str("arrow").unwrap(),
            OutputFormat::Arrow
        );
        assert!(OutputFormat::from_str("unknown").is_err());
    }

    #[test]
    fn test_format_csv() {
        let batch = create_test_batch();
        let output = format_batches(&[batch], OutputFormat::Csv).unwrap();
        assert!(output.contains("id,name"));
        assert!(output.contains("1,Alice"));
    }

    #[test]
    fn test_format_json() {
        let batch = create_test_batch();
        let output = format_batches(&[batch], OutputFormat::Json).unwrap();
        assert!(output.contains("\"id\":1"));
        assert!(output.contains("\"name\":\"Alice\""));
    }

    // --- Arrow IPC tests ---

    #[test]
    fn test_format_arrow_ipc_cannot_be_string() {
        let batch = create_test_batch();
        let result = format_batches(&[batch], OutputFormat::Arrow);
        assert!(result.is_err(), "Arrow IPC should not convert to string");
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("cannot be converted"), "Error: {}", msg);
    }

    #[test]
    fn test_arrow_ipc_write_to_file() {
        let batch = create_test_batch();
        let path = std::env::temp_dir().join("blaze_test_ipc.arrow");
        {
            let mut writer = OutputWriter::file(OutputFormat::Arrow, &path).unwrap();
            writer.write_batches(&[batch.clone()]).unwrap();
        }
        // Read back and verify
        let file = File::open(&path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let schema = reader.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");

        let mut total_rows = 0;
        for batch_result in reader {
            let read_batch = batch_result.unwrap();
            total_rows += read_batch.num_rows();
        }
        assert_eq!(total_rows, 3);
        let _ = std::fs::remove_file(&path);
    }

    // --- Table format tests ---

    #[test]
    fn test_format_table() {
        let batch = create_test_batch();
        let output = format_batches(&[batch], OutputFormat::Table).unwrap();
        // Table format should contain column headers and data
        assert!(output.contains("id"), "Table should contain 'id' header");
        assert!(output.contains("name"), "Table should contain 'name' header");
        assert!(output.contains("Alice"), "Table should contain data");
        assert!(output.contains("Bob"), "Table should contain data");
        // Table format uses borders
        assert!(output.contains("+") || output.contains("|"),
            "Table should have borders or separators");
    }

    #[test]
    fn test_format_table_write_to_file() {
        let batch = create_test_batch();
        let path = std::env::temp_dir().join("blaze_test_table.txt");
        {
            let mut writer = OutputWriter::file(OutputFormat::Table, &path).unwrap();
            writer.write_batches(&[batch]).unwrap();
        }
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("Alice"));
        assert!(content.contains("id"));
        let _ = std::fs::remove_file(&path);
    }

    // --- Empty batch tests ---

    #[test]
    fn test_format_empty_batches() {
        let result = format_batches(&[], OutputFormat::Csv).unwrap();
        assert!(result.is_empty(), "Empty batch list should produce empty output");

        let result = format_batches(&[], OutputFormat::Json).unwrap();
        assert!(result.is_empty());

        let result = format_batches(&[], OutputFormat::Table).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_write_empty_batches() {
        let mut writer = OutputWriter::stdout(OutputFormat::Csv);
        let result = writer.write_batches(&[]);
        assert!(result.is_ok(), "Writing empty batches should succeed");
    }

    // --- Format parsing edge cases ---

    #[test]
    fn test_format_parsing_aliases() {
        assert_eq!(OutputFormat::from_str("jsonl").unwrap(), OutputFormat::Json);
        assert_eq!(OutputFormat::from_str("ndjson").unwrap(), OutputFormat::Json);
        assert_eq!(OutputFormat::from_str("ipc").unwrap(), OutputFormat::Arrow);
    }

    #[test]
    fn test_format_parsing_case_insensitive() {
        assert_eq!(OutputFormat::from_str("TABLE").unwrap(), OutputFormat::Table);
        assert_eq!(OutputFormat::from_str("Csv").unwrap(), OutputFormat::Csv);
        assert_eq!(OutputFormat::from_str("JSON").unwrap(), OutputFormat::Json);
        assert_eq!(OutputFormat::from_str("ARROW").unwrap(), OutputFormat::Arrow);
    }

    #[test]
    fn test_format_display() {
        assert_eq!(format!("{}", OutputFormat::Table), "table");
        assert_eq!(format!("{}", OutputFormat::Csv), "csv");
        assert_eq!(format!("{}", OutputFormat::Json), "json");
        assert_eq!(format!("{}", OutputFormat::Arrow), "arrow");
    }

    #[test]
    fn test_format_default() {
        let fmt: OutputFormat = Default::default();
        assert_eq!(fmt, OutputFormat::Table);
    }

    // --- Multiple batch tests ---

    #[test]
    fn test_format_csv_multiple_batches() {
        let batch1 = create_test_batch();
        let batch2 = create_test_batch();
        let output = format_batches(&[batch1, batch2], OutputFormat::Csv).unwrap();
        // Should contain header only once + 6 data rows
        let lines: Vec<&str> = output.lines().collect();
        assert!(lines.len() >= 7, "Should have header + 6 data rows, got {}", lines.len());
    }
}
