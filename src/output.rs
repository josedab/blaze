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
}
