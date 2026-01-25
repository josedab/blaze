//! Embedded visualization engine for query results.
//!
//! Renders charts from `RecordBatch` data to text/terminal output
//! using Unicode block characters, and to SVG strings.

use std::fmt::Write as FmtWrite;

use arrow::array::{Array, AsArray, StringArray};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::record_batch::RecordBatch;

use crate::error::{BlazeError, Result};

/// Chart type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartType {
    /// Horizontal bar chart
    Bar,
    /// Vertical column chart (rendered sideways in terminal)
    Column,
    /// Line chart using Unicode braille
    Line,
    /// Frequency distribution
    Histogram,
    /// Pretty-printed table (see `output.rs` for the canonical implementation)
    Table,
}

/// Chart configuration.
#[derive(Debug, Clone)]
pub struct ChartConfig {
    /// Terminal width (default: 80)
    pub width: usize,
    /// Chart height in rows (default: 20)
    pub height: usize,
    /// Show axis labels
    pub show_labels: bool,
    /// Show values on bars
    pub show_values: bool,
    /// Optional chart title
    pub title: Option<String>,
    /// Truncate labels to this width (default: 20)
    pub max_label_width: usize,
}

impl Default for ChartConfig {
    fn default() -> Self {
        Self {
            width: 80,
            height: 20,
            show_labels: true,
            show_values: true,
            title: None,
            max_label_width: 20,
        }
    }
}

/// A rendered chart.
#[derive(Debug)]
pub struct Chart {
    pub chart_type: ChartType,
    pub config: ChartConfig,
    pub output: String,
}

/// Renders charts from `RecordBatch` data.
pub struct ChartRenderer {
    config: ChartConfig,
}

// Unicode block characters for sub-character precision.
const FULL_BLOCK: char = '█';
const HALF_BLOCK: char = '▌';
const THIN_BLOCK: char = '▎';

// Sparkline levels (8 levels, index 0 = lowest).
const SPARK_CHARS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];

impl ChartRenderer {
    /// Create a new renderer with the given configuration.
    pub fn new(config: ChartConfig) -> Self {
        Self { config }
    }

    // ── helpers ───────────────────────────────────────────────────────

    /// Extract a string column by index from a batch.
    fn extract_strings(batch: &RecordBatch, col: usize) -> Result<Vec<String>> {
        if col >= batch.num_columns() {
            return Err(BlazeError::invalid_argument(format!(
                "Column index {} out of range (batch has {} columns)",
                col,
                batch.num_columns()
            )));
        }
        let array = batch.column(col);
        let string_array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| {
                BlazeError::type_error(format!(
                    "Expected Utf8 column at index {}, found {:?}",
                    col,
                    array.data_type()
                ))
            })?;
        Ok((0..string_array.len())
            .map(|i| {
                if string_array.is_null(i) {
                    "NULL".to_string()
                } else {
                    string_array.value(i).to_string()
                }
            })
            .collect())
    }

    /// Extract a numeric column as `f64` values by index.
    fn extract_f64(batch: &RecordBatch, col: usize) -> Result<Vec<f64>> {
        if col >= batch.num_columns() {
            return Err(BlazeError::invalid_argument(format!(
                "Column index {} out of range (batch has {} columns)",
                col,
                batch.num_columns()
            )));
        }
        let array = batch.column(col);
        match array.data_type() {
            ArrowDataType::Float64 => {
                let arr = array.as_primitive::<arrow::datatypes::Float64Type>();
                Ok((0..arr.len())
                    .map(|i| if arr.is_null(i) { 0.0 } else { arr.value(i) })
                    .collect())
            }
            ArrowDataType::Float32 => {
                let arr = array.as_primitive::<arrow::datatypes::Float32Type>();
                Ok((0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            0.0
                        } else {
                            arr.value(i) as f64
                        }
                    })
                    .collect())
            }
            ArrowDataType::Int64 => {
                let arr = array.as_primitive::<arrow::datatypes::Int64Type>();
                Ok((0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            0.0
                        } else {
                            arr.value(i) as f64
                        }
                    })
                    .collect())
            }
            ArrowDataType::Int32 => {
                let arr = array.as_primitive::<arrow::datatypes::Int32Type>();
                Ok((0..arr.len())
                    .map(|i| {
                        if arr.is_null(i) {
                            0.0
                        } else {
                            arr.value(i) as f64
                        }
                    })
                    .collect())
            }
            other => Err(BlazeError::type_error(format!(
                "Cannot convert {:?} to f64 for charting",
                other
            ))),
        }
    }

    /// Truncate a label to `max_label_width`, padding with spaces.
    fn pad_label(&self, label: &str) -> String {
        let max = self.config.max_label_width;
        if label.len() > max {
            format!("{}…", &label[..max - 1])
        } else {
            format!("{:<width$}", label, width = max)
        }
    }

    /// Format a number with thousands separators.
    fn format_number(v: f64) -> String {
        if v == v.floor() && v.abs() < 1e15 {
            let n = v as i64;
            let s = n.to_string();
            let negative = n < 0;
            let digits: &str = if negative { &s[1..] } else { &s };
            let mut result = String::new();
            for (i, ch) in digits.chars().rev().enumerate() {
                if i > 0 && i % 3 == 0 {
                    result.push(',');
                }
                result.push(ch);
            }
            let formatted: String = result.chars().rev().collect();
            if negative {
                format!("-{}", formatted)
            } else {
                formatted
            }
        } else {
            format!("{:.1}", v)
        }
    }

    // ── public API ───────────────────────────────────────────────────

    /// Render a horizontal bar chart.
    ///
    /// `label_col` and `value_col` are zero-based column indices.
    pub fn render_bar(
        &self,
        batch: &RecordBatch,
        label_col: usize,
        value_col: usize,
    ) -> Result<Chart> {
        if batch.num_rows() == 0 {
            return Ok(Chart {
                chart_type: ChartType::Bar,
                config: self.config.clone(),
                output: "(empty result set)\n".to_string(),
            });
        }

        let labels = Self::extract_strings(batch, label_col)?;
        let values = Self::extract_f64(batch, value_col)?;

        let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let max_val = if max_val <= 0.0 { 1.0 } else { max_val };

        // Compute the available width for bars.
        let value_width = if self.config.show_values { 12 } else { 0 };
        let bar_area = self
            .config
            .width
            .saturating_sub(self.config.max_label_width + 2 + value_width);

        let mut out = String::new();
        if let Some(ref title) = self.config.title {
            writeln!(out, "{}", title).unwrap();
            writeln!(
                out,
                "{}",
                "─".repeat(self.config.width.min(title.len() + 20))
            )
            .unwrap();
        }

        for (label, &val) in labels.iter().zip(values.iter()) {
            let ratio = val / max_val;
            let full_blocks = (ratio * bar_area as f64) as usize;
            let remainder = (ratio * bar_area as f64) - full_blocks as f64;

            let mut bar = FULL_BLOCK.to_string().repeat(full_blocks);
            if remainder >= 0.5 {
                bar.push(HALF_BLOCK);
            } else if remainder >= 0.25 {
                bar.push(THIN_BLOCK);
            }

            let padded = self.pad_label(label);
            if self.config.show_values {
                writeln!(
                    out,
                    "{}  {:<bw$} {}",
                    padded,
                    bar,
                    Self::format_number(val),
                    bw = bar_area
                )
                .unwrap();
            } else {
                writeln!(out, "{}  {}", padded, bar).unwrap();
            }
        }

        Ok(Chart {
            chart_type: ChartType::Bar,
            config: self.config.clone(),
            output: out,
        })
    }

    /// Render a frequency histogram from a numeric column.
    ///
    /// `value_col` is a zero-based column index. `num_bins` controls bucket count.
    pub fn render_histogram(
        &self,
        batch: &RecordBatch,
        value_col: usize,
        num_bins: usize,
    ) -> Result<Chart> {
        if batch.num_rows() == 0 {
            return Ok(Chart {
                chart_type: ChartType::Histogram,
                config: self.config.clone(),
                output: "(empty result set)\n".to_string(),
            });
        }

        let values = Self::extract_f64(batch, value_col)?;
        let min_val = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        let num_bins = num_bins.max(1);
        let bin_width = if (max_val - min_val).abs() < f64::EPSILON {
            1.0
        } else {
            (max_val - min_val) / num_bins as f64
        };

        // Count per bin.
        let mut counts = vec![0usize; num_bins];
        for &v in &values {
            let idx = ((v - min_val) / bin_width) as usize;
            let idx = idx.min(num_bins - 1);
            counts[idx] += 1;
        }

        // Build labels like "[20-25)".
        let bin_labels: Vec<String> = (0..num_bins)
            .map(|i| {
                let lo = min_val + i as f64 * bin_width;
                let hi = lo + bin_width;
                format!("[{:.0}-{:.0})", lo, hi)
            })
            .collect();

        let col_name = batch.schema().field(value_col).name().clone();

        // Render through a temporary batch-like approach (reuse bar logic).
        let max_count = *counts.iter().max().unwrap_or(&1);
        let max_count = if max_count == 0 { 1 } else { max_count };

        let value_width = if self.config.show_values { 12 } else { 0 };
        let bar_area = self
            .config
            .width
            .saturating_sub(self.config.max_label_width + 2 + value_width);

        let mut out = String::new();
        let title = self
            .config
            .title
            .clone()
            .unwrap_or_else(|| format!("Distribution of {} ({} bins)", col_name, num_bins));
        writeln!(out, "{}", title).unwrap();
        writeln!(
            out,
            "{}",
            "─".repeat(self.config.width.min(title.len() + 20))
        )
        .unwrap();

        for (label, &count) in bin_labels.iter().zip(counts.iter()) {
            let ratio = count as f64 / max_count as f64;
            let full_blocks = (ratio * bar_area as f64) as usize;
            let bar = FULL_BLOCK.to_string().repeat(full_blocks);
            let padded = self.pad_label(label);
            if self.config.show_values {
                writeln!(out, "{}  {:<bw$} {}", padded, bar, count, bw = bar_area).unwrap();
            } else {
                writeln!(out, "{}  {}", padded, bar).unwrap();
            }
        }

        Ok(Chart {
            chart_type: ChartType::Histogram,
            config: self.config.clone(),
            output: out,
        })
    }

    /// Render a compact sparkline from a slice of values.
    ///
    /// Maps each value to one of 8 Unicode block levels: ▁▂▃▄▅▆▇█
    pub fn render_sparkline(values: &[f64]) -> String {
        if values.is_empty() {
            return String::new();
        }
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range = max - min;

        values
            .iter()
            .map(|&v| {
                let idx = if range.abs() < f64::EPSILON {
                    3 // middle level when all values are equal
                } else {
                    let normalized = (v - min) / range; // 0.0 .. 1.0
                    ((normalized * 7.0).round() as usize).min(7)
                };
                SPARK_CHARS[idx]
            })
            .collect()
    }

    /// Render an ASCII line chart.
    ///
    /// `x_col` supplies the x-axis labels, `y_col` the numeric values.
    pub fn render_line(&self, batch: &RecordBatch, x_col: usize, y_col: usize) -> Result<Chart> {
        if batch.num_rows() == 0 {
            return Ok(Chart {
                chart_type: ChartType::Line,
                config: self.config.clone(),
                output: "(empty result set)\n".to_string(),
            });
        }

        let x_labels = Self::extract_strings(batch, x_col)?;
        let y_values = Self::extract_f64(batch, y_col)?;

        let min_y = y_values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_y = y_values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range = if (max_y - min_y).abs() < f64::EPSILON {
            1.0
        } else {
            max_y - min_y
        };

        let height = self.config.height;
        let plot_width = y_values.len();

        // Build a 2D grid (row 0 = top).
        let mut grid = vec![vec![' '; plot_width]; height];
        for (col, &v) in y_values.iter().enumerate() {
            let normalized = (v - min_y) / range;
            let row =
                height - 1 - ((normalized * (height - 1) as f64).round() as usize).min(height - 1);
            grid[row][col] = '●';
        }

        // Connect consecutive points with '─' on the same row, '│' vertically,
        // or just leave markers. Keep it simple: markers only.
        let mut out = String::new();
        if let Some(ref title) = self.config.title {
            writeln!(out, "{}", title).unwrap();
        }

        let label_w = 8; // y-axis label width
        for (r, row) in grid.iter().enumerate() {
            if self.config.show_labels {
                let y_val = max_y - (r as f64 / (height - 1) as f64) * range;
                write!(out, "{:>w$.1} │", y_val, w = label_w).unwrap();
            }
            let line: String = row.iter().collect();
            writeln!(out, "{}", line).unwrap();
        }

        // x-axis
        if self.config.show_labels {
            write!(out, "{:>w$} └", "", w = label_w).unwrap();
            writeln!(out, "{}", "─".repeat(plot_width)).unwrap();
            // Print first and last label.
            if let (Some(first), Some(last)) = (x_labels.first(), x_labels.last()) {
                write!(out, "{:>w$}  ", "", w = label_w).unwrap();
                if plot_width > first.len() + last.len() {
                    let gap = plot_width - first.len() - last.len();
                    writeln!(out, "{}{:>gap$}", first, last, gap = gap).unwrap();
                } else {
                    writeln!(out, "{}", first).unwrap();
                }
            }
        }

        Ok(Chart {
            chart_type: ChartType::Line,
            config: self.config.clone(),
            output: out,
        })
    }

    /// Produce quick summary statistics for every numeric column in the batch.
    ///
    /// Returns a formatted text table with count, min, max, and mean.
    pub fn render_summary(batch: &RecordBatch) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok("(empty result set)\n".to_string());
        }

        let schema = batch.schema();
        let mut out = String::new();
        writeln!(
            out,
            "{:<20} {:>10} {:>12} {:>12} {:>12}",
            "Column", "Count", "Min", "Max", "Mean"
        )
        .unwrap();
        writeln!(out, "{}", "─".repeat(68)).unwrap();

        let mut found_numeric = false;
        for (i, field) in schema.fields().iter().enumerate() {
            match field.data_type() {
                ArrowDataType::Float64
                | ArrowDataType::Float32
                | ArrowDataType::Int64
                | ArrowDataType::Int32 => {}
                _ => continue,
            }
            found_numeric = true;
            let values = Self::extract_f64(batch, i)?;
            let count = values.len();
            let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            let mean = if count > 0 {
                values.iter().sum::<f64>() / count as f64
            } else {
                0.0
            };

            writeln!(
                out,
                "{:<20} {:>10} {:>12.2} {:>12.2} {:>12.2}",
                field.name(),
                count,
                min,
                max,
                mean,
            )
            .unwrap();
        }

        if !found_numeric {
            writeln!(out, "(no numeric columns)").unwrap();
        }

        Ok(out)
    }
}

impl std::fmt::Display for Chart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float64Array;
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("region", ArrowDataType::Utf8, false),
            ArrowField::new("sales", ArrowDataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["North", "South", "East", "West"])),
                Arc::new(Float64Array::from(vec![1200.0, 850.0, 720.0, 380.0])),
            ],
        )
        .unwrap()
    }

    fn make_numeric_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "age",
            ArrowDataType::Float64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Float64Array::from(vec![
                22.0, 27.0, 31.0, 35.0, 29.0, 40.0, 33.0, 26.0, 38.0, 24.0,
            ]))],
        )
        .unwrap()
    }

    fn make_empty_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("region", ArrowDataType::Utf8, false),
            ArrowField::new("sales", ArrowDataType::Float64, false),
        ]));
        RecordBatch::new_empty(schema)
    }

    #[test]
    fn test_chart_config_defaults() {
        let cfg = ChartConfig::default();
        assert_eq!(cfg.width, 80);
        assert_eq!(cfg.height, 20);
        assert!(cfg.show_labels);
        assert!(cfg.show_values);
        assert!(cfg.title.is_none());
        assert_eq!(cfg.max_label_width, 20);
    }

    #[test]
    fn test_render_bar_chart() {
        let batch = make_test_batch();
        let renderer = ChartRenderer::new(ChartConfig {
            title: Some("Sales by Region".to_string()),
            ..ChartConfig::default()
        });
        let chart = renderer.render_bar(&batch, 0, 1).unwrap();
        assert_eq!(chart.chart_type, ChartType::Bar);

        let out = &chart.output;
        assert!(out.contains("Sales by Region"));
        assert!(out.contains("North"));
        assert!(out.contains("South"));
        assert!(out.contains("1,200"));
        assert!(out.contains("380"));
        // The bar for North (max) should be the longest.
        assert!(out.contains(FULL_BLOCK));
    }

    #[test]
    fn test_render_histogram() {
        let batch = make_numeric_batch();
        let renderer = ChartRenderer::new(ChartConfig::default());
        let chart = renderer.render_histogram(&batch, 0, 4).unwrap();
        assert_eq!(chart.chart_type, ChartType::Histogram);

        let out = &chart.output;
        assert!(out.contains("Distribution of age"));
        // Should have 4 bin labels.
        assert!(out.contains('['));
        assert!(out.contains(')'));
    }

    #[test]
    fn test_render_sparkline() {
        let values = vec![1.0, 3.0, 5.0, 7.0, 8.0, 7.0, 5.0, 3.0, 1.0];
        let spark = ChartRenderer::render_sparkline(&values);
        assert_eq!(spark.chars().count(), 9);
        // First and last should be the lowest level.
        assert_eq!(spark.chars().next().unwrap(), SPARK_CHARS[0]);
        assert_eq!(spark.chars().last().unwrap(), SPARK_CHARS[0]);
    }

    #[test]
    fn test_render_line_chart() {
        let batch = make_test_batch();
        let renderer = ChartRenderer::new(ChartConfig {
            height: 10,
            title: Some("Sales trend".to_string()),
            ..ChartConfig::default()
        });
        let chart = renderer.render_line(&batch, 0, 1).unwrap();
        assert_eq!(chart.chart_type, ChartType::Line);
        assert!(chart.output.contains('●'));
        assert!(chart.output.contains("Sales trend"));
    }

    #[test]
    fn test_render_summary() {
        let batch = make_test_batch();
        let summary = ChartRenderer::render_summary(&batch).unwrap();
        assert!(summary.contains("sales"));
        assert!(summary.contains("Count"));
        assert!(summary.contains("Min"));
        assert!(summary.contains("Max"));
        assert!(summary.contains("Mean"));
    }

    #[test]
    fn test_empty_batch_handling() {
        let batch = make_empty_batch();
        let renderer = ChartRenderer::new(ChartConfig::default());

        let bar = renderer.render_bar(&batch, 0, 1).unwrap();
        assert!(bar.output.contains("empty"));

        let hist = renderer.render_histogram(&batch, 1, 5).unwrap();
        assert!(hist.output.contains("empty"));

        let line = renderer.render_line(&batch, 0, 1).unwrap();
        assert!(line.output.contains("empty"));

        let summary = ChartRenderer::render_summary(&batch).unwrap();
        assert!(summary.contains("empty"));
    }

    #[test]
    fn test_sparkline_single_value() {
        let spark = ChartRenderer::render_sparkline(&[42.0]);
        assert_eq!(spark.chars().count(), 1);
        // Single value should map to the middle level.
        assert_eq!(spark.chars().next().unwrap(), SPARK_CHARS[3]);
    }
}
