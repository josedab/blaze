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
    /// Scatter plot with two numeric axes
    Scatter,
    /// Proportional pie chart (text-based)
    Pie,
    /// Grid-based heatmap with intensity shading
    Heatmap,
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

    /// Render a scatter plot from two numeric columns.
    ///
    /// `x_col` and `y_col` are zero-based column indices for numeric data.
    pub fn render_scatter(&self, batch: &RecordBatch, x_col: usize, y_col: usize) -> Result<Chart> {
        if batch.num_rows() == 0 {
            return Ok(Chart {
                chart_type: ChartType::Scatter,
                config: self.config.clone(),
                output: "(empty result set)\n".to_string(),
            });
        }

        let x_vals = Self::extract_f64(batch, x_col)?;
        let y_vals = Self::extract_f64(batch, y_col)?;

        let x_min = x_vals.iter().cloned().fold(f64::INFINITY, f64::min);
        let x_max = x_vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let y_min = y_vals.iter().cloned().fold(f64::INFINITY, f64::min);
        let y_max = y_vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let x_range = if (x_max - x_min).abs() < f64::EPSILON {
            1.0
        } else {
            x_max - x_min
        };
        let y_range = if (y_max - y_min).abs() < f64::EPSILON {
            1.0
        } else {
            y_max - y_min
        };

        let height = self.config.height;
        let plot_width = self.config.width.saturating_sub(12);

        let mut grid = vec![vec![' '; plot_width]; height];

        for (&x, &y) in x_vals.iter().zip(y_vals.iter()) {
            let col = (((x - x_min) / x_range) * (plot_width - 1) as f64).round() as usize;
            let row = height - 1 - (((y - y_min) / y_range) * (height - 1) as f64).round() as usize;
            let col = col.min(plot_width - 1);
            let row = row.min(height - 1);
            grid[row][col] = '•';
        }

        let mut out = String::new();
        if let Some(ref title) = self.config.title {
            writeln!(out, "{}", title).unwrap();
        }

        let label_w = 8;
        for (r, row) in grid.iter().enumerate() {
            if self.config.show_labels {
                let y_val = y_max - (r as f64 / (height - 1).max(1) as f64) * y_range;
                write!(out, "{:>w$.1} │", y_val, w = label_w).unwrap();
            }
            let line: String = row.iter().collect();
            writeln!(out, "{}", line).unwrap();
        }

        if self.config.show_labels {
            write!(out, "{:>w$} └", "", w = label_w).unwrap();
            writeln!(out, "{}", "─".repeat(plot_width)).unwrap();
        }

        Ok(Chart {
            chart_type: ChartType::Scatter,
            config: self.config.clone(),
            output: out,
        })
    }

    /// Render a text-based pie chart showing proportions of values.
    ///
    /// `label_col` and `value_col` are zero-based column indices.
    pub fn render_pie(
        &self,
        batch: &RecordBatch,
        label_col: usize,
        value_col: usize,
    ) -> Result<Chart> {
        if batch.num_rows() == 0 {
            return Ok(Chart {
                chart_type: ChartType::Pie,
                config: self.config.clone(),
                output: "(empty result set)\n".to_string(),
            });
        }

        let labels = Self::extract_strings(batch, label_col)?;
        let values = Self::extract_f64(batch, value_col)?;

        let total: f64 = values.iter().filter(|v| **v > 0.0).sum();
        if total <= 0.0 {
            return Ok(Chart {
                chart_type: ChartType::Pie,
                config: self.config.clone(),
                output: "(no positive values)\n".to_string(),
            });
        }

        let pie_chars = ['█', '▓', '▒', '░', '▪', '▫', '◆', '◇', '●', '○'];

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

        // Summary with percentages
        let bar_width = self.config.width.saturating_sub(35);
        for (i, (label, &val)) in labels.iter().zip(values.iter()).enumerate() {
            let pct = if val > 0.0 {
                (val / total) * 100.0
            } else {
                0.0
            };
            let fill = ((pct / 100.0) * bar_width as f64).round() as usize;
            let ch = pie_chars[i % pie_chars.len()];
            let bar: String = std::iter::repeat(ch).take(fill).collect();
            let padded = self.pad_label(label);
            writeln!(
                out,
                "{}  {:<bw$} {:>5.1}%",
                padded,
                bar,
                pct,
                bw = bar_width
            )
            .unwrap();
        }

        Ok(Chart {
            chart_type: ChartType::Pie,
            config: self.config.clone(),
            output: out,
        })
    }

    /// Render a text-based heatmap from a numeric column.
    ///
    /// Displays values as colored/shaded Unicode blocks in a grid layout.
    pub fn render_heatmap(
        &self,
        batch: &RecordBatch,
        value_col: usize,
        cols_per_row: usize,
    ) -> Result<Chart> {
        if batch.num_rows() == 0 {
            return Ok(Chart {
                chart_type: ChartType::Heatmap,
                config: self.config.clone(),
                output: "(empty result set)\n".to_string(),
            });
        }

        let values = Self::extract_f64(batch, value_col)?;
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let range = if (max - min).abs() < f64::EPSILON {
            1.0
        } else {
            max - min
        };

        let heat_chars = [' ', '░', '▒', '▓', '█'];
        let cols = cols_per_row.max(1);

        let mut out = String::new();
        if let Some(ref title) = self.config.title {
            writeln!(out, "{}", title).unwrap();
        }

        for chunk in values.chunks(cols) {
            let row: String = chunk
                .iter()
                .map(|&v| {
                    let idx = (((v - min) / range) * 4.0).round() as usize;
                    heat_chars[idx.min(4)]
                })
                .collect();
            writeln!(out, "│{}│", row).unwrap();
        }

        // Legend
        writeln!(
            out,
            "  Low {} {} {} {} High",
            heat_chars[0], heat_chars[1], heat_chars[2], heat_chars[3]
        )
        .unwrap();

        Ok(Chart {
            chart_type: ChartType::Heatmap,
            config: self.config.clone(),
            output: out,
        })
    }

    /// Auto-detect the best chart type based on the data schema.
    ///
    /// Rules:
    /// - 1 string + 1 numeric column → Bar chart
    /// - 2 numeric columns → Scatter plot
    /// - 1 numeric column → Histogram
    /// - Otherwise → Summary statistics
    pub fn auto_detect_chart_type(batch: &RecordBatch) -> ChartType {
        if batch.num_rows() == 0 {
            return ChartType::Table;
        }

        let schema = batch.schema();
        let mut string_cols = Vec::new();
        let mut numeric_cols = Vec::new();

        for (i, field) in schema.fields().iter().enumerate() {
            match field.data_type() {
                ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => string_cols.push(i),
                ArrowDataType::Float64
                | ArrowDataType::Float32
                | ArrowDataType::Int64
                | ArrowDataType::Int32 => numeric_cols.push(i),
                _ => {}
            }
        }

        match (string_cols.len(), numeric_cols.len()) {
            (1, 1) => ChartType::Bar,
            (_, 2) => ChartType::Scatter,
            (_, 1) => ChartType::Histogram,
            (1, n) if n > 1 => ChartType::Bar,
            _ => ChartType::Table,
        }
    }

    /// Render a chart using auto-detected chart type.
    pub fn render_auto(&self, batch: &RecordBatch) -> Result<Chart> {
        let chart_type = Self::auto_detect_chart_type(batch);
        let schema = batch.schema();

        let mut string_cols = Vec::new();
        let mut numeric_cols = Vec::new();
        for (i, field) in schema.fields().iter().enumerate() {
            match field.data_type() {
                ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => string_cols.push(i),
                ArrowDataType::Float64
                | ArrowDataType::Float32
                | ArrowDataType::Int64
                | ArrowDataType::Int32 => numeric_cols.push(i),
                _ => {}
            }
        }

        match chart_type {
            ChartType::Bar => {
                let label_col = string_cols.first().copied().unwrap_or(0);
                let value_col = numeric_cols.first().copied().unwrap_or(1);
                self.render_bar(batch, label_col, value_col)
            }
            ChartType::Scatter => {
                let x = numeric_cols.first().copied().unwrap_or(0);
                let y = numeric_cols.get(1).copied().unwrap_or(1);
                self.render_scatter(batch, x, y)
            }
            ChartType::Histogram => {
                let col = numeric_cols.first().copied().unwrap_or(0);
                self.render_histogram(batch, col, 10)
            }
            _ => {
                let summary = Self::render_summary(batch)?;
                Ok(Chart {
                    chart_type: ChartType::Table,
                    config: self.config.clone(),
                    output: summary,
                })
            }
        }
    }

    /// Render a bar chart as an SVG string.
    pub fn render_bar_svg(
        &self,
        batch: &RecordBatch,
        label_col: usize,
        value_col: usize,
    ) -> Result<String> {
        if batch.num_rows() == 0 {
            return Ok("<svg></svg>".to_string());
        }

        let labels = Self::extract_strings(batch, label_col)?;
        let values = Self::extract_f64(batch, value_col)?;
        let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let max_val = if max_val <= 0.0 { 1.0 } else { max_val };

        let bar_height = 30;
        let gap = 5;
        let chart_width = 600;
        let chart_height = (bar_height + gap) * labels.len() + 40;
        let bar_area = chart_width - 200;

        let mut svg = String::new();
        writeln!(
            svg,
            r#"<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}">"#,
            chart_width, chart_height
        )
        .unwrap();

        if let Some(ref title) = self.config.title {
            writeln!(
                svg,
                r#"  <text x="{}" y="20" font-size="16" font-weight="bold" text-anchor="middle">{}</text>"#,
                chart_width / 2, title
            )
            .unwrap();
        }

        let colors = [
            "#4285f4", "#ea4335", "#fbbc04", "#34a853", "#ff6d01", "#46bdc6",
        ];

        for (i, (label, &val)) in labels.iter().zip(values.iter()).enumerate() {
            let y = 40 + i * (bar_height + gap);
            let width = ((val / max_val) * bar_area as f64) as usize;
            let color = colors[i % colors.len()];

            writeln!(
                svg,
                r#"  <text x="130" y="{}" font-size="12" text-anchor="end" dominant-baseline="middle">{}</text>"#,
                y + bar_height / 2, label
            )
            .unwrap();
            writeln!(
                svg,
                r#"  <rect x="140" y="{}" width="{}" height="{}" fill="{}" rx="3"/>"#,
                y,
                width,
                bar_height - 2,
                color
            )
            .unwrap();
            writeln!(
                svg,
                r#"  <text x="{}" y="{}" font-size="11" dominant-baseline="middle">{}</text>"#,
                145 + width,
                y + bar_height / 2,
                Self::format_number(val)
            )
            .unwrap();
        }

        writeln!(svg, "</svg>").unwrap();
        Ok(svg)
    }
}

impl std::fmt::Display for Chart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.output)
    }
}

// ---------------------------------------------------------------------------
// Vega-Lite Spec Generation (Feature 10)
// ---------------------------------------------------------------------------

/// Generates Vega-Lite JSON specifications from chart configurations.
#[derive(Debug)]
pub struct VegaLiteGenerator;

impl VegaLiteGenerator {
    /// Generate a Vega-Lite JSON specification from a chart type and data.
    pub fn generate(chart_type: ChartType, title: &str, width: usize, height: usize, data: &[Vec<(String, f64)>]) -> Result<String> {
        let mark = match chart_type {
            ChartType::Bar | ChartType::Column | ChartType::Histogram => "bar",
            ChartType::Line => "line",
            ChartType::Scatter => "point",
            ChartType::Pie => "arc",
            ChartType::Heatmap => "rect",
            ChartType::Table => "text",
        };

        // Build data values
        let mut values = Vec::new();
        for series in data {
            for (label, value) in series {
                values.push(format!(
                    r#"{{"category": "{}", "value": {}}}"#,
                    label.replace('"', "\\\""),
                    value
                ));
            }
        }
        let values_str = values.join(", ");

        let spec = format!(
            r#"{{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "title": "{}",
  "width": {},
  "height": {},
  "mark": "{}",
  "data": {{
    "values": [{}]
  }},
  "encoding": {{
    "x": {{"field": "category", "type": "nominal"}},
    "y": {{"field": "value", "type": "quantitative"}}
  }}
}}"#,
            title,
            width,
            height,
            mark,
            values_str
        );

        Ok(spec)
    }
}

// ---------------------------------------------------------------------------
// Dashboard Layout (Feature 10)
// ---------------------------------------------------------------------------

/// A dashboard containing multiple charts arranged in a layout.
#[derive(Debug)]
pub struct Dashboard {
    /// Dashboard title.
    pub title: String,
    /// Charts with their position (row, col).
    pub panels: Vec<DashboardPanel>,
    /// Number of columns in the grid.
    pub columns: usize,
}

/// A single panel in a dashboard.
#[derive(Debug)]
pub struct DashboardPanel {
    pub chart: Chart,
    pub row: usize,
    pub col: usize,
    pub width_span: usize,
    pub height_span: usize,
}

impl Dashboard {
    pub fn new(title: impl Into<String>, columns: usize) -> Self {
        Self {
            title: title.into(),
            panels: Vec::new(),
            columns: columns.max(1),
        }
    }

    /// Add a chart panel at the specified grid position.
    pub fn add_panel(&mut self, chart: Chart, row: usize, col: usize) {
        self.panels.push(DashboardPanel {
            chart,
            row,
            col,
            width_span: 1,
            height_span: 1,
        });
    }

    /// Add a chart panel that spans multiple grid cells.
    pub fn add_wide_panel(
        &mut self,
        chart: Chart,
        row: usize,
        col: usize,
        width_span: usize,
        height_span: usize,
    ) {
        self.panels.push(DashboardPanel {
            chart,
            row,
            col,
            width_span,
            height_span,
        });
    }

    pub fn panel_count(&self) -> usize {
        self.panels.len()
    }

    /// Generate an HTML page with all charts embedded.
    pub fn to_html(&self) -> String {
        let mut html = String::new();
        let _ = write!(html, r#"<!DOCTYPE html>
<html>
<head><title>{}</title>
<style>
  .dashboard {{ display: grid; grid-template-columns: repeat({}, 1fr); gap: 16px; padding: 16px; }}
  .panel {{ border: 1px solid #ddd; border-radius: 8px; padding: 12px; background: #fff; }}
  h1 {{ text-align: center; font-family: sans-serif; }}
  pre {{ font-family: monospace; font-size: 12px; overflow-x: auto; }}
</style>
</head>
<body>
<h1>{}</h1>
<div class="dashboard">
"#, self.title, self.columns, self.title);

        for panel in &self.panels {
            let span_style = if panel.width_span > 1 || panel.height_span > 1 {
                format!(
                    " style=\"grid-column: span {}; grid-row: span {};\"",
                    panel.width_span, panel.height_span
                )
            } else {
                String::new()
            };
            let _ = write!(
                html,
                "<div class=\"panel\"{}><pre>{}</pre></div>\n",
                span_style, panel.chart.output
            );
        }

        let _ = write!(html, "</div>\n</body>\n</html>");
        html
    }
}

// ---------------------------------------------------------------------------
// Interactive Filter Builder (Feature 10)
// ---------------------------------------------------------------------------

/// Generates SQL WHERE clauses from interactive filter specifications.
#[derive(Debug, Clone)]
pub enum FilterOp {
    Equals(String, String),
    NotEquals(String, String),
    GreaterThan(String, String),
    LessThan(String, String),
    Between(String, String, String),
    In(String, Vec<String>),
    Like(String, String),
    IsNull(String),
    IsNotNull(String),
}

/// Builds SQL WHERE clauses from filter operations.
#[derive(Debug)]
pub struct InteractiveFilterBuilder {
    filters: Vec<FilterOp>,
}

impl InteractiveFilterBuilder {
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    pub fn add_filter(&mut self, filter: FilterOp) {
        self.filters.push(filter);
    }

    /// Generate a SQL WHERE clause from all filters.
    pub fn to_sql(&self) -> String {
        if self.filters.is_empty() {
            return String::new();
        }

        let clauses: Vec<String> = self
            .filters
            .iter()
            .map(|f| match f {
                FilterOp::Equals(col, val) => format!("{col} = '{val}'"),
                FilterOp::NotEquals(col, val) => format!("{col} != '{val}'"),
                FilterOp::GreaterThan(col, val) => format!("{col} > {val}"),
                FilterOp::LessThan(col, val) => format!("{col} < {val}"),
                FilterOp::Between(col, lo, hi) => format!("{col} BETWEEN {lo} AND {hi}"),
                FilterOp::In(col, vals) => {
                    let list = vals.iter().map(|v| format!("'{v}'")).collect::<Vec<_>>().join(", ");
                    format!("{col} IN ({list})")
                }
                FilterOp::Like(col, pat) => format!("{col} LIKE '{pat}'"),
                FilterOp::IsNull(col) => format!("{col} IS NULL"),
                FilterOp::IsNotNull(col) => format!("{col} IS NOT NULL"),
            })
            .collect();

        format!("WHERE {}", clauses.join(" AND "))
    }

    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }

    pub fn clear(&mut self) {
        self.filters.clear();
    }
}

impl Default for InteractiveFilterBuilder {
    fn default() -> Self {
        Self::new()
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

    fn make_two_numeric_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("x", ArrowDataType::Float64, false),
            ArrowField::new("y", ArrowDataType::Float64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
                Arc::new(Float64Array::from(vec![2.0, 4.0, 1.0, 5.0, 3.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_render_scatter() {
        let batch = make_two_numeric_batch();
        let renderer = ChartRenderer::new(ChartConfig {
            height: 10,
            title: Some("X vs Y".to_string()),
            ..ChartConfig::default()
        });
        let chart = renderer.render_scatter(&batch, 0, 1).unwrap();
        assert_eq!(chart.chart_type, ChartType::Scatter);
        assert!(chart.output.contains('•'));
        assert!(chart.output.contains("X vs Y"));
    }

    #[test]
    fn test_render_pie() {
        let batch = make_test_batch();
        let renderer = ChartRenderer::new(ChartConfig {
            title: Some("Market Share".to_string()),
            ..ChartConfig::default()
        });
        let chart = renderer.render_pie(&batch, 0, 1).unwrap();
        assert_eq!(chart.chart_type, ChartType::Pie);
        assert!(chart.output.contains('%'));
        assert!(chart.output.contains("Market Share"));
    }

    #[test]
    fn test_render_heatmap() {
        let batch = make_numeric_batch();
        let renderer = ChartRenderer::new(ChartConfig {
            title: Some("Heat".to_string()),
            ..ChartConfig::default()
        });
        let chart = renderer.render_heatmap(&batch, 0, 5).unwrap();
        assert_eq!(chart.chart_type, ChartType::Heatmap);
        assert!(chart.output.contains('│'));
    }

    #[test]
    fn test_auto_detect_chart_type() {
        // 1 string + 1 numeric → Bar
        let batch = make_test_batch();
        assert_eq!(
            ChartRenderer::auto_detect_chart_type(&batch),
            ChartType::Bar
        );

        // 2 numeric → Scatter
        let batch = make_two_numeric_batch();
        assert_eq!(
            ChartRenderer::auto_detect_chart_type(&batch),
            ChartType::Scatter
        );

        // 1 numeric → Histogram
        let batch = make_numeric_batch();
        assert_eq!(
            ChartRenderer::auto_detect_chart_type(&batch),
            ChartType::Histogram
        );
    }

    #[test]
    fn test_render_auto() {
        let batch = make_test_batch();
        let renderer = ChartRenderer::new(ChartConfig::default());
        let chart = renderer.render_auto(&batch).unwrap();
        // Auto should pick Bar for 1 string + 1 numeric
        assert_eq!(chart.chart_type, ChartType::Bar);
    }

    #[test]
    fn test_render_bar_svg() {
        let batch = make_test_batch();
        let renderer = ChartRenderer::new(ChartConfig {
            title: Some("SVG Chart".to_string()),
            ..ChartConfig::default()
        });
        let svg = renderer.render_bar_svg(&batch, 0, 1).unwrap();
        assert!(svg.contains("<svg"));
        assert!(svg.contains("</svg>"));
        assert!(svg.contains("North"));
        assert!(svg.contains("SVG Chart"));
        assert!(svg.contains("rect"));
    }

    #[test]
    fn test_scatter_empty() {
        let batch = make_empty_batch();
        let renderer = ChartRenderer::new(ChartConfig::default());
        let chart = renderer.render_scatter(&batch, 0, 1).unwrap();
        assert!(chart.output.contains("empty"));
    }

    #[test]
    fn test_pie_empty() {
        let batch = make_empty_batch();
        let renderer = ChartRenderer::new(ChartConfig::default());
        let chart = renderer.render_pie(&batch, 0, 1).unwrap();
        assert!(chart.output.contains("empty"));
    }

    // -----------------------------------------------------------------------
    // Vega-Lite Generation tests (Feature 10)
    // -----------------------------------------------------------------------

    #[test]
    fn test_vega_lite_bar() {
        let data = vec![vec![
            ("Q1".into(), 100.0),
            ("Q2".into(), 200.0),
            ("Q3".into(), 150.0),
        ]];
        let spec = VegaLiteGenerator::generate(ChartType::Bar, "Sales", 400, 300, &data).unwrap();
        assert!(spec.contains("\"$schema\""));
        assert!(spec.contains("\"mark\": \"bar\""));
        assert!(spec.contains("\"title\": \"Sales\""));
        assert!(spec.contains("Q1"));
    }

    #[test]
    fn test_vega_lite_line() {
        let data = vec![vec![("Jan".into(), 10.0), ("Feb".into(), 20.0)]];
        let spec = VegaLiteGenerator::generate(ChartType::Line, "Trend", 400, 300, &data).unwrap();
        assert!(spec.contains("\"mark\": \"line\""));
    }

    // -----------------------------------------------------------------------
    // Dashboard tests (Feature 10)
    // -----------------------------------------------------------------------

    #[test]
    fn test_dashboard_layout() {
        let mut dash = Dashboard::new("Test Dashboard", 2);
        let chart1 = Chart {
            chart_type: ChartType::Bar,
            config: ChartConfig::default(),
            output: "Chart 1".into(),
        };
        let chart2 = Chart {
            chart_type: ChartType::Line,
            config: ChartConfig::default(),
            output: "Chart 2".into(),
        };
        dash.add_panel(chart1, 0, 0);
        dash.add_panel(chart2, 0, 1);
        assert_eq!(dash.panel_count(), 2);
    }

    #[test]
    fn test_dashboard_html_export() {
        let mut dash = Dashboard::new("Revenue Dashboard", 2);
        dash.add_panel(
            Chart {
                chart_type: ChartType::Bar,
                config: ChartConfig::default(),
                output: "Bar chart output".into(),
            },
            0, 0,
        );
        dash.add_wide_panel(
            Chart {
                chart_type: ChartType::Line,
                config: ChartConfig::default(),
                output: "Wide line chart".into(),
            },
            1, 0, 2, 1,
        );

        let html = dash.to_html();
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("Revenue Dashboard"));
        assert!(html.contains("Bar chart output"));
        assert!(html.contains("grid-column: span 2"));
    }

    // -----------------------------------------------------------------------
    // Interactive Filter tests (Feature 10)
    // -----------------------------------------------------------------------

    #[test]
    fn test_filter_builder_basic() {
        let mut builder = InteractiveFilterBuilder::new();
        builder.add_filter(FilterOp::Equals("status".into(), "active".into()));
        builder.add_filter(FilterOp::GreaterThan("age".into(), "18".into()));

        let sql = builder.to_sql();
        assert_eq!(sql, "WHERE status = 'active' AND age > 18");
    }

    #[test]
    fn test_filter_builder_complex() {
        let mut builder = InteractiveFilterBuilder::new();
        builder.add_filter(FilterOp::In("color".into(), vec!["red".into(), "blue".into()]));
        builder.add_filter(FilterOp::Between("price".into(), "10".into(), "100".into()));
        builder.add_filter(FilterOp::IsNotNull("email".into()));

        let sql = builder.to_sql();
        assert!(sql.contains("color IN ('red', 'blue')"));
        assert!(sql.contains("price BETWEEN 10 AND 100"));
        assert!(sql.contains("email IS NOT NULL"));
    }

    #[test]
    fn test_filter_builder_empty() {
        let builder = InteractiveFilterBuilder::new();
        assert_eq!(builder.to_sql(), "");
    }

    #[test]
    fn test_filter_builder_like() {
        let mut builder = InteractiveFilterBuilder::new();
        builder.add_filter(FilterOp::Like("name".into(), "%Smith%".into()));
        let sql = builder.to_sql();
        assert_eq!(sql, "WHERE name LIKE '%Smith%'");
    }
}
