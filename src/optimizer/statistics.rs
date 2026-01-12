//! Statistics for Cost-Based Optimization
//!
//! Provides table and column statistics for cardinality estimation and cost modeling.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::error::{BlazeError, Result};
use crate::types::DataType;

/// Statistics for a single column.
#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    /// Column name
    pub name: String,
    /// Data type of the column
    pub data_type: DataType,
    /// Number of distinct values (NDV)
    pub distinct_count: Option<usize>,
    /// Number of null values
    pub null_count: usize,
    /// Minimum value (as string for simplicity)
    pub min_value: Option<String>,
    /// Maximum value (as string for simplicity)
    pub max_value: Option<String>,
    /// Average column width in bytes
    pub avg_width: usize,
    /// Histogram for value distribution
    pub histogram: Option<Histogram>,
}

impl ColumnStatistics {
    /// Create new column statistics.
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            distinct_count: None,
            null_count: 0,
            min_value: None,
            max_value: None,
            avg_width: 8, // Default estimate
            histogram: None,
        }
    }

    /// Set the distinct count.
    pub fn with_distinct_count(mut self, count: usize) -> Self {
        self.distinct_count = Some(count);
        self
    }

    /// Set the null count.
    pub fn with_null_count(mut self, count: usize) -> Self {
        self.null_count = count;
        self
    }

    /// Set min and max values.
    pub fn with_range(mut self, min: impl Into<String>, max: impl Into<String>) -> Self {
        self.min_value = Some(min.into());
        self.max_value = Some(max.into());
        self
    }

    /// Set average width.
    pub fn with_avg_width(mut self, width: usize) -> Self {
        self.avg_width = width;
        self
    }

    /// Set histogram.
    pub fn with_histogram(mut self, histogram: Histogram) -> Self {
        self.histogram = Some(histogram);
        self
    }

    /// Estimate selectivity for an equality predicate.
    pub fn selectivity_eq(&self, _value: &str) -> f64 {
        if let Some(distinct) = self.distinct_count {
            if distinct > 0 {
                return 1.0 / distinct as f64;
            }
        }
        // Default selectivity for equality
        0.1
    }

    /// Estimate selectivity for a range predicate.
    pub fn selectivity_range(&self, _lower: Option<&str>, _upper: Option<&str>) -> f64 {
        // Use histogram if available
        if let Some(ref histogram) = self.histogram {
            // Simple estimation based on bucket coverage
            let total_buckets = histogram.buckets.len();
            if total_buckets == 0 {
                return 0.33; // Default
            }
            // For now, return a fraction based on bucket count
            return 0.33;
        }

        // Default selectivity for range
        0.33
    }

    /// Estimate selectivity for a LIKE predicate.
    pub fn selectivity_like(&self, pattern: &str) -> f64 {
        if pattern.starts_with('%') && pattern.ends_with('%') {
            // Contains pattern - higher selectivity
            0.05
        } else if pattern.starts_with('%') {
            // Ends with pattern
            0.1
        } else if pattern.ends_with('%') {
            // Starts with pattern - can use index
            0.25
        } else {
            // Exact match
            self.selectivity_eq(pattern)
        }
    }

    /// Estimate selectivity for IS NULL predicate.
    pub fn selectivity_null(&self, row_count: usize) -> f64 {
        if row_count > 0 {
            self.null_count as f64 / row_count as f64
        } else {
            0.01 // Default null selectivity
        }
    }
}

/// Histogram for value distribution.
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Histogram buckets
    pub buckets: Vec<HistogramBucket>,
    /// Type of histogram
    pub histogram_type: HistogramType,
}

/// Type of histogram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramType {
    /// Equal-width buckets
    Equiwidth,
    /// Equal-height (equi-depth) buckets
    Equiheight,
    /// Singleton buckets for most common values
    Singleton,
}

/// A single histogram bucket.
#[derive(Debug, Clone)]
pub struct HistogramBucket {
    /// Lower bound of the bucket
    pub lower_bound: String,
    /// Upper bound of the bucket
    pub upper_bound: String,
    /// Number of values in the bucket
    pub frequency: usize,
    /// Number of distinct values in the bucket
    pub distinct_count: usize,
}

impl Histogram {
    /// Create a new equi-width histogram.
    pub fn equiwidth(buckets: Vec<HistogramBucket>) -> Self {
        Self {
            buckets,
            histogram_type: HistogramType::Equiwidth,
        }
    }

    /// Create a new equi-height histogram.
    pub fn equiheight(buckets: Vec<HistogramBucket>) -> Self {
        Self {
            buckets,
            histogram_type: HistogramType::Equiheight,
        }
    }

    /// Create a singleton histogram for most common values.
    pub fn singleton(buckets: Vec<HistogramBucket>) -> Self {
        Self {
            buckets,
            histogram_type: HistogramType::Singleton,
        }
    }

    /// Get the total frequency across all buckets.
    pub fn total_frequency(&self) -> usize {
        self.buckets.iter().map(|b| b.frequency).sum()
    }

    /// Estimate frequency for a specific value.
    pub fn estimate_frequency(&self, value: &str) -> usize {
        for bucket in &self.buckets {
            if value >= bucket.lower_bound.as_str() && value <= bucket.upper_bound.as_str() {
                // Assume uniform distribution within bucket
                if bucket.distinct_count > 0 {
                    return bucket.frequency / bucket.distinct_count;
                }
                return bucket.frequency;
            }
        }
        0
    }

    /// Estimate the fraction of values in a range.
    pub fn estimate_range_selectivity(&self, lower: Option<&str>, upper: Option<&str>) -> f64 {
        let total = self.total_frequency();
        if total == 0 {
            return 0.5;
        }

        let mut matching_freq = 0usize;
        for bucket in &self.buckets {
            let bucket_lower = bucket.lower_bound.as_str();
            let bucket_upper = bucket.upper_bound.as_str();

            // Check if bucket overlaps with range
            let overlaps = match (lower, upper) {
                (Some(l), Some(u)) => bucket_upper >= l && bucket_lower <= u,
                (Some(l), None) => bucket_upper >= l,
                (None, Some(u)) => bucket_lower <= u,
                (None, None) => true,
            };

            if overlaps {
                matching_freq += bucket.frequency;
            }
        }

        matching_freq as f64 / total as f64
    }
}

impl HistogramBucket {
    /// Create a new histogram bucket.
    pub fn new(
        lower_bound: impl Into<String>,
        upper_bound: impl Into<String>,
        frequency: usize,
        distinct_count: usize,
    ) -> Self {
        Self {
            lower_bound: lower_bound.into(),
            upper_bound: upper_bound.into(),
            frequency,
            distinct_count,
        }
    }
}

/// Statistics for a table.
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Table name
    pub table_name: String,
    /// Number of rows in the table
    pub row_count: usize,
    /// Total size in bytes
    pub size_bytes: usize,
    /// Column statistics
    pub columns: HashMap<String, ColumnStatistics>,
    /// Last time statistics were updated
    pub last_updated: Option<u64>,
}

impl TableStatistics {
    /// Create new table statistics.
    pub fn new(table_name: impl Into<String>, row_count: usize) -> Self {
        Self {
            table_name: table_name.into(),
            row_count,
            size_bytes: 0,
            columns: HashMap::new(),
            last_updated: None,
        }
    }

    /// Set the size in bytes.
    pub fn with_size(mut self, size_bytes: usize) -> Self {
        self.size_bytes = size_bytes;
        self
    }

    /// Add column statistics.
    pub fn with_column(mut self, column: ColumnStatistics) -> Self {
        self.columns.insert(column.name.clone(), column);
        self
    }

    /// Get column statistics by name.
    pub fn column(&self, name: &str) -> Option<&ColumnStatistics> {
        self.columns.get(name)
    }

    /// Get the average row width.
    pub fn avg_row_width(&self) -> usize {
        if self.columns.is_empty() {
            return 100; // Default estimate
        }
        self.columns.values().map(|c| c.avg_width).sum()
    }

    /// Estimate the number of pages needed to store this table.
    pub fn num_pages(&self, page_size: usize) -> usize {
        let total_size = self.row_count * self.avg_row_width();
        (total_size / page_size) + 1
    }
}

/// Manager for table statistics.
#[derive(Debug, Default)]
pub struct StatisticsManager {
    /// Statistics for each table
    tables: Arc<RwLock<HashMap<String, TableStatistics>>>,
}

impl StatisticsManager {
    /// Create a new statistics manager.
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register statistics for a table.
    pub fn register(&self, stats: TableStatistics) -> Result<()> {
        let mut tables = self.tables.write().map_err(|e| {
            BlazeError::internal(format!("Failed to acquire write lock: {}", e))
        })?;
        tables.insert(stats.table_name.clone(), stats);
        Ok(())
    }

    /// Get statistics for a table.
    pub fn get(&self, table_name: &str) -> Option<TableStatistics> {
        let tables = self.tables.read().ok()?;
        tables.get(table_name).cloned()
    }

    /// Remove statistics for a table.
    pub fn remove(&self, table_name: &str) -> Result<Option<TableStatistics>> {
        let mut tables = self.tables.write().map_err(|e| {
            BlazeError::internal(format!("Failed to acquire write lock: {}", e))
        })?;
        Ok(tables.remove(table_name))
    }

    /// List all tables with statistics.
    pub fn list_tables(&self) -> Vec<String> {
        let tables = self.tables.read().ok();
        tables.map(|t| t.keys().cloned().collect()).unwrap_or_default()
    }

    /// Estimate join cardinality between two tables.
    pub fn estimate_join_cardinality(
        &self,
        left_table: &str,
        right_table: &str,
        left_key: &str,
        right_key: &str,
    ) -> usize {
        let left_stats = self.get(left_table);
        let right_stats = self.get(right_table);

        match (left_stats, right_stats) {
            (Some(left), Some(right)) => {
                let left_rows = left.row_count;
                let right_rows = right.row_count;

                // Get distinct counts for join keys
                let left_distinct = left.column(left_key)
                    .and_then(|c| c.distinct_count)
                    .unwrap_or(left_rows);
                let right_distinct = right.column(right_key)
                    .and_then(|c| c.distinct_count)
                    .unwrap_or(right_rows);

                // Use the standard join cardinality formula:
                // |L ⋈ R| = |L| * |R| / max(V(L.a), V(R.b))
                let max_distinct = left_distinct.max(right_distinct).max(1);
                (left_rows * right_rows) / max_distinct
            }
            (Some(left), None) => left.row_count,
            (None, Some(right)) => right.row_count,
            (None, None) => 1000, // Default estimate
        }
    }
}

impl Clone for StatisticsManager {
    fn clone(&self) -> Self {
        Self {
            tables: self.tables.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_statistics() {
        let stats = ColumnStatistics::new("id", DataType::Int32)
            .with_distinct_count(1000)
            .with_null_count(5)
            .with_range("1", "1000");

        assert_eq!(stats.name, "id");
        assert_eq!(stats.distinct_count, Some(1000));
        assert_eq!(stats.null_count, 5);
        assert_eq!(stats.min_value, Some("1".to_string()));
        assert_eq!(stats.max_value, Some("1000".to_string()));
    }

    #[test]
    fn test_selectivity_eq() {
        let stats = ColumnStatistics::new("status", DataType::Utf8)
            .with_distinct_count(5);

        let selectivity = stats.selectivity_eq("active");
        assert!((selectivity - 0.2).abs() < 0.001);
    }

    #[test]
    fn test_histogram() {
        let histogram = Histogram::equiheight(vec![
            HistogramBucket::new("000", "100", 1000, 100),
            HistogramBucket::new("101", "200", 1000, 100),
            HistogramBucket::new("201", "300", 1000, 100),
        ]);

        assert_eq!(histogram.total_frequency(), 3000);
        // "050" is lexicographically between "000" and "100"
        assert!(histogram.estimate_frequency("050") > 0);
        // "150" is lexicographically between "101" and "200"
        assert!(histogram.estimate_frequency("150") > 0);
    }

    #[test]
    fn test_table_statistics() {
        let stats = TableStatistics::new("users", 10000)
            .with_size(1024 * 1024)
            .with_column(ColumnStatistics::new("id", DataType::Int32)
                .with_distinct_count(10000)
                .with_avg_width(4))
            .with_column(ColumnStatistics::new("name", DataType::Utf8)
                .with_distinct_count(8000)
                .with_avg_width(50));

        assert_eq!(stats.row_count, 10000);
        assert_eq!(stats.avg_row_width(), 54);
        assert!(stats.column("id").is_some());
    }

    #[test]
    fn test_statistics_manager() {
        let manager = StatisticsManager::new();

        let stats = TableStatistics::new("orders", 100000);
        manager.register(stats).unwrap();

        let retrieved = manager.get("orders");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().row_count, 100000);

        assert!(manager.get("nonexistent").is_none());
    }

    #[test]
    fn test_join_cardinality_estimation() {
        let manager = StatisticsManager::new();

        // Register orders table
        let orders = TableStatistics::new("orders", 10000)
            .with_column(ColumnStatistics::new("customer_id", DataType::Int32)
                .with_distinct_count(1000));
        manager.register(orders).unwrap();

        // Register customers table
        let customers = TableStatistics::new("customers", 1000)
            .with_column(ColumnStatistics::new("id", DataType::Int32)
                .with_distinct_count(1000));
        manager.register(customers).unwrap();

        // Estimate join cardinality
        let cardinality = manager.estimate_join_cardinality(
            "orders", "customers", "customer_id", "id"
        );

        // Expected: 10000 * 1000 / max(1000, 1000) = 10000
        assert_eq!(cardinality, 10000);
    }
}
