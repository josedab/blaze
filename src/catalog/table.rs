//! Table abstraction for Blaze.

use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::planner::PhysicalExpr;
use crate::types::Schema;

/// Type of table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    /// A base table with stored data
    Base,
    /// A view (virtual table)
    View,
    /// A temporary table
    Temporary,
    /// An external table
    External,
}

/// Statistics about a table.
#[derive(Debug, Clone, Default)]
pub struct TableStatistics {
    /// Number of rows (if known)
    pub num_rows: Option<usize>,
    /// Total size in bytes (if known)
    pub total_byte_size: Option<usize>,
    /// Per-column statistics
    pub column_statistics: Vec<ColumnStatistics>,
}

/// Statistics about a column.
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    /// Number of null values
    pub null_count: Option<usize>,
    /// Number of distinct values
    pub distinct_count: Option<usize>,
    /// Minimum value (as string for simplicity)
    pub min_value: Option<String>,
    /// Maximum value (as string for simplicity)
    pub max_value: Option<String>,
}

/// Trait for table providers.
///
/// A table provider is responsible for providing schema information
/// and creating scan operations for reading data.
pub trait TableProvider: Debug + Send + Sync {
    /// Return this provider as `Any` for downcasting.
    fn as_any(&self) -> &dyn Any;

    /// Get the schema of this table.
    fn schema(&self) -> &Schema;

    /// Get the table type.
    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Get table statistics.
    fn statistics(&self) -> Option<TableStatistics> {
        None
    }

    /// Scan the table, optionally projecting columns and applying filters.
    ///
    /// # Arguments
    /// * `projection` - Optional list of column indices to project
    /// * `filters` - Filters to push down (not implemented yet)
    /// * `limit` - Optional row limit
    fn scan(
        &self,
        projection: Option<&[usize]>,
        filters: &[()], // TODO: Replace with actual filter type
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>>;

    /// Check if this table supports filter pushdown.
    fn supports_filter_pushdown(&self) -> bool {
        false
    }

    /// Check if this table supports limit pushdown.
    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    /// Scan the table with physical expression filters for filter pushdown.
    /// This allows storage implementations to apply filters during scan.
    ///
    /// Default implementation ignores filters and calls the basic scan method.
    fn scan_with_filters(
        &self,
        projection: Option<&[usize]>,
        filters: &[Arc<dyn PhysicalExpr>],
        limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        // Default: ignore filters and fall back to basic scan
        let _ = filters;
        self.scan(projection, &[], limit)
    }

    /// Insert data into the table.
    fn insert(&self, _batches: Vec<RecordBatch>) -> Result<usize> {
        Err(crate::error::BlazeError::not_implemented(
            "Insert not supported for this table type",
        ))
    }

    /// Delete data from the table.
    fn delete(&self, _predicate: Option<&()>) -> Result<usize> {
        Err(crate::error::BlazeError::not_implemented(
            "Delete not supported for this table type",
        ))
    }

    /// Update data in the table.
    fn update(&self, _assignments: &[()], _predicate: Option<&()>) -> Result<usize> {
        Err(crate::error::BlazeError::not_implemented(
            "Update not supported for this table type",
        ))
    }
}

/// A table definition (metadata about a table).
#[derive(Debug, Clone)]
pub struct Table {
    /// Table name
    pub name: String,
    /// Table schema
    pub schema: Schema,
    /// Table type
    pub table_type: TableType,
    /// Optional comment
    pub comment: Option<String>,
    /// Table provider
    provider: Option<Arc<dyn TableProvider>>,
}

impl Table {
    /// Create a new table definition.
    pub fn new(name: impl Into<String>, schema: Schema) -> Self {
        Self {
            name: name.into(),
            schema,
            table_type: TableType::Base,
            comment: None,
            provider: None,
        }
    }

    /// Set the table type.
    pub fn with_table_type(mut self, table_type: TableType) -> Self {
        self.table_type = table_type;
        self
    }

    /// Set the table comment.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Set the table provider.
    pub fn with_provider(mut self, provider: Arc<dyn TableProvider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// Get the table provider.
    pub fn provider(&self) -> Option<&Arc<dyn TableProvider>> {
        self.provider.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DataType, Field};

    #[test]
    fn test_table_creation() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]);

        let table = Table::new("users", schema.clone())
            .with_table_type(TableType::Base)
            .with_comment("User table");

        assert_eq!(table.name, "users");
        assert_eq!(table.table_type, TableType::Base);
        assert_eq!(table.comment, Some("User table".to_string()));
        assert_eq!(table.schema.len(), 2);
    }
}
