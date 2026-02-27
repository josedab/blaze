//! View table implementation for Blaze.
//!
//! A view stores the original SQL query and its schema.
//! Views are expanded by the binder during planning, not scanned directly.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::catalog::{TableProvider, TableType};
use crate::error::{BlazeError, Result};
use crate::planner::PhysicalExpr;
use crate::types::Schema;

/// A view table that stores the defining SQL query.
pub struct ViewTable {
    sql: String,
    schema: Schema,
}

impl fmt::Debug for ViewTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ViewTable")
            .field("sql", &self.sql)
            .field("schema", &self.schema)
            .finish()
    }
}

impl ViewTable {
    /// Create a new view table with the given SQL and schema.
    pub fn new(sql: String, schema: Schema) -> Self {
        Self { sql, schema }
    }

    /// Get the SQL query that defines this view.
    pub fn view_sql(&self) -> &str {
        &self.sql
    }
}

impl TableProvider for ViewTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn scan(
        &self,
        _projection: Option<&[usize]>,
        _filters: &[()],
        _limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        Err(BlazeError::internal(
            "Views should be expanded during planning, not scanned directly",
        ))
    }

    fn scan_with_filters(
        &self,
        _projection: Option<&[usize]>,
        _filters: &[Arc<dyn PhysicalExpr>],
        _limit: Option<usize>,
    ) -> Result<Vec<RecordBatch>> {
        Err(BlazeError::internal(
            "Views should be expanded during planning, not scanned directly",
        ))
    }
}
