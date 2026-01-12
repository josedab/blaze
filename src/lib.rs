//! Blaze - Next-Generation Embedded OLAP Query Engine
//!
//! Blaze is a high-performance, memory-safe embedded analytical query engine
//! written in Rust. It provides SQL:2016 compliance with native Apache Arrow
//! and Parquet integration.
//!
//! # Features
//!
//! - **Memory Safety**: Built in Rust for guaranteed memory safety
//! - **SQL:2016 Compliance**: Full support for analytical SQL queries
//! - **Vectorized Execution**: Columnar processing with SIMD optimization
//! - **Native Arrow/Parquet**: First-class support for modern data formats
//! - **Embeddable**: In-process database with zero network overhead
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use blaze::{Connection, Result};
//!
//! fn main() -> Result<()> {
//!     // Create an in-memory database
//!     let conn = Connection::in_memory()?;
//!
//!     // Create a table
//!     conn.execute("CREATE TABLE users (id INT, name VARCHAR)")?;
//!
//!     // Query data
//!     let results = conn.query("SELECT * FROM users")?;
//!
//!     for batch in results {
//!         println!("{:?}", batch);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # Reading External Files
//!
//! ```rust,no_run
//! use blaze::{Connection, Result};
//!
//! fn main() -> Result<()> {
//!     let conn = Connection::in_memory()?;
//!
//!     // Register a CSV file as a table
//!     conn.register_csv("sales", "data/sales.csv")?;
//!
//!     // Register a Parquet file as a table
//!     conn.register_parquet("customers", "data/customers.parquet")?;
//!
//!     // Query across files
//!     let results = conn.query("
//!         SELECT c.name, SUM(s.amount)
//!         FROM sales s
//!         JOIN customers c ON s.customer_id = c.id
//!         GROUP BY c.name
//!     ")?;
//!
//!     Ok(())
//! }
//! ```

pub mod adaptive;
pub mod approx;
pub mod catalog;
pub mod error;
pub mod executor;
pub mod gpu;
pub mod json;
pub mod lakehouse;
pub mod nlq;
pub mod optimizer;
pub mod parallel;
pub mod planner;
pub mod simd;
pub mod sql;
pub mod storage;
pub mod streaming;
pub mod timeseries;
pub mod types;
pub mod wasm;

// Re-export commonly used types
pub use error::{BlazeError, Result};
pub use types::{DataType, Field, ScalarValue, Schema};

use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use catalog::{CatalogList, TableProvider};
use executor::ExecutionContext;
use planner::{Binder, Optimizer, PhysicalPlanner};
use sql::parser::Parser;
use storage::{CsvTable, MemoryTable, ParquetTable};

/// Database connection for executing queries.
///
/// The `Connection` is the main entry point for interacting with Blaze.
/// It manages the catalog, handles query parsing, planning, and execution.
pub struct Connection {
    /// The catalog list containing catalogs, schemas, and tables
    catalog_list: Arc<CatalogList>,
    /// Execution context with configuration
    execution_context: ExecutionContext,
    /// Query optimizer
    optimizer: Optimizer,
}

impl Connection {
    /// Create a new in-memory database connection.
    ///
    /// # Example
    ///
    /// ```rust
    /// use blaze::Connection;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// ```
    pub fn in_memory() -> Result<Self> {
        let catalog_list = Arc::new(CatalogList::default());
        Ok(Self {
            execution_context: ExecutionContext::new()
                .with_catalog_list(catalog_list.clone()),
            catalog_list,
            optimizer: Optimizer::default(),
        })
    }

    /// Create a connection with custom configuration.
    pub fn with_config(config: ConnectionConfig) -> Result<Self> {
        let catalog_list = Arc::new(CatalogList::default());
        let mut ctx = ExecutionContext::new()
            .with_batch_size(config.batch_size)
            .with_catalog_list(catalog_list.clone());

        if let Some(limit) = config.memory_limit {
            ctx = ctx.with_memory_limit(limit);
        }

        Ok(Self {
            execution_context: ctx,
            catalog_list,
            optimizer: Optimizer::default(),
        })
    }

    /// Execute a SQL query and return results.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// let results = conn.query("SELECT 1 + 1").unwrap();
    /// ```
    pub fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // Parse SQL
        let statements = Parser::parse(sql)?;

        // Take the first statement (ignore any additional statements)
        let Some(statement) = statements.into_iter().next() else {
            return Ok(vec![]);
        };

        // Bind and create logical plan
        let binder = Binder::new(self.catalog_list.clone());
        let logical_plan = binder.bind(statement)?;

        // Optimize logical plan
        let optimized_plan = self.optimizer.optimize(&logical_plan)?;

        // Create physical plan
        let physical_planner = PhysicalPlanner::new();
        let physical_plan = physical_planner.create_physical_plan(&optimized_plan)?;

        // Execute
        self.execution_context.execute(&physical_plan)
    }

    /// Execute a SQL statement that doesn't return results.
    ///
    /// This is used for DDL statements like CREATE TABLE, DROP TABLE, etc.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// conn.execute("CREATE TABLE users (id INT, name VARCHAR)").unwrap();
    /// ```
    pub fn execute(&self, sql: &str) -> Result<usize> {
        // Parse SQL
        let statements = Parser::parse(sql)?;

        // Take the first statement (ignore any additional statements)
        let Some(statement) = statements.into_iter().next() else {
            return Ok(0);
        };

        // Handle DDL statements directly
        match &statement {
            sql::Statement::CreateTable(create) => {
                let schema = Schema::new(
                    create.columns
                        .iter()
                        .map(|col| {
                            Field::new(
                                &col.name,
                                DataType::from_sql_type(&col.data_type),
                                col.nullable,
                            )
                        })
                        .collect(),
                );

                let table_name = create.name.last().cloned().unwrap_or_default();
                let table = MemoryTable::empty(schema);
                self.register_table(&table_name, Arc::new(table))?;
                Ok(0)
            }
            sql::Statement::DropTable(drop) => {
                let table_name = drop.name.last().cloned().unwrap_or_default();
                match self.deregister_table(&table_name) {
                    Ok(_) => Ok(0),
                    Err(_) if drop.if_exists => Ok(0),
                    Err(e) => Err(e),
                }
            }
            _ => {
                // For other statements, bind and execute through the query path
                let binder = Binder::new(self.catalog_list.clone());
                let logical_plan = binder.bind(statement)?;
                let optimized_plan = self.optimizer.optimize(&logical_plan)?;
                let physical_planner = PhysicalPlanner::new();
                let physical_plan = physical_planner.create_physical_plan(&optimized_plan)?;
                let results = self.execution_context.execute(&physical_plan)?;
                let count: usize = results.iter().map(|b| b.num_rows()).sum();
                Ok(count)
            }
        }
    }

    /// Register a CSV file as a table.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// conn.register_csv("sales", "data/sales.csv").unwrap();
    /// ```
    pub fn register_csv(&self, name: &str, path: impl AsRef<Path>) -> Result<()> {
        let table = CsvTable::open(path)?;
        self.register_table(name, Arc::new(table))
    }

    /// Register a CSV file with custom options.
    pub fn register_csv_with_options(
        &self,
        name: &str,
        path: impl AsRef<Path>,
        options: storage::CsvOptions,
    ) -> Result<()> {
        let table = CsvTable::open_with_options(path, options)?;
        self.register_table(name, Arc::new(table))
    }

    /// Register a Parquet file as a table.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// conn.register_parquet("customers", "data/customers.parquet").unwrap();
    /// ```
    pub fn register_parquet(&self, name: &str, path: impl AsRef<Path>) -> Result<()> {
        let table = ParquetTable::open(path)?;
        self.register_table(name, Arc::new(table))
    }

    /// Register a Parquet file with custom options.
    pub fn register_parquet_with_options(
        &self,
        name: &str,
        path: impl AsRef<Path>,
        options: storage::ParquetOptions,
    ) -> Result<()> {
        let table = ParquetTable::open_with_options(path, options)?;
        self.register_table(name, Arc::new(table))
    }

    /// Register record batches as a table.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    /// use arrow::record_batch::RecordBatch;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// // Assuming `batches` is a Vec<RecordBatch>
    /// // conn.register_batches("my_table", batches).unwrap();
    /// ```
    pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Err(BlazeError::invalid_argument("Cannot register empty batches"));
        }

        let schema = Schema::from_arrow(batches[0].schema().as_ref())?;
        let table = MemoryTable::new(schema, batches);
        self.register_table(name, Arc::new(table))
    }

    /// Register a custom table provider in the default schema.
    pub fn register_table(
        &self,
        name: &str,
        table: Arc<dyn TableProvider>,
    ) -> Result<()> {
        // Get the default catalog and schema
        let catalog = self.catalog_list.catalog("default")
            .ok_or_else(|| BlazeError::schema("Default catalog not found"))?;
        let schema = catalog.schema("main")
            .ok_or_else(|| BlazeError::schema("Default schema 'main' not found"))?;
        schema.register_table(name, table)?;
        Ok(())
    }

    /// Deregister a table from the default schema.
    pub fn deregister_table(&self, name: &str) -> Result<()> {
        let catalog = self.catalog_list.catalog("default")
            .ok_or_else(|| BlazeError::schema("Default catalog not found"))?;
        let schema = catalog.schema("main")
            .ok_or_else(|| BlazeError::schema("Default schema 'main' not found"))?;
        schema.deregister_table(name)?;
        Ok(())
    }

    /// List all tables in the default schema.
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog_list.catalog("default")
            .and_then(|c| c.schema("main"))
            .map(|s| s.table_names())
            .unwrap_or_default()
    }

    /// Get the schema of a table.
    pub fn table_schema(&self, name: &str) -> Option<Schema> {
        self.catalog_list.catalog("default")
            .and_then(|c| c.schema("main"))
            .and_then(|s| s.table(name))
            .map(|t| t.schema().clone())
    }

    /// Get a reference to the catalog list.
    pub fn catalog_list(&self) -> Arc<CatalogList> {
        self.catalog_list.clone()
    }

    /// Set the batch size for query execution.
    pub fn set_batch_size(&mut self, batch_size: usize) {
        self.execution_context = ExecutionContext::new().with_batch_size(batch_size);
    }
}

impl Default for Connection {
    fn default() -> Self {
        Self::in_memory().expect("Failed to create in-memory connection")
    }
}

/// Configuration options for a database connection.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Batch size for vectorized execution
    pub batch_size: usize,
    /// Maximum memory limit (in bytes)
    pub memory_limit: Option<usize>,
    /// Number of threads for parallel execution
    pub num_threads: Option<usize>,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            memory_limit: None,
            num_threads: None,
        }
    }
}

impl ConnectionConfig {
    /// Minimum allowed batch size.
    const MIN_BATCH_SIZE: usize = 1;
    /// Maximum allowed batch size (16 million rows).
    const MAX_BATCH_SIZE: usize = 16_777_216;

    /// Create a new configuration builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the batch size.
    ///
    /// # Panics
    /// Panics if batch_size is 0 or exceeds 16 million.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        assert!(
            batch_size >= Self::MIN_BATCH_SIZE && batch_size <= Self::MAX_BATCH_SIZE,
            "batch_size must be between {} and {}, got {}",
            Self::MIN_BATCH_SIZE,
            Self::MAX_BATCH_SIZE,
            batch_size
        );
        self.batch_size = batch_size;
        self
    }

    /// Set the memory limit.
    pub fn with_memory_limit(mut self, limit: usize) -> Self {
        self.memory_limit = Some(limit);
        self
    }

    /// Set the number of threads.
    pub fn with_num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = Some(num_threads);
        self
    }
}

/// Prelude module for common imports.
pub mod prelude {
    pub use crate::error::{BlazeError, Result};
    pub use crate::types::{DataType, Field, ScalarValue, Schema};
    pub use crate::{Connection, ConnectionConfig};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_in_memory() {
        let conn = Connection::in_memory().unwrap();
        assert!(conn.list_tables().is_empty());
    }

    #[test]
    fn test_connection_with_config() {
        let config = ConnectionConfig::new()
            .with_batch_size(4096)
            .with_memory_limit(1024 * 1024 * 1024);

        let conn = Connection::with_config(config).unwrap();
        assert!(conn.list_tables().is_empty());
    }
}
