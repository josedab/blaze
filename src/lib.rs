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
//! - **Prepared Statements**: Safe parameterized queries with `$1`, `$2` syntax
//!
//! # Quick Start
//!
//! The most common way to use Blaze is to register external files as queryable tables:
//!
//! ```rust,no_run
//! use blaze::{Connection, Result};
//!
//! fn main() -> Result<()> {
//!     let conn = Connection::in_memory()?;
//!
//!     // Register files as tables
//!     conn.register_csv("sales", "data/sales.csv")?;
//!     conn.register_parquet("customers", "data/customers.parquet")?;
//!
//!     // Query across files
//!     let results = conn.query("
//!         SELECT c.name, SUM(s.amount) as total
//!         FROM sales s
//!         JOIN customers c ON s.customer_id = c.id
//!         GROUP BY c.name
//!         ORDER BY total DESC
//!     ")?;
//!
//!     for batch in &results {
//!         println!("Got {} rows", batch.num_rows());
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! # In-Memory Tables with DDL
//!
//! You can also create in-memory tables with DDL statements. Note that
//! `INSERT`, `UPDATE`, and `DELETE` only work with in-memory tables.
//!
//! ```rust,no_run
//! use blaze::{Connection, Result};
//!
//! fn main() -> Result<()> {
//!     let conn = Connection::in_memory()?;
//!
//!     // Create an in-memory table
//!     conn.execute("CREATE TABLE users (id INT, name VARCHAR, active BOOLEAN)")?;
//!
//!     // Insert data (in-memory tables only)
//!     conn.execute("INSERT INTO users VALUES (1, 'Alice', true)")?;
//!     conn.execute("INSERT INTO users VALUES (2, 'Bob', false)")?;
//!
//!     // Query the data
//!     let results = conn.query("SELECT * FROM users WHERE active = true")?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Prepared Statements
//!
//! Use prepared statements for safe, efficient parameterized queries:
//!
//! ```rust,no_run
//! use blaze::{Connection, ScalarValue, PreparedStatementCache, Result};
//!
//! fn main() -> Result<()> {
//!     let conn = Connection::in_memory()?;
//!     conn.execute("CREATE TABLE users (id INT, name VARCHAR)")?;
//!
//!     // Prepare a query with parameters
//!     let stmt = conn.prepare("SELECT * FROM users WHERE id = $1")?;
//!
//!     // Execute with different parameter values
//!     let results1 = stmt.execute(&[ScalarValue::Int64(Some(1))])?;
//!     let results2 = stmt.execute(&[ScalarValue::Int64(Some(2))])?;
//!
//!     // For frequently-used queries, use the statement cache
//!     let cache = PreparedStatementCache::new(100);
//!     let stmt = conn.prepare_cached("SELECT * FROM users WHERE id = $1", &cache)?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Registering Arrow Data
//!
//! You can register Arrow RecordBatches directly as tables:
//!
//! ```rust,no_run
//! use blaze::{Connection, Result};
//! use arrow::array::{Int64Array, StringArray};
//! use arrow::datatypes::{DataType, Field, Schema};
//! use arrow::record_batch::RecordBatch;
//! use std::sync::Arc;
//!
//! fn main() -> Result<()> {
//!     let conn = Connection::in_memory()?;
//!
//!     // Create Arrow data
//!     let schema = Arc::new(Schema::new(vec![
//!         Field::new("id", DataType::Int64, false),
//!         Field::new("name", DataType::Utf8, false),
//!     ]));
//!
//!     let batch = RecordBatch::try_new(
//!         schema,
//!         vec![
//!             Arc::new(Int64Array::from(vec![1, 2, 3])),
//!             Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
//!         ],
//!     )?;
//!
//!     // Register as a queryable table
//!     conn.register_batches("users", vec![batch])?;
//!
//!     // Query the data
//!     let results = conn.query("SELECT * FROM users WHERE id > 1")?;
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
pub mod output;
pub mod parallel;
pub mod planner;
pub mod prepared;
pub mod simd;
pub mod sql;
pub mod storage;
pub mod streaming;
pub mod timeseries;
pub mod types;
pub mod wasm;

// C FFI bindings (conditional on feature)
#[cfg(feature = "c-ffi")]
pub mod ffi;

// Python bindings (conditional on feature)
#[cfg(feature = "python")]
pub mod python;

// Re-export commonly used types
pub use error::{BlazeError, Result};
pub use prepared::{PreparedStatement, PreparedStatementCache, ParameterInfo};
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
            sql::Statement::Insert(insert) => {
                self.execute_insert(insert.clone())
            }
            sql::Statement::Update(update) => {
                self.execute_update(update.clone())
            }
            sql::Statement::Delete(delete) => {
                self.execute_delete(delete.clone())
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

    /// Execute an INSERT statement.
    fn execute_insert(&self, insert: sql::parser::Insert) -> Result<usize> {
        use arrow::array::*;

        // Get the table name
        let table_name = insert.table_name.last().cloned().unwrap_or_default();

        // Get the table from the catalog
        let table = self.catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?
            .get_table(&table_name)
            .ok_or_else(|| BlazeError::catalog(format!("Table '{}' not found", table_name)))?;

        // Get the table as MemoryTable (DML only works on MemoryTable)
        let memory_table = table.as_any()
            .downcast_ref::<MemoryTable>()
            .ok_or_else(|| BlazeError::not_implemented(
                "INSERT is only supported for in-memory tables"
            ))?;

        let table_schema = table.schema();

        // Process the insert source
        let batches = match insert.source {
            sql::parser::InsertSource::Values(rows) => {
                // Convert VALUES into RecordBatch
                let mut columns: Vec<Vec<ScalarValue>> = vec![Vec::new(); table_schema.len()];

                for row in &rows {
                    if row.len() != table_schema.len() {
                        return Err(BlazeError::analysis(format!(
                            "INSERT value count ({}) doesn't match column count ({})",
                            row.len(),
                            table_schema.len()
                        )));
                    }

                    for (i, value) in row.iter().enumerate() {
                        let scalar = self.eval_literal(value)?;
                        columns[i].push(scalar);
                    }
                }

                // Convert to arrays
                let arrow_schema = Arc::new(table_schema.to_arrow());
                let arrays: Vec<ArrayRef> = columns
                    .into_iter()
                    .map(|col| ScalarValue::vec_to_array(&col))
                    .collect::<Result<Vec<_>>>()?;

                vec![RecordBatch::try_new(arrow_schema, arrays)?]
            }
            sql::parser::InsertSource::Query(query) => {
                // Execute the query
                let statement = sql::Statement::Query(query);
                let binder = Binder::new(self.catalog_list.clone());
                let logical_plan = binder.bind(statement)?;
                let optimized_plan = self.optimizer.optimize(&logical_plan)?;
                let physical_planner = PhysicalPlanner::new();
                let physical_plan = physical_planner.create_physical_plan(&optimized_plan)?;
                self.execution_context.execute(&physical_plan)?
            }
        };

        // Count the rows being inserted
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();

        // Append to the table
        memory_table.append(batches);

        Ok(row_count)
    }

    /// Execute an UPDATE statement.
    fn execute_update(&self, update: sql::parser::Update) -> Result<usize> {
        use arrow::compute::filter_record_batch;

        // Get the table name
        let table_name = update.table_name.last().cloned().unwrap_or_default();

        // Get the table from the catalog
        let table = self.catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?
            .get_table(&table_name)
            .ok_or_else(|| BlazeError::catalog(format!("Table '{}' not found", table_name)))?;

        // Get the table as MemoryTable
        let memory_table = table.as_any()
            .downcast_ref::<MemoryTable>()
            .ok_or_else(|| BlazeError::not_implemented(
                "UPDATE is only supported for in-memory tables"
            ))?;

        // Get current data
        let batches = memory_table.batches();
        if batches.is_empty() {
            return Ok(0);
        }

        let table_schema = table.schema();
        let arrow_schema = Arc::new(table_schema.to_arrow());

        // Build the predicate expression
        let predicate_expr = if let Some(ref selection) = update.selection {
            let binder = Binder::new(self.catalog_list.clone());
            let mut ctx = planner::BindContext::new();
            let logical_expr = binder.bind_expr_public(selection.clone(), &mut ctx)?;

            let physical_planner = PhysicalPlanner::new();
            Some(physical_planner.create_physical_expr_public(&logical_expr, &arrow_schema)?)
        } else {
            None
        };

        // Process updates
        let mut updated_count = 0;
        let mut new_batches = Vec::new();

        for batch in batches {
            // Find rows to update
            let mask = if let Some(ref pred) = predicate_expr {
                let result = pred.evaluate(&batch)?;
                result.as_any().downcast_ref::<arrow::array::BooleanArray>()
                    .cloned()
                    .ok_or_else(|| BlazeError::type_error("Predicate must return boolean"))?
            } else {
                // Update all rows
                arrow::array::BooleanArray::from(vec![true; batch.num_rows()])
            };

            // Count updated rows
            updated_count += mask.iter().filter(|v| v.unwrap_or(false)).count();

            // Build updated batch by applying assignments
            let mut columns: Vec<arrow::array::ArrayRef> = Vec::new();

            for (col_idx, field) in table_schema.fields().iter().enumerate() {
                // Check if this column has an assignment
                let updated_col = if let Some((_, expr)) = update.assignments.iter().find(|(col, _)| col == field.name()) {
                    // Evaluate the assignment expression
                    let binder = Binder::new(self.catalog_list.clone());
                    let mut ctx = planner::BindContext::new();
                    let logical_expr = binder.bind_expr_public(expr.clone(), &mut ctx)?;

                    let physical_planner = PhysicalPlanner::new();
                    let phys_expr = physical_planner.create_physical_expr_public(&logical_expr, &arrow_schema)?;
                    let new_values = phys_expr.evaluate(&batch)?;

                    // Merge: use new values where mask is true, old values otherwise
                    Self::merge_arrays(batch.column(col_idx), &new_values, &mask)?
                } else {
                    batch.column(col_idx).clone()
                };

                columns.push(updated_col);
            }

            new_batches.push(RecordBatch::try_new(arrow_schema.clone(), columns)?);
        }

        // Replace table data
        memory_table.replace(new_batches);

        Ok(updated_count)
    }

    /// Execute a DELETE statement.
    fn execute_delete(&self, delete: sql::parser::Delete) -> Result<usize> {
        use arrow::compute::filter_record_batch;

        // Get the table name
        let table_name = delete.table_name.last().cloned().unwrap_or_default();

        // Get the table from the catalog
        let table = self.catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?
            .get_table(&table_name)
            .ok_or_else(|| BlazeError::catalog(format!("Table '{}' not found", table_name)))?;

        // Get the table as MemoryTable
        let memory_table = table.as_any()
            .downcast_ref::<MemoryTable>()
            .ok_or_else(|| BlazeError::not_implemented(
                "DELETE is only supported for in-memory tables"
            ))?;

        // Get current data
        let batches = memory_table.batches();
        if batches.is_empty() {
            return Ok(0);
        }

        let table_schema = table.schema();
        let arrow_schema = Arc::new(table_schema.to_arrow());

        // Build the predicate expression
        let predicate_expr = if let Some(ref selection) = delete.selection {
            let binder = Binder::new(self.catalog_list.clone());
            let mut ctx = planner::BindContext::new();
            let logical_expr = binder.bind_expr_public(selection.clone(), &mut ctx)?;

            let physical_planner = PhysicalPlanner::new();
            Some(physical_planner.create_physical_expr_public(&logical_expr, &arrow_schema)?)
        } else {
            // DELETE with no WHERE deletes all rows
            None
        };

        // Process deletes
        let mut deleted_count = 0;
        let mut new_batches = Vec::new();

        for batch in batches {
            let keep_mask = if let Some(ref pred) = predicate_expr {
                // Evaluate predicate
                let result = pred.evaluate(&batch)?;
                let delete_mask = result.as_any().downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| BlazeError::type_error("Predicate must return boolean"))?;

                // Count deleted rows
                deleted_count += delete_mask.iter().filter(|v| v.unwrap_or(false)).count();

                // Invert: keep rows where predicate is false
                arrow::compute::not(delete_mask)?
            } else {
                // Delete all rows
                deleted_count += batch.num_rows();
                arrow::array::BooleanArray::from(vec![false; batch.num_rows()])
            };

            // Filter to keep non-deleted rows
            let filtered = filter_record_batch(&batch, &keep_mask)?;
            if filtered.num_rows() > 0 {
                new_batches.push(filtered);
            }
        }

        // Replace table data
        memory_table.replace(new_batches);

        Ok(deleted_count)
    }

    /// Merge two arrays based on a boolean mask.
    fn merge_arrays(
        old_values: &arrow::array::ArrayRef,
        new_values: &arrow::array::ArrayRef,
        mask: &arrow::array::BooleanArray,
    ) -> Result<arrow::array::ArrayRef> {
        use arrow::array::*;
        use arrow::datatypes::DataType as ArrowDataType;

        match old_values.data_type() {
            ArrowDataType::Int64 => {
                let old_arr = old_values.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64Array for old values in merge"))?;
                let new_arr = new_values.as_any().downcast_ref::<Int64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Int64Array for new values in merge"))?;
                let mut builder = Int64Builder::new();

                for i in 0..old_arr.len() {
                    if mask.value(i) {
                        builder.append_value(new_arr.value(i));
                    } else {
                        if old_arr.is_null(i) {
                            builder.append_null();
                        } else {
                            builder.append_value(old_arr.value(i));
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ArrowDataType::Utf8 => {
                let old_arr = old_values.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected StringArray for old values in merge"))?;
                let new_arr = new_values.as_any().downcast_ref::<StringArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected StringArray for new values in merge"))?;
                let mut builder = StringBuilder::new();

                for i in 0..old_arr.len() {
                    if mask.value(i) {
                        builder.append_value(new_arr.value(i));
                    } else {
                        if old_arr.is_null(i) {
                            builder.append_null();
                        } else {
                            builder.append_value(old_arr.value(i));
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ArrowDataType::Float64 => {
                let old_arr = old_values.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Float64Array for old values in merge"))?;
                let new_arr = new_values.as_any().downcast_ref::<Float64Array>()
                    .ok_or_else(|| BlazeError::type_error("Expected Float64Array for new values in merge"))?;
                let mut builder = Float64Builder::new();

                for i in 0..old_arr.len() {
                    if mask.value(i) {
                        builder.append_value(new_arr.value(i));
                    } else {
                        if old_arr.is_null(i) {
                            builder.append_null();
                        } else {
                            builder.append_value(old_arr.value(i));
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ArrowDataType::Boolean => {
                let old_arr = old_values.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected BooleanArray for old values in merge"))?;
                let new_arr = new_values.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| BlazeError::type_error("Expected BooleanArray for new values in merge"))?;
                let mut builder = BooleanBuilder::new();

                for i in 0..old_arr.len() {
                    if mask.value(i) {
                        builder.append_value(new_arr.value(i));
                    } else {
                        if old_arr.is_null(i) {
                            builder.append_null();
                        } else {
                            builder.append_value(old_arr.value(i));
                        }
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => Err(BlazeError::not_implemented(format!(
                "UPDATE for data type {:?}",
                old_values.data_type()
            ))),
        }
    }

    /// Evaluate a literal expression to a ScalarValue.
    fn eval_literal(&self, expr: &sql::parser::Expr) -> Result<ScalarValue> {
        use sql::parser::Literal;
        match expr {
            sql::parser::Expr::Literal(Literal::Integer(n)) => Ok(ScalarValue::Int64(Some(*n))),
            sql::parser::Expr::Literal(Literal::Float(n)) => Ok(ScalarValue::Float64(Some(*n))),
            sql::parser::Expr::Literal(Literal::String(s)) => Ok(ScalarValue::Utf8(Some(s.clone()))),
            sql::parser::Expr::Literal(Literal::Boolean(b)) => Ok(ScalarValue::Boolean(Some(*b))),
            sql::parser::Expr::Literal(Literal::Null) => Ok(ScalarValue::Null),
            _ => Err(BlazeError::analysis("Only literal values are supported in INSERT VALUES")),
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
        // Validate table name
        if name.is_empty() {
            return Err(BlazeError::invalid_argument("Table name cannot be empty"));
        }
        if name.chars().all(|c| c.is_whitespace()) {
            return Err(BlazeError::invalid_argument("Table name cannot be only whitespace"));
        }

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

    /// Prepare a SQL statement for repeated execution with parameters.
    ///
    /// This method parses the SQL and creates a logical plan that can be
    /// executed multiple times with different parameter values. Parameters
    /// are specified using the `$1`, `$2`, etc. syntax.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::{Connection, ScalarValue};
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// conn.execute("CREATE TABLE users (id INT, name VARCHAR)").unwrap();
    ///
    /// // Prepare a query with parameters
    /// let stmt = conn.prepare("SELECT * FROM users WHERE id = $1").unwrap();
    ///
    /// // Execute with different parameter values
    /// let results1 = stmt.execute(&[ScalarValue::Int64(Some(1))]).unwrap();
    /// let results2 = stmt.execute(&[ScalarValue::Int64(Some(2))]).unwrap();
    /// ```
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        // Parse SQL
        let statements = Parser::parse(sql)?;

        let Some(statement) = statements.into_iter().next() else {
            return Err(BlazeError::analysis("Empty SQL statement"));
        };

        // Bind and create logical plan
        let binder = Binder::new(self.catalog_list.clone());
        let logical_plan = binder.bind(statement)?;

        // Extract parameter placeholders
        let parameters = prepared::extract_parameters(&logical_plan);

        // Create the prepared statement
        Ok(PreparedStatement::new(
            sql.to_string(),
            logical_plan,
            parameters,
            self.catalog_list.clone(),
            self.execution_context.clone(),
        ))
    }

    /// Prepare a SQL statement using the statement cache.
    ///
    /// This method uses a cache to avoid re-parsing and re-binding
    /// frequently used queries. If the query is already in the cache,
    /// the cached logical plan is reused.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::{Connection, PreparedStatementCache, ScalarValue};
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// let cache = PreparedStatementCache::new(100);
    ///
    /// // First call parses and caches the plan
    /// let stmt1 = conn.prepare_cached("SELECT $1 + $2", &cache).unwrap();
    ///
    /// // Second call reuses the cached plan
    /// let stmt2 = conn.prepare_cached("SELECT $1 + $2", &cache).unwrap();
    /// ```
    pub fn prepare_cached(
        &self,
        sql: &str,
        cache: &PreparedStatementCache,
    ) -> Result<PreparedStatement> {
        let catalog_list = self.catalog_list.clone();

        let (logical_plan, parameters) = cache.get_or_create(sql, || {
            // Parse SQL
            let statements = Parser::parse(sql)?;

            let Some(statement) = statements.into_iter().next() else {
                return Err(BlazeError::analysis("Empty SQL statement"));
            };

            // Bind and create logical plan
            let binder = Binder::new(catalog_list.clone());
            let logical_plan = binder.bind(statement)?;

            // Extract parameter placeholders
            let parameters = prepared::extract_parameters(&logical_plan);

            Ok((logical_plan, parameters))
        })?;

        // Create the prepared statement
        Ok(PreparedStatement::new(
            sql.to_string(),
            logical_plan,
            parameters,
            self.catalog_list.clone(),
            self.execution_context.clone(),
        ))
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
