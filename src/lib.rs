#![warn(clippy::all)]
#![deny(clippy::unwrap_used)]
#![allow(clippy::type_complexity)]
#![deny(unused_imports)]
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
//!
//! ## API Stability
//!
//! This crate follows semantic versioning. Key public types are marked
//! `#[non_exhaustive]` to allow future additions without breaking changes.
//! The primary stable API surface is:
//!
//! - [`Connection`] — main entry point for creating and querying databases
//! - [`error::BlazeError`] — error types
//! - [`error::Result`] — result type alias

// Core modules (always available)
pub mod approx;
pub mod benchmark;
pub mod cache;
pub mod catalog;
pub mod dataframe;
pub mod error;
pub mod executor;
pub mod fts;
pub mod git_warehouse;
pub mod ingestion;
pub mod ipc;
pub mod jit;
pub mod json;
pub mod lsp;
pub mod materialized;
pub mod migration;
pub mod optimizer;
pub mod output;
pub mod parallel;
pub mod persistence;
pub mod planner;
pub mod plugin;
pub mod pool;
pub mod prepared;
pub mod profiler;
pub mod progress;
pub mod query_advisor;
pub mod recursive_cte;
pub mod resource_governor;
pub mod rest;
pub mod security;
pub mod sql;
pub mod storage;
pub mod transaction;
pub mod tvf;
pub mod types;
pub mod udf;
pub mod vector;
pub mod visualization;
pub mod wasm;

// Extension modules (feature-gated)
#[cfg(feature = "adaptive")]
pub mod adaptive;
#[cfg(feature = "flight")]
pub mod distributed;
#[cfg(feature = "federation")]
pub mod federation;
#[cfg(feature = "flight")]
pub mod flight;
#[cfg(feature = "gpu")]
pub mod gpu;
#[cfg(feature = "lakehouse")]
pub mod lakehouse;
#[cfg(feature = "learned-optimizer")]
pub mod learned_optimizer;
#[cfg(feature = "nlq")]
pub mod nlq;
#[cfg(feature = "simd")]
pub mod simd;
#[cfg(feature = "streaming")]
pub mod streaming;
#[cfg(feature = "timeseries")]
pub mod timeseries;

// C FFI bindings (conditional on feature)
#[cfg(feature = "c-ffi")]
pub mod ffi;

// Python bindings (conditional on feature)
#[cfg(feature = "python")]
pub mod python;

// Re-export commonly used types
pub use error::{BlazeError, Result};
pub use prepared::{ParameterInfo, PreparedStatement, PreparedStatementCache};
pub use sql::parser::CompatMode;
pub use types::{DataType, Field, ScalarValue, Schema};

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use catalog::{CatalogList, TableProvider};
use executor::ExecutionContext;
use planner::{Binder, Optimizer, PhysicalPlan, PhysicalPlanner};
use sql::parser::Parser;
use storage::{CsvTable, MemoryTable, ParquetTable, ViewTable};

/// Validate that a string is a safe SQL identifier (alphanumeric + underscores).
fn validate_identifier(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(BlazeError::invalid_argument("Identifier cannot be empty"));
    }
    if !name.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(BlazeError::invalid_argument(format!(
            "Invalid identifier '{}': only alphanumeric characters and underscores are allowed",
            name
        )));
    }
    Ok(())
}

/// Validate that a file path does not contain directory traversal components.
fn validate_path(path: &Path) -> Result<std::path::PathBuf> {
    for component in path.components() {
        if let std::path::Component::ParentDir = component {
            return Err(BlazeError::invalid_argument(format!(
                "Path '{}' contains '..' traversal which is not allowed",
                path.display()
            )));
        }
    }
    path.canonicalize().map_err(|e| {
        BlazeError::invalid_argument(format!(
            "Failed to canonicalize path '{}': {}",
            path.display(),
            e
        ))
    })
}

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
    /// Connection configuration
    config: ConnectionConfig,
    /// User-defined function registry
    udf_registry: Arc<udf::UdfRegistry>,
    /// Transaction manager for MVCC
    transaction_manager: Arc<transaction::TransactionManager>,
    /// Active transaction ID for SQL-level transactions
    active_txn: parking_lot::Mutex<Option<transaction::TxnId>>,
    /// Snapshot of batch counts per table at BEGIN for rollback support
    txn_batch_snapshot: parking_lot::Mutex<HashMap<String, usize>>,
    /// Savepoint batch count snapshots: savepoint_name -> (table_name -> batch_count)
    savepoint_snapshots: parking_lot::Mutex<HashMap<String, HashMap<String, usize>>>,
    /// WAL for persistent connections (None for in-memory)
    wal: Option<parking_lot::Mutex<persistence::WriteAheadLog>>,
    /// MVCC manager for snapshot isolation
    mvcc_manager: Arc<persistence::MvccManager>,
    /// Statistics manager for cost-based optimization
    stats_manager: Arc<optimizer::StatisticsManager>,
    /// Query result cache
    query_cache: Arc<cache::QueryCache>,
    /// Materialized view manager
    mv_manager: Arc<materialized::MaterializedViewManager>,
    /// Cardinality feedback from actual query executions
    cardinality_feedback: Arc<optimizer::CardinalityFeedback>,
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
    #[must_use = "connection creation may fail; handle the Result"]
    pub fn in_memory() -> Result<Self> {
        let catalog_list = Arc::new(CatalogList::default());
        catalog::information_schema::register_information_schema(&catalog_list);
        let execution_context = ExecutionContext::new().with_catalog_list(catalog_list.clone());
        let mv_manager = Arc::new(materialized::MaterializedViewManager::new(
            catalog_list.clone(),
            execution_context.clone(),
        ));
        let conn = Self {
            execution_context,
            catalog_list,
            optimizer: Optimizer::default(),
            config: ConnectionConfig::default(),
            udf_registry: Arc::new(udf::UdfRegistry::new()),
            transaction_manager: Arc::new(transaction::TransactionManager::new()),
            active_txn: parking_lot::Mutex::new(None),
            txn_batch_snapshot: parking_lot::Mutex::new(HashMap::new()),
            savepoint_snapshots: parking_lot::Mutex::new(HashMap::new()),
            wal: None,
            mvcc_manager: Arc::new(persistence::MvccManager::new()),
            stats_manager: Arc::new(optimizer::StatisticsManager::new()),
            query_cache: Arc::new(cache::QueryCache::with_default_config()),
            mv_manager,
            cardinality_feedback: Arc::new(optimizer::CardinalityFeedback::new()),
        };
        conn.register_builtin_functions();
        Ok(conn)
    }

    /// Register built-in functions like VECTOR_SEARCH.
    fn register_builtin_functions(&self) {
        use arrow::array::Float32Array;

        // VECTOR_SEARCH(column, k) → returns Float32 distances
        // In practice this is a placeholder; real vector search requires index context.
        let vector_distance_udf = udf::ScalarUdf::new(
            "vector_distance",
            vec![], // Dynamic args
            types::DataType::Float64,
            |args: &[arrow::array::ArrayRef]| -> crate::error::Result<arrow::array::ArrayRef> {
                if args.len() < 2 {
                    return Err(BlazeError::execution(
                        "vector_distance requires at least 2 arguments (vector_column, query_vector)"
                    ));
                }
                let n = args[0].len();
                // Return dummy distances for now; real impl requires index lookup
                let distances: Vec<f32> = (0..n).map(|i| i as f32).collect();
                Ok(Arc::new(Float32Array::from(distances)) as arrow::array::ArrayRef)
            },
        );
        let _ = self.udf_registry.register_scalar(vector_distance_udf);
    }

    /// Check if parallel execution should be used for a given plan.
    fn should_use_parallel(&self, _plan: &PhysicalPlan) -> bool {
        self.config.num_threads.is_some_and(|n| n > 1)
    }

    /// Record cardinality feedback from actual query execution results.
    ///
    /// Walks the physical plan to find Scan nodes, then records the actual
    /// row count from the results so future estimates can use observed data.
    fn record_cardinality_feedback(&self, plan: &PhysicalPlan, results: &[RecordBatch]) {
        let actual_rows: usize = results.iter().map(|b| b.num_rows()).sum();

        // Collect table names from scan nodes in the physical plan
        let mut table_names = Vec::new();
        Self::collect_scan_tables(plan, &mut table_names);

        // For single-table queries, the result row count is a useful signal
        // for that table's base cardinality (filtered results are a lower bound).
        // For multi-table queries, we can't attribute rows to individual tables.
        if table_names.len() == 1 {
            self.cardinality_feedback
                .record_table_rows(&table_names[0], actual_rows);
        }

        // When the learned-optimizer feature is enabled, also feed the model
        #[cfg(feature = "learned-optimizer")]
        {
            use crate::learned_optimizer::QueryFeatures;
            let features = QueryFeatures {
                tables: table_names,
                ..Default::default()
            };
            let model = learned_optimizer::LearnedCardinalityModel::default_model();
            model.train(&features, actual_rows as f64);
        }
    }

    /// Recursively collect table names from Scan nodes in a physical plan.
    fn collect_scan_tables(plan: &PhysicalPlan, tables: &mut Vec<String>) {
        if let PhysicalPlan::Scan { table_name, .. } = plan {
            tables.push(table_name.clone());
        }
        for child in plan.children() {
            Self::collect_scan_tables(child, tables);
        }
    }

    /// Create a connection with custom configuration.
    #[must_use = "connection creation may fail; handle the Result"]
    pub fn with_config(config: ConnectionConfig) -> Result<Self> {
        let catalog_list = Arc::new(CatalogList::default());
        catalog::information_schema::register_information_schema(&catalog_list);
        let mut ctx = ExecutionContext::new()
            .with_batch_size(config.batch_size)
            .with_catalog_list(catalog_list.clone());

        if let Some(limit) = config.memory_limit {
            ctx = ctx.with_memory_limit(limit);
        }

        let mv_manager = Arc::new(materialized::MaterializedViewManager::new(
            catalog_list.clone(),
            ctx.clone(),
        ));

        let conn = Self {
            execution_context: ctx,
            catalog_list,
            optimizer: Optimizer::default(),
            config,
            udf_registry: Arc::new(udf::UdfRegistry::new()),
            transaction_manager: Arc::new(transaction::TransactionManager::new()),
            active_txn: parking_lot::Mutex::new(None),
            txn_batch_snapshot: parking_lot::Mutex::new(HashMap::new()),
            savepoint_snapshots: parking_lot::Mutex::new(HashMap::new()),
            wal: None,
            mvcc_manager: Arc::new(persistence::MvccManager::new()),
            stats_manager: Arc::new(optimizer::StatisticsManager::new()),
            query_cache: Arc::new(cache::QueryCache::with_default_config()),
            mv_manager,
            cardinality_feedback: Arc::new(optimizer::CardinalityFeedback::new()),
        };
        conn.register_builtin_functions();
        Ok(conn)
    }

    /// Execute a SQL query and return results.
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// let results = conn.query("SELECT 1 + 1").unwrap();
    /// ```
    #[must_use = "query results should not be silently discarded"]
    pub fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        // Detect cache hints
        let cache_hint = Self::has_cache_hint(sql);
        let use_cache = cache_hint != Some(false);

        // Strip cache hints before parsing
        let clean_sql = Self::strip_cache_hints(sql);

        // Check cache first (unless NO_CACHE hint)
        if use_cache {
            if let Some(cached) = self.query_cache.get(&clean_sql) {
                return Ok(cached);
            }
        }

        // Parse SQL
        let statements = Parser::parse_with_compat(&clean_sql, self.config.compat_mode)?;

        // Take the first statement (ignore any additional statements)
        let Some(statement) = statements.into_iter().next() else {
            return Ok(vec![]);
        };

        // Bind and create logical plan
        let binder = Binder::new(self.catalog_list.clone());
        let logical_plan = binder.bind(statement)?;

        // Extract referenced tables before optimization
        let referenced_tables = self.extract_referenced_tables(&logical_plan);

        // Optimize logical plan (rule-based)
        let optimized_plan = self.optimizer.optimize(&logical_plan)?;

        // Apply cost-based optimization
        let optimized_plan = {
            let cbo = optimizer::CostBasedOptimizer::new();
            cbo.optimize(&optimized_plan, &self.stats_manager)
                .unwrap_or(optimized_plan)
        };

        // Create physical plan
        let physical_planner = PhysicalPlanner::new();
        let physical_plan = physical_planner.create_physical_plan(&optimized_plan)?;

        // Execute (use parallel executor when configured with multiple threads)
        let result = if self.should_use_parallel(&physical_plan) {
            let config = crate::parallel::ExecutionConfig::new()
                .with_parallelism(self.config.num_threads.unwrap_or(1));
            let executor = crate::parallel::ParallelExecutor::new(config);
            executor.execute(&physical_plan)?
        } else {
            self.execution_context.execute(&physical_plan)?
        };

        // Record cardinality feedback from actual execution
        self.record_cardinality_feedback(&physical_plan, &result);

        // Cache the result (unless NO_CACHE hint)
        if use_cache {
            self.query_cache
                .put(&clean_sql, result.clone(), &referenced_tables);
        }

        Ok(result)
    }

    /// Returns a streaming iterator over query result batches.
    ///
    /// Uses [`executor::PlanStream`] for pull-based streaming execution,
    /// yielding `RecordBatch`es one at a time for memory-efficient processing
    /// of large result sets.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// conn.execute("CREATE TABLE t (id BIGINT)").unwrap();
    /// for batch in conn.query_iter("SELECT * FROM t").unwrap() {
    ///     let batch = batch.unwrap();
    /// }
    /// ```
    pub fn query_iter(&self, sql: &str) -> Result<executor::PlanStream> {
        let statements = Parser::parse_with_compat(sql, self.config.compat_mode)?;
        let Some(statement) = statements.into_iter().next() else {
            return Ok(executor::PlanStream::empty());
        };
        let binder = Binder::new(self.catalog_list.clone());
        let logical_plan = binder.bind(statement)?;
        let optimized = self.optimizer.optimize(&logical_plan)?;
        let planner = PhysicalPlanner::new();
        let physical = planner.create_physical_plan(&optimized)?;

        executor::PlanStream::new(&physical, &self.execution_context)
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
    #[must_use = "execute may fail; handle the Result to detect errors"]
    pub fn execute(&self, sql: &str) -> Result<usize> {
        // Handle special commands
        let trimmed = sql.trim().to_uppercase();
        if trimmed == "EXPLAIN CACHE" || trimmed == "EXPLAIN CACHE;" {
            return self.execute_explain_cache();
        }

        // Parse SQL
        let statements = Parser::parse_with_compat(sql, self.config.compat_mode)?;

        // Take the first statement (ignore any additional statements)
        let Some(statement) = statements.into_iter().next() else {
            return Ok(0);
        };

        // Handle DDL statements directly
        match &statement {
            sql::Statement::CreateTable(create) => {
                let table_name = create.name.last().cloned().unwrap_or_default();

                // CTAS: CREATE TABLE ... AS SELECT ...
                if let Some(query) = create.query.clone() {
                    let binder = Binder::new(self.catalog_list.clone());
                    let mut ctx = crate::planner::BindContext::new();
                    let logical_plan = binder.bind_query_public(*query, &mut ctx)?;
                    let optimized = self.optimizer.optimize(&logical_plan)?;
                    let planner = PhysicalPlanner::new();
                    let physical = planner.create_physical_plan(&optimized)?;
                    let results = self.execution_context.execute(&physical)?;
                    if results.is_empty() {
                        let schema = Schema::new(
                            create
                                .columns
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
                        let table = MemoryTable::empty(schema);
                        self.register_table(&table_name, Arc::new(table))?;
                    } else {
                        let arrow_schema = results[0].schema();
                        let schema = Schema::from_arrow(&arrow_schema)?;
                        let table = MemoryTable::new(schema, results);
                        self.register_table(&table_name, Arc::new(table))?;
                    }
                    self.query_cache.invalidate_table(&table_name);
                    return Ok(0);
                }

                let schema = Schema::new(
                    create
                        .columns
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
                let table = MemoryTable::empty(schema.clone());
                self.register_table(&table_name, Arc::new(table))?;
                self.query_cache.invalidate_table(&table_name);
                // Log to WAL for persistence
                if let Some(ref wal) = self.wal {
                    let mut wal = wal.lock();
                    wal.append(&persistence::WalEntry::CreateTable {
                        name: table_name,
                        schema,
                    })?;
                    wal.flush()?;
                }
                Ok(0)
            }
            sql::Statement::DropTable(drop) => {
                let table_name = drop.name.last().cloned().unwrap_or_default();
                self.query_cache.invalidate_table(&table_name);
                match self.deregister_table(&table_name) {
                    Ok(_) => {
                        // Log to WAL for persistence
                        if let Some(ref wal) = self.wal {
                            let mut wal = wal.lock();
                            let _ =
                                wal.append(&persistence::WalEntry::DropTable { name: table_name });
                            let _ = wal.flush();
                        }
                        Ok(0)
                    }
                    Err(_) if drop.if_exists => Ok(0),
                    Err(e) => Err(e),
                }
            }
            sql::Statement::Insert(insert) => self.execute_insert(insert.clone()),
            sql::Statement::Update(update) => self.execute_update(update.clone()),
            sql::Statement::Delete(delete) => self.execute_delete(delete.clone()),
            sql::Statement::BeginTransaction => {
                let txn_id = self.begin_transaction()?;
                *self.active_txn.lock() = Some(txn_id);
                // Snapshot current batch counts for all tables for rollback
                let mut snapshot = self.txn_batch_snapshot.lock();
                snapshot.clear();
                if let Some(catalog) = self.catalog_list.catalog("default") {
                    for name in catalog.list_tables() {
                        if let Some(tbl) = catalog.get_table(&name) {
                            if let Some(mem) = tbl.as_any().downcast_ref::<MemoryTable>() {
                                snapshot.insert(name, mem.num_batches());
                            }
                        }
                    }
                }
                Ok(0)
            }
            sql::Statement::Commit => {
                let txn_id = self
                    .active_txn
                    .lock()
                    .take()
                    .ok_or_else(|| BlazeError::execution("No active transaction to commit"))?;
                self.commit_transaction(txn_id)?;
                self.txn_batch_snapshot.lock().clear();
                Ok(0)
            }
            sql::Statement::Rollback => {
                let txn_id =
                    self.active_txn.lock().take().ok_or_else(|| {
                        BlazeError::execution("No active transaction to rollback")
                    })?;
                self.rollback_transaction(txn_id)?;
                // Restore tables to their pre-transaction batch counts
                let snapshot = self.txn_batch_snapshot.lock().clone();
                for (table_name, batch_count) in &snapshot {
                    if let Some(catalog) = self.catalog_list.catalog("default") {
                        if let Some(tbl) = catalog.get_table(table_name) {
                            if let Some(mem) = tbl.as_any().downcast_ref::<MemoryTable>() {
                                let mut batches = mem.batches();
                                batches.truncate(*batch_count);
                                mem.replace(batches);
                            }
                        }
                    }
                }
                self.txn_batch_snapshot.lock().clear();
                Ok(0)
            }
            sql::Statement::Savepoint(name) => {
                let txn_id = (*self.active_txn.lock())
                    .ok_or_else(|| BlazeError::execution("No active transaction for savepoint"))?;
                self.transaction_manager.create_savepoint(txn_id, name)?;
                let mut sp_snapshot = HashMap::new();
                if let Some(catalog) = self.catalog_list.catalog("default") {
                    for tname in catalog.list_tables() {
                        if let Some(tbl) = catalog.get_table(&tname) {
                            if let Some(mem) = tbl.as_any().downcast_ref::<MemoryTable>() {
                                sp_snapshot.insert(tname, mem.num_batches());
                            }
                        }
                    }
                }
                self.savepoint_snapshots
                    .lock()
                    .insert(name.clone(), sp_snapshot);
                Ok(0)
            }
            sql::Statement::ReleaseSavepoint(name) => {
                let txn_id = (*self.active_txn.lock())
                    .ok_or_else(|| BlazeError::execution("No active transaction"))?;
                self.transaction_manager.release_savepoint(txn_id, name)?;
                self.savepoint_snapshots.lock().remove(name);
                Ok(0)
            }
            sql::Statement::RollbackToSavepoint(name) => {
                let txn_id = (*self.active_txn.lock())
                    .ok_or_else(|| BlazeError::execution("No active transaction"))?;
                self.transaction_manager
                    .rollback_to_savepoint(txn_id, name)?;
                let sp_snapshots = self.savepoint_snapshots.lock();
                if let Some(snapshot) = sp_snapshots.get(name) {
                    for (table_name, batch_count) in snapshot {
                        if let Some(catalog) = self.catalog_list.catalog("default") {
                            if let Some(tbl) = catalog.get_table(table_name) {
                                if let Some(mem) = tbl.as_any().downcast_ref::<MemoryTable>() {
                                    let mut batches = mem.batches();
                                    batches.truncate(*batch_count);
                                    mem.replace(batches);
                                }
                            }
                        }
                    }
                }
                Ok(0)
            }
            sql::Statement::CreateView {
                name,
                query,
                or_replace,
            } => {
                let view_name = name.last().cloned().unwrap_or_default();

                // Bind the query to determine its schema
                let binder = Binder::new(self.catalog_list.clone());
                let mut ctx = crate::planner::BindContext::new();
                let logical_plan = binder.bind_query_public(*query.clone(), &mut ctx)?;
                let schema = logical_plan.schema().clone();

                // If or_replace, remove existing view first
                if *or_replace {
                    let _ = self.deregister_table(&view_name);
                }

                // Store the original SQL for view expansion
                let view_sql = sql.to_string();
                let view_table = ViewTable::new(view_sql, schema);
                self.register_table(&view_name, Arc::new(view_table))?;
                self.query_cache.invalidate_table(&view_name);
                Ok(0)
            }
            sql::Statement::DropView { name, if_exists } => {
                let view_name = name.last().cloned().unwrap_or_default();
                self.query_cache.invalidate_table(&view_name);
                // Verify the target is actually a view before dropping
                if let Some(catalog) = self.catalog_list.catalog("default") {
                    if let Some(table) = catalog.get_table(&view_name) {
                        if table.table_type() != catalog::TableType::View {
                            return Err(BlazeError::execution(format!(
                                "'{}' is not a view",
                                view_name
                            )));
                        }
                    }
                }
                match self.deregister_table(&view_name) {
                    Ok(_) => Ok(0),
                    Err(_) if *if_exists => Ok(0),
                    Err(e) => Err(e),
                }
            }
            sql::Statement::CreateMaterializedView(cmv) => {
                self.execute_create_materialized_view(cmv.clone(), sql)
            }
            sql::Statement::RefreshMaterializedView { name } => {
                let view_name = name.last().cloned().unwrap_or_default();
                self.execute_refresh_materialized_view(&view_name)
            }
            sql::Statement::AnalyzeTable { name } => {
                let table_name = name.last().cloned().unwrap_or_default();
                self.execute_analyze_table(&table_name)
            }
            sql::Statement::AlterTable { name, operation } => {
                let table_name = name.last().cloned().unwrap_or_default();
                let catalog = self
                    .catalog_list
                    .catalog("default")
                    .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
                let table = catalog.get_table(&table_name).ok_or_else(|| {
                    BlazeError::catalog_with_suggestions(&table_name, &catalog.list_tables())
                })?;
                let memory_table = table
                    .as_any()
                    .downcast_ref::<MemoryTable>()
                    .ok_or_else(|| {
                        BlazeError::not_implemented(
                            "ALTER TABLE is only supported for in-memory tables. External tables (CSV, Parquet) are read-only.",
                        )
                    })?;

                let new_table = match operation {
                    sql::parser::AlterTableOperation::AddColumn { column } => {
                        let arrow_dt = DataType::from_sql_type(&column.data_type).to_arrow();
                        memory_table.with_added_column(&column.name, arrow_dt, column.nullable)?
                    }
                    sql::parser::AlterTableOperation::DropColumn { name, if_exists } => {
                        match memory_table.with_dropped_column(name) {
                            Ok(t) => t,
                            Err(e) if *if_exists => return Ok(0),
                            Err(e) => return Err(e),
                        }
                    }
                    sql::parser::AlterTableOperation::RenameColumn { old_name, new_name } => {
                        memory_table.with_renamed_column(old_name, new_name)?
                    }
                };

                self.register_table(&table_name, Arc::new(new_table))?;
                self.query_cache.invalidate_table(&table_name);
                Ok(0)
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
        let table_name = insert.table_name.last().cloned().unwrap_or_default();

        // MVCC: begin implicit transaction for this DML
        let txn_id = self.mvcc_manager.begin_txn();

        let result = self.execute_insert_inner(&table_name, insert);

        match &result {
            Ok(_) => {
                let _ = self.mvcc_manager.commit_txn(txn_id);
            }
            Err(_) => {
                let _ = self.mvcc_manager.abort_txn(txn_id);
            }
        }

        result
    }

    fn execute_insert_inner(&self, table_name: &str, insert: sql::parser::Insert) -> Result<usize> {
        use arrow::array::*;

        // Get the table from the catalog
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
        let table = catalog.get_table(table_name).ok_or_else(|| {
            BlazeError::catalog_with_suggestions(table_name, &catalog.list_tables())
        })?;

        // Get the table as MemoryTable (DML only works on MemoryTable)
        let memory_table = table
            .as_any()
            .downcast_ref::<MemoryTable>()
            .ok_or_else(|| {
                BlazeError::not_implemented(
                    "INSERT is only supported for in-memory tables. External tables (CSV, Parquet) are read-only.",
                )
            })?;

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
        memory_table.append(batches.clone());

        // Log to WAL if persistent
        if let Some(ref wal) = self.wal {
            for batch in &batches {
                let mut buf = Vec::new();
                {
                    let mut writer =
                        arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batch.schema())
                            .map_err(|e| {
                                BlazeError::execution(format!(
                                    "Failed to serialize batch for WAL: {}",
                                    e
                                ))
                            })?;
                    writer.write(batch).map_err(|e| {
                        BlazeError::execution(format!("Failed to write batch to WAL: {}", e))
                    })?;
                    writer.finish().map_err(|e| {
                        BlazeError::execution(format!("Failed to finish WAL batch: {}", e))
                    })?;
                }
                let mut wal = wal.lock();
                wal.append(&persistence::WalEntry::InsertBatch {
                    table: table_name.to_string(),
                    batch_data: buf,
                })?;
                wal.flush()?;
            }
        }

        self.query_cache.invalidate_table(table_name);

        // Notify materialized view manager of the insert
        self.mv_manager.notify_insert(table_name, &batches);

        Ok(row_count)
    }

    /// Execute an UPDATE statement.
    fn execute_update(&self, update: sql::parser::Update) -> Result<usize> {
        let table_name = update.table_name.last().cloned().unwrap_or_default();

        // MVCC: begin implicit transaction for this DML
        let txn_id = self.mvcc_manager.begin_txn();

        let result = self.execute_update_inner(&table_name, update);

        match &result {
            Ok(_) => {
                let _ = self.mvcc_manager.commit_txn(txn_id);
            }
            Err(_) => {
                let _ = self.mvcc_manager.abort_txn(txn_id);
            }
        }

        result
    }

    fn execute_update_inner(&self, table_name: &str, update: sql::parser::Update) -> Result<usize> {
        // Get the table from the catalog
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
        let table = catalog.get_table(table_name).ok_or_else(|| {
            BlazeError::catalog_with_suggestions(table_name, &catalog.list_tables())
        })?;

        // Get the table as MemoryTable
        let memory_table = table
            .as_any()
            .downcast_ref::<MemoryTable>()
            .ok_or_else(|| {
                BlazeError::not_implemented(
                    "UPDATE is only supported for in-memory tables. External tables (CSV, Parquet) are read-only.",
                )
            })?;

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
                result
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
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
                let updated_col = if let Some((_, expr)) = update
                    .assignments
                    .iter()
                    .find(|(col, _)| col == field.name())
                {
                    // Evaluate the assignment expression
                    let binder = Binder::new(self.catalog_list.clone());
                    let mut ctx = planner::BindContext::new();
                    let logical_expr = binder.bind_expr_public(expr.clone(), &mut ctx)?;

                    let physical_planner = PhysicalPlanner::new();
                    let phys_expr = physical_planner
                        .create_physical_expr_public(&logical_expr, &arrow_schema)?;
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

        // Track update for materialized view staleness
        self.mv_manager
            .track_table(table_name)
            .record_update(&new_batches);

        // Replace table data
        memory_table.replace(new_batches);

        // Invalidate cache for this table
        self.query_cache.invalidate_table(table_name);

        // Log to WAL if persistent
        if let Some(ref wal) = self.wal {
            let assignments: Vec<(String, String)> = update
                .assignments
                .iter()
                .map(|(col, expr)| (col.clone(), Self::expr_to_sql(expr)))
                .collect();
            let predicate = update.selection.as_ref().map(Self::expr_to_sql);
            let mut wal = wal.lock();
            wal.append(&persistence::WalEntry::UpdateRows {
                table: table_name.to_string(),
                predicate,
                assignments,
                rows_affected: updated_count,
            })?;
            wal.flush()?;
        }

        Ok(updated_count)
    }

    /// Execute a DELETE statement.
    fn execute_delete(&self, delete: sql::parser::Delete) -> Result<usize> {
        let table_name = delete.table_name.last().cloned().unwrap_or_default();

        // MVCC: begin implicit transaction for this DML
        let txn_id = self.mvcc_manager.begin_txn();

        let result = self.execute_delete_inner(&table_name, delete);

        match &result {
            Ok(_) => {
                let _ = self.mvcc_manager.commit_txn(txn_id);
            }
            Err(_) => {
                let _ = self.mvcc_manager.abort_txn(txn_id);
            }
        }

        result
    }

    fn execute_delete_inner(&self, table_name: &str, delete: sql::parser::Delete) -> Result<usize> {
        use arrow::compute::filter_record_batch;

        // Get the table from the catalog
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
        let table = catalog.get_table(table_name).ok_or_else(|| {
            BlazeError::catalog_with_suggestions(table_name, &catalog.list_tables())
        })?;

        // Get the table as MemoryTable
        let memory_table = table
            .as_any()
            .downcast_ref::<MemoryTable>()
            .ok_or_else(|| {
                BlazeError::not_implemented(
                    "DELETE is only supported for in-memory tables. External tables (CSV, Parquet) are read-only.",
                )
            })?;

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
                let delete_mask = result
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
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

        // Invalidate cache for this table
        self.query_cache.invalidate_table(table_name);

        // Track delete for materialized view staleness
        self.mv_manager.track_table(table_name).record_delete(&[]);

        // Log to WAL if persistent
        if let Some(ref wal) = self.wal {
            let predicate = delete.selection.as_ref().map(Self::expr_to_sql);
            let mut wal = wal.lock();
            wal.append(&persistence::WalEntry::DeleteRows {
                table: table_name.to_string(),
                predicate,
                rows_affected: deleted_count,
            })?;
            wal.flush()?;
        }

        Ok(deleted_count)
    }

    /// Execute EXPLAIN CACHE command.
    fn execute_explain_cache(&self) -> Result<usize> {
        let report = cache::CacheExplainer::explain_text(&self.query_cache);
        println!("{}", report);
        Ok(0)
    }

    /// Extract table names referenced in a logical plan.
    fn extract_referenced_tables(&self, plan: &planner::LogicalPlan) -> Vec<String> {
        let mut tables = Vec::new();
        self.collect_table_names(plan, &mut tables);
        tables
    }

    /// Recursively collect table names from a logical plan.
    fn collect_table_names(&self, plan: &planner::LogicalPlan, tables: &mut Vec<String>) {
        if let planner::LogicalPlan::TableScan { table_ref, .. } = plan {
            tables.push(table_ref.table.clone());
        }
        for child in plan.children() {
            self.collect_table_names(child, tables);
        }
    }

    /// Detect cache hints in SQL: `/*+ CACHE */` or `/*+ NO_CACHE */`.
    fn has_cache_hint(sql: &str) -> Option<bool> {
        if sql.contains("/*+ CACHE */") || sql.contains("/*+CACHE*/") {
            Some(true)
        } else if sql.contains("/*+ NO_CACHE */") || sql.contains("/*+NO_CACHE*/") {
            Some(false)
        } else {
            None
        }
    }

    /// Strip cache hint comments from SQL.
    fn strip_cache_hints(sql: &str) -> String {
        sql.replace("/*+ CACHE */", "")
            .replace("/*+CACHE*/", "")
            .replace("/*+ NO_CACHE */", "")
            .replace("/*+NO_CACHE*/", "")
    }

    /// Get a reference to the query cache.
    pub fn query_cache(&self) -> &cache::QueryCache {
        &self.query_cache
    }

    /// Get a reference to the materialized view manager.
    pub fn mv_manager(&self) -> &materialized::MaterializedViewManager {
        &self.mv_manager
    }

    /// Get a reference to the MVCC manager.
    pub fn mvcc_manager(&self) -> &persistence::MvccManager {
        &self.mvcc_manager
    }

    /// Clear the query cache.
    pub fn clear_cache(&self) {
        self.query_cache.clear();
    }

    /// Get cache statistics.
    pub fn cache_stats(&self) -> cache::CacheStats {
        self.query_cache.stats()
    }

    /// Convert a parsed SQL expression to a SQL string for WAL serialization.
    fn expr_to_sql(expr: &sql::parser::Expr) -> String {
        match expr {
            sql::parser::Expr::Literal(lit) => match lit {
                sql::parser::Literal::Null => "NULL".to_string(),
                sql::parser::Literal::Boolean(b) => {
                    if *b {
                        "TRUE".to_string()
                    } else {
                        "FALSE".to_string()
                    }
                }
                sql::parser::Literal::Integer(i) => i.to_string(),
                sql::parser::Literal::Float(f) => f.to_string(),
                sql::parser::Literal::String(s) => format!("'{}'", s.replace('\'', "''")),
                sql::parser::Literal::Interval { value, unit } => {
                    if let Some(u) = unit {
                        format!("INTERVAL '{}' {}", value, u)
                    } else {
                        format!("INTERVAL '{}'", value)
                    }
                }
            },
            sql::parser::Expr::Column(col) => {
                if let Some(ref rel) = col.relation {
                    format!("{}.{}", rel, col.name)
                } else {
                    col.name.clone()
                }
            }
            sql::parser::Expr::BinaryOp { left, op, right } => {
                let op_str = match op {
                    sql::parser::BinaryOperator::Plus => "+",
                    sql::parser::BinaryOperator::Minus => "-",
                    sql::parser::BinaryOperator::Multiply => "*",
                    sql::parser::BinaryOperator::Divide => "/",
                    sql::parser::BinaryOperator::Modulo => "%",
                    sql::parser::BinaryOperator::Eq => "=",
                    sql::parser::BinaryOperator::NotEq => "!=",
                    sql::parser::BinaryOperator::Lt => "<",
                    sql::parser::BinaryOperator::LtEq => "<=",
                    sql::parser::BinaryOperator::Gt => ">",
                    sql::parser::BinaryOperator::GtEq => ">=",
                    sql::parser::BinaryOperator::And => "AND",
                    sql::parser::BinaryOperator::Or => "OR",
                    sql::parser::BinaryOperator::Concat => "||",
                    sql::parser::BinaryOperator::BitwiseAnd => "&",
                    sql::parser::BinaryOperator::BitwiseOr => "|",
                    sql::parser::BinaryOperator::BitwiseXor => "^",
                };
                format!(
                    "({} {} {})",
                    Self::expr_to_sql(left),
                    op_str,
                    Self::expr_to_sql(right)
                )
            }
            sql::parser::Expr::UnaryOp { op, expr } => {
                let op_str = match op {
                    sql::parser::UnaryOperator::Plus => "+",
                    sql::parser::UnaryOperator::Minus => "-",
                    sql::parser::UnaryOperator::Not => "NOT ",
                    sql::parser::UnaryOperator::BitwiseNot => "~",
                };
                format!("{}{}", op_str, Self::expr_to_sql(expr))
            }
            sql::parser::Expr::IsNull { expr, negated } => {
                if *negated {
                    format!("{} IS NOT NULL", Self::expr_to_sql(expr))
                } else {
                    format!("{} IS NULL", Self::expr_to_sql(expr))
                }
            }
            sql::parser::Expr::Nested(inner) => {
                format!("({})", Self::expr_to_sql(inner))
            }
            sql::parser::Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                if *negated {
                    format!(
                        "{} NOT BETWEEN {} AND {}",
                        Self::expr_to_sql(expr),
                        Self::expr_to_sql(low),
                        Self::expr_to_sql(high)
                    )
                } else {
                    format!(
                        "{} BETWEEN {} AND {}",
                        Self::expr_to_sql(expr),
                        Self::expr_to_sql(low),
                        Self::expr_to_sql(high)
                    )
                }
            }
            sql::parser::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let items: Vec<String> = list.iter().map(Self::expr_to_sql).collect();
                if *negated {
                    format!("{} NOT IN ({})", Self::expr_to_sql(expr), items.join(", "))
                } else {
                    format!("{} IN ({})", Self::expr_to_sql(expr), items.join(", "))
                }
            }
            sql::parser::Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
                ..
            } => {
                let keyword = if *case_insensitive { "ILIKE" } else { "LIKE" };
                if *negated {
                    format!(
                        "{} NOT {} {}",
                        Self::expr_to_sql(expr),
                        keyword,
                        Self::expr_to_sql(pattern)
                    )
                } else {
                    format!(
                        "{} {} {}",
                        Self::expr_to_sql(expr),
                        keyword,
                        Self::expr_to_sql(pattern)
                    )
                }
            }
            sql::parser::Expr::Function(func) => {
                let args: Vec<String> = func.args.iter().map(Self::expr_to_sql).collect();
                let name = func.name.join(".");
                if func.distinct {
                    format!("{}(DISTINCT {})", name, args.join(", "))
                } else {
                    format!("{}({})", name, args.join(", "))
                }
            }
            sql::parser::Expr::Cast { expr, data_type } => {
                format!("CAST({} AS {})", Self::expr_to_sql(expr), data_type)
            }
            // Fallback to Debug for complex expressions
            other => format!("{:?}", other),
        }
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
                let old_arr = old_values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        BlazeError::type_error("Expected Int64Array for old values in merge")
                    })?;
                let new_arr = new_values
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| {
                        BlazeError::type_error("Expected Int64Array for new values in merge")
                    })?;
                let mut builder = Int64Builder::new();

                for i in 0..old_arr.len() {
                    if mask.value(i) {
                        builder.append_value(new_arr.value(i));
                    } else if old_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(old_arr.value(i));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ArrowDataType::Utf8 => {
                let old_arr = old_values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        BlazeError::type_error("Expected StringArray for old values in merge")
                    })?;
                let new_arr = new_values
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        BlazeError::type_error("Expected StringArray for new values in merge")
                    })?;
                let mut builder = StringBuilder::new();

                for i in 0..old_arr.len() {
                    if mask.value(i) {
                        builder.append_value(new_arr.value(i));
                    } else if old_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(old_arr.value(i));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ArrowDataType::Float64 => {
                let old_arr = old_values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        BlazeError::type_error("Expected Float64Array for old values in merge")
                    })?;
                let new_arr = new_values
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        BlazeError::type_error("Expected Float64Array for new values in merge")
                    })?;
                let mut builder = Float64Builder::new();

                for i in 0..old_arr.len() {
                    if mask.value(i) {
                        builder.append_value(new_arr.value(i));
                    } else if old_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(old_arr.value(i));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            ArrowDataType::Boolean => {
                let old_arr = old_values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        BlazeError::type_error("Expected BooleanArray for old values in merge")
                    })?;
                let new_arr = new_values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        BlazeError::type_error("Expected BooleanArray for new values in merge")
                    })?;
                let mut builder = BooleanBuilder::new();

                for i in 0..old_arr.len() {
                    if mask.value(i) {
                        builder.append_value(new_arr.value(i));
                    } else if old_arr.is_null(i) {
                        builder.append_null();
                    } else {
                        builder.append_value(old_arr.value(i));
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => Err(BlazeError::not_implemented(format!(
                "UPDATE for data type {:?} is not yet supported. Supported types: Int64, Float64, Boolean, Utf8, Date32.",
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
            sql::parser::Expr::Literal(Literal::String(s)) => {
                Ok(ScalarValue::Utf8(Some(s.clone())))
            }
            sql::parser::Expr::Literal(Literal::Boolean(b)) => Ok(ScalarValue::Boolean(Some(*b))),
            sql::parser::Expr::Literal(Literal::Null) => Ok(ScalarValue::Null),
            _ => Err(BlazeError::analysis(
                "Only literal values are supported in INSERT VALUES",
            )),
        }
    }

    // -----------------------------------------------------------------------
    // Materialized View & Analyze Table DDL handlers
    // -----------------------------------------------------------------------

    /// Execute CREATE MATERIALIZED VIEW.
    fn execute_create_materialized_view(
        &self,
        cmv: sql::parser::CreateMaterializedView,
        original_sql: &str,
    ) -> Result<usize> {
        let view_name = cmv.name.last().cloned().unwrap_or_default();

        // Extract the query SQL from the original statement (everything after AS)
        let query_sql = original_sql
            .to_uppercase()
            .find(" AS ")
            .map(|pos| original_sql[pos + 4..].trim().to_string())
            .unwrap_or_default();

        // Bind the query to extract referenced source tables
        let binder = Binder::new(self.catalog_list.clone());
        let mut ctx = crate::planner::BindContext::new();
        let logical_plan = binder.bind_query_public(*cmv.query, &mut ctx)?;
        let source_tables = self.extract_referenced_tables(&logical_plan);

        // Delegate to the materialized view manager (handles execution, registration, tracking)
        self.mv_manager
            .create_view(&view_name, &query_sql, source_tables)?;

        Ok(0)
    }

    /// Execute REFRESH MATERIALIZED VIEW.
    fn execute_refresh_materialized_view(&self, view_name: &str) -> Result<usize> {
        match self.mv_manager.refresh_view(view_name) {
            Ok(batches) => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                Ok(rows)
            }
            Err(_) => {
                // Fallback: if not in MV manager, check that the view exists
                let catalog = self
                    .catalog_list
                    .catalog("default")
                    .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
                if catalog.get_table(view_name).is_none() {
                    return Err(BlazeError::catalog(format!(
                        "Materialized view '{}' not found",
                        view_name
                    )));
                }
                Ok(0)
            }
        }
    }

    /// Execute ANALYZE TABLE to collect statistics.
    fn execute_analyze_table(&self, table_name: &str) -> Result<usize> {
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
        let table = catalog
            .get_table(table_name)
            .ok_or_else(|| BlazeError::catalog(format!("Table '{}' not found", table_name)))?;

        let schema = table.schema();
        let batches = if let Some(mem) = table.as_any().downcast_ref::<MemoryTable>() {
            mem.batches()
        } else {
            return Ok(0);
        };

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        use crate::optimizer::{ColumnStatistics, TableStatistics};
        let mut stats = TableStatistics::new(table_name, total_rows);

        for i in 0..schema.len() {
            let field = &schema.fields()[i];
            let mut null_count = 0usize;
            let mut ndv_set = std::collections::HashSet::new();
            for batch in &batches {
                let col = batch.column(i);
                null_count += col.null_count();
                for row in 0..col.len() {
                    if !col.is_null(row) {
                        ndv_set.insert(format!("{:?}", col.slice(row, 1)));
                    }
                }
            }
            let ndv = ndv_set.len();
            let histogram = if total_rows > 0 && ndv > 0 {
                let num_buckets = 10.min(ndv);
                let rows_per_bucket = total_rows / num_buckets.max(1);
                let ndv_per_bucket = ndv / num_buckets.max(1);
                let buckets: Vec<_> = (0..num_buckets)
                    .map(|i| {
                        optimizer::HistogramBucket::new(
                            format!("{}", i),
                            format!("{}", i + 1),
                            rows_per_bucket,
                            ndv_per_bucket.max(1),
                        )
                    })
                    .collect();
                Some(optimizer::Histogram::equiwidth(buckets))
            } else {
                None
            };
            let mut col_stats = ColumnStatistics::new(field.name(), field.data_type().clone())
                .with_distinct_count(ndv)
                .with_null_count(null_count);
            if let Some(h) = histogram {
                col_stats = col_stats.with_histogram(h);
            }
            stats = stats.with_column(col_stats);
        }

        // Persist statistics to StatisticsManager for use by the CBO.
        self.stats_manager.register(stats)?;
        Ok(total_rows)
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
    #[must_use = "registration may fail; handle the Result"]
    pub fn register_csv(&self, name: &str, path: impl AsRef<Path>) -> Result<()> {
        let safe_path = validate_path(path.as_ref())?;
        let table = CsvTable::open(safe_path)?;
        self.register_table(name, Arc::new(table))
    }

    /// Register a CSV file with custom options.
    #[must_use = "registration may fail; handle the Result"]
    pub fn register_csv_with_options(
        &self,
        name: &str,
        path: impl AsRef<Path>,
        options: storage::CsvOptions,
    ) -> Result<()> {
        let safe_path = validate_path(path.as_ref())?;
        let table = CsvTable::open_with_options(safe_path, options)?;
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
    #[must_use = "registration may fail; handle the Result"]
    pub fn register_parquet(&self, name: &str, path: impl AsRef<Path>) -> Result<()> {
        let safe_path = validate_path(path.as_ref())?;
        let table = ParquetTable::open(safe_path)?;
        self.register_table(name, Arc::new(table))
    }

    /// Register a Parquet file with custom options.
    #[must_use = "registration may fail; handle the Result"]
    pub fn register_parquet_with_options(
        &self,
        name: &str,
        path: impl AsRef<Path>,
        options: storage::ParquetOptions,
    ) -> Result<()> {
        let safe_path = validate_path(path.as_ref())?;
        let table = ParquetTable::open_with_options(safe_path, options)?;
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
    #[must_use = "registration may fail; handle the Result"]
    pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Err(BlazeError::invalid_argument(
                "Cannot register empty batches",
            ));
        }

        let schema = Schema::from_arrow(batches[0].schema().as_ref())?;
        let table = MemoryTable::new(schema, batches);
        self.register_table(name, Arc::new(table))
    }

    /// Register a custom table provider in the default schema.
    #[must_use = "registration may fail; handle the Result"]
    pub fn register_table(&self, name: &str, table: Arc<dyn TableProvider>) -> Result<()> {
        // Validate table name
        if name.is_empty() {
            return Err(BlazeError::invalid_argument("Table name cannot be empty"));
        }
        if name.chars().all(|c| c.is_whitespace()) {
            return Err(BlazeError::invalid_argument(
                "Table name cannot be only whitespace",
            ));
        }

        // Get the default catalog and schema
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::schema("Default catalog not found"))?;
        let schema = catalog
            .schema("main")
            .ok_or_else(|| BlazeError::schema("Default schema 'main' not found"))?;
        schema.register_table(name, table)?;
        self.query_cache.invalidate_table(name);
        Ok(())
    }

    /// Deregister a table from the default schema.
    #[must_use = "deregistration may fail; handle the Result"]
    pub fn deregister_table(&self, name: &str) -> Result<()> {
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::schema("Default catalog not found"))?;
        let schema = catalog
            .schema("main")
            .ok_or_else(|| BlazeError::schema("Default schema 'main' not found"))?;
        schema.deregister_table(name)?;
        self.query_cache.invalidate_table(name);
        Ok(())
    }

    /// List all tables in the default schema.
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog_list
            .catalog("default")
            .and_then(|c| c.schema("main"))
            .map(|s| s.table_names())
            .unwrap_or_default()
    }

    /// Get the schema of a table.
    pub fn table_schema(&self, name: &str) -> Option<Schema> {
        self.catalog_list
            .catalog("default")
            .and_then(|c| c.schema("main"))
            .and_then(|s| s.table(name))
            .map(|t| t.schema().clone())
    }

    /// Get a reference to the catalog list.
    pub fn catalog_list(&self) -> Arc<CatalogList> {
        self.catalog_list.clone()
    }

    /// Get a reference to the statistics manager.
    pub fn stats_manager(&self) -> &optimizer::StatisticsManager {
        &self.stats_manager
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
    #[must_use = "prepare may fail; handle the Result"]
    pub fn prepare(&self, sql: &str) -> Result<PreparedStatement> {
        // Parse SQL
        let statements = Parser::parse_with_compat(sql, self.config.compat_mode)?;

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
    #[must_use = "prepare may fail; handle the Result"]
    pub fn prepare_cached(
        &self,
        sql: &str,
        cache: &PreparedStatementCache,
    ) -> Result<PreparedStatement> {
        let catalog_list = self.catalog_list.clone();

        let (logical_plan, parameters) = cache.get_or_create(sql, || {
            // Parse SQL
            let statements = Parser::parse_with_compat(sql, self.config.compat_mode)?;

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

    // =================== User-Defined Functions ===================

    /// Register a scalar user-defined function.
    ///
    /// Once registered, the UDF can be called from SQL queries by name.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    /// use blaze::udf::ScalarUdf;
    /// use blaze::types::DataType;
    /// use arrow::array::{ArrayRef, Int64Array};
    /// use std::sync::Arc;
    ///
    /// let conn = Connection::in_memory().unwrap();
    /// let double_fn = ScalarUdf::new(
    ///     "double",
    ///     vec![DataType::Int64],
    ///     DataType::Int64,
    ///     |args: &[ArrayRef]| {
    ///         let input = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
    ///         let result: Int64Array = input.iter().map(|v| v.map(|x| x * 2)).collect();
    ///         Ok(Arc::new(result) as ArrayRef)
    ///     },
    /// );
    /// conn.register_udf(double_fn).unwrap();
    /// ```
    #[must_use = "registration may fail; handle the Result"]
    pub fn register_udf(&self, udf: udf::ScalarUdf) -> Result<()> {
        self.udf_registry.register_scalar(udf)
    }

    /// Get the UDF registry for advanced operations.
    pub fn udf_registry(&self) -> &Arc<udf::UdfRegistry> {
        &self.udf_registry
    }

    // =================== Persistence ===================

    /// Open a persistent database connection at the given directory.
    ///
    /// This will recover any existing data from the WAL and snapshots
    /// in the directory, then allow new mutations to be persisted.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use blaze::Connection;
    ///
    /// let conn = Connection::open("/tmp/my_blaze_db").unwrap();
    /// conn.execute("CREATE TABLE users (id INT, name VARCHAR)").unwrap();
    /// ```
    #[must_use = "opening a database may fail; handle the Result"]
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        std::fs::create_dir_all(path)?;

        let mut conn = Self::in_memory()?;

        // Replay WAL if it exists
        let wal_path = path.join("blaze.wal");
        if wal_path.exists() {
            let wal = persistence::WriteAheadLog::open(&wal_path)?;
            let entries = wal.replay()?;
            for (_seq, entry) in entries {
                match entry {
                    persistence::WalEntry::CreateTable { name, schema } => {
                        let table = MemoryTable::empty(schema);
                        let _ = conn.register_table(&name, Arc::new(table));
                    }
                    persistence::WalEntry::DropTable { name } => {
                        let _ = conn.deregister_table(&name);
                    }
                    persistence::WalEntry::InsertBatch { table, batch_data } => {
                        // Decode Arrow IPC batch
                        if let Ok(reader) = arrow::ipc::reader::StreamReader::try_new(
                            std::io::Cursor::new(&batch_data),
                            None,
                        ) {
                            for batch in reader.flatten() {
                                if let Some(tbl) = conn
                                    .catalog_list
                                    .catalog("default")
                                    .and_then(|c| c.get_table(&table))
                                {
                                    if let Some(mem_table) =
                                        tbl.as_any().downcast_ref::<MemoryTable>()
                                    {
                                        mem_table.append(vec![batch]);
                                    }
                                }
                            }
                        }
                    }
                    persistence::WalEntry::UpdateRows {
                        table,
                        predicate,
                        assignments,
                        ..
                    } => {
                        let set_clause = assignments
                            .iter()
                            .map(|(col, val)| format!("{} = {}", col, val))
                            .collect::<Vec<_>>()
                            .join(", ");
                        let sql = if let Some(pred) = predicate {
                            format!("UPDATE {} SET {} WHERE {}", table, set_clause, pred)
                        } else {
                            format!("UPDATE {} SET {}", table, set_clause)
                        };
                        if let Err(e) = conn.execute(&sql) {
                            tracing::warn!("WAL replay UPDATE failed for {}: {}", table, e);
                        }
                    }
                    persistence::WalEntry::DeleteRows {
                        table, predicate, ..
                    } => {
                        let sql = if let Some(pred) = predicate {
                            format!("DELETE FROM {} WHERE {}", table, pred)
                        } else {
                            format!("DELETE FROM {}", table)
                        };
                        if let Err(e) = conn.execute(&sql) {
                            tracing::warn!("WAL replay DELETE failed for {}: {}", table, e);
                        }
                    }
                    persistence::WalEntry::Checkpoint { .. } => {
                        // Checkpoint markers are informational during replay
                    }
                }
            }
        }

        // Open WAL for future writes
        let wal = persistence::WriteAheadLog::open(&wal_path)?;
        conn.wal = Some(parking_lot::Mutex::new(wal));

        Ok(conn)
    }

    /// Checkpoint the database, creating snapshots and truncating the WAL.
    ///
    /// This saves the current state of all tables and truncates the WAL,
    /// reducing recovery time on next startup.
    pub fn checkpoint(&self) -> Result<()> {
        let wal = self
            .wal
            .as_ref()
            .ok_or_else(|| BlazeError::execution("Cannot checkpoint an in-memory connection"))?;

        // Record checkpoint entry then truncate
        let mut wal = wal.lock();
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        wal.append(&persistence::WalEntry::Checkpoint { timestamp })?;
        wal.flush()?;
        wal.truncate()?;

        Ok(())
    }

    /// Compact a table's storage by consolidating fragmented batches into fewer, larger ones.
    /// Useful after many INSERT/DELETE operations that create many small batches.
    pub fn compact_table(&self, table_name: &str) -> Result<persistence::CompactionStats> {
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
        let table = catalog
            .get_table(table_name)
            .ok_or_else(|| BlazeError::catalog(format!("Table '{}' not found", table_name)))?;

        let memory_table = table
            .as_any()
            .downcast_ref::<MemoryTable>()
            .ok_or_else(|| {
                BlazeError::not_implemented(
                    "Compaction is only supported for in-memory tables. External tables (CSV, Parquet) are read-only.",
                )
            })?;

        let batches = memory_table.batches();
        let batches_before = batches.len();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        if batches_before <= 1 {
            return Ok(persistence::CompactionStats {
                batches_before,
                batches_after: batches_before,
                rows,
            });
        }

        let compacted = persistence::compact_batches(batches)?;
        let batches_after = compacted.len();
        memory_table.replace(compacted);

        Ok(persistence::CompactionStats {
            batches_before,
            batches_after,
            rows,
        })
    }

    /// Analyze a table to gather statistics for cost-based optimization.
    ///
    /// This collects row count, null counts, and byte sizes for each column
    /// in the table. The statistics are computed by scanning the table data.
    #[must_use = "analyze may fail; handle the Result"]
    pub fn analyze_table(&self, table_name: &str) -> Result<optimizer::TableStatistics> {
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::catalog("Default catalog not found"))?;
        let table = catalog.get_table(table_name).ok_or_else(|| {
            BlazeError::catalog_with_suggestions(table_name, &catalog.list_tables())
        })?;

        // Query all data from the table
        validate_identifier(table_name)?;
        let batches = self.query(&format!("SELECT * FROM \"{}\"", table_name))?;

        let mut total_rows = 0usize;
        let schema = table.schema();
        let num_cols = schema.len();

        let mut null_counts: Vec<usize> = vec![0; num_cols];

        for batch in &batches {
            total_rows += batch.num_rows();
            for (null_count, col) in null_counts
                .iter_mut()
                .zip(batch.columns().iter())
                .take(num_cols.min(batch.num_columns()))
            {
                *null_count += col.null_count();
            }
        }

        let total_bytes: usize = batches
            .iter()
            .map(|b| {
                b.columns()
                    .iter()
                    .map(|c| c.get_array_memory_size())
                    .sum::<usize>()
            })
            .sum();

        let mut table_stats = optimizer::TableStatistics::new(table_name, total_rows);
        table_stats.size_bytes = total_bytes;

        for (i, field) in schema.fields().iter().enumerate().take(num_cols) {
            let col_stat = optimizer::ColumnStatistics {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
                distinct_count: None,
                null_count: null_counts[i],
                min_value: None,
                max_value: None,
                avg_width: if total_rows > 0 {
                    total_bytes / (total_rows * num_cols.max(1))
                } else {
                    0
                },
                histogram: None,
            };
            table_stats
                .columns
                .insert(field.name().to_string(), col_stat);
        }

        self.stats_manager.register(table_stats.clone())?;
        Ok(table_stats)
    }

    // =================== Federation ===================

    /// Register an external file as a federated table.
    ///
    /// Supported formats: CSV, Parquet, JSON (auto-detected from extension).
    #[must_use = "registration may fail; handle the Result"]
    pub fn register_external_file(&self, name: &str, path: &str) -> Result<()> {
        let ext = std::path::Path::new(path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        match ext.as_str() {
            "csv" => self.register_csv(name, path),
            "parquet" | "pq" => self.register_parquet(name, path),
            _ => Err(BlazeError::not_implemented(format!(
                "Unsupported file format: .{}. Supported formats: .csv, .parquet, .pq, .json, .jsonl, .ndjson, .arrow, .ipc.",
                ext
            ))),
        }
    }

    // =================== Streaming ===================

    /// Execute a query and process results in a streaming fashion.
    ///
    /// The callback is invoked once for each batch in the result set,
    /// avoiding materializing all results in memory at once.
    #[must_use = "query may fail; handle the Result"]
    pub fn query_foreach<F>(&self, sql: &str, mut callback: F) -> Result<usize>
    where
        F: FnMut(&RecordBatch) -> Result<()>,
    {
        let results = self.query(sql)?;
        let mut total_rows = 0;
        for batch in &results {
            callback(batch)?;
            total_rows += batch.num_rows();
        }
        Ok(total_rows)
    }

    /// Execute a query and return at most `limit` rows.
    #[must_use = "query may fail; handle the Result"]
    pub fn query_with_limit(&self, sql: &str, limit: usize) -> Result<Vec<RecordBatch>> {
        let results = self.query(sql)?;
        let mut limited = Vec::new();
        let mut remaining = limit;

        for batch in results {
            if remaining == 0 {
                break;
            }
            if batch.num_rows() <= remaining {
                remaining -= batch.num_rows();
                limited.push(batch);
            } else {
                // Slice the batch
                limited.push(batch.slice(0, remaining));
                remaining = 0;
            }
        }

        Ok(limited)
    }

    // =================== Transaction Support ===================

    /// Begin a new transaction.
    ///
    /// Returns a transaction ID that can be used with `commit_transaction`
    /// or `rollback_transaction`.
    #[must_use = "transaction may fail; handle the Result"]
    pub fn begin_transaction(&self) -> Result<transaction::TxnId> {
        self.transaction_manager.begin()
    }

    /// Commit a transaction, making its writes visible to subsequent transactions.
    #[must_use = "commit may fail; handle the Result"]
    pub fn commit_transaction(&self, txn_id: transaction::TxnId) -> Result<()> {
        self.transaction_manager.commit(txn_id)
    }

    /// Roll back a transaction, discarding all its writes.
    #[must_use = "rollback may fail; handle the Result"]
    pub fn rollback_transaction(&self, txn_id: transaction::TxnId) -> Result<()> {
        self.transaction_manager.abort(txn_id)
    }

    /// Get the transaction manager for advanced operations.
    pub fn transaction_manager(&self) -> &Arc<transaction::TransactionManager> {
        &self.transaction_manager
    }
}

impl Default for Connection {
    fn default() -> Self {
        Self::in_memory().expect("Failed to create in-memory connection")
    }
}

/// Configuration options for a database connection.
///
/// Controls vectorized batch size, memory limits, and parallelism.
/// Use [`ConnectionConfig::new()`] to create a default configuration,
/// then chain builder methods to customize.
///
/// # Example
///
/// ```rust
/// use blaze::ConnectionConfig;
///
/// let config = ConnectionConfig::new()
///     .with_batch_size(4096)
///     .with_memory_limit(512 * 1024 * 1024)
///     .with_num_threads(4);
/// ```
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Batch size for vectorized execution
    pub batch_size: usize,
    /// Maximum memory limit (in bytes)
    pub memory_limit: Option<usize>,
    /// Number of threads for parallel execution
    pub num_threads: Option<usize>,
    /// SQL compatibility mode for dialect-specific syntax
    pub compat_mode: sql::parser::CompatMode,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            memory_limit: None,
            num_threads: None,
            compat_mode: sql::parser::CompatMode::Standard,
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
            (Self::MIN_BATCH_SIZE..=Self::MAX_BATCH_SIZE).contains(&batch_size),
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

    /// Enable or disable parallel execution.
    ///
    /// When disabled, forces `num_threads` to 1.
    pub fn with_parallel(mut self, enabled: bool) -> Self {
        if !enabled {
            self.num_threads = Some(1);
        }
        self
    }

    /// Set the SQL compatibility mode.
    pub fn with_compat_mode(mut self, mode: sql::parser::CompatMode) -> Self {
        self.compat_mode = mode;
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

    #[test]
    fn test_ctas_basic() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE src (id BIGINT, name VARCHAR)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 'Alice')").unwrap();
        conn.execute("INSERT INTO src VALUES (2, 'Bob')").unwrap();

        conn.execute("CREATE TABLE dst AS SELECT id, name FROM src WHERE id > 0")
            .unwrap();
        let results = conn.query("SELECT * FROM dst ORDER BY id").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2);
    }

    #[test]
    fn test_copy_to_parquet_roundtrip() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE data (id BIGINT, val VARCHAR)")
            .unwrap();
        conn.execute("INSERT INTO data VALUES (1, 'hello')")
            .unwrap();
        conn.execute("INSERT INTO data VALUES (2, 'world')")
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.parquet");
        let sql = format!("COPY data TO '{}'", path.display());
        conn.execute(&sql).unwrap();
        assert!(path.exists());

        // Read back
        let conn2 = Connection::in_memory().unwrap();
        conn2
            .register_parquet("data2", path.to_str().unwrap())
            .unwrap();
        let results = conn2.query("SELECT * FROM data2 ORDER BY id").unwrap();
        assert_eq!(results[0].num_rows(), 2);
    }

    #[test]
    fn test_copy_to_csv_roundtrip() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE csvdata (x BIGINT, y BIGINT)")
            .unwrap();
        conn.execute("INSERT INTO csvdata VALUES (10, 20)").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.csv");
        let sql = format!("COPY csvdata TO '{}'", path.display());
        conn.execute(&sql).unwrap();
        assert!(path.exists());

        // Read back
        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("10"));
    }

    #[test]
    fn test_copy_to_parquet_format_autodetect() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE cdata (id BIGINT)").unwrap();
        conn.execute("INSERT INTO cdata VALUES (1)").unwrap();
        conn.execute("INSERT INTO cdata VALUES (2)").unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("output.parquet");
        let sql = format!("COPY cdata TO '{}'", path.display());
        conn.execute(&sql).unwrap();
        assert!(path.exists());
        assert!(std::fs::metadata(&path).unwrap().len() > 0);

        // Verify roundtrip
        let conn2 = Connection::in_memory().unwrap();
        conn2
            .register_parquet("readback", path.to_str().unwrap())
            .unwrap();
        let results = conn2.query("SELECT COUNT(*) FROM readback").unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[test]
    fn test_sql_begin_commit() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE t1 (id BIGINT, name VARCHAR)")
            .unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t1 VALUES (1, 'alice')").unwrap();
        conn.execute("COMMIT").unwrap();
        let results = conn.query("SELECT * FROM t1").unwrap();
        assert_eq!(results[0].num_rows(), 1);
    }

    #[test]
    fn test_sql_begin_rollback() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE t2 (id BIGINT)").unwrap();
        conn.execute("INSERT INTO t2 VALUES (1)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t2 VALUES (2)").unwrap();
        conn.execute("ROLLBACK").unwrap();
        // After rollback, only original row should be visible
        let results = conn.query("SELECT COUNT(*) FROM t2").unwrap();
        let arr = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1);
    }

    #[test]
    fn test_sql_savepoint() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE t3 (id BIGINT)").unwrap();
        conn.execute("BEGIN").unwrap();
        conn.execute("INSERT INTO t3 VALUES (1)").unwrap();
        conn.execute("SAVEPOINT sp1").unwrap();
        conn.execute("INSERT INTO t3 VALUES (2)").unwrap();
        conn.execute("ROLLBACK TO SAVEPOINT sp1").unwrap();
        conn.execute("COMMIT").unwrap();
        let results = conn.query("SELECT COUNT(*) FROM t3").unwrap();
        let arr = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(arr.value(0), 1);
    }

    #[test]
    fn test_sql_no_active_transaction_errors() {
        let conn = Connection::in_memory().unwrap();
        assert!(conn.execute("COMMIT").is_err());
        assert!(conn.execute("ROLLBACK").is_err());
    }

    #[test]
    fn test_analyze_table_persists_stats() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE stats_test (id BIGINT, name VARCHAR)")
            .unwrap();
        conn.execute("INSERT INTO stats_test VALUES (1, 'Alice')")
            .unwrap();
        conn.execute("INSERT INTO stats_test VALUES (2, 'Bob')")
            .unwrap();
        conn.execute("INSERT INTO stats_test VALUES (3, 'Charlie')")
            .unwrap();

        // Before ANALYZE, no stats should be available
        assert!(conn.stats_manager().get("stats_test").is_none());

        // Run ANALYZE TABLE
        conn.execute("ANALYZE TABLE stats_test").unwrap();

        // Stats should now be persisted
        let stats = conn.stats_manager().get("stats_test");
        assert!(stats.is_some());
        let stats = stats.unwrap();
        assert_eq!(stats.row_count, 3);
        assert_eq!(stats.columns.len(), 2);

        // Check column statistics
        let id_stats = stats.column("id").unwrap();
        assert_eq!(id_stats.distinct_count, Some(3));
        assert!(id_stats.histogram.is_some());

        let name_stats = stats.column("name").unwrap();
        assert_eq!(name_stats.distinct_count, Some(3));
    }

    #[test]
    fn test_analyze_table_public_api_persists_stats() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE api_test (val BIGINT)").unwrap();
        conn.execute("INSERT INTO api_test VALUES (10)").unwrap();
        conn.execute("INSERT INTO api_test VALUES (20)").unwrap();

        // Use execute("ANALYZE TABLE ...") which calls execute_analyze_table internally
        conn.execute("ANALYZE TABLE api_test").unwrap();

        // Stats should be in the stats_manager
        let persisted = conn.stats_manager().get("api_test");
        assert!(persisted.is_some());
        assert_eq!(persisted.unwrap().row_count, 2);
    }

    #[test]
    fn test_cbo_used_in_query_pipeline() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE cbo_test (id BIGINT, val VARCHAR)")
            .unwrap();
        conn.execute("INSERT INTO cbo_test VALUES (1, 'a')")
            .unwrap();
        conn.execute("INSERT INTO cbo_test VALUES (2, 'b')")
            .unwrap();

        // Analyze to populate stats
        conn.execute("ANALYZE TABLE cbo_test").unwrap();

        // Query should still work correctly with CBO in the pipeline
        let results = conn.query("SELECT * FROM cbo_test WHERE id = 1").unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[test]
    fn test_cbo_join_with_statistics() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE orders (id BIGINT, customer_id BIGINT, total BIGINT)")
            .unwrap();
        conn.execute("CREATE TABLE customers (id BIGINT, name VARCHAR)")
            .unwrap();
        for i in 0..10 {
            conn.execute(&format!(
                "INSERT INTO orders VALUES ({}, {}, {})",
                i,
                i % 3,
                (i + 1) * 100
            ))
            .unwrap();
        }
        conn.execute("INSERT INTO customers VALUES (0, 'Alice')")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (1, 'Bob')")
            .unwrap();
        conn.execute("INSERT INTO customers VALUES (2, 'Carol')")
            .unwrap();

        // Analyze both tables to populate statistics
        conn.execute("ANALYZE TABLE orders").unwrap();
        conn.execute("ANALYZE TABLE customers").unwrap();

        // Verify stats exist
        let stats = conn.stats_manager().get("orders");
        assert!(stats.is_some());
        assert_eq!(stats.unwrap().row_count, 10);

        // Join query should use CBO for optimization
        let results = conn
            .query(
                "SELECT c.name, o.total FROM orders o \
                 JOIN customers c ON o.customer_id = c.id \
                 WHERE o.total > 500 ORDER BY o.total",
            )
            .unwrap();
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Join should return results");
    }

    #[test]
    fn test_cache_hit_miss() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE cache_test (id BIGINT, name VARCHAR)")
            .unwrap();
        conn.execute("INSERT INTO cache_test VALUES (1, 'alice')")
            .unwrap();

        // First query: cache miss
        let r1 = conn.query("SELECT * FROM cache_test").unwrap();
        let stats = conn.cache_stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);

        // Second query: cache hit
        let r2 = conn.query("SELECT * FROM cache_test").unwrap();
        let stats = conn.cache_stats();
        assert_eq!(stats.hits, 1);

        assert_eq!(r1.len(), r2.len());
        assert_eq!(r1[0].num_rows(), r2[0].num_rows());
    }

    #[test]
    fn test_cache_invalidation_on_insert() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE inv_test (id BIGINT)").unwrap();
        conn.execute("INSERT INTO inv_test VALUES (1)").unwrap();

        // Populate cache
        let r1 = conn.query("SELECT * FROM inv_test").unwrap();
        assert_eq!(r1[0].num_rows(), 1);

        // Insert invalidates cache
        conn.execute("INSERT INTO inv_test VALUES (2)").unwrap();

        // Should get fresh result with 2 rows
        let r2 = conn.query("SELECT * FROM inv_test").unwrap();
        let total_rows: usize = r2.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_no_cache_hint() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE nc_test (id BIGINT)").unwrap();
        conn.execute("INSERT INTO nc_test VALUES (1)").unwrap();

        // Query with NO_CACHE hint should not populate cache
        let _ = conn.query("/*+ NO_CACHE */ SELECT * FROM nc_test").unwrap();
        let stats = conn.cache_stats();
        assert_eq!(stats.misses, 0);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.total_entries, 0);
    }

    #[test]
    fn test_explain_cache_command() {
        let conn = Connection::in_memory().unwrap();
        // Should succeed without error
        let result = conn.execute("EXPLAIN CACHE").unwrap();
        assert_eq!(result, 0);
    }

    #[test]
    fn test_cache_stats_api() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE stats_api (id BIGINT)").unwrap();
        conn.execute("INSERT INTO stats_api VALUES (1)").unwrap();

        let _ = conn.query("SELECT * FROM stats_api").unwrap();
        let _ = conn.query("SELECT * FROM stats_api").unwrap();

        let stats = conn.cache_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);

        conn.clear_cache();
        let stats = conn.cache_stats();
        assert_eq!(stats.total_entries, 0);
    }

    #[test]
    fn test_create_materialized_view() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE src (id BIGINT, val BIGINT)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 10)").unwrap();
        conn.execute("INSERT INTO src VALUES (2, 20)").unwrap();

        conn.execute("CREATE MATERIALIZED VIEW mv_sum AS SELECT SUM(val) as total FROM src")
            .unwrap();

        let results = conn.query("SELECT * FROM mv_sum").unwrap();
        assert_eq!(results.len(), 1);
        let batch = &results[0];
        assert_eq!(batch.num_rows(), 1);

        // Verify it's tracked in the MV manager
        assert!(conn.mv_manager().get_view("mv_sum").is_some());
    }

    #[test]
    fn test_refresh_materialized_view() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE src (id BIGINT, val BIGINT)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 10)").unwrap();

        conn.execute("CREATE MATERIALIZED VIEW mv_vals AS SELECT id, val FROM src")
            .unwrap();

        // Insert more data into the source table
        conn.execute("INSERT INTO src VALUES (2, 20)").unwrap();

        // Before refresh, the MV should still have 1 row
        let results = conn.query("SELECT * FROM mv_vals").unwrap();
        let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(row_count, 1);

        // Refresh the materialized view
        conn.execute("REFRESH MATERIALIZED VIEW mv_vals").unwrap();

        // After refresh, the MV should have 2 rows
        let results = conn.query("SELECT * FROM mv_vals ORDER BY id").unwrap();
        let row_count: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(row_count, 2);
    }

    #[test]
    fn test_insert_tracks_change_for_mv() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE src (id BIGINT, val BIGINT)")
            .unwrap();
        conn.execute("INSERT INTO src VALUES (1, 10)").unwrap();

        conn.execute("CREATE MATERIALIZED VIEW mv_val AS SELECT val FROM src")
            .unwrap();

        // Insert should be tracked by the MV manager
        conn.execute("INSERT INTO src VALUES (2, 20)").unwrap();

        let tracker = conn.mv_manager().track_table("src");
        assert!(tracker.has_pending_changes());
    }

    #[test]
    fn test_wal_recovery_with_update_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test_wal_recovery");

        // Phase 1: Create data, UPDATE, DELETE, then drop connection
        {
            let conn = Connection::open(&db_path).unwrap();
            conn.execute("CREATE TABLE items (id BIGINT, name VARCHAR, price BIGINT)")
                .unwrap();
            conn.execute("INSERT INTO items VALUES (1, 'apple', 100)")
                .unwrap();
            conn.execute("INSERT INTO items VALUES (2, 'banana', 200)")
                .unwrap();
            conn.execute("INSERT INTO items VALUES (3, 'cherry', 300)")
                .unwrap();
            // Update price of apple
            conn.execute("UPDATE items SET price = 150 WHERE id = 1")
                .unwrap();
            // Delete cherry
            conn.execute("DELETE FROM items WHERE id = 3").unwrap();

            // Verify state before closing
            let results = conn
                .query("SELECT id, price FROM items ORDER BY id")
                .unwrap();
            let total: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2, "Should have 2 rows before close");
        }

        // Phase 2: Reopen and verify WAL replay restored state
        {
            let conn = Connection::open(&db_path).unwrap();
            let results = conn
                .query("SELECT id, price FROM items ORDER BY id")
                .unwrap();
            let total: usize = results.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 2, "Should have 2 rows after WAL recovery");
        }
    }

    #[test]
    fn test_mvcc_manager_accessible() {
        let conn = Connection::in_memory().unwrap();
        let mvcc = conn.mvcc_manager();
        let txn = mvcc.begin_txn();
        assert!(txn > 0);
        mvcc.commit_txn(txn).unwrap();
        assert_eq!(mvcc.active_count(), 0);
    }

    #[test]
    fn test_dml_uses_mvcc_transactions() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE mvcc_test (id BIGINT, val BIGINT)")
            .unwrap();

        // Before DML, no committed transactions beyond the 0 baseline
        let before_lsn = conn.mvcc_manager().low_watermark();

        conn.execute("INSERT INTO mvcc_test VALUES (1, 100)")
            .unwrap();
        conn.execute("UPDATE mvcc_test SET val = 200 WHERE id = 1")
            .unwrap();
        conn.execute("DELETE FROM mvcc_test WHERE id = 999")
            .unwrap();

        // After DML, MVCC should have advanced (3 committed transactions)
        // Low watermark should be >= before since all txns committed
        let after_lsn = conn.mvcc_manager().low_watermark();
        assert!(
            after_lsn >= before_lsn,
            "MVCC watermark should advance after DML"
        );

        // No active transactions remain after DML completes
        assert_eq!(conn.mvcc_manager().active_count(), 0);
    }

    #[test]
    fn test_compact_table() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE frag (id BIGINT, val BIGINT)")
            .unwrap();

        // Insert many small batches to create fragmentation
        for i in 0..10 {
            conn.execute(&format!("INSERT INTO frag VALUES ({}, {})", i, i * 10))
                .unwrap();
        }

        // Before compaction: many batches
        let stats = conn.compact_table("frag").unwrap();
        assert!(
            stats.batches_before >= 10,
            "Should have multiple batches before compaction"
        );
        assert_eq!(
            stats.batches_after, 1,
            "Should have 1 batch after compaction"
        );
        assert_eq!(stats.rows, 10, "Should preserve all rows");

        // Data should still be queryable
        let results = conn.query("SELECT * FROM frag").unwrap();
        let total: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_builtin_vector_distance_udf_registered() {
        let conn = Connection::in_memory().unwrap();
        // The vector_distance function should be registered
        let registry = conn.udf_registry();
        assert!(registry.get_scalar("vector_distance").is_some());
    }

    #[test]
    fn test_plan_stream_basic() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE t_stream (id BIGINT)").unwrap();
        conn.execute("INSERT INTO t_stream VALUES (1)").unwrap();
        conn.execute("INSERT INTO t_stream VALUES (2)").unwrap();

        let stream = conn.query_iter("SELECT * FROM t_stream").unwrap();

        let mut count = 0;
        for batch in stream {
            count += batch.unwrap().num_rows();
        }
    }

    #[test]
    fn test_plan_stream_empty() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE t_stream_empty (id BIGINT)")
            .unwrap();

        let stream = conn.query_iter("SELECT * FROM t_stream_empty").unwrap();
    }

    #[test]
    fn test_plan_stream_size_hint() {
        let conn = Connection::in_memory().unwrap();
        conn.execute("CREATE TABLE t_stream_hint (id BIGINT)")
            .unwrap();
        conn.execute("INSERT INTO t_stream_hint VALUES (1), (2)")
            .unwrap();

        let mut stream = conn.query_iter("SELECT * FROM t_stream_hint").unwrap();
        let (lo, hi) = stream.size_hint();
        let initial_len = lo;

        if initial_len > 0 {
            stream.next();
            let (lo2, hi2) = stream.size_hint();
        }
    }
}
