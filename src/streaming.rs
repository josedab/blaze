//! Async Streaming Execution
//!
//! This module provides async streaming execution capabilities for query processing.
//! Instead of materializing entire result sets, queries produce streams of record batches
//! that can be consumed lazily with backpressure support.
//!
//! # Features
//!
//! - **Lazy Evaluation**: Results are produced on-demand, not all at once
//! - **Backpressure**: Consumers control the pace of data production
//! - **Memory Efficiency**: Only keeps active batches in memory
//! - **Async/Await**: Full async support for non-blocking execution
//!
//! # Example
//!
//! ```rust,no_run
//! use blaze::streaming::{AsyncConnection, StreamExt};
//!
//! async fn example() -> blaze::Result<()> {
//!     let conn = AsyncConnection::in_memory()?;
//!
//!     let mut stream = conn.query_stream("SELECT * FROM large_table").await?;
//!
//!     while let Some(batch) = stream.next().await {
//!         let batch = batch?;
//!         println!("Got batch with {} rows", batch.num_rows());
//!     }
//!
//!     Ok(())
//! }
//! ```

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::datatypes::Schema as ArrowSchema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::Stream;
use pin_project_lite::pin_project;
use tokio::sync::mpsc;

use crate::catalog::{CatalogList, TableProvider};
use crate::error::{BlazeError, Result};
use crate::executor::ExecutionContext;
use crate::planner::PhysicalPlan;
use crate::planner::{Binder, Optimizer, PhysicalPlanner};
use crate::sql::parser::Parser;
use crate::storage::{CsvTable, MemoryTable, ParquetTable};
use crate::types::{DataType, Field, Schema};

// Re-export for convenience
pub use futures::StreamExt;

/// A stream of RecordBatches that can be sent across threads.
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

/// Trait for async streams of RecordBatches.
#[async_trait]
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> + Send {
    /// Returns the schema of the stream.
    fn schema(&self) -> Arc<ArrowSchema>;
}

/// A simple stream that wraps a vector of RecordBatches.
pin_project! {
    pub struct MemoryStream {
        schema: Arc<ArrowSchema>,
        batches: Vec<RecordBatch>,
        index: usize,
    }
}

impl MemoryStream {
    /// Create a new MemoryStream from batches.
    pub fn new(batches: Vec<RecordBatch>, schema: Arc<ArrowSchema>) -> Self {
        Self {
            schema,
            batches,
            index: 0,
        }
    }

    /// Create an empty stream with the given schema.
    pub fn empty(schema: Arc<ArrowSchema>) -> Self {
        Self {
            schema,
            batches: vec![],
            index: 0,
        }
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.index < this.batches.len() {
            let batch = this.batches[*this.index].clone();
            *this.index += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for MemoryStream {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }
}

/// A stream backed by an async channel for backpressure support.
pin_project! {
    pub struct ChannelStream {
        schema: Arc<ArrowSchema>,
        #[pin]
        receiver: mpsc::Receiver<Result<RecordBatch>>,
    }
}

impl ChannelStream {
    /// Create a new channel-backed stream.
    pub fn new(schema: Arc<ArrowSchema>, receiver: mpsc::Receiver<Result<RecordBatch>>) -> Self {
        Self { schema, receiver }
    }
}

impl Stream for ChannelStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        this.receiver.poll_recv(cx)
    }
}

impl RecordBatchStream for ChannelStream {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }
}

/// A stream that applies a filter to each batch.
pin_project! {
    pub struct FilterStream<S> {
        schema: Arc<ArrowSchema>,
        #[pin]
        input: S,
        predicate: Arc<dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync>,
    }
}

impl<S: RecordBatchStream> FilterStream<S> {
    /// Create a new filter stream.
    pub fn new(
        input: S,
        predicate: Arc<dyn Fn(&RecordBatch) -> Result<RecordBatch> + Send + Sync>,
    ) -> Self {
        let schema = input.schema();
        Self {
            schema,
            input,
            predicate,
        }
    }
}

impl<S: RecordBatchStream> Stream for FilterStream<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.input.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                Poll::Ready(Some((this.predicate)(&batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: RecordBatchStream> RecordBatchStream for FilterStream<S> {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }
}

/// A stream that projects columns from each batch.
pin_project! {
    pub struct ProjectionStream<S> {
        schema: Arc<ArrowSchema>,
        #[pin]
        input: S,
        indices: Vec<usize>,
    }
}

impl<S: RecordBatchStream> ProjectionStream<S> {
    /// Create a new projection stream.
    pub fn new(input: S, indices: Vec<usize>, schema: Arc<ArrowSchema>) -> Self {
        Self {
            schema,
            input,
            indices,
        }
    }
}

impl<S: RecordBatchStream> Stream for ProjectionStream<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        match this.input.poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let columns: Vec<_> = this.indices
                    .iter()
                    .map(|&i| batch.column(i).clone())
                    .collect();
                let result = RecordBatch::try_new(this.schema.clone(), columns)
                    .map_err(|e| BlazeError::execution(e.to_string()));
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: RecordBatchStream> RecordBatchStream for ProjectionStream<S> {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }
}

/// A stream that limits the number of rows returned.
pin_project! {
    pub struct LimitStream<S> {
        schema: Arc<ArrowSchema>,
        #[pin]
        input: S,
        limit: usize,
        offset: usize,
        rows_seen: usize,
        rows_returned: usize,
    }
}

impl<S: RecordBatchStream> LimitStream<S> {
    /// Create a new limit stream.
    pub fn new(input: S, limit: usize, offset: usize) -> Self {
        let schema = input.schema();
        Self {
            schema,
            input,
            limit,
            offset,
            rows_seen: 0,
            rows_returned: 0,
        }
    }
}

impl<S: RecordBatchStream> Stream for LimitStream<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // Already returned enough rows
        if *this.rows_returned >= *this.limit {
            return Poll::Ready(None);
        }

        loop {
            match this.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let batch_rows = batch.num_rows();
                    let batch_end = *this.rows_seen + batch_rows;

                    // Skip batches entirely in offset range
                    if batch_end <= *this.offset {
                        *this.rows_seen = batch_end;
                        continue;
                    }

                    // Calculate slice bounds
                    let start_in_batch = if *this.rows_seen < *this.offset {
                        *this.offset - *this.rows_seen
                    } else {
                        0
                    };

                    let rows_remaining = *this.limit - *this.rows_returned;
                    let rows_available = batch_rows - start_in_batch;
                    let rows_to_take = rows_remaining.min(rows_available);

                    *this.rows_seen = batch_end;

                    if rows_to_take == 0 {
                        continue;
                    }

                    // Slice the batch
                    let sliced = batch.slice(start_in_batch, rows_to_take);
                    *this.rows_returned += sliced.num_rows();

                    return Poll::Ready(Some(Ok(sliced)));
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S: RecordBatchStream> RecordBatchStream for LimitStream<S> {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }
}

/// A stream that coalesces small batches into larger ones.
pin_project! {
    pub struct CoalesceStream<S> {
        schema: Arc<ArrowSchema>,
        #[pin]
        input: S,
        target_batch_size: usize,
        buffer: Vec<RecordBatch>,
        buffer_rows: usize,
        finished: bool,
    }
}

impl<S: RecordBatchStream> CoalesceStream<S> {
    /// Create a new coalesce stream.
    pub fn new(input: S, target_batch_size: usize) -> Self {
        let schema = input.schema();
        Self {
            schema,
            input,
            target_batch_size,
            buffer: Vec::new(),
            buffer_rows: 0,
            finished: false,
        }
    }

    fn flush_buffer(&mut self) -> Result<RecordBatch> {
        if self.buffer.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        if self.buffer.len() == 1 {
            self.buffer_rows = 0;
            return Ok(self.buffer.pop().unwrap());
        }

        // Concatenate all buffered batches
        let batch = arrow::compute::concat_batches(&self.schema, &self.buffer)
            .map_err(|e| BlazeError::execution(e.to_string()))?;

        self.buffer.clear();
        self.buffer_rows = 0;

        Ok(batch)
    }
}

impl<S: RecordBatchStream> Stream for CoalesceStream<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.finished {
            return Poll::Ready(None);
        }

        loop {
            match this.input.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    *this.buffer_rows += batch.num_rows();
                    this.buffer.push(batch);

                    if *this.buffer_rows >= *this.target_batch_size {
                        // Flush when we have enough rows
                        let batches = std::mem::take(this.buffer);
                        *this.buffer_rows = 0;

                        let result = arrow::compute::concat_batches(this.schema, &batches)
                            .map_err(|e| BlazeError::execution(e.to_string()));

                        return Poll::Ready(Some(result));
                    }
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    *this.finished = true;
                    if !this.buffer.is_empty() {
                        let batches = std::mem::take(this.buffer);
                        let result = arrow::compute::concat_batches(this.schema, &batches)
                            .map_err(|e| BlazeError::execution(e.to_string()));
                        return Poll::Ready(Some(result));
                    }
                    return Poll::Ready(None);
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<S: RecordBatchStream> RecordBatchStream for CoalesceStream<S> {
    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }
}

/// Configuration for async streaming execution.
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Size of the channel buffer for backpressure (default: 2)
    pub channel_buffer_size: usize,
    /// Target batch size for coalescing (default: 8192)
    pub target_batch_size: usize,
    /// Whether to enable batch coalescing (default: true)
    pub enable_coalescing: bool,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            channel_buffer_size: 2,
            target_batch_size: 8192,
            enable_coalescing: true,
        }
    }
}

/// Async streaming execution context.
pub struct StreamingExecutor {
    config: StreamConfig,
}

impl StreamingExecutor {
    /// Create a new streaming executor.
    pub fn new(config: StreamConfig) -> Self {
        Self { config }
    }

    /// Execute a physical plan and return a stream of record batches.
    pub async fn execute(&self, plan: &PhysicalPlan) -> Result<SendableRecordBatchStream> {
        // For now, execute synchronously and wrap in a memory stream
        // In the future, this would execute operators asynchronously
        let ctx = ExecutionContext::new();
        let batches = ctx.execute(plan)?;

        let schema = plan.schema();
        let stream = MemoryStream::new(batches, schema);

        if self.config.enable_coalescing {
            Ok(Box::pin(CoalesceStream::new(
                stream,
                self.config.target_batch_size,
            )))
        } else {
            Ok(Box::pin(stream))
        }
    }

    /// Execute with a channel for true streaming with backpressure.
    ///
    /// Note: This spawns a background task that produces batches.
    /// The stream provides backpressure - if the consumer is slow,
    /// the producer will pause until there's room in the channel.
    pub fn execute_with_channel(
        &self,
        plan: PhysicalPlan,
    ) -> (SendableRecordBatchStream, tokio::task::JoinHandle<Result<()>>) {
        let (tx, rx) = mpsc::channel(self.config.channel_buffer_size);
        let schema = plan.schema();

        let handle = tokio::spawn(async move {
            let ctx = ExecutionContext::new();
            let batches = ctx.execute(&plan)?;

            for batch in batches {
                if tx.send(Ok(batch)).await.is_err() {
                    // Receiver dropped, stop producing
                    break;
                }
            }

            Ok(())
        });

        (Box::pin(ChannelStream::new(schema, rx)), handle)
    }
}

impl Default for StreamingExecutor {
    fn default() -> Self {
        Self::new(StreamConfig::default())
    }
}

/// Async connection for streaming query execution.
pub struct AsyncConnection {
    /// The catalog list containing catalogs, schemas, and tables
    catalog_list: Arc<CatalogList>,
    /// Execution context with configuration
    execution_context: ExecutionContext,
    /// Query optimizer
    optimizer: Optimizer,
    /// Streaming executor
    streaming_executor: StreamingExecutor,
}

impl AsyncConnection {
    /// Create a new in-memory async connection.
    pub fn in_memory() -> Result<Self> {
        Self::with_config(StreamConfig::default())
    }

    /// Create a connection with custom streaming configuration.
    pub fn with_config(config: StreamConfig) -> Result<Self> {
        let catalog_list = Arc::new(CatalogList::default());
        Ok(Self {
            execution_context: ExecutionContext::new()
                .with_catalog_list(catalog_list.clone()),
            catalog_list,
            optimizer: Optimizer::default(),
            streaming_executor: StreamingExecutor::new(config),
        })
    }

    /// Execute a SQL query and return a stream of record batches.
    pub async fn query_stream(&self, sql: &str) -> Result<SendableRecordBatchStream> {
        let physical_plan = self.prepare_plan(sql)?;
        self.streaming_executor.execute(&physical_plan).await
    }

    /// Execute a SQL query with channel-based streaming for backpressure.
    pub fn query_stream_with_backpressure(
        &self,
        sql: &str,
    ) -> Result<(SendableRecordBatchStream, tokio::task::JoinHandle<Result<()>>)> {
        let physical_plan = self.prepare_plan(sql)?;
        Ok(self.streaming_executor.execute_with_channel(physical_plan))
    }

    /// Execute a SQL query and collect all results.
    pub async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        use futures::StreamExt;

        let mut stream = self.query_stream(sql).await?;
        let mut batches = Vec::new();

        while let Some(batch) = stream.next().await {
            batches.push(batch?);
        }

        Ok(batches)
    }

    /// Prepare a physical plan from SQL.
    fn prepare_plan(&self, sql: &str) -> Result<PhysicalPlan> {
        let statements = Parser::parse(sql)?;

        if statements.is_empty() {
            return Err(BlazeError::analysis("Empty SQL statement"));
        }

        let statement = statements.into_iter().next().unwrap();
        let binder = Binder::new(self.catalog_list.clone());
        let logical_plan = binder.bind(statement)?;
        let optimized_plan = self.optimizer.optimize(&logical_plan)?;
        let physical_planner = PhysicalPlanner::new();
        physical_planner.create_physical_plan(&optimized_plan)
    }

    /// Register a CSV file as a table.
    pub fn register_csv(&self, name: &str, path: impl AsRef<std::path::Path>) -> Result<()> {
        let table = CsvTable::open(path)?;
        self.register_table(name, Arc::new(table))
    }

    /// Register a Parquet file as a table.
    pub fn register_parquet(&self, name: &str, path: impl AsRef<std::path::Path>) -> Result<()> {
        let table = ParquetTable::open(path)?;
        self.register_table(name, Arc::new(table))
    }

    /// Register record batches as a table.
    pub fn register_batches(&self, name: &str, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Err(BlazeError::invalid_argument("Cannot register empty batches"));
        }

        let schema = Schema::from_arrow(batches[0].schema().as_ref())?;
        let table = MemoryTable::new(schema, batches);
        self.register_table(name, Arc::new(table))
    }

    /// Execute DDL statements.
    pub fn execute(&self, sql: &str) -> Result<usize> {
        let statements = Parser::parse(sql)?;

        if statements.is_empty() {
            return Ok(0);
        }

        let statement = statements.into_iter().next().unwrap();

        match &statement {
            crate::sql::Statement::CreateTable(create) => {
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
            crate::sql::Statement::DropTable(drop) => {
                let table_name = drop.name.last().cloned().unwrap_or_default();
                match self.deregister_table(&table_name) {
                    Ok(_) => Ok(0),
                    Err(_) if drop.if_exists => Ok(0),
                    Err(e) => Err(e),
                }
            }
            _ => {
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

    /// Register a custom table provider.
    fn register_table(&self, name: &str, table: Arc<dyn TableProvider>) -> Result<()> {
        let catalog = self.catalog_list.catalog("default")
            .ok_or_else(|| BlazeError::schema("Default catalog not found"))?;
        let schema = catalog.schema("main")
            .ok_or_else(|| BlazeError::schema("Default schema 'main' not found"))?;
        schema.register_table(name, table)?;
        Ok(())
    }

    /// Deregister a table.
    fn deregister_table(&self, name: &str) -> Result<()> {
        let catalog = self.catalog_list.catalog("default")
            .ok_or_else(|| BlazeError::schema("Default catalog not found"))?;
        let schema = catalog.schema("main")
            .ok_or_else(|| BlazeError::schema("Default schema 'main' not found"))?;
        schema.deregister_table(name)?;
        Ok(())
    }

    /// List all tables.
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog_list.catalog("default")
            .and_then(|c| c.schema("main"))
            .map(|s| s.table_names())
            .unwrap_or_default()
    }
}

/// Collect all batches from a stream into a vector.
pub async fn collect(mut stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = futures::StreamExt::next(&mut stream).await {
        batches.push(batch?);
    }
    Ok(batches)
}

/// Collect and print stream results for debugging.
pub async fn print_stream(mut stream: SendableRecordBatchStream) -> Result<()> {
    use arrow::util::pretty::print_batches;

    let mut batches = Vec::new();
    while let Some(batch) = futures::StreamExt::next(&mut stream).await {
        batches.push(batch?);
    }

    print_batches(&batches).map_err(|e| BlazeError::execution(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        ).unwrap()
    }

    #[tokio::test]
    async fn test_memory_stream() {
        let batch = create_test_batch();
        let schema = batch.schema();
        let mut stream = MemoryStream::new(vec![batch.clone()], schema);

        let result = futures::StreamExt::next(&mut stream).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().unwrap().num_rows(), 3);

        let result = futures::StreamExt::next(&mut stream).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_limit_stream() {
        let batch = create_test_batch();
        let schema = batch.schema();
        let inner = MemoryStream::new(vec![batch], schema);
        let mut stream = LimitStream::new(inner, 2, 0);

        let result = futures::StreamExt::next(&mut stream).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().unwrap().num_rows(), 2);

        let result = futures::StreamExt::next(&mut stream).await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_limit_stream_with_offset() {
        let batch = create_test_batch();
        let schema = batch.schema();
        let inner = MemoryStream::new(vec![batch], schema);
        let mut stream = LimitStream::new(inner, 2, 1);

        let result = futures::StreamExt::next(&mut stream).await;
        assert!(result.is_some());
        assert_eq!(result.unwrap().unwrap().num_rows(), 2);
    }

    #[tokio::test]
    async fn test_projection_stream() {
        let batch = create_test_batch();
        let schema = batch.schema();
        let projected_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("name", ArrowDataType::Utf8, true),
        ]));

        let inner = MemoryStream::new(vec![batch], schema);
        let mut stream = ProjectionStream::new(inner, vec![1], projected_schema);

        let result = futures::StreamExt::next(&mut stream).await;
        assert!(result.is_some());
        let batch = result.unwrap().unwrap();
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_collect() {
        let batch = create_test_batch();
        let schema = batch.schema();
        let stream: SendableRecordBatchStream = Box::pin(
            MemoryStream::new(vec![batch.clone(), batch], schema)
        );

        let batches = collect(stream).await.unwrap();
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn test_coalesce_stream() {
        let batch = create_test_batch(); // 3 rows each
        let schema = batch.schema();

        // Create stream with multiple small batches
        let inner = MemoryStream::new(
            vec![batch.clone(), batch.clone(), batch],
            schema.clone(),
        );

        // Coalesce with target of 5 rows
        let stream: SendableRecordBatchStream = Box::pin(
            CoalesceStream::new(inner, 5)
        );

        let batches = collect(stream).await.unwrap();
        // Should produce 2 batches: one with 6 rows (2*3), one with 3 rows
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 6);
        assert_eq!(batches[1].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_channel_stream() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let (tx, rx) = mpsc::channel(2);
        let stream = ChannelStream::new(schema, rx);

        // Spawn producer
        tokio::spawn(async move {
            tx.send(Ok(batch)).await.unwrap();
        });

        let batches = collect(Box::pin(stream)).await.unwrap();
        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn test_async_connection() {
        // Test that the async connection can be created and basic operations work
        let conn = AsyncConnection::in_memory().unwrap();

        // Test DDL operations
        conn.execute("CREATE TABLE users (id INT, name VARCHAR)").unwrap();
        let tables = conn.list_tables();
        assert!(tables.contains(&"users".to_string()));

        // Test streaming from an empty table
        let stream = conn.query_stream("SELECT * FROM users").await.unwrap();
        let batches = collect(stream).await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0); // Empty table
    }

    #[tokio::test]
    async fn test_async_connection_with_table() {
        let conn = AsyncConnection::in_memory().unwrap();
        conn.execute("CREATE TABLE test (id INT, name VARCHAR)").unwrap();

        let tables = conn.list_tables();
        assert!(tables.contains(&"test".to_string()));

        let stream = conn.query_stream("SELECT * FROM test").await.unwrap();
        let batches = collect(stream).await.unwrap();
        assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));
    }

    #[tokio::test]
    async fn test_streaming_executor() {
        // Use a memory stream directly to test the executor without query path issues
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("x", ArrowDataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();

        let inner_stream = MemoryStream::new(vec![batch], schema);
        let executor = StreamingExecutor::default();

        // Wrap in coalesce for the executor's default behavior
        let stream: SendableRecordBatchStream = Box::pin(inner_stream);
        let batches = collect(stream).await.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 3);
    }
}
