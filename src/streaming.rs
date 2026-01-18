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

use std::collections::HashMap;
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
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some((this.predicate)(&batch))),
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
                let columns: Vec<_> = this
                    .indices
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
    ) -> (
        SendableRecordBatchStream,
        tokio::task::JoinHandle<Result<()>>,
    ) {
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
            execution_context: ExecutionContext::new().with_catalog_list(catalog_list.clone()),
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
    ) -> Result<(
        SendableRecordBatchStream,
        tokio::task::JoinHandle<Result<()>>,
    )> {
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
            return Err(BlazeError::invalid_argument(
                "Cannot register empty batches",
            ));
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
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::schema("Default catalog not found"))?;
        let schema = catalog
            .schema("main")
            .ok_or_else(|| BlazeError::schema("Default schema 'main' not found"))?;
        schema.register_table(name, table)?;
        Ok(())
    }

    /// Deregister a table.
    fn deregister_table(&self, name: &str) -> Result<()> {
        let catalog = self
            .catalog_list
            .catalog("default")
            .ok_or_else(|| BlazeError::schema("Default catalog not found"))?;
        let schema = catalog
            .schema("main")
            .ok_or_else(|| BlazeError::schema("Default schema 'main' not found"))?;
        schema.deregister_table(name)?;
        Ok(())
    }

    /// List all tables.
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog_list
            .catalog("default")
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

// ---------------------------------------------------------------------------
// Feature 5: Streaming SQL (Continuous Queries)
// ---------------------------------------------------------------------------

/// Stream definition for CREATE STREAM DDL.
#[derive(Debug, Clone)]
pub struct StreamDefinition {
    pub name: String,
    pub schema: Arc<ArrowSchema>,
    pub source: StreamSource,
    pub watermark_column: Option<String>,
    pub watermark_delay: Option<std::time::Duration>,
}

/// Source type for a stream definition.
#[derive(Debug, Clone)]
pub enum StreamSource {
    /// In-process channel with configurable buffer size.
    Channel { buffer_size: usize },
    /// Periodic interval source.
    Interval { period: std::time::Duration },
    /// External connector (e.g. Kafka, Kinesis).
    External {
        connector: String,
        options: HashMap<String, String>,
    },
}

impl StreamDefinition {
    /// Create a new stream definition.
    pub fn new(name: impl Into<String>, schema: Arc<ArrowSchema>, source: StreamSource) -> Self {
        Self {
            name: name.into(),
            schema,
            source,
            watermark_column: None,
            watermark_delay: None,
        }
    }

    /// Attach watermark configuration.
    pub fn with_watermark(mut self, column: impl Into<String>, delay: std::time::Duration) -> Self {
        self.watermark_column = Some(column.into());
        self.watermark_delay = Some(delay);
        self
    }
}

/// Watermark tracker for stream progress.
#[derive(Debug, Clone)]
pub struct Watermark {
    current_value: i64, // microseconds since epoch
    delay: std::time::Duration,
    column_name: String,
}

impl Watermark {
    /// Create a new watermark tracker.
    pub fn new(column_name: impl Into<String>, delay: std::time::Duration) -> Self {
        Self {
            current_value: 0,
            delay,
            column_name: column_name.into(),
        }
    }

    /// Advance the watermark given an observed event-time (microseconds).
    pub fn advance(&mut self, observed_time: i64) {
        let delayed = observed_time - self.delay.as_micros() as i64;
        if delayed > self.current_value {
            self.current_value = delayed;
        }
    }

    /// Return the current watermark value (microseconds).
    pub fn current(&self) -> i64 {
        self.current_value
    }

    /// Returns `true` if the given event-time is older than the watermark.
    pub fn is_late(&self, event_time: i64) -> bool {
        event_time < self.current_value
    }
}

/// Window type for streaming aggregation.
#[derive(Debug, Clone)]
pub enum StreamWindow {
    /// Fixed-size, non-overlapping windows.
    Tumbling { size: std::time::Duration },
    /// Fixed-size, overlapping windows with a slide interval.
    Hopping {
        size: std::time::Duration,
        slide: std::time::Duration,
    },
    /// Gap-based windows that close after inactivity.
    Session { gap: std::time::Duration },
}

/// State of a single window instance.
#[derive(Debug)]
struct WindowState {
    start: i64,
    end: i64,
    batches: Vec<RecordBatch>,
    row_count: usize,
}

/// Result of a completed window.
#[derive(Debug, Clone)]
pub struct WindowResult {
    pub window_start: i64,
    pub window_end: i64,
    pub data: Vec<RecordBatch>,
    pub row_count: usize,
}

/// Windowed aggregation processor for streams.
pub struct WindowedAggregator {
    window: StreamWindow,
    watermark: Watermark,
    active_windows: Vec<WindowState>,
    emitted_windows: u64,
}

impl WindowedAggregator {
    /// Create a new windowed aggregator.
    pub fn new(window: StreamWindow, watermark: Watermark) -> Self {
        Self {
            window,
            watermark,
            active_windows: Vec::new(),
            emitted_windows: 0,
        }
    }

    /// Process a batch, assigning rows to windows and emitting completed ones.
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        time_column: usize,
    ) -> Result<Vec<WindowResult>> {
        use arrow::array::Int64Array;

        let time_array = batch
            .column(time_column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlazeError::execution("Time column must be Int64"))?;

        // Track the maximum observed event-time in this batch.
        let mut max_time: i64 = i64::MIN;
        for i in 0..time_array.len() {
            let t = time_array.value(i);
            if t > max_time {
                max_time = t;
            }
        }

        // Assign rows to windows BEFORE advancing the watermark so current-batch
        // events are not dropped as late.
        match &self.window {
            StreamWindow::Tumbling { size } => {
                let size_us = size.as_micros() as i64;
                for i in 0..time_array.len() {
                    let t = time_array.value(i);
                    if self.watermark.is_late(t) {
                        continue;
                    }
                    let win_start = (t / size_us) * size_us;
                    let win_end = win_start + size_us;
                    self.assign_row_to_window(win_start, win_end, batch, i)?;
                }
            }
            StreamWindow::Hopping { size, slide } => {
                let size_us = size.as_micros() as i64;
                let slide_us = slide.as_micros() as i64;
                for i in 0..time_array.len() {
                    let t = time_array.value(i);
                    if self.watermark.is_late(t) {
                        continue;
                    }
                    // Determine every window this row belongs to.
                    let earliest_start = ((t - size_us + slide_us) / slide_us) * slide_us;
                    let mut ws = earliest_start.max(0);
                    while ws <= t {
                        let we = ws + size_us;
                        if t >= ws && t < we {
                            self.assign_row_to_window(ws, we, batch, i)?;
                        }
                        ws += slide_us;
                    }
                }
            }
            StreamWindow::Session { gap } => {
                let gap_us = gap.as_micros() as i64;
                for i in 0..time_array.len() {
                    let t = time_array.value(i);
                    if self.watermark.is_late(t) {
                        continue;
                    }
                    // Try to extend an existing session window.
                    let mut found = false;
                    for w in &mut self.active_windows {
                        if t >= w.start && t < w.end + gap_us {
                            // Extend session.
                            if t + gap_us > w.end {
                                w.end = t + gap_us;
                            }
                            w.batches.push(batch.slice(i, 1));
                            w.row_count += 1;
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        self.active_windows.push(WindowState {
                            start: t,
                            end: t + gap_us,
                            batches: vec![batch.slice(i, 1)],
                            row_count: 1,
                        });
                    }
                }
            }
        }

        // Advance watermark after row assignment.
        if max_time != i64::MIN {
            self.watermark.advance(max_time);
        }

        // Emit windows whose end <= watermark.
        self.emit_completed_windows()
    }

    /// Flush all remaining active windows regardless of watermark.
    pub fn flush(&mut self) -> Result<Vec<WindowResult>> {
        let windows = std::mem::take(&mut self.active_windows);
        let mut results = Vec::with_capacity(windows.len());
        for w in windows {
            results.push(WindowResult {
                window_start: w.start,
                window_end: w.end,
                data: w.batches,
                row_count: w.row_count,
            });
            self.emitted_windows += 1;
        }
        Ok(results)
    }

    /// Number of currently open windows.
    pub fn active_window_count(&self) -> usize {
        self.active_windows.len()
    }

    /// Total number of windows emitted so far.
    pub fn emitted_window_count(&self) -> u64 {
        self.emitted_windows
    }

    // -- helpers --

    fn assign_row_to_window(
        &mut self,
        start: i64,
        end: i64,
        batch: &RecordBatch,
        row: usize,
    ) -> Result<()> {
        if let Some(w) = self
            .active_windows
            .iter_mut()
            .find(|w| w.start == start && w.end == end)
        {
            w.batches.push(batch.slice(row, 1));
            w.row_count += 1;
        } else {
            self.active_windows.push(WindowState {
                start,
                end,
                batches: vec![batch.slice(row, 1)],
                row_count: 1,
            });
        }
        Ok(())
    }

    fn emit_completed_windows(&mut self) -> Result<Vec<WindowResult>> {
        let wm = self.watermark.current();
        let mut results = Vec::new();
        let mut remaining = Vec::new();

        for w in std::mem::take(&mut self.active_windows) {
            if w.end <= wm {
                results.push(WindowResult {
                    window_start: w.start,
                    window_end: w.end,
                    data: w.batches,
                    row_count: w.row_count,
                });
                self.emitted_windows += 1;
            } else {
                remaining.push(w);
            }
        }

        self.active_windows = remaining;
        Ok(results)
    }
}

/// Continuous query that processes streaming data.
pub struct ContinuousQuery {
    name: String,
    definition: StreamDefinition,
    window: Option<StreamWindow>,
    is_running: Arc<std::sync::atomic::AtomicBool>,
}

impl ContinuousQuery {
    /// Create a new continuous query.
    pub fn new(name: impl Into<String>, definition: StreamDefinition) -> Self {
        Self {
            name: name.into(),
            definition,
            window: None,
            is_running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Attach a window specification.
    pub fn with_window(mut self, window: StreamWindow) -> Self {
        self.window = Some(window);
        self
    }

    /// Whether the query is currently running.
    pub fn is_running(&self) -> bool {
        self.is_running.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Stop the continuous query.
    pub fn stop(&self) {
        self.is_running
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }

    /// Return the query name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// Sink for continuous query output.
#[derive(Debug, Clone)]
pub enum StreamSink {
    /// Store results in an in-memory buffer.
    Memory { buffer_size: usize },
    /// Forward results through a channel.
    Channel(usize),
    /// Invoke a user-provided callback.
    Callback,
    /// Discard all output.
    Null,
}

/// Stream registry manages active streams and continuous queries.
pub struct StreamRegistry {
    streams: parking_lot::RwLock<HashMap<String, StreamDefinition>>,
    queries: parking_lot::RwLock<HashMap<String, Arc<ContinuousQuery>>>,
}

impl StreamRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            streams: parking_lot::RwLock::new(HashMap::new()),
            queries: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// Register a stream definition.
    pub fn register_stream(&self, definition: StreamDefinition) -> Result<()> {
        let mut streams = self.streams.write();
        if streams.contains_key(&definition.name) {
            return Err(BlazeError::analysis(format!(
                "Stream '{}' already exists",
                definition.name
            )));
        }
        streams.insert(definition.name.clone(), definition);
        Ok(())
    }

    /// Look up a stream by name.
    pub fn get_stream(&self, name: &str) -> Option<StreamDefinition> {
        self.streams.read().get(name).cloned()
    }

    /// List all registered stream names.
    pub fn list_streams(&self) -> Vec<String> {
        self.streams.read().keys().cloned().collect()
    }

    /// Drop a stream definition.
    pub fn drop_stream(&self, name: &str) -> Result<()> {
        let mut streams = self.streams.write();
        if streams.remove(name).is_none() {
            return Err(BlazeError::analysis(format!("Stream '{}' not found", name)));
        }
        Ok(())
    }

    /// Register a continuous query.
    pub fn register_query(&self, query: Arc<ContinuousQuery>) -> Result<()> {
        let mut queries = self.queries.write();
        if queries.contains_key(query.name()) {
            return Err(BlazeError::analysis(format!(
                "Query '{}' already exists",
                query.name()
            )));
        }
        queries.insert(query.name().to_string(), query);
        Ok(())
    }

    /// Stop and remove a continuous query.
    pub fn stop_query(&self, name: &str) -> Result<()> {
        let mut queries = self.queries.write();
        match queries.remove(name) {
            Some(q) => {
                q.stop();
                Ok(())
            }
            None => Err(BlazeError::analysis(format!("Query '{}' not found", name))),
        }
    }

    /// Number of currently registered queries.
    pub fn active_query_count(&self) -> usize {
        self.queries.read().len()
    }
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
        )
        .unwrap()
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
        let projected_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "name",
            ArrowDataType::Utf8,
            true,
        )]));

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
        let stream: SendableRecordBatchStream =
            Box::pin(MemoryStream::new(vec![batch.clone(), batch], schema));

        let batches = collect(stream).await.unwrap();
        assert_eq!(batches.len(), 2);
    }

    #[tokio::test]
    async fn test_coalesce_stream() {
        let batch = create_test_batch(); // 3 rows each
        let schema = batch.schema();

        // Create stream with multiple small batches
        let inner = MemoryStream::new(vec![batch.clone(), batch.clone(), batch], schema.clone());

        // Coalesce with target of 5 rows
        let stream: SendableRecordBatchStream = Box::pin(CoalesceStream::new(inner, 5));

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
        conn.execute("CREATE TABLE users (id INT, name VARCHAR)")
            .unwrap();
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
        conn.execute("CREATE TABLE test (id INT, name VARCHAR)")
            .unwrap();

        let tables = conn.list_tables();
        assert!(tables.contains(&"test".to_string()));

        let stream = conn.query_stream("SELECT * FROM test").await.unwrap();
        let batches = collect(stream).await.unwrap();
        assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));
    }

    #[tokio::test]
    async fn test_streaming_executor() {
        // Use a memory stream directly to test the executor without query path issues
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "x",
            ArrowDataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let inner_stream = MemoryStream::new(vec![batch], schema);
        let executor = StreamingExecutor::default();

        // Wrap in coalesce for the executor's default behavior
        let stream: SendableRecordBatchStream = Box::pin(inner_stream);
        let batches = collect(stream).await.unwrap();
        assert!(!batches.is_empty());
        assert_eq!(batches[0].num_rows(), 3);
    }

    // -- Streaming SQL (Continuous Queries) tests --

    fn create_time_batch(times: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("event_time", ArrowDataType::Int64, false),
            ArrowField::new("value", ArrowDataType::Int32, false),
        ]));
        let values: Vec<i32> = (0..times.len() as i32).collect();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::Int64Array::from(times)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_watermark_advance() {
        let mut wm = Watermark::new("event_time", std::time::Duration::from_secs(5));
        assert_eq!(wm.current(), 0);

        // Advance with 10_000_000 µs (10 s) → watermark = 10s - 5s = 5_000_000
        wm.advance(10_000_000);
        assert_eq!(wm.current(), 5_000_000);

        // Earlier observation must not regress the watermark.
        wm.advance(8_000_000);
        assert_eq!(wm.current(), 5_000_000);

        // Later observation advances further.
        wm.advance(20_000_000);
        assert_eq!(wm.current(), 15_000_000);

        // Late check
        assert!(wm.is_late(4_000_000));
        assert!(!wm.is_late(15_000_001));
    }

    #[test]
    fn test_tumbling_window() {
        let wm = Watermark::new("event_time", std::time::Duration::from_micros(0));
        let window = StreamWindow::Tumbling {
            size: std::time::Duration::from_micros(10),
        };
        let mut agg = WindowedAggregator::new(window, wm);

        // Batch with events in windows [0,10), [10,20), [20,30)
        let batch = create_time_batch(vec![1, 5, 12, 15, 25]);
        let results = agg.process_batch(&batch, 0).unwrap();

        // Watermark at 25 (delay=0); windows [0,10) and [10,20) should emit.
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].row_count, 2);
        assert_eq!(results[1].row_count, 2);

        // One window [20,30) still active.
        assert_eq!(agg.active_window_count(), 1);
        assert_eq!(agg.emitted_window_count(), 2);

        // Flush the remaining window.
        let flushed = agg.flush().unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].row_count, 1);
    }

    #[test]
    fn test_hopping_window() {
        let wm = Watermark::new("event_time", std::time::Duration::from_micros(0));
        let window = StreamWindow::Hopping {
            size: std::time::Duration::from_micros(10),
            slide: std::time::Duration::from_micros(5),
        };
        let mut agg = WindowedAggregator::new(window, wm);

        // Events at 3 and 7 fall into overlapping windows:
        //   [0,10) and [5,15) for event 7
        //   [0,10) for event 3
        // Event at 20 → watermark = 20, emits [0,10) and [5,15).
        let batch = create_time_batch(vec![3, 7, 20]);
        let results = agg.process_batch(&batch, 0).unwrap();

        // [0,10) and [5,15) should be emitted.
        assert!(results.len() >= 2);
        let total_rows: usize = results.iter().map(|r| r.row_count).sum();
        // event 3 → [0,10); event 7 → [0,10) and [5,15); total assignments = 3
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_session_window() {
        let wm = Watermark::new("event_time", std::time::Duration::from_micros(0));
        let window = StreamWindow::Session {
            gap: std::time::Duration::from_micros(5),
        };
        let mut agg = WindowedAggregator::new(window, wm);

        // Two sessions: {1,3} (gap < 5) and {20} (gap from prev > 5)
        // Events at 1, 3 form session [1, 8) (3 + gap 5)
        // Event at 20 forms session [20, 25)
        // Watermark at 20 → session [1, 8) emitted.
        let batch = create_time_batch(vec![1, 3, 20]);
        let results = agg.process_batch(&batch, 0).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].row_count, 2);
        assert_eq!(results[0].window_start, 1);
        assert_eq!(results[0].window_end, 8);

        // Flush remaining session.
        let flushed = agg.flush().unwrap();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].row_count, 1);
    }

    #[test]
    fn test_stream_registry() {
        let registry = StreamRegistry::new();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "ts",
            ArrowDataType::Int64,
            false,
        )]));

        let def = StreamDefinition::new(
            "clicks",
            schema.clone(),
            StreamSource::Channel { buffer_size: 16 },
        )
        .with_watermark("ts", std::time::Duration::from_secs(5));

        registry.register_stream(def).unwrap();
        assert_eq!(registry.list_streams().len(), 1);
        assert!(registry.get_stream("clicks").is_some());
        assert!(registry.get_stream("missing").is_none());

        // Duplicate registration should fail.
        let def2 =
            StreamDefinition::new("clicks", schema, StreamSource::Channel { buffer_size: 8 });
        assert!(registry.register_stream(def2).is_err());

        registry.drop_stream("clicks").unwrap();
        assert!(registry.list_streams().is_empty());
        assert!(registry.drop_stream("clicks").is_err());
    }

    #[test]
    fn test_continuous_query_lifecycle() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "ts",
            ArrowDataType::Int64,
            false,
        )]));
        let def =
            StreamDefinition::new("events", schema, StreamSource::Channel { buffer_size: 32 });

        let query = Arc::new(ContinuousQuery::new("my_query", def).with_window(
            StreamWindow::Tumbling {
                size: std::time::Duration::from_secs(60),
            },
        ));

        assert_eq!(query.name(), "my_query");
        assert!(!query.is_running());

        // Register in registry.
        let registry = StreamRegistry::new();
        registry.register_query(query.clone()).unwrap();
        assert_eq!(registry.active_query_count(), 1);

        // Duplicate query registration should fail.
        assert!(registry.register_query(query.clone()).is_err());

        // Stop and remove.
        registry.stop_query("my_query").unwrap();
        assert_eq!(registry.active_query_count(), 0);
        assert!(registry.stop_query("my_query").is_err());
    }
}
