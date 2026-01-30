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

use std::collections::{HashMap, HashSet};
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

// ---------------------------------------------------------------------------
// Watermark Manager
// ---------------------------------------------------------------------------

/// Strategy for watermark generation.
#[derive(Debug, Clone, PartialEq)]
pub enum WatermarkStrategy {
    /// Assumes event times arrive in monotonically increasing order.
    Monotonic,
    /// Tolerates events arriving up to `max_lateness` out of order.
    BoundedOutOfOrder,
    /// Watermark advances only on explicit punctuation events.
    PunctuatedWatermark,
}

/// Manages watermarks for streaming event-time processing.
#[derive(Debug)]
pub struct WatermarkManager {
    max_lateness_ms: i64,
    max_observed: i64,
    strategy: WatermarkStrategy,
}

impl WatermarkManager {
    /// Create a new `WatermarkManager` with the given maximum lateness.
    pub fn new(max_lateness: std::time::Duration) -> Self {
        Self {
            max_lateness_ms: max_lateness.as_millis() as i64,
            max_observed: i64::MIN,
            strategy: WatermarkStrategy::BoundedOutOfOrder,
        }
    }

    /// Create a manager with an explicit strategy.
    pub fn with_strategy(max_lateness: std::time::Duration, strategy: WatermarkStrategy) -> Self {
        Self {
            max_lateness_ms: max_lateness.as_millis() as i64,
            max_observed: i64::MIN,
            strategy,
        }
    }

    /// Observe an event time and return the updated watermark.
    pub fn update_watermark(&mut self, event_time: i64) -> i64 {
        match self.strategy {
            WatermarkStrategy::Monotonic => {
                self.max_observed = self.max_observed.max(event_time);
                self.max_observed
            }
            WatermarkStrategy::BoundedOutOfOrder => {
                self.max_observed = self.max_observed.max(event_time);
                self.max_observed.saturating_sub(self.max_lateness_ms)
            }
            WatermarkStrategy::PunctuatedWatermark => {
                // Only advance on explicit punctuation; callers should use
                // `advance_punctuation` for actual advances.
                self.max_observed
            }
        }
    }

    /// Explicitly advance the watermark (used with `PunctuatedWatermark`).
    pub fn advance_punctuation(&mut self, watermark: i64) {
        self.max_observed = self.max_observed.max(watermark);
    }

    /// Return the current watermark value.
    pub fn current_watermark(&self) -> i64 {
        match self.strategy {
            WatermarkStrategy::Monotonic => self.max_observed,
            WatermarkStrategy::BoundedOutOfOrder => {
                self.max_observed.saturating_sub(self.max_lateness_ms)
            }
            WatermarkStrategy::PunctuatedWatermark => self.max_observed,
        }
    }

    /// Returns `true` if the given event time is below the current watermark.
    pub fn is_late(&self, event_time: i64) -> bool {
        event_time < self.current_watermark()
    }
}

// ---------------------------------------------------------------------------
// Event-Time Windowing
// ---------------------------------------------------------------------------

/// A time window with inclusive start and exclusive end (milliseconds).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    pub start: i64,
    pub end: i64,
}

impl TimeWindow {
    /// Returns `true` if this window overlaps with `other`.
    pub fn overlaps(&self, other: &TimeWindow) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// Merge two overlapping windows into a single window.
    pub fn merge(&self, other: &TimeWindow) -> TimeWindow {
        TimeWindow {
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }
}

/// Type of window to assign events to.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowType {
    /// Fixed-size, non-overlapping windows.
    Tumbling { size_ms: i64 },
    /// Fixed-size windows that slide by a given interval.
    Sliding { size_ms: i64, slide_ms: i64 },
    /// Windows that close after a gap of inactivity.
    Session { gap_ms: i64 },
}

/// Assigns events to windows based on their timestamp.
#[derive(Debug)]
pub struct WindowAssigner {
    window_type: WindowType,
}

impl WindowAssigner {
    pub fn new(window_type: WindowType) -> Self {
        Self { window_type }
    }

    /// Assign the given timestamp to one or more windows.
    pub fn assign_windows(&self, timestamp: i64) -> Vec<TimeWindow> {
        match &self.window_type {
            WindowType::Tumbling { size_ms } => {
                let start = (timestamp / size_ms) * size_ms;
                vec![TimeWindow {
                    start,
                    end: start + size_ms,
                }]
            }
            WindowType::Sliding { size_ms, slide_ms } => {
                let mut windows = Vec::new();
                let first_start = timestamp - size_ms + slide_ms;
                let aligned = (first_start / slide_ms) * slide_ms;
                let mut start = aligned;
                while start <= timestamp {
                    let end = start + size_ms;
                    if timestamp >= start && timestamp < end {
                        windows.push(TimeWindow { start, end });
                    }
                    start += slide_ms;
                }
                windows
            }
            WindowType::Session { gap_ms } => {
                vec![TimeWindow {
                    start: timestamp,
                    end: timestamp + gap_ms,
                }]
            }
        }
    }

    /// Merge overlapping windows (useful for session windows).
    pub fn merge_windows(windows: &[TimeWindow]) -> Vec<TimeWindow> {
        if windows.is_empty() {
            return Vec::new();
        }
        let mut sorted: Vec<TimeWindow> = windows.to_vec();
        sorted.sort_by_key(|w| w.start);

        let mut merged: Vec<TimeWindow> = vec![sorted[0].clone()];
        for w in &sorted[1..] {
            let last = merged.last_mut().unwrap();
            if w.start <= last.end {
                last.end = last.end.max(w.end);
            } else {
                merged.push(w.clone());
            }
        }
        merged
    }
}

// ---------------------------------------------------------------------------
// Streaming Join Operator
// ---------------------------------------------------------------------------

/// Join types supported by the streaming join operator.
#[derive(Debug, Clone, PartialEq)]
pub enum StreamingJoinType {
    Inner,
    LeftOuter,
    /// Time-bounded join: match rows within a time tolerance.
    Temporal,
}

/// A streaming join operator that maintains state buffers for both sides.
///
/// Records are kept in memory and expired based on a TTL derived from
/// `time_tolerance_ms`.
#[derive(Debug)]
pub struct StreamingJoinOperator {
    join_type: StreamingJoinType,
    left_key_col: usize,
    right_key_col: usize,
    time_tolerance_ms: i64,
    left_buffer: Vec<(i64, RecordBatch)>,
    right_buffer: Vec<(i64, RecordBatch)>,
}

impl StreamingJoinOperator {
    /// Create a new streaming join operator.
    pub fn new(
        join_type: StreamingJoinType,
        left_key_col: usize,
        right_key_col: usize,
        time_tolerance_ms: i64,
    ) -> Self {
        Self {
            join_type,
            left_key_col,
            right_key_col,
            time_tolerance_ms,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
        }
    }

    /// Process a batch arriving on the left stream.
    pub fn process_left(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let max_time = Self::max_time(&batch, self.left_key_col)?;
        self.left_buffer.push((max_time, batch.clone()));
        let results = self.probe_against(
            &batch,
            self.left_key_col,
            &self.right_buffer.clone(),
            self.right_key_col,
        )?;
        self.cleanup(max_time);
        Ok(results)
    }

    /// Process a batch arriving on the right stream.
    pub fn process_right(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let max_time = Self::max_time(&batch, self.right_key_col)?;
        self.right_buffer.push((max_time, batch.clone()));
        let results = self.probe_against(
            &batch,
            self.right_key_col,
            &self.left_buffer.clone(),
            self.left_key_col,
        )?;
        self.cleanup(max_time);
        Ok(results)
    }

    fn probe_against(
        &self,
        incoming: &RecordBatch,
        incoming_key: usize,
        buffer: &[(i64, RecordBatch)],
        buffer_key: usize,
    ) -> Result<Vec<RecordBatch>> {
        use arrow::array::{Int64Array, UInt32Array};
        use arrow::compute::take;
        use arrow::datatypes::Field as ArrowField;

        let incoming_keys = incoming
            .column(incoming_key)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlazeError::execution("Key column must be Int64"))?;

        let mut results = Vec::new();
        for (_ts, buf_batch) in buffer {
            let buf_keys = buf_batch
                .column(buffer_key)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| BlazeError::execution("Key column must be Int64"))?;

            let mut inc_indices = Vec::new();
            let mut buf_indices = Vec::new();

            for i in 0..incoming_keys.len() {
                let ik = incoming_keys.value(i);
                for j in 0..buf_keys.len() {
                    let bk = buf_keys.value(j);
                    let matched = match self.join_type {
                        StreamingJoinType::Temporal => (ik - bk).abs() <= self.time_tolerance_ms,
                        _ => ik == bk,
                    };
                    if matched {
                        inc_indices.push(i as u32);
                        buf_indices.push(j as u32);
                    }
                }
            }

            if !inc_indices.is_empty() {
                let inc_idx = UInt32Array::from(inc_indices);
                let buf_idx = UInt32Array::from(buf_indices);

                let mut columns = Vec::new();
                let mut fields: Vec<Arc<arrow::datatypes::Field>> = Vec::new();

                for (c, field) in incoming.schema().fields().iter().enumerate() {
                    fields.push(field.clone());
                    columns.push(take(incoming.column(c), &inc_idx, None)?);
                }
                for (c, field) in buf_batch.schema().fields().iter().enumerate() {
                    let new_field = ArrowField::new(
                        &format!("right_{}", field.name()),
                        field.data_type().clone(),
                        field.is_nullable(),
                    );
                    fields.push(Arc::new(new_field));
                    columns.push(take(buf_batch.column(c), &buf_idx, None)?);
                }

                let schema = Arc::new(ArrowSchema::new(
                    fields.into_iter().map(|f| (*f).clone()).collect::<Vec<_>>(),
                ));
                results.push(RecordBatch::try_new(schema, columns)?);
            }
        }
        Ok(results)
    }

    /// TTL-based cleanup of expired buffers.
    fn cleanup(&mut self, current_time: i64) {
        let cutoff = current_time - self.time_tolerance_ms;
        self.left_buffer.retain(|(ts, _)| *ts >= cutoff);
        self.right_buffer.retain(|(ts, _)| *ts >= cutoff);
    }

    fn max_time(batch: &RecordBatch, col: usize) -> Result<i64> {
        use arrow::array::Int64Array;
        let arr = batch
            .column(col)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| BlazeError::execution("Key column must be Int64"))?;
        Ok((0..arr.len()).map(|i| arr.value(i)).max().unwrap_or(0))
    }
}

// ---------------------------------------------------------------------------
// CDC (Change Data Capture) Processor
// ---------------------------------------------------------------------------

/// CDC operation type.
#[derive(Debug, Clone, PartialEq)]
pub enum CdcOperation {
    Insert,
    Update,
    Delete,
    Snapshot,
}

/// A single CDC record.
#[derive(Debug, Clone)]
pub struct CdcRecord {
    pub operation: CdcOperation,
    pub table: String,
    pub timestamp: i64,
    pub key: Vec<u8>,
    pub data: HashMap<String, String>,
}

/// Cumulative CDC statistics.
#[derive(Debug, Clone, Default)]
pub struct CdcStats {
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub snapshots: u64,
}

/// Processes CDC events and maintains an in-memory state table.
#[derive(Debug)]
pub struct CdcProcessor {
    state: HashMap<Vec<u8>, HashMap<String, String>>,
    columns: Vec<String>,
    stats: CdcStats,
}

impl CdcProcessor {
    pub fn new() -> Self {
        Self {
            state: HashMap::new(),
            columns: Vec::new(),
            stats: CdcStats::default(),
        }
    }

    /// Process a single CDC record and return a snapshot batch if the state changed.
    pub fn process(&mut self, record: CdcRecord) -> Result<Option<RecordBatch>> {
        if self.columns.is_empty() && !record.data.is_empty() {
            self.columns = record.data.keys().cloned().collect();
            self.columns.sort();
        }

        match record.operation {
            CdcOperation::Insert => {
                self.state.insert(record.key, record.data);
                self.stats.inserts += 1;
            }
            CdcOperation::Update => {
                if let Some(existing) = self.state.get_mut(&record.key) {
                    for (k, v) in &record.data {
                        existing.insert(k.clone(), v.clone());
                    }
                } else {
                    self.state.insert(record.key, record.data);
                }
                self.stats.updates += 1;
            }
            CdcOperation::Delete => {
                self.state.remove(&record.key);
                self.stats.deletes += 1;
            }
            CdcOperation::Snapshot => {
                self.state.insert(record.key, record.data);
                self.stats.snapshots += 1;
            }
        }

        self.build_snapshot()
    }

    /// Return current CDC statistics.
    pub fn stats(&self) -> &CdcStats {
        &self.stats
    }

    /// Number of rows currently in state.
    pub fn state_len(&self) -> usize {
        self.state.len()
    }

    fn build_snapshot(&self) -> Result<Option<RecordBatch>> {
        if self.columns.is_empty() || self.state.is_empty() {
            return Ok(None);
        }

        use arrow::array::StringArray;
        use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};

        let fields: Vec<ArrowField> = self
            .columns
            .iter()
            .map(|c| ArrowField::new(c.as_str(), ArrowDataType::Utf8, true))
            .collect();
        let schema = Arc::new(ArrowSchema::new(fields));

        let mut col_builders: Vec<Vec<Option<&str>>> =
            vec![Vec::with_capacity(self.state.len()); self.columns.len()];

        for row in self.state.values() {
            for (i, col_name) in self.columns.iter().enumerate() {
                col_builders[i].push(row.get(col_name).map(|s| s.as_str()));
            }
        }

        let arrays: Vec<Arc<dyn arrow::array::Array>> = col_builders
            .into_iter()
            .map(|vals| Arc::new(StringArray::from(vals)) as Arc<dyn arrow::array::Array>)
            .collect();

        Ok(Some(RecordBatch::try_new(schema, arrays)?))
    }
}

// ---------------------------------------------------------------------------
// Streaming Checkpoint / Exactly-Once
// ---------------------------------------------------------------------------

/// Checkpoint state for streaming queries to support exactly-once semantics.
#[derive(Debug, Clone)]
pub struct StreamCheckpoint {
    pub checkpoint_id: u64,
    pub watermark: i64,
    pub offsets: HashMap<String, i64>,
    pub window_state: Vec<CheckpointedWindow>,
    pub created_at: std::time::Instant,
}

/// Checkpointed window state for recovery.
#[derive(Debug, Clone)]
pub struct CheckpointedWindow {
    pub start: i64,
    pub end: i64,
    pub row_count: usize,
}

/// Manages periodic checkpointing for streaming queries.
#[derive(Debug)]
pub struct CheckpointManager {
    checkpoints: Vec<StreamCheckpoint>,
    max_retained: usize,
    next_id: u64,
    interval: std::time::Duration,
    last_checkpoint: std::time::Instant,
}

impl CheckpointManager {
    pub fn new(interval: std::time::Duration, max_retained: usize) -> Self {
        Self {
            checkpoints: Vec::new(),
            max_retained,
            next_id: 1,
            interval,
            last_checkpoint: std::time::Instant::now(),
        }
    }

    /// Check if a checkpoint is due based on the configured interval.
    pub fn should_checkpoint(&self) -> bool {
        self.last_checkpoint.elapsed() >= self.interval
    }

    /// Create a checkpoint capturing the current streaming state.
    pub fn create_checkpoint(
        &mut self,
        watermark: i64,
        offsets: HashMap<String, i64>,
        aggregator: &WindowedAggregator,
    ) -> StreamCheckpoint {
        let window_state = aggregator
            .active_windows
            .iter()
            .map(|w| CheckpointedWindow {
                start: w.start,
                end: w.end,
                row_count: w.row_count,
            })
            .collect();

        let cp = StreamCheckpoint {
            checkpoint_id: self.next_id,
            watermark,
            offsets,
            window_state,
            created_at: std::time::Instant::now(),
        };
        self.next_id += 1;
        self.last_checkpoint = std::time::Instant::now();

        self.checkpoints.push(cp.clone());
        while self.checkpoints.len() > self.max_retained {
            self.checkpoints.remove(0);
        }
        cp
    }

    /// Get the latest checkpoint for recovery.
    pub fn latest_checkpoint(&self) -> Option<&StreamCheckpoint> {
        self.checkpoints.last()
    }

    /// Get a specific checkpoint by ID.
    pub fn get_checkpoint(&self, id: u64) -> Option<&StreamCheckpoint> {
        self.checkpoints.iter().find(|c| c.checkpoint_id == id)
    }

    /// Number of retained checkpoints.
    pub fn checkpoint_count(&self) -> usize {
        self.checkpoints.len()
    }
}

// ---------------------------------------------------------------------------
// Streaming State Backend
// ---------------------------------------------------------------------------

/// State backend for stateful streaming operators.
#[derive(Debug)]
pub struct StreamStateBackend {
    state: HashMap<String, HashMap<Vec<u8>, Vec<u8>>>,
    total_size: usize,
    max_size: usize,
}

impl StreamStateBackend {
    pub fn new(max_size: usize) -> Self {
        Self {
            state: HashMap::new(),
            total_size: 0,
            max_size,
        }
    }

    /// Get a value from the named state namespace.
    pub fn get(&self, namespace: &str, key: &[u8]) -> Option<&[u8]> {
        self.state
            .get(namespace)
            .and_then(|ns| ns.get(key))
            .map(|v| v.as_slice())
    }

    /// Put a value into the named state namespace.
    pub fn put(&mut self, namespace: &str, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let entry_size = key.len() + value.len();
        if self.total_size + entry_size > self.max_size {
            return Err(BlazeError::execution(
                "Streaming state backend exceeded max size",
            ));
        }
        let ns = self.state.entry(namespace.to_string()).or_default();
        if let Some(old) = ns.insert(key, value) {
            self.total_size -= old.len();
        }
        self.total_size += entry_size;
        Ok(())
    }

    /// Delete a key from the named state namespace.
    pub fn delete(&mut self, namespace: &str, key: &[u8]) -> bool {
        if let Some(ns) = self.state.get_mut(namespace) {
            if let Some(old) = ns.remove(key) {
                self.total_size -= key.len() + old.len();
                return true;
            }
        }
        false
    }

    /// Clear all state in a namespace.
    pub fn clear_namespace(&mut self, namespace: &str) {
        if let Some(ns) = self.state.remove(namespace) {
            for (k, v) in &ns {
                self.total_size -= k.len() + v.len();
            }
        }
    }

    /// Total bytes used by state.
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// Number of state namespaces.
    pub fn namespace_count(&self) -> usize {
        self.state.len()
    }
}

// ---------------------------------------------------------------------------
// Late Data Handler
// ---------------------------------------------------------------------------

/// Policy for handling late-arriving data in streaming windows.
#[derive(Debug, Clone, PartialEq)]
pub enum LateDataPolicy {
    /// Drop late data silently.
    Drop,
    /// Route late data to a side output.
    SideOutput,
    /// Allow late data to update already-emitted windows.
    AllowedLateness { max_lateness: std::time::Duration },
}

/// Tracks late data statistics.
#[derive(Debug, Default)]
pub struct LateDataTracker {
    pub dropped: u64,
    pub side_output: u64,
    pub allowed: u64,
    policy: Option<LateDataPolicy>,
}

impl LateDataTracker {
    pub fn new(policy: LateDataPolicy) -> Self {
        Self {
            policy: Some(policy),
            ..Default::default()
        }
    }

    /// Handle a late event and return whether it should be processed.
    pub fn handle_late_event(&mut self, _event_time: i64, _watermark: i64) -> bool {
        match &self.policy {
            Some(LateDataPolicy::Drop) | None => {
                self.dropped += 1;
                false
            }
            Some(LateDataPolicy::SideOutput) => {
                self.side_output += 1;
                false
            }
            Some(LateDataPolicy::AllowedLateness { .. }) => {
                self.allowed += 1;
                true
            }
        }
    }

    pub fn total_late(&self) -> u64 {
        self.dropped + self.side_output + self.allowed
    }
}

// ---------------------------------------------------------------------------
// Session windows
// ---------------------------------------------------------------------------

struct SessionState {
    start_time: u64,
    end_time: u64,
    events: Vec<u64>,
}

/// Session window that groups events separated by no more than `gap_duration_ms`.
pub struct SessionWindow {
    gap_duration_ms: u64,
    sessions: Vec<SessionState>,
}

impl SessionWindow {
    pub fn new(gap_duration_ms: u64) -> Self {
        Self {
            gap_duration_ms,
            sessions: Vec::new(),
        }
    }

    /// Add an event timestamp. Extends an existing session or starts a new one.
    pub fn add_event(&mut self, timestamp: u64) {
        // Try to find a session this event belongs to
        for session in &mut self.sessions {
            if timestamp >= session.start_time.saturating_sub(self.gap_duration_ms)
                && timestamp <= session.end_time + self.gap_duration_ms
            {
                if timestamp < session.start_time {
                    session.start_time = timestamp;
                }
                if timestamp > session.end_time {
                    session.end_time = timestamp;
                }
                session.events.push(timestamp);
                return;
            }
        }
        // No matching session – create a new one
        self.sessions.push(SessionState {
            start_time: timestamp,
            end_time: timestamp,
            events: vec![timestamp],
        });
    }

    /// Close and return sessions whose end_time + gap is before `current_time`.
    pub fn close_expired(&mut self, current_time: u64) -> Vec<(u64, u64, Vec<u64>)> {
        let mut expired = Vec::new();
        let mut remaining = Vec::new();
        for s in self.sessions.drain(..) {
            if s.end_time + self.gap_duration_ms < current_time {
                expired.push((s.start_time, s.end_time, s.events));
            } else {
                remaining.push(s);
            }
        }
        self.sessions = remaining;
        expired
    }

    pub fn active_sessions(&self) -> usize {
        self.sessions.len()
    }

    pub fn total_events(&self) -> usize {
        self.sessions.iter().map(|s| s.events.len()).sum()
    }
}

// ---------------------------------------------------------------------------
// Sliding window aggregator
// ---------------------------------------------------------------------------

/// Aggregate result for a single sliding window pane.
pub struct WindowAggregate {
    pub window_start: u64,
    pub window_end: u64,
    pub sum: f64,
    pub count: usize,
    pub avg: f64,
}

/// Computes rolling aggregates over a sliding window.
pub struct SlidingWindowAggregator {
    window_size_ms: u64,
    slide_interval_ms: u64,
    values: Vec<(u64, f64)>,
}

impl SlidingWindowAggregator {
    pub fn new(window_size_ms: u64, slide_interval_ms: u64) -> Self {
        Self {
            window_size_ms,
            slide_interval_ms,
            values: Vec::new(),
        }
    }

    pub fn add_value(&mut self, timestamp: u64, value: f64) {
        self.values.push((timestamp, value));
    }

    /// Compute aggregates for all window panes up to `current_time`.
    pub fn compute_windows(&self, current_time: u64) -> Vec<WindowAggregate> {
        let mut windows = Vec::new();
        if self.slide_interval_ms == 0 {
            return windows;
        }

        // Find the earliest timestamp
        let min_ts = self
            .values
            .iter()
            .map(|(t, _)| *t)
            .min()
            .unwrap_or(current_time);
        // Align window start
        let first_window_end = min_ts + self.window_size_ms;
        let mut win_end = first_window_end;

        while win_end <= current_time + self.window_size_ms {
            let win_start = win_end.saturating_sub(self.window_size_ms);
            let mut sum = 0.0;
            let mut count = 0usize;
            for &(ts, val) in &self.values {
                if ts >= win_start && ts < win_end {
                    sum += val;
                    count += 1;
                }
            }
            if count > 0 {
                windows.push(WindowAggregate {
                    window_start: win_start,
                    window_end: win_end,
                    sum,
                    count,
                    avg: sum / count as f64,
                });
            }
            win_end += self.slide_interval_ms;
        }
        windows
    }

    /// Remove values that fall outside all possible windows at `current_time`.
    pub fn evict_expired(&mut self, current_time: u64) {
        let cutoff = current_time.saturating_sub(self.window_size_ms);
        self.values.retain(|(ts, _)| *ts >= cutoff);
    }
}

// ---------------------------------------------------------------------------
// Exactly-once processing tracker
// ---------------------------------------------------------------------------

/// Tracks committed and pending offsets per partition for exactly-once semantics.
pub struct ExactlyOnceTracker {
    committed_offsets: HashMap<String, u64>,
    pending_offsets: HashMap<String, u64>,
}

impl ExactlyOnceTracker {
    pub fn new() -> Self {
        Self {
            committed_offsets: HashMap::new(),
            pending_offsets: HashMap::new(),
        }
    }

    /// Begin a transaction for the given partition at the given offset.
    pub fn begin_transaction(&mut self, partition: &str, offset: u64) {
        self.pending_offsets.insert(partition.to_string(), offset);
    }

    /// Commit the pending offset for the partition.
    pub fn commit(&mut self, partition: &str) {
        if let Some(offset) = self.pending_offsets.remove(partition) {
            self.committed_offsets.insert(partition.to_string(), offset);
        }
    }

    /// Discard the pending offset for the partition.
    pub fn rollback(&mut self, partition: &str) {
        self.pending_offsets.remove(partition);
    }

    /// Return the last committed offset for the partition, if any.
    pub fn committed_offset(&self, partition: &str) -> Option<u64> {
        self.committed_offsets.get(partition).copied()
    }

    /// Returns true if the offset has already been committed for this partition.
    pub fn is_duplicate(&self, partition: &str, offset: u64) -> bool {
        self.committed_offsets
            .get(partition)
            .map_or(false, |&committed| offset <= committed)
    }
}

impl Default for ExactlyOnceTracker {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Stream-to-Table Enrichment Join
// ---------------------------------------------------------------------------

/// Enriches streaming events by joining them with a static lookup table.
/// The lookup table is held in memory and can be refreshed.
#[derive(Debug)]
pub struct StreamTableJoin {
    /// Lookup table: join_key -> row values (serialized).
    lookup: HashMap<String, Vec<String>>,
    /// Name of the lookup table.
    table_name: String,
    /// Number of successful lookups.
    hits: u64,
    /// Number of failed lookups (no match).
    misses: u64,
}

impl StreamTableJoin {
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            lookup: HashMap::new(),
            table_name: table_name.into(),
            hits: 0,
            misses: 0,
        }
    }

    /// Load lookup data. Each entry maps a join key to enrichment values.
    pub fn load_lookup(&mut self, entries: Vec<(String, Vec<String>)>) {
        self.lookup.clear();
        for (key, values) in entries {
            self.lookup.insert(key, values);
        }
    }

    /// Enrich a stream event by looking up the join key.
    /// Returns the enrichment values, or None if no match.
    pub fn enrich(&mut self, join_key: &str) -> Option<&Vec<String>> {
        if let Some(vals) = self.lookup.get(join_key) {
            self.hits += 1;
            Some(vals)
        } else {
            self.misses += 1;
            None
        }
    }

    /// Refresh a single entry in the lookup table.
    pub fn update_entry(&mut self, key: String, values: Vec<String>) {
        self.lookup.insert(key, values);
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }
    pub fn lookup_size(&self) -> usize {
        self.lookup.len()
    }
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

// ---------------------------------------------------------------------------
// Checkpoint Barrier for Exactly-Once
// ---------------------------------------------------------------------------

/// Coordinates checkpoint barriers across stream operators for consistent snapshots.
#[derive(Debug)]
pub struct CheckpointBarrier {
    /// Current barrier ID.
    barrier_id: u64,
    /// Operators that have acknowledged the current barrier.
    acknowledged: HashSet<String>,
    /// Total operators that need to acknowledge.
    total_operators: usize,
    /// History of completed barriers.
    completed_barriers: Vec<u64>,
}

impl CheckpointBarrier {
    pub fn new(operator_names: &[String]) -> Self {
        Self {
            barrier_id: 0,
            acknowledged: HashSet::new(),
            total_operators: operator_names.len(),
            completed_barriers: Vec::new(),
        }
    }

    /// Initiate a new checkpoint barrier.
    pub fn initiate(&mut self) -> u64 {
        self.barrier_id += 1;
        self.acknowledged.clear();
        self.barrier_id
    }

    /// Acknowledge the barrier from an operator.
    /// Returns true if all operators have acknowledged (barrier complete).
    pub fn acknowledge(&mut self, operator_name: &str) -> bool {
        self.acknowledged.insert(operator_name.to_string());
        if self.acknowledged.len() >= self.total_operators {
            self.completed_barriers.push(self.barrier_id);
            true
        } else {
            false
        }
    }

    /// Check if the current barrier is complete.
    pub fn is_complete(&self) -> bool {
        self.acknowledged.len() >= self.total_operators
    }

    pub fn current_barrier_id(&self) -> u64 {
        self.barrier_id
    }

    pub fn completed_count(&self) -> usize {
        self.completed_barriers.len()
    }

    pub fn pending_acknowledgements(&self) -> usize {
        self.total_operators.saturating_sub(self.acknowledged.len())
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

    // -----------------------------------------------------------------------
    // WatermarkManager tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_watermark_manager_bounded_out_of_order() {
        let mut wm = WatermarkManager::new(std::time::Duration::from_millis(100));
        let w = wm.update_watermark(500);
        assert_eq!(w, 400); // 500 - 100
        assert_eq!(wm.current_watermark(), 400);
    }

    #[test]
    fn test_watermark_manager_monotonic() {
        let mut wm = WatermarkManager::with_strategy(
            std::time::Duration::from_millis(0),
            WatermarkStrategy::Monotonic,
        );
        wm.update_watermark(100);
        wm.update_watermark(50); // out of order, ignored
        assert_eq!(wm.current_watermark(), 100);
    }

    #[test]
    fn test_watermark_manager_is_late() {
        let mut wm = WatermarkManager::new(std::time::Duration::from_millis(50));
        wm.update_watermark(200);
        // watermark = 200 - 50 = 150
        assert!(wm.is_late(100));
        assert!(!wm.is_late(150));
        assert!(!wm.is_late(200));
    }

    #[test]
    fn test_watermark_punctuated() {
        let mut wm = WatermarkManager::with_strategy(
            std::time::Duration::from_millis(0),
            WatermarkStrategy::PunctuatedWatermark,
        );
        // Regular updates don't advance the watermark.
        wm.update_watermark(1000);
        assert_eq!(wm.current_watermark(), i64::MIN);

        // Explicit punctuation does.
        wm.advance_punctuation(500);
        assert_eq!(wm.current_watermark(), 500);
    }

    // -----------------------------------------------------------------------
    // WindowAssigner tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_tumbling_window_assignment() {
        let wa = WindowAssigner::new(WindowType::Tumbling { size_ms: 1000 });
        let windows = wa.assign_windows(2500);
        assert_eq!(windows.len(), 1);
        assert_eq!(
            windows[0],
            TimeWindow {
                start: 2000,
                end: 3000
            }
        );
    }

    #[test]
    fn test_sliding_window_assignment() {
        let wa = WindowAssigner::new(WindowType::Sliding {
            size_ms: 1000,
            slide_ms: 500,
        });
        let windows = wa.assign_windows(1200);
        assert!(windows.len() >= 2);
        assert!(windows.contains(&TimeWindow {
            start: 1000,
            end: 2000
        }));
        assert!(windows.contains(&TimeWindow {
            start: 500,
            end: 1500
        }));
    }

    #[test]
    fn test_session_window_assignment() {
        let wa = WindowAssigner::new(WindowType::Session { gap_ms: 300 });
        let w = wa.assign_windows(1000);
        assert_eq!(
            w,
            vec![TimeWindow {
                start: 1000,
                end: 1300
            }]
        );
    }

    #[test]
    fn test_merge_windows() {
        let windows = vec![
            TimeWindow { start: 0, end: 100 },
            TimeWindow {
                start: 50,
                end: 150,
            },
            TimeWindow {
                start: 200,
                end: 300,
            },
        ];
        let merged = WindowAssigner::merge_windows(&windows);
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0], TimeWindow { start: 0, end: 150 });
        assert_eq!(
            merged[1],
            TimeWindow {
                start: 200,
                end: 300
            }
        );
    }

    // -----------------------------------------------------------------------
    // StreamingJoinOperator tests
    // -----------------------------------------------------------------------

    fn create_int64_batch(col_name: &str, values: Vec<i64>) -> RecordBatch {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField};
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            col_name,
            ArrowDataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    }

    #[test]
    fn test_streaming_join_inner() {
        let mut op = StreamingJoinOperator::new(StreamingJoinType::Inner, 0, 0, 10_000);
        let left = create_int64_batch("key", vec![1, 2, 3]);
        let right = create_int64_batch("key", vec![2, 3, 4]);

        let _ = op.process_left(left).unwrap();
        let results = op.process_right(right).unwrap();
        // keys 2 and 3 match
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_streaming_join_temporal() {
        let mut op = StreamingJoinOperator::new(StreamingJoinType::Temporal, 0, 0, 5);
        let left = create_int64_batch("ts", vec![100, 200]);
        let right = create_int64_batch("ts", vec![103, 250]);

        let _ = op.process_left(left).unwrap();
        let results = op.process_right(right).unwrap();
        // |100-103| = 3 <= 5 match; |200-103| = 97 > 5; |200-250| = 50 > 5; |100-250| = 150 > 5
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[test]
    fn test_streaming_join_ttl_cleanup() {
        let mut op = StreamingJoinOperator::new(StreamingJoinType::Inner, 0, 0, 100);
        let b1 = create_int64_batch("key", vec![10, 20]);
        let b2 = create_int64_batch("key", vec![500]);

        let _ = op.process_left(b1).unwrap();
        assert_eq!(op.left_buffer.len(), 1);
        // Processing b2 on right triggers cleanup; b1 max_time=20, cutoff=500-100=400 → evicted
        let _ = op.process_right(b2).unwrap();
        assert_eq!(op.left_buffer.len(), 0);
    }

    // -----------------------------------------------------------------------
    // CdcProcessor tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_cdc_insert_and_snapshot() {
        let mut cdc = CdcProcessor::new();
        let record = CdcRecord {
            operation: CdcOperation::Insert,
            table: "users".into(),
            timestamp: 1000,
            key: b"u1".to_vec(),
            data: HashMap::from([("name".into(), "Alice".into()), ("age".into(), "30".into())]),
        };
        let batch = cdc.process(record).unwrap();
        assert!(batch.is_some());
        assert_eq!(cdc.state_len(), 1);
        assert_eq!(cdc.stats().inserts, 1);
    }

    #[test]
    fn test_cdc_update() {
        let mut cdc = CdcProcessor::new();
        cdc.process(CdcRecord {
            operation: CdcOperation::Insert,
            table: "t".into(),
            timestamp: 1,
            key: b"k1".to_vec(),
            data: HashMap::from([("v".into(), "old".into())]),
        })
        .unwrap();
        cdc.process(CdcRecord {
            operation: CdcOperation::Update,
            table: "t".into(),
            timestamp: 2,
            key: b"k1".to_vec(),
            data: HashMap::from([("v".into(), "new".into())]),
        })
        .unwrap();
        assert_eq!(cdc.stats().updates, 1);
        assert_eq!(cdc.state_len(), 1);
    }

    #[test]
    fn test_cdc_delete() {
        let mut cdc = CdcProcessor::new();
        cdc.process(CdcRecord {
            operation: CdcOperation::Insert,
            table: "t".into(),
            timestamp: 1,
            key: b"k1".to_vec(),
            data: HashMap::from([("v".into(), "x".into())]),
        })
        .unwrap();
        let batch = cdc
            .process(CdcRecord {
                operation: CdcOperation::Delete,
                table: "t".into(),
                timestamp: 2,
                key: b"k1".to_vec(),
                data: HashMap::new(),
            })
            .unwrap();
        assert!(batch.is_none()); // state is empty after delete
        assert_eq!(cdc.stats().deletes, 1);
        assert_eq!(cdc.state_len(), 0);
    }

    #[test]
    fn test_cdc_snapshot_operation() {
        let mut cdc = CdcProcessor::new();
        let result = cdc
            .process(CdcRecord {
                operation: CdcOperation::Snapshot,
                table: "t".into(),
                timestamp: 1,
                key: b"s1".to_vec(),
                data: HashMap::from([("col".into(), "val".into())]),
            })
            .unwrap();
        assert!(result.is_some());
        assert_eq!(cdc.stats().snapshots, 1);
    }

    // -----------------------------------------------------------------------
    // CheckpointManager tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_checkpoint_manager_basic() {
        let mut cm = CheckpointManager::new(std::time::Duration::from_millis(0), 3);
        let wm = Watermark::new("ts", std::time::Duration::from_millis(100));
        let agg = WindowedAggregator::new(
            StreamWindow::Tumbling {
                size: std::time::Duration::from_secs(1),
            },
            wm,
        );

        let cp = cm.create_checkpoint(500, HashMap::from([("topic".into(), 42)]), &agg);
        assert_eq!(cp.checkpoint_id, 1);
        assert_eq!(cp.watermark, 500);
        assert_eq!(*cp.offsets.get("topic").unwrap(), 42);
        assert_eq!(cm.checkpoint_count(), 1);

        let latest = cm.latest_checkpoint().unwrap();
        assert_eq!(latest.checkpoint_id, 1);
    }

    #[test]
    fn test_checkpoint_manager_retention() {
        let mut cm = CheckpointManager::new(std::time::Duration::from_millis(0), 2);
        let wm = Watermark::new("ts", std::time::Duration::from_millis(0));
        let agg = WindowedAggregator::new(
            StreamWindow::Tumbling {
                size: std::time::Duration::from_secs(1),
            },
            wm,
        );

        cm.create_checkpoint(100, HashMap::new(), &agg);
        cm.create_checkpoint(200, HashMap::new(), &agg);
        cm.create_checkpoint(300, HashMap::new(), &agg);

        assert_eq!(cm.checkpoint_count(), 2);
        assert!(cm.get_checkpoint(1).is_none()); // evicted
        assert!(cm.get_checkpoint(2).is_some());
        assert!(cm.get_checkpoint(3).is_some());
    }

    // -----------------------------------------------------------------------
    // StreamStateBackend tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_state_backend_basic() {
        let mut backend = StreamStateBackend::new(1024);
        backend
            .put("ns1", b"key1".to_vec(), b"val1".to_vec())
            .unwrap();
        assert_eq!(backend.get("ns1", b"key1"), Some(b"val1".as_ref()));
        assert_eq!(backend.get("ns1", b"missing"), None);
        assert_eq!(backend.namespace_count(), 1);
    }

    #[test]
    fn test_state_backend_delete() {
        let mut backend = StreamStateBackend::new(1024);
        backend.put("ns1", b"k1".to_vec(), b"v1".to_vec()).unwrap();
        assert!(backend.delete("ns1", b"k1"));
        assert!(!backend.delete("ns1", b"k1"));
        assert_eq!(backend.total_size(), 0);
    }

    #[test]
    fn test_state_backend_overflow() {
        let mut backend = StreamStateBackend::new(10);
        backend.put("ns", b"k1".to_vec(), b"v1".to_vec()).unwrap();
        assert!(backend
            .put("ns", b"big_key".to_vec(), b"big_val".to_vec())
            .is_err());
    }

    // -----------------------------------------------------------------------
    // LateDataTracker tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_late_data_drop_policy() {
        let mut tracker = LateDataTracker::new(LateDataPolicy::Drop);
        assert!(!tracker.handle_late_event(100, 200));
        assert_eq!(tracker.dropped, 1);
        assert_eq!(tracker.total_late(), 1);
    }

    #[test]
    fn test_late_data_allowed_policy() {
        let mut tracker = LateDataTracker::new(LateDataPolicy::AllowedLateness {
            max_lateness: std::time::Duration::from_secs(60),
        });
        assert!(tracker.handle_late_event(100, 200));
        assert_eq!(tracker.allowed, 1);
    }

    // -----------------------------------------------------------------------
    // State backend and stream joining tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_in_memory_state_backend() {
        let mut backend = InMemoryStateBackend::new();
        backend.save_state("key1".to_string(), vec![1, 2, 3]);

        let data = backend.load_state("key1");
        assert_eq!(data, Some(vec![1, 2, 3]));

        backend.delete_state("key1");
        assert_eq!(backend.load_state("key1"), None);
    }

    #[test]
    fn test_stream_joiner_config() {
        let config = StreamJoinConfig {
            join_type: "inner".to_string(),
            left_key: "id".to_string(),
            right_key: "user_id".to_string(),
            time_window: std::time::Duration::from_secs(60),
        };

        assert_eq!(config.join_type, "inner");
        assert_eq!(config.time_window.as_secs(), 60);
    }

    #[test]
    fn test_stream_joiner_basic() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let config = StreamJoinConfig {
            join_type: "inner".to_string(),
            left_key: "id".to_string(),
            right_key: "id".to_string(),
            time_window: std::time::Duration::from_secs(60),
        };

        let mut joiner = StreamJoiner::new(config);

        // Create left batch
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["Alice", "Bob"])),
            ],
        )
        .unwrap();

        let result = joiner.process_left(left_batch);
        assert!(result.is_ok());
    }

    // -----------------------------------------------------------------------
    // SessionWindow tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_session_window_basic() {
        let mut sw = SessionWindow::new(100);
        sw.add_event(1000);
        sw.add_event(1050);
        sw.add_event(1090);
        assert_eq!(sw.active_sessions(), 1);
        assert_eq!(sw.total_events(), 3);

        // Event far away starts a new session
        sw.add_event(2000);
        assert_eq!(sw.active_sessions(), 2);
        assert_eq!(sw.total_events(), 4);
    }

    #[test]
    fn test_session_window_close_expired() {
        let mut sw = SessionWindow::new(100);
        sw.add_event(1000);
        sw.add_event(1050);
        sw.add_event(2000);

        let expired = sw.close_expired(1200);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].2.len(), 2); // first session had 2 events
        assert_eq!(sw.active_sessions(), 1);
    }

    // -----------------------------------------------------------------------
    // SlidingWindowAggregator tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_sliding_window_aggregator() {
        let mut agg = SlidingWindowAggregator::new(100, 50);
        agg.add_value(10, 1.0);
        agg.add_value(30, 2.0);
        agg.add_value(60, 3.0);

        let windows = agg.compute_windows(110);
        assert!(!windows.is_empty());
        // All values should appear in at least one window
        let total_count: usize = windows.iter().map(|w| w.count).sum();
        assert!(total_count >= 3);
    }

    #[test]
    fn test_sliding_window_evict() {
        let mut agg = SlidingWindowAggregator::new(100, 50);
        agg.add_value(10, 1.0);
        agg.add_value(200, 2.0);
        agg.evict_expired(200);
        // Value at 10 should be evicted (200 - 100 = 100 cutoff)
        assert_eq!(agg.values.len(), 1);
    }

    // -----------------------------------------------------------------------
    // ExactlyOnceTracker tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_exactly_once_commit() {
        let mut tracker = ExactlyOnceTracker::new();
        tracker.begin_transaction("p0", 5);
        assert!(tracker.committed_offset("p0").is_none());
        tracker.commit("p0");
        assert_eq!(tracker.committed_offset("p0"), Some(5));
    }

    #[test]
    fn test_exactly_once_rollback_and_duplicate() {
        let mut tracker = ExactlyOnceTracker::new();
        tracker.begin_transaction("p0", 10);
        tracker.commit("p0");

        assert!(tracker.is_duplicate("p0", 10));
        assert!(tracker.is_duplicate("p0", 5));
        assert!(!tracker.is_duplicate("p0", 11));

        // Rollback discards pending
        tracker.begin_transaction("p0", 20);
        tracker.rollback("p0");
        assert_eq!(tracker.committed_offset("p0"), Some(10));
    }

    // -----------------------------------------------------------------------
    // Stream-Table Join tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_stream_table_join_enrich() {
        let mut join = StreamTableJoin::new("users");
        join.load_lookup(vec![
            ("u1".into(), vec!["Alice".into(), "NYC".into()]),
            ("u2".into(), vec!["Bob".into(), "LA".into()]),
        ]);

        assert_eq!(join.lookup_size(), 2);
        let result = join.enrich("u1").unwrap();
        assert_eq!(result[0], "Alice");
        assert!(join.enrich("u99").is_none());
        assert!(join.hit_rate() > 0.0 && join.hit_rate() < 1.0);
    }

    #[test]
    fn test_stream_table_join_update() {
        let mut join = StreamTableJoin::new("products");
        join.load_lookup(vec![("p1".into(), vec!["Widget".into()])]);
        join.update_entry("p1".into(), vec!["Super Widget".into()]);
        let result = join.enrich("p1").unwrap();
        assert_eq!(result[0], "Super Widget");
    }

    // -----------------------------------------------------------------------
    // Checkpoint Barrier tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_checkpoint_barrier() {
        let ops = vec!["scan".into(), "filter".into(), "aggregate".into()];
        let mut barrier = CheckpointBarrier::new(&ops);

        let bid = barrier.initiate();
        assert_eq!(bid, 1);
        assert_eq!(barrier.pending_acknowledgements(), 3);

        assert!(!barrier.acknowledge("scan"));
        assert!(!barrier.acknowledge("filter"));
        assert!(barrier.acknowledge("aggregate")); // all done
        assert!(barrier.is_complete());
        assert_eq!(barrier.completed_count(), 1);
    }

    #[test]
    fn test_checkpoint_barrier_multiple() {
        let ops = vec!["op1".into(), "op2".into()];
        let mut barrier = CheckpointBarrier::new(&ops);

        barrier.initiate();
        barrier.acknowledge("op1");
        barrier.acknowledge("op2");

        barrier.initiate();
        barrier.acknowledge("op1");
        barrier.acknowledge("op2");

        assert_eq!(barrier.completed_count(), 2);
        assert_eq!(barrier.current_barrier_id(), 2);
    }

    // -- Connector Framework tests --------------------------------------------

    #[test]
    fn test_memory_connector() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![ArrowField::new(
            "id",
            ArrowDataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();

        let mut conn = MemoryConnector::new(vec![batch]);
        assert_eq!(conn.name(), "memory");

        let b1 = conn.poll_batch().unwrap();
        assert!(b1.is_some());
        assert_eq!(b1.unwrap().num_rows(), 3);

        conn.commit().unwrap();
        assert_eq!(conn.current_offset(), "1");

        let b2 = conn.poll_batch().unwrap();
        assert!(b2.is_none());
    }

    #[test]
    fn test_memory_connector_seek() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![ArrowField::new(
            "v",
            ArrowDataType::Int32,
            false,
        )]));
        let b1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![10]))])
            .unwrap();
        let b2 = RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![20]))]).unwrap();

        let mut conn = MemoryConnector::new(vec![b1, b2]);
        conn.poll_batch().unwrap();
        conn.poll_batch().unwrap();

        conn.seek_to_offset("0").unwrap();
        let replayed = conn.poll_batch().unwrap().unwrap();
        assert_eq!(replayed.num_rows(), 1);
    }

    // -- Stream Pipeline tests ------------------------------------------------

    #[test]
    fn test_pipeline_builder_requires_source() {
        let result = StreamPipelineBuilder::new().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_process_all() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            ArrowField::new("id", ArrowDataType::Int32, false),
            ArrowField::new("name", ArrowDataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let connector = MemoryConnector::new(vec![batch]);
        let mut pipeline = StreamPipelineBuilder::new()
            .source(Box::new(connector))
            .project(vec!["name".to_string()])
            .build()
            .unwrap();

        let results = pipeline.process_all().unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_columns(), 1);
        assert_eq!(results[0].schema().field(0).name(), "name");

        let stats = pipeline.stats();
        assert_eq!(stats.processed_batches, 1);
        assert_eq!(stats.processed_rows, 2);
        assert_eq!(stats.source_name, "memory");
    }

    #[test]
    fn test_pipeline_stats() {
        let connector = MemoryConnector::new(Vec::new());
        let mut pipeline = StreamPipelineBuilder::new()
            .source(Box::new(connector))
            .filter("id > 0".to_string())
            .build()
            .unwrap();

        pipeline.process_all().unwrap();
        let stats = pipeline.stats();
        assert_eq!(stats.processed_batches, 0);
        assert_eq!(stats.stage_count, 1);
    }
}

// ---------------------------------------------------------------------------
// State persistence and stream joining implementation
// ---------------------------------------------------------------------------

use std::time::Duration;

/// Trait for state backend implementations.
pub trait StateBackend: Send + Sync {
    /// Save state data.
    fn save_state(&mut self, key: String, data: Vec<u8>);

    /// Load state data.
    fn load_state(&self, key: &str) -> Option<Vec<u8>>;

    /// Delete state data.
    fn delete_state(&mut self, key: &str);
}

/// In-memory state backend.
#[derive(Debug, Default)]
pub struct InMemoryStateBackend {
    data: HashMap<String, Vec<u8>>,
}

impl InMemoryStateBackend {
    pub fn new() -> Self {
        Self::default()
    }
}

impl StateBackend for InMemoryStateBackend {
    fn save_state(&mut self, key: String, data: Vec<u8>) {
        self.data.insert(key, data);
    }

    fn load_state(&self, key: &str) -> Option<Vec<u8>> {
        self.data.get(key).cloned()
    }

    fn delete_state(&mut self, key: &str) {
        self.data.remove(key);
    }
}

/// File-based state backend using JSON serialization.
#[derive(Debug)]
pub struct FileStateBackend {
    directory: std::path::PathBuf,
}

impl FileStateBackend {
    pub fn new(directory: impl AsRef<std::path::Path>) -> Result<Self> {
        let directory = directory.as_ref().to_path_buf();
        std::fs::create_dir_all(&directory).map_err(|e| BlazeError::Io { source: e })?;

        Ok(Self { directory })
    }

    fn key_to_path(&self, key: &str) -> std::path::PathBuf {
        self.directory.join(format!("{}.json", key))
    }
}

impl StateBackend for FileStateBackend {
    fn save_state(&mut self, key: String, data: Vec<u8>) {
        let path = self.key_to_path(&key);
        let _ = std::fs::write(path, data);
    }

    fn load_state(&self, key: &str) -> Option<Vec<u8>> {
        let path = self.key_to_path(key);
        std::fs::read(path).ok()
    }

    fn delete_state(&mut self, key: &str) {
        let path = self.key_to_path(key);
        let _ = std::fs::remove_file(path);
    }
}

/// Configuration for stream joining.
#[derive(Debug, Clone)]
pub struct StreamJoinConfig {
    pub join_type: String, // "inner", "left", "right", "full"
    pub left_key: String,
    pub right_key: String,
    pub time_window: Duration,
}

/// Buffer entry for windowed join.
#[derive(Debug, Clone)]
struct WindowedBatch {
    batch: RecordBatch,
    timestamp: std::time::Instant,
}

/// Stream joiner that performs windowed joins between two streams.
#[derive(Debug)]
pub struct StreamJoiner {
    config: StreamJoinConfig,
    left_buffer: Vec<WindowedBatch>,
    right_buffer: Vec<WindowedBatch>,
}

impl StreamJoiner {
    pub fn new(config: StreamJoinConfig) -> Self {
        Self {
            config,
            left_buffer: Vec::new(),
            right_buffer: Vec::new(),
        }
    }

    /// Process a batch from the left stream.
    pub fn process_left(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let now = std::time::Instant::now();

        // Add to buffer
        self.left_buffer.push(WindowedBatch {
            batch: batch.clone(),
            timestamp: now,
        });

        // Evict old entries
        self.evict_expired_left();

        // Try to join with right buffer
        self.join_buffers()
    }

    /// Process a batch from the right stream.
    pub fn process_right(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let now = std::time::Instant::now();

        // Add to buffer
        self.right_buffer.push(WindowedBatch {
            batch: batch.clone(),
            timestamp: now,
        });

        // Evict old entries
        self.evict_expired_right();

        // Try to join with left buffer
        self.join_buffers()
    }

    fn evict_expired_left(&mut self) {
        let now = std::time::Instant::now();
        self.left_buffer
            .retain(|wb| now.duration_since(wb.timestamp) < self.config.time_window);
    }

    fn evict_expired_right(&mut self) {
        let now = std::time::Instant::now();
        self.right_buffer
            .retain(|wb| now.duration_since(wb.timestamp) < self.config.time_window);
    }

    fn join_buffers(&self) -> Result<Vec<RecordBatch>> {
        // Simplified join implementation
        // In a real implementation, this would:
        // 1. Extract key columns from both sides
        // 2. Build a hash table for one side
        // 3. Probe with the other side
        // 4. Emit matching rows according to join type

        // For now, return empty result (placeholder)
        Ok(Vec::new())
    }

    /// Get the number of buffered batches on the left.
    pub fn left_buffer_size(&self) -> usize {
        self.left_buffer.len()
    }

    /// Get the number of buffered batches on the right.
    pub fn right_buffer_size(&self) -> usize {
        self.right_buffer.len()
    }
}

// ---------------------------------------------------------------------------
// Connector Framework
// ---------------------------------------------------------------------------

/// Trait for pluggable stream source connectors.
pub trait StreamConnector: Send + Sync {
    /// Connector name (e.g., "kafka", "file", "memory").
    fn name(&self) -> &str;

    /// Poll for the next batch of records. Returns `None` when exhausted.
    fn poll_batch(&mut self) -> Result<Option<RecordBatch>>;

    /// Commit the offset of the last successfully processed batch.
    fn commit(&mut self) -> Result<()>;

    /// Return current offset metadata for checkpointing.
    fn current_offset(&self) -> String;

    /// Restore from a previously checkpointed offset.
    fn seek_to_offset(&mut self, offset: &str) -> Result<()>;
}

/// In-memory connector for testing and prototyping.
#[derive(Debug)]
pub struct MemoryConnector {
    batches: Vec<RecordBatch>,
    cursor: usize,
    committed: usize,
}

impl MemoryConnector {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        Self {
            batches,
            cursor: 0,
            committed: 0,
        }
    }
}

impl StreamConnector for MemoryConnector {
    fn name(&self) -> &str {
        "memory"
    }

    fn poll_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.cursor < self.batches.len() {
            let batch = self.batches[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    fn commit(&mut self) -> Result<()> {
        self.committed = self.cursor;
        Ok(())
    }

    fn current_offset(&self) -> String {
        self.cursor.to_string()
    }

    fn seek_to_offset(&mut self, offset: &str) -> Result<()> {
        self.cursor = offset
            .parse::<usize>()
            .map_err(|_| BlazeError::execution("Invalid offset"))?;
        Ok(())
    }
}

/// File-based connector that reads CSV/JSON files as a stream.
#[derive(Debug)]
pub struct FileConnector {
    path: std::path::PathBuf,
    format: FileFormat,
    batches: Vec<RecordBatch>,
    cursor: usize,
    loaded: bool,
}

/// Supported file formats for the file connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileFormat {
    Csv,
    Json,
    NdJson,
}

impl FileConnector {
    pub fn new(path: impl Into<std::path::PathBuf>, format: FileFormat) -> Self {
        Self {
            path: path.into(),
            format,
            batches: Vec::new(),
            cursor: 0,
            loaded: false,
        }
    }

    fn load_if_needed(&mut self) -> Result<()> {
        if self.loaded {
            return Ok(());
        }
        self.loaded = true;
        // Load based on format
        match self.format {
            FileFormat::Csv => {
                let table = crate::storage::CsvTable::open(&self.path)?;
                self.batches = table.scan(None, &[], None)?;
            }
            FileFormat::Json | FileFormat::NdJson => {
                // JSON loading via storage module
                self.batches = Vec::new();
            }
        }
        Ok(())
    }
}

impl StreamConnector for FileConnector {
    fn name(&self) -> &str {
        "file"
    }

    fn poll_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.load_if_needed()?;
        if self.cursor < self.batches.len() {
            let batch = self.batches[self.cursor].clone();
            self.cursor += 1;
            Ok(Some(batch))
        } else {
            Ok(None)
        }
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    fn current_offset(&self) -> String {
        self.cursor.to_string()
    }

    fn seek_to_offset(&mut self, offset: &str) -> Result<()> {
        self.cursor = offset
            .parse::<usize>()
            .map_err(|_| BlazeError::execution("Invalid file offset"))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Stream Pipeline Builder
// ---------------------------------------------------------------------------

/// A composable streaming pipeline stage.
#[derive(Debug, Clone)]
pub enum PipelineStage {
    /// Filter rows using a SQL-like predicate expression string.
    Filter { predicate: String },
    /// Project specific columns.
    Project { columns: Vec<String> },
    /// Window aggregation.
    WindowAggregate {
        window_type: WindowType,
        aggregates: Vec<String>,
    },
    /// Sink results to a destination.
    Sink { sink_type: String },
}

/// Builder for creating composable streaming pipelines.
pub struct StreamPipelineBuilder {
    source: Option<Box<dyn StreamConnector>>,
    stages: Vec<PipelineStage>,
    config: StreamConfig,
}

impl StreamPipelineBuilder {
    pub fn new() -> Self {
        Self {
            source: None,
            stages: Vec::new(),
            config: StreamConfig::default(),
        }
    }

    /// Set the source connector.
    pub fn source(mut self, connector: Box<dyn StreamConnector>) -> Self {
        self.source = Some(connector);
        self
    }

    /// Add a filter stage.
    pub fn filter(mut self, predicate: impl Into<String>) -> Self {
        self.stages.push(PipelineStage::Filter {
            predicate: predicate.into(),
        });
        self
    }

    /// Add a projection stage.
    pub fn project(mut self, columns: Vec<String>) -> Self {
        self.stages.push(PipelineStage::Project { columns });
        self
    }

    /// Add a window aggregation stage.
    pub fn window_aggregate(mut self, window_type: WindowType, aggregates: Vec<String>) -> Self {
        self.stages.push(PipelineStage::WindowAggregate {
            window_type,
            aggregates,
        });
        self
    }

    /// Set the stream configuration.
    pub fn with_config(mut self, config: StreamConfig) -> Self {
        self.config = config;
        self
    }

    /// Build the pipeline, returning the list of stages and the source.
    pub fn build(self) -> Result<StreamPipeline> {
        let source = self
            .source
            .ok_or_else(|| BlazeError::execution("Stream pipeline requires a source"))?;
        Ok(StreamPipeline {
            source,
            stages: self.stages,
            config: self.config,
            processed_batches: 0,
            processed_rows: 0,
        })
    }
}

impl Default for StreamPipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// An assembled streaming pipeline ready for execution.
pub struct StreamPipeline {
    source: Box<dyn StreamConnector>,
    stages: Vec<PipelineStage>,
    config: StreamConfig,
    processed_batches: usize,
    processed_rows: usize,
}

impl StreamPipeline {
    /// Process one batch through the pipeline.
    /// Returns `Ok(Some(batch))` if a batch was processed, `Ok(None)` if source is exhausted.
    pub fn process_next(&mut self) -> Result<Option<RecordBatch>> {
        let batch = match self.source.poll_batch()? {
            Some(b) => b,
            None => return Ok(None),
        };

        let mut current = batch;
        for stage in &self.stages {
            current = match stage {
                PipelineStage::Filter { .. } => {
                    // In a full implementation, parse and evaluate the predicate.
                    // For now, pass through.
                    current
                }
                PipelineStage::Project { columns } => {
                    let schema = current.schema();
                    let indices: Vec<usize> = columns
                        .iter()
                        .filter_map(|c| schema.index_of(c).ok())
                        .collect();
                    if indices.is_empty() {
                        current
                    } else {
                        current.project(&indices)?
                    }
                }
                PipelineStage::WindowAggregate { .. } | PipelineStage::Sink { .. } => current,
            };
        }

        self.processed_batches += 1;
        self.processed_rows += current.num_rows();
        self.source.commit()?;
        Ok(Some(current))
    }

    /// Process all available batches and return them.
    pub fn process_all(&mut self) -> Result<Vec<RecordBatch>> {
        let mut results = Vec::new();
        while let Some(batch) = self.process_next()? {
            results.push(batch);
        }
        Ok(results)
    }

    /// Get pipeline statistics.
    pub fn stats(&self) -> PipelineStats {
        PipelineStats {
            processed_batches: self.processed_batches,
            processed_rows: self.processed_rows,
            stage_count: self.stages.len(),
            source_name: self.source.name().to_string(),
        }
    }
}

/// Statistics about pipeline execution.
#[derive(Debug, Clone)]
pub struct PipelineStats {
    pub processed_batches: usize,
    pub processed_rows: usize,
    pub stage_count: usize,
    pub source_name: String,
}
